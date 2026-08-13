# Model v3 — Changes

**Changes from v2 only.** Everything not mentioned here is unchanged.

Three things drove this revision:

1. The delivery hierarchy is **Project → Programme → Release → Initiative → Workstream → Epic → Task → Sub-task**, and levels 2–8 are all issue types.
2. Sprints are barely used, so sprint-based metrics can't carry the delivery dashboards.
3. Resourcing is about **Lead/Owner vs Involved**, not about users.

---

## 1. The hierarchy

### What this confirms

In v2 I flagged that Jira's `hierarchyLevel` had unexplained gaps at 4 and 2. Those gaps are almost certainly Release and Workstream:

| Level | Issue type | Jira `hierarchyLevel` |
|---|---|---|
| 1 | **Project** | *not an issue* — `Dim_Project` |
| 2 | Programme | 5 |
| 3 | Release | 4 *(to confirm)* |
| 4 | Initiative | 3 |
| 5 | Workstream | 2 *(to confirm)* |
| 6 | Epic | 1 |
| 7 | Task / Story / Bug / Risk | 0 |
| 8 | Sub-task | −1 |

**Verify before building** — the sample only returned 10 issue types:

```sql
SELECT [name], hierarchyLevel, subtask FROM [jira].[issuetypes] ORDER BY hierarchyLevel DESC;
```

Either way, the model **does not depend on those numbers**. Levels come from `Config_Hierarchy` (issue type name → level), so a renamed type or a new tier is a row edit. This also matches the type-based placement approach you already moved to.

### What changes in the model

**Workstream and Release are no longer custom fields.** This is the significant one. In v2 I treated `fields_workstream` and `fields_release` as attributes and proposed `Dim_Workstream` (config) and `Config_ReleaseTranche` (to clean the free text). Both largely go away:

| v2 | v3 |
|---|---|
| `Dim_Workstream` — config table of 9 rows | **Removed.** Workstream is an issue; it lives in `Dim_Issue` and is reached through ancestry |
| `Config_ReleaseTranche` — maps free-text release | **Removed.** Release is an issue with real dates, status and an owner |
| `fields_workstream`, `fields_release` | Kept on `Dim_Issue` as `Workstream_Text` / `Release_Text` — **fallback and reconciliation only** |

This is a better position than v2. A Release that's an issue has its own start date, target end, status, owner and RAID links — so the tranche overview dashboard gets real data instead of a text label. It also closes the open question from v1 about which field was the source of truth for releases.

**New: `Bridge_IssueAncestry`.** One row per issue per ancestor, at every level. This is the piece that makes the hierarchy future-proof:

- roll **any** measure up to **any** level without new tables — story points by Workstream, defects by Release, risk exposure by Initiative
- drill from Programme all the way to Sub-task in one matrix
- ancestor-level dates roll up for the Gantt
- add a tier later (e.g. Portfolio above Programme) and only the config table changes

**`Dim_Issue` gains two sets of hierarchy columns**, and the distinction matters:

| Column set | Purpose |
|---|---|
| `Programme_Key/Name`, `Release_Key/Name`, `Initiative_Key/Name`, `Workstream_Key/Name`, `Epic_Key/Name` | **Typed** — sparse but correct. Use for slicers, filters and measures. An Epic sitting directly under a Programme has NULL Release and NULL Workstream, which is truthful. |
| `Level_1` … `Level_8` | **Dense** — compacted upward, no gaps. Use *only* for the xViz Gantt, which needs a dense path. |

Keeping these separate avoids the trap where a dense display path gets used as a filter and quietly reassigns issues to the wrong workstream.

### Implementation note

Spark SQL has no recursive CTE, and Fabric Warehouse's support is limited. The ancestry is built as **8 bounded self-joins** instead — the depth is capped by the hierarchy itself, so recursion buys nothing. This works in both Spark SQL and T-SQL and fits the iterative pattern already in your toolkit.

---

## 2. Sprints demoted

`Bridge_IssueSprint`, `Dim_Sprint` and the sprint columns stay in the model — they cost nothing and the client may adopt sprints later — but they move **out of the core** and get hidden by default.

**What you lose without sprints:** velocity, sprint burndown, sprint commitment vs delivery (say/do ratio), spillover.

**What replaces them, using data the client already has:**

| Sprint metric | Non-sprint equivalent | Source |
|---|---|---|
| Velocity | Throughput per week/month (issues or points completed) | `Fact_IssueDaily`, `Fact_Issue` |
| Sprint burndown | Rolling burndown against a **Release** issue's target end date | `Fact_IssueDaily` + ancestry |
| Sprint commitment vs delivery | Scope added/removed from a Release after baseline | `Fact_IssueDaily` |
| Sprint capacity | Cycle-time percentiles + Monte Carlo completion forecast | `Fact_IssueTransition` |

For a programme running to release dates rather than two-week cadences, this is arguably the stronger set. A Monte Carlo forecast built from historic throughput answers "will Release 2 land in Q3" more honestly than a velocity average.

**`Fact_IssueDaily` changes accordingly:** `Sprint_ID` becomes nullable and secondary; `Release_Issue_ID`, `Workstream_Issue_ID` and `Programme_Issue_ID` are added so every trend rolls up the real hierarchy.

This raises `Fact_IssueDaily`'s priority. With sprints, Jira's own boards give you burndown for free. Without them, the daily snapshot is the *only* source of trend for the whole programme. It moves from build step 5 to build step 2 — start capturing it early, because you can't backfill what you didn't snapshot (beyond what `jira.histories` supports).

---

## 3. Resourcing by role, not by user

Agreed — the client's question is "who owns this and who's involved", not "show me a user list". But you still need a thin person table: without it there's no way to filter to a named individual or count distinct people. The shift is that **`Dim_Person` is supporting cast and the role is the star**.

### Structure

```
Fact_ResourceAllocation      Issue × Person × Role        ← the analytical table
        │
        ├── Dim_Person       one row per person (thin: name, email, active)
        └── Dim_Role         Lead / Owner / Involved / Reporter
```

`Dim_Role` is a four-row config table carrying `Is_Lead`, `Contributes_To_Effort` and `Display_Order`. Adding "Approver" or "SME" later is a row.

### Where Lead comes from — needs a decision

Right now `Lead` maps to **assignee**, which matches the Gantt screenshot ("Lead Name" alongside "Resource Names"). But there are three other candidates in the data:

- `fields_business_owner_value`
- `fields_technical_contact_value`
- `fields_business_contact_value`

If the client means Business Owner rather than assignee, that's a **row change in `Config_ResourceRole`**, not a rebuild. I've made the source field config-driven for exactly this reason — worth asking them directly, because "who leads this" is the column senior stakeholders will scrutinise first.

### The simple path for the Gantt

`Dim_Issue` gets three denormalised columns so the timeline renders without touching the fact table — matching the screenshot layout exactly:

- `Lead_Name` — single value
- `Involved_Names` — comma-separated list
- `Involved_Count` — integer

Casual users get readable columns; analysis uses `Fact_ResourceAllocation`.

### `Allocation_Weight`

Lead = 1.0, Involved = 0.0. Involved people count towards **headcount and engagement** but not towards **effort totals**, so summing story points across the allocation fact doesn't multiply work by the number of names attached to it. If the client later wants Involved to carry a share of effort, change the weight in config.

### What this unlocks

- workload by lead — open items, overdue items, story points owned
- over-commitment — leads owning work across many workstreams simultaneously
- involvement breadth — people spread thin across too many initiatives
- key-person risk — issues where one person is both lead and sole involved party
- engagement gaps — Epics or Releases with no lead assigned at all

That last one is usually the quickest win on a programme this size, and it needs no capacity data.

---

## 4. Revised object list

**Removed:** `Dim_Workstream`, `Config_ReleaseTranche`, `Dim_Board` (already folded into `Dim_Sprint` in v2)
**Added:** `Bridge_IssueAncestry`, `Config_Hierarchy`, `Dim_Role`, `Config_ResourceRole`
**Renamed:** `Dim_User` → `Dim_Person`
**Demoted (hidden by default):** `Dim_Sprint`, `Bridge_IssueSprint`

Net: 29 objects — 14 dimensions, 6 facts, 7 bridges, 4 config (a wash on v2, with the hierarchy properly handled).

---

## 5. Revised build order

1. `Config_Hierarchy`, `Dim_Date`, `Dim_Issue`, `Bridge_IssueAncestry`, `Fact_Issue` → **timeline, hierarchy drill-down**
2. **`Fact_IssueDaily`** — moved up. Without sprints this is the only trend source; start capturing immediately
3. `Fact_ResourceAllocation`, `Dim_Person`, `Dim_Role` → **lead/involved reporting**
4. `Bridge_TestSetTest`, `Fact_TestRun` → **testing hierarchy**
5. `Bridge_IssueLink` + config → **traceability, RAID, decisions**
6. `Fact_IssueTransition` → **cycle time, flow, quality**
7. `Fact_Worklog` → effort *(subject to the logging-coverage check)*
8. External feeds — budget, capacity, benefits

---

## 6. Questions this raises

1. **Are Release and Workstream confirmed as issue types?** Run the query in §1. If Workstream is *only* the text field, the ancestry drops a level and `Dim_Workstream` comes back.
2. **Is "Lead" the assignee, or the Business Owner field?** Config change either way, but it should be right on day one.
3. **Can an issue sit under more than one Workstream?** The hierarchy assumes one parent. If work genuinely spans workstreams, that needs a bridge rather than an ancestor — worth asking, because it's a common source of "the numbers don't add up".
4. **How complete is the parent chain?** Query (f) in the SQL counts issues that don't reach a Programme. Orphans will fall out of every rolled-up total silently.
