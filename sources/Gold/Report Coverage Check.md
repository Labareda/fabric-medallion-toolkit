# Report Coverage Check

Three changes in this pass: `Validation Checks` removed, `Dim_Team` added, and one thing I'd introduced last time reverted because it risked your working Gantt.

---

## 1. A contradiction worth resolving in the wheel

Your wheel's inline comment on `build_dense_levels` says:

> *"These were built for xViz Gantt nesting, but in practice the TYPED columns (Programme/Release/.../Task) nest correctly in xViz and the dense Level_N did not, so the typed columns drive the Gantt and these are unnecessary."*

Your `Dim_Issue` notebook says the reverse — dense levels nest correctly "where nothing else does (verified in Power BI)".

Since the current model demonstrably works with `build_dense_levels=True` and no typed levels, the **notebook is right and the wheel comment is stale**. Worth correcting in the wheel, or it will send you the wrong way in six months.

**The two walks don't collide.** I traced the execution order in `enrich_issue_hierarchy`:

1. Typed walk runs → produces `Level_1..N` → immediately renamed to `Programme_Label`, `Release_Label`, etc. → unmapped levels dropped
2. **Then** dense walk runs → joins its own fresh `Level_1..7`
3. `Depth` is computed from the dense columns

By the time the dense join happens, no `Level_N` column exists. Your Gantt columns are byte-for-byte what they were. Adding the typed columns is additive only.

---

## 2. Reverted: `Planned_End_Date`

Last version I changed this to `COALESCE(fields_target_end, fields_duedate)`. **Now back to `duedate` only.**

`fields_target_end` is Jira's planning-tier end date and tends to be populated on Programme / Release / Initiative rows where `duedate` isn't. COALESCEing it in would give those summary rows *their own* dates — so `Has_Own_Dates` flips true and `rollup_hierarchy_dates` stops spanning their children. Summary bars would change shape across your whole timeline.

That might well be an improvement, but not silently, on a chart you've already validated. The alternative is commented in place; compare side by side before switching.

---

## 3. `Dim_Team`

Merged on **`Team_Name`**, not `Team_Id`. Some issues carry a name with no id (field set before the team object existed, or the team later deleted from the Atlassian platform) and keying on id would drop them. `Team_Id` rides along as an attribute.

`Fact_Issue` gains `Team_Key`, resolved on name, with the Unknown member catching issues that have no team.

Two things to be aware of:

- **Team ≠ Workstream.** A team can serve several workstreams and vice versa. Keep them as independent slicers rather than assuming one implies the other.
- **Expect thin coverage.** The team field was sparsely populated in the sample. Worth running this once before building a team dashboard, since a mostly-empty visual reads as broken rather than sparse:

```sql
SELECT COUNT(*) AS issues, COUNT(fields_team_name) AS with_team
FROM Silver.jira.issues
```

---

## 4. Timeline — xViz Gantt v3.3.01

**Verdict: yes, unchanged.** Every column the working version uses is still produced identically.

| xViz field well | Column | Table |
|---|---|---|
| Task Name (hierarchy) | `Level_1` … `Level_7` | Dim_Issue |
| Start Date | `Rollup_Start_Date` | Fact_Issue |
| End Date | `Rollup_End_Date` | Fact_Issue |
| Lead Name | `Lead_Name` | Dim_Issue |
| Resource Names | `Resource_Names` | Dim_Issue |
| Predecessor | `Predecessor_Issue_Code` | Dim_Issue |
| Connector type | `Connector_Type` | Dim_Issue |
| Sort | `Sort_Path` ASC | Dim_Issue |
| Milestone flag | `Is_Milestone` | Dim_Issue |

Settings unchanged: **Hide Blanks OFF** (trailing nulls in `Level_5..7` are expected), sort by `Sort_Path` ascending only.

**How `Sort_Path` and `Rank` divide the work:** `Rank` is Jira's LexoRank and orders every issue *globally* — sorting by it directly scatters children away from parents. `Sort_Path` concatenates each ancestor's rank from the root down, so a parent's path is a literal prefix of every child's. `Rank` is the input; `Sort_Path` is what you sort on.

**One thing to watch.** `max_depth` defaults to 7, and your tier chain is Programme → Release → Initiative → Workstream → Epic → Task → Sub-task — exactly 7. There is no headroom. If any project nests one level deeper, `Level_8` is silently truncated and the tree stops nesting at the wrong place. One query:

```sql
SELECT MAX(Depth) FROM Gold.gold.dim_issue
```

If that returns 7, pass `max_depth=9` to `enrich_issue_hierarchy` and add `Level_8`/`Level_9` to the schema.

**New capability, optional:** `Actual_Start_Date` and `Actual_End_Date` are now populated (own field first, changelog second), so you can add a second baseline bar series for planned-vs-actual, with `Slip_Days` driving the colour. Purely additive — ignore it and the Gantt behaves as before.

---

## 5. Test Report

**Verdict: supported**, with one modelling step to do in Power BI.

```
Requirement / Story   Dim_Issue          ← Issue_Category = 'Requirement'
      │ "is tested by"
      ▼                Bridge_Issue_Link
   Test Set            Dim_Issue          ← Test_Level = 'Test Set'
      │                Bridge_Test_Set_Test
      ▼
    Test               Dim_Issue          ← Test_Level = 'Test'
      │
      ▼                Fact_Test_Run
  Test Run  ────────►  Test Execution (Dim_Issue)
```

| Report element | Source |
|---|---|
| Test Set → Test hierarchy | `Bridge_Test_Set_Test` |
| Pass / fail / blocked by set | `Fact_Test_Run` + `Dim_Test_Status` |
| Current pass rate | `Fact_Test_Run[Is_Latest_Run] = TRUE` |
| Pass-rate trend | `Fact_Test_Run`, all runs |
| First-time vs eventual pass rate | latest vs all runs, same table |
| Coverage % of requirements | `Bridge_Issue_Link` "is tested by" |
| Requirements with **no** test coverage | `Bridge_Issue_Link`, absence of link |
| Defects raised from failures | `Bridge_Issue_Link` "created by" |
| Execution progress | `Fact_Test_Run` grouped on Execution |
| Who executed | `Dim_Resource` via `Executed_By_Key` |

**The one step:** `Fact_Test_Run` has two issue keys — `Test_Issue_Key` and `Execution_Issue_Key`. Power BI won't allow two active relationships to `Dim_Issue`. Make **Test** the active path and add a role-playing copy (`Dim_Test_Execution`, a duplicate of `Dim_Issue`) for the execution end. Same pattern for `Bridge_Test_Set_Test`, where Test is active and Test Set gets `Dim_Test_Set`.

**Still open:** run this once — it decides whether the hierarchy flattens or stays a bridge.

```sql
SELECT test_issue_key, COUNT(DISTINCT test_set_issue_key) AS set_count
FROM Silver.xray.test_sets
GROUP BY test_issue_key HAVING COUNT(DISTINCT test_set_issue_key) > 1
```

No rows → flatten `Test_Set_Code` onto `Dim_Issue` for a genuine expand/collapse. Rows → keep the bridge, and say plainly in the report that a test in two sets means set totals won't sum to the programme total.

---

## 6. Resource Report

**Verdict: supported for workload; blocked for utilisation.**

| Report element | Source | Status |
|---|---|---|
| Who leads what | `Fact_Resource_Allocation` Role = Lead | ✅ |
| Who is involved | Role = Involved | ✅ |
| Lead + Involved on one page | `Dim_Role` slicer | ✅ |
| Workload per person (issues, points) | `Fact_Resource_Allocation` + `Fact_Issue` | ✅ |
| Overdue items per lead | + `Fact_Issue[Is_Overdue]` | ✅ |
| Spread across workstreams | + `Dim_Issue[Workstream_Label]` | ✅ |
| Breakdown by team | + `Dim_Team` | ⚠ field coverage |
| Work with **no lead** | `Dim_Issue[Has_No_Lead]` | ✅ |
| Hours logged per person | `Fact_Worklog` | ⚠ logging coverage |
| Workload trend over time | `Fact_Issue_Daily` | ✅ once it has run a while |
| **Utilisation %** | — | ❌ **not possible** |
| Contractor spend / forecast | — | ❌ not possible |

**Utilisation is arithmetically impossible from Jira.** `Fact_Worklog` gives hours *spent*; utilisation is spent ÷ available, and Jira holds no availability data at all. It needs a `Fact_Capacity` feed (person × week planned hours) from a resource plan, Tempo, or a spreadsheet. Worth flagging to the client before the resource dashboard is promised — this is the single most common cause of "why is this page blank".

`Allocation_Weight` (Lead 1.0, Involved 0.0) is what stops effort being multiplied by the number of names attached to an issue. Use `SUM(Allocation_Weight)` for effort and `SUM(Allocation_Count)` for headcount — they answer different questions.

---

## 7. The 14 dashboards

| # | Dashboard | Tables | Status |
|---|---|---|---|
| 1 | Tranche / release overview | Dim_Issue (`Release_Label`), Fact_Issue, Fact_Issue_Daily, Bridge_Issue_Link | ⚠ budget external |
| 2 | Workstream health scorecard | Dim_Issue (`Workstream_Label`), Fact_Issue | ⚠ capacity external |
| 3 | Delivery execution | Fact_Status_History, Fact_Issue_Daily | ✅ flow metrics, not sprint |
| 4 | Requirements lifecycle | Bridge_Issue_Link, Fact_Status_History | ✅ |
| 5 | Risk & issue intelligence | Fact_Issue (risk scores), Fact_Issue_Daily, Bridge_Issue_Label | ✅ |
| 6 | Policy readiness | Dim_Issue + Dim_IssueType (`Issue_Category`) | ⚠ if policies live in Confluence |
| 7 | Business change adoption | — | ❌ external |
| 8 | Development quality | Fact_Issue (Defect), Fact_Status_History (`Is_Reopen`) | ✅ |
| 9 | Data migration & quality | Dim_Issue, Fact_Test_Run | ✅ partial |
| 10 | End-to-end testing | Fact_Test_Run, Bridge_Test_Set_Test, Bridge_Issue_Link | ⚠ test environments missing |
| 11 | Release readiness | Bridge_Issue_Link traceability + Fact_Test_Run + Fact_Issue | ✅ |
| 12 | Benefits realisation | — | ❌ external |
| 13 | Resource & capacity | Fact_Resource_Allocation, Fact_Worklog, Dim_Team | ⚠ capacity external |
| 14 | Decision & action management | Dim_IssueType (`Issue_Category` = Governance), Fact_Issue (`Age_Days`) | ✅ |

**Nine fully supported, three partially, two not at all.**

The two that aren't — Business Change Adoption and Benefits Realisation — need training records, readiness assessments and a benefits register. None of that is in Jira in any form. They need their own feeds, with the grain and conformed keys defined in the earlier design doc so they slot in without remodelling.

Dashboard 3 works, but as **flow metrics rather than sprint metrics** — throughput, cycle-time percentiles, ageing WIP, flow efficiency, Monte Carlo forecasting from `Fact_Status_History`. Since sprints are barely used, that substitution is what makes it possible at all, and for release-driven delivery it's the more honest measure.

---

## 8. Build order

`Dim_Team` slots in before `Fact_Issue`:

1. `Dim_Date`
2. `Dim_Project` ← must precede Dim_Issue (Sort_Path root prefix)
3. `Dim_IssueType`, `Dim_Status`, `Dim_Priority`, `Dim_Resolution`, `Dim_Resource`, `Dim_Role`, `Dim_Link_Type`, `Dim_Version`, `Dim_Test_Status`, **`Dim_Team`**
4. `Dim_Issue`
5. `Fact_Issue`
6. `Fact_Resource_Allocation`, `Fact_Status_History`, `Fact_Worklog`, `Fact_Test_Run`
7. `Bridge_Issue_Link`, `Bridge_Issue_Label`, `Bridge_Test_Set_Test`
8. `Fact_Issue_Daily` — daily schedule, **start now**

`Fact_Issue_Daily` is the only table that can't be rebuilt retrospectively. Everything else regenerates from Silver on demand.

---

## 9. Three things I'd still check

1. `SELECT MAX(Depth) FROM Gold.gold.dim_issue` — if it returns 7, raise `max_depth`.
2. The Test Set cardinality query in §5 — decides flatten vs bridge.
3. Team field coverage in §3 — decides whether a team dashboard is worth building yet.
