# Model Assessment & Power BI Timeline

---

## Part A — Is the model still correct?

Yes, with one correction of mine and four of the existing code.

### First, a correction to my own advice

In my last message I said `Dim_Issue` and `Fact_Issue` should be merged, because a 1:1 with single-direction filtering breaks slicers. **That was wrong, and your split is right.**

Power BI *forces* bidirectional cross-filtering on one-to-one relationships — you can't set it to single. So a Status slicer (which reaches `Fact_Issue`) does filter `Dim_Issue`'s hierarchy columns. The problem I described can't occur.

Your split is also the better structure for a different reason: `Dim_Issue` carries the hierarchy walk output, which is expensive to compute and rarely changes, while `Fact_Issue` carries dates and measures that change every refresh. Keeping them apart means the hierarchy isn't rebuilt to update a story point value. Your comment in `Fact_Issue` — FKs belong on the fact, `Dim_Issue` holds no keys and no denormalised text — is textbook and I'd keep it.

Two things that were already right and I'd flag as load-bearing: **no `Assignee_Key` on `Fact_Issue`** (one path from `Dim_Resource`, via `Fact_Resource_Allocation`) and **no sentinel dates** (NULL renders a row with no bar; 1900-01-01 stretches the axis a century). Both are the kind of decision that's easy to undo by accident later.

### Four corrections to the existing notebooks

**1. `Dim_IssueType` reads the wrong column names — it will fail.**

It selects `issue_type_id`, `issue_type_name`, `is_subtask`, `hierarchy_level`. Those are the **Power BI Connector for Jira** column names, from the ERD. Your Silver comes from the Jira REST API through `auto_standardize`, so the columns are the API's own: `id`, `name`, `subtask`, `hierarchyLevel`. `Dim_Issue` already joins `it.id` / `it.name` / `it.hierarchylevel` correctly, so the two notebooks disagreed. Fixed.

**2. Actual start *is* derivable — the changelog has a timestamp.**

`Fact_Issue` says there's no per-transition timestamp, only `issue_created`. That's true of `history_items` **on its own**. But `Silver.jira.histories` carries `created` as a real timestamp — B2S maps it explicitly:

```python
fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ")
```

and `history_items` carries `history_id`. Join the two and every field change has a timestamp.

This is the biggest single unlock in the review. It gives you `Actual_Start_Date`, and it makes `Fact_Status_History` buildable — time in status, flow efficiency, reopen rate, blocked days, cycle-time percentiles, Monte Carlo forecasting.

That matters more than usual **because the client barely uses sprints**. No sprints means no velocity and no burndown; flow metrics from the changelog are the replacement. For a programme running to release dates rather than a two-week cadence, they're arguably the better measure anyway.

**3. The Actual start / Actual end custom fields exist and weren't being read.**

`fields_actual_start` and `fields_actual_end` are on this instance (custom fields 10008 and 10009 — visible in the connector ERD, and present in the Silver extract). `Fact_Issue` used `resolutiondate` alone. Now: own field wins, changelog fills the gap.

**4. Story points are split across two fields.**

Both `fields_story_point_estimate` (10016) and `fields_story_points` (10035) exist, and different teams populate different ones. Taking only the first silently zeroes every team using the other. Now `COALESCE`d — check (9) in the validation notebook shows how much this is actually costing you.

### One design change: typed tier columns are back on

You switched off `typed_level_names` on the grounds that `Hierarchy_Level_Name` plus a `Sort_Path` prefix filter covers tier reporting.

That's true for **drill-down** ("this item and everything under it") but not for **grouping**. The Workstream Health dashboard needs *"sum story points BY workstream"* across every descendant at once. A prefix filter answers one workstream at a time; it can't produce a column to group on. Same for "% work complete by workstream" in the tranche overview.

So both walks now run, which is what the wheel's own docstring describes:

| Columns | Placement | Use |
|---|---|---|
| `Level_1..7` | dense, by depth | xViz Gantt nesting only |
| `Programme_Label`, `Release_Label`, `Initiative_Label`, `Workstream_Label`, `Epic_Label` | typed, by the issue type's tier | slicers, grouping, the scorecard |

The typed walk is also what makes "Workstream is level 1 or 2 depending on the project" work without special-casing: placement is by the issue type's own rank, so the same tier lands in the same column whichever project it's in.

**Never filter on the dense columns.** A tier skipped in one project shifts everything up a slot and regroups issues under the wrong workstream — which looks like real data, not like an error.

### On the connector ERD

Worth knowing if you're weighing the Power BI Connector against your own extract: it ships `TimeInStatus` and `TimeWithAssignee` pre-computed, which is most of what `Fact_Status_History` derives. It also explodes each multi-value custom field into its own table (`Workstream_10069`, `Release_10145`, `People_Involved_10956`).

That last point is worth checking against your instance: if `Workstream` is a multi-select custom field, an issue can carry more than one, which would sit awkwardly beside the single-ancestor hierarchy assumption. Check (4) in the validation notebook will show whether it bites.

---

## Part B — Notebooks

21 build notebooks plus validation. Build order matters — `Dim_Project` before `Dim_Issue` (Sort_Path root prefix), `Dim_Issue` before every fact.

| # | Notebook | Notes |
|---|---|---|
| 1 | `Dim_Date` | wheel-built, plus sentinel |
| 2 | `Dim_Project` | **must precede Dim_Issue** |
| 3 | `Dim_IssueType` | column names fixed; adds `Issue_Category`, `Tier_Name` |
| 4 | `Dim_Status` | adds `Status_Group`, `Flow_State` |
| 5 | `Dim_Priority` | |
| 6 | `Dim_Resolution` | |
| 7 | `Dim_Resource` | users ∪ people-involved |
| 8 | `Dim_Role` | Lead / Involved / Reporter |
| 9 | `Dim_Link_Type` | |
| 10 | `Dim_Version` | Jira versions ≠ the Release tier |
| 11 | `Dim_Test_Status` | |
| 12 | `Dim_Issue` | typed tiers re-enabled |
| 13 | `Fact_Issue` | actual start, coalesced points, risk scores |
| 14 | `Fact_Resource_Allocation` | |
| 15 | `Fact_Status_History` | **new** |
| 16 | `Fact_Worklog` | check coverage first |
| 17 | `Fact_Test_Run` | |
| 18 | `Bridge_Test_Set_Test` | **new** |
| 19 | `Bridge_Issue_Link` | both directions |
| 20 | `Bridge_Issue_Label` | |
| 21 | `Fact_Issue_Daily` | **schedule daily, start now** |
| — | `Validation Checks` | run before trusting anything |

`Fact_Issue_Daily` is the one that can't be rebuilt retrospectively. Everything else regenerates from Silver on demand; a snapshot you never took is gone. Schedule it before the dashboards are finished.

---

## Part C — The Power BI timeline

### How Sort_Path and Rank work together

They do different jobs and the distinction is the whole trick:

- **`Rank`** is Jira's LexoRank. It orders every issue *globally*. Sorting by it directly scatters children away from their parents — a Sub-task of PSP-193 could sort between two unrelated Epics.
- **`Sort_Path`** concatenates each ancestor's rank from the root down, so **a parent's path is a literal prefix of every one of its children's**. One ascending sort on `Sort_Path` produces the entire tree in Jira's own order, parents immediately above their children.

So: `Rank` is the *input*, `Sort_Path` is the *output you sort on*. You never sort a timeline by `Rank`.

### Setting it up in the semantic model

1. Mark `Dim_Date` as the date table.
2. Set **Sort by column**: `Display_Label` → `Sort_Path`. This is what makes every visual respect tree order without per-visual configuration.
3. Also set `Priority_Name` → `Sort_Order`, `Status_Name` → `Sort_Order`, `Test_Status_Name` → `Sort_Order`.
4. Hide `Sort_Path`, `Rank`, `Depth`, every `_Key` column, and every bridge.

### xViz Gantt configuration

| Field well | Column | Why |
|---|---|---|
| Task Name (hierarchy) | `Level_1` … `Level_7` in order | dense levels nest; typed ones would leave interior gaps and stop the nesting |
| Start Date | `Rollup_Start_Date` | **not** `Planned_Start_Date` |
| End Date | `Rollup_End_Date` | **not** `Planned_End_Date` |
| Resource | `Lead_Name` | matches the "Lead Name" column in the reference screenshot |
| Tooltip | `Resource_Names`, `Hierarchy_Level_Name`, `Status_Name` | |
| Sort | `Sort_Path` ascending | the only sort needed |

Two settings that are easy to miss:

- **Turn "Hide Blanks" OFF.** Trailing nulls in `Level_5..7` are expected for shallow branches. Hiding them collapses rows that should render.
- **Point at the Rollup columns.** Roughly 10,500 of ~12,000 issues have no dates of their own. Using the raw planned dates gives you a Gantt where 85% of rows draw nothing, including every summary row.

### Planned vs actual

Add a second bar series using `Actual_Start_Date` / `Actual_End_Date` — now populated, given the changelog fix. `Slip_Days` drives conditional formatting: red where positive, green where negative or null.

### Drill-down: "this item and everything below it"

This is where `Sort_Path` earns its place. Because a parent's path prefixes its children's, subtree filtering is a string comparison:

```dax
Selected Sort Path =
VAR _Sel = SELECTEDVALUE ( Dim_Issue[Sort_Path] )
RETURN _Sel

Is In Selected Subtree =
VAR _Sel = [Selected Sort Path]
RETURN
    IF (
        ISBLANK ( _Sel ),
        1,
        IF ( LEFT ( SELECTEDVALUE ( Dim_Issue[Sort_Path] ), LEN ( _Sel ) ) = _Sel, 1, 0 )
    )
```

Apply as a visual-level filter where `Is In Selected Subtree = 1`. No parent-child DAX, no `PATH()` functions, no recursion — one string prefix test.

### Measures for the timeline

```dax
-- Progress of a summary row, weighted by its descendants
Subtree % Complete =
DIVIDE (
    CALCULATE ( COUNTROWS ( Fact_Issue ), Fact_Issue[Is_Done] = TRUE () ),
    COUNTROWS ( Fact_Issue )
)

-- Schedule health, rolled up
Subtree Slip Days = MAX ( Fact_Issue[Slip_Days] )

Timeline Bar Colour =
SWITCH (
    TRUE (),
    [Subtree Slip Days] > 10, "#C0392B",
    [Subtree Slip Days] > 0,  "#E67E22",
    ISBLANK ( MAX ( Fact_Issue[Rollup_Start_Date] ) ), "#BDC3C7",
    "#27AE60"
)

-- Today marker for the Gantt's reference line
Today = TODAY ()
```

### Workstream grouping (the typed columns paying off)

```dax
Workstream % Complete =
CALCULATE (
    DIVIDE (
        CALCULATE ( COUNTROWS ( Fact_Issue ), Fact_Issue[Is_Done] = TRUE () ),
        COUNTROWS ( Fact_Issue )
    ),
    ALLEXCEPT ( Dim_Issue, Dim_Issue[Workstream_Label] )
)
```

Group a matrix on `Dim_Issue[Workstream_Label]` and this works across every descendant at once — the thing the `Sort_Path` prefix filter can't do.

### Trend, once the snapshot has run a while

```dax
Open Issues (Snapshot) =
CALCULATE ( SUM ( Fact_Issue_Daily[Issue_Count] ), Fact_Issue_Daily[Is_Open] = TRUE () )

Scope Change vs 30 Days Ago =
VAR _Now = [Open Issues (Snapshot)]
VAR _Then =
    CALCULATE (
        [Open Issues (Snapshot)],
        Fact_Issue_Daily[Snapshot_Date] = MAX ( Fact_Issue_Daily[Snapshot_Date] ) - 30
    )
RETURN _Now - _Then
```

---

## Part D — Still open

1. **Test Set → Test cardinality** — check (1). Decides whether the testing hierarchy flattens onto `Dim_Issue` or stays a bridge.
2. **Is Workstream also a multi-select custom field?** The connector ERD shows `Workstream_10069` as its own table. If an issue can carry two workstreams, the single-ancestor assumption needs revisiting.
3. **Worklog coverage** — check (5). Determines whether the resource dashboards are viable.
4. **Capacity, budget, benefits** — none exist in Jira. Utilisation is impossible without planned availability; flag this before dashboards 7, 12 and 13 get promised.
