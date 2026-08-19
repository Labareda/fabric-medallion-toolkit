# Programme_Reports semantic model

Import-mode TMDL definition for the Gold star schema in `sources/Gold`. 13 dimensions generated directly from each notebook's `TableSchema` (so the column list matches what actually gets written to `Gold.gold.*`) plus one hand-added role-playing dimension (`Dim_Due_Date`, see below), 8 facts, 48 relationships.

`Fact_Test_Run` (run-by-run trend history) was deliberately left out -- the test report only needs the current-status matrix (`Fact_Test`) and requirement coverage (`Fact_Test_Coverage`), not a trend/first-time-pass-rate view. The notebook still exists in git history if that's needed later.

## Before opening

Every table's M partition points at a placeholder connection string. Find-and-replace `<YOUR_GOLD_SQL_ENDPOINT>` across `definition/tables/*.tmdl` with the Gold lakehouse's SQL analytics endpoint (Fabric workspace -> Gold lakehouse -> Settings -> SQL analytics endpoint -> copy the server hostname). All tables use the same value.

## Opening it

- **Power BI Desktop**: File -> Open -> point at this folder (Desktop's TMDL view opens `.SemanticModel` folders directly), or open via Tabular Editor 3.
- **Fabric workspace**: connect the workspace to this Git repo (Workspace settings -> Git integration) and sync -- the workspace will pick up `Programme_Reports.SemanticModel` as a Semantic Model item.

## After opening, two manual steps (deliberately not hand-authored into the TMDL -- see below)

1. **Mark `Dim_Date` as a date table** on the `date` column (Model view -> right-click `Dim_Date` -> Mark as date table -> `date`), and do the same for **`Dim_Due_Date`**. This flips internal metadata that isn't safe to hand-write from outside Desktop. Only `Dim_Date` needs to be the model's single official "Date Table" for time-intelligence functions (YTD etc.) -- `Dim_Due_Date` just needs the same date-table treatment so slicers/relative-date filters work correctly against it too.
2. **Refresh** once connections are set, to confirm every partition resolves.

## Two date tables, not one -- Dim_Date and Dim_Due_Date

Fact_Issue's Rollup_Start_Date relates to `Dim_Date`, and Rollup_End_Date relates to a SEPARATE table, `Dim_Due_Date` -- both ACTIVE. This is deliberate, not an oversight: a single shared date table can only have one active relationship per fact, so with everything pointed at one `Dim_Date`, a start/end pair can never both be simultaneously filterable the way a Gantt/timeline needs (e.g. "show items overlapping this date range" needs Start and End both live at once). `Dim_Due_Date` is a second IMPORT of the exact same `Gold.gold.dim_date` table under a different model name -- the standard "role-playing dimension via duplicate import" pattern, not a second physical Gold table.

Every OTHER date role (Planned_Start/End, Actual_Start/End, Created_Date, Valid_To_Date) stays as a single INACTIVE relationship against `Dim_Date` -- those aren't used as a simultaneous pair for a visual, just for the odd measure, so `USERELATIONSHIP()` is enough and doesn't need a third/fourth physical date table:

| Fact | Active | Inactive (use USERELATIONSHIP against Dim_Date) |
|---|---|---|
| Fact_Issue | Rollup_Start_Date -> Dim_Date, Rollup_End_Date -> Dim_Due_Date | Planned_Start_Date, Planned_End_Date, Actual_Start_Date, Actual_End_Date, Created_Date |
| Fact_Issue_History | Valid_From_Date -> Dim_Date | Valid_To_Date |
| Fact_Test | Latest_Run_Date -> Dim_Date | -- |
| Fact_Worklog | Started_Date -> Dim_Date | -- |

Example for a measure that needs Created_Date instead of the active Rollup_Start_Date:

```
Issues Created =
CALCULATE(
    [Total Issues],
    USERELATIONSHIP('Fact_Issue'[Created_Date], 'Dim_Date'[date])
)
```

`Rollup_Start_Date`/`Rollup_End_Date` were chosen as Fact_Issue's active pair because the Gantt/timeline report is the primary consumer and the notebook's own comment says to point visuals at the rollup columns, not the raw ones.

## Fact_Resource_Day_Allocation -- capacity / conflict detection

New fact, grain: one row per resource per issue per WORKING DAY the issue is planned to run through. Exists to answer "is this person double-booked" -- something `Fact_Resource_Allocation` (issue-level, no day breakdown) can't. Jira has no "hours per day" field anywhere, so this table MANUFACTURES one on explicit assumptions -- read the notebook's own header comment (`Gold - Fact_Resource_Day_Allocation.py`) before trusting its numbers for anything beyond "spot a likely conflict":

1. Only **Lead** allocations count (matches `Fact_Resource_Allocation`'s own effort-weighting).
2. An issue's `Original_Estimate_Hours` is spread **evenly** across its own working days (`Planned_Start_Date`..`Planned_End_Date`, not the rolled-up range -- that would double-count a parent's hours on top of its children's).
3. Issues missing a planned range or an estimate are **excluded**, not guessed at.
4. Weekends are excluded; public holidays are **not** -- no holiday calendar in this model.

Reads two other facts (`Fact_Resource_Allocation`, `Fact_Issue`) -- the second exception to "facts don't read facts" alongside `Fact_Test_Coverage`. `[Conflicting Resource-Days]` on the table is a starter measure (SUMMARIZE to the resource-day grain, compare against `Dim_Resource[Daily_Capacity_Hours]`) -- needs testing against real data, this pattern is easy to get subtly wrong.

## Week buckets on Dim_Date / Dim_Due_Date

`week_start_date` (Monday), `week_end_date` (Sunday), and `week_label` (e.g. "Aug 2026 10-16") were added to `build_date_dimension` in the wheel (0.3.69) -- for a Resource report grid with weekly columns like a Jira Team Timeline view, instead of one column per day.

## Bridge_Issue_Link -- linked work items, any type, and the Blocked Tests report

Bridge table, grain: one row per issue per link record, from that issue's OWN perspective -- mirrors Jira's "Linked work items" panel exactly (created, is blocked by, tests, relates to, implements, ... whatever's in your data). Both sides of every link get their own row deliberately: issue A "is blocked by" B gets a row on A, and B "blocks" A gets its own separate row on B. Both are correct; that's what lets a report start from either issue and filter to whichever relation and direction it wants (`Link_Type_Name`, `Direction`, `Link_Label`).

An earlier version of this model had a SECOND table, `Bridge_Issue_Blocks`, hardcoded to just the "Blocks" link type for the Blocked Tests report specifically -- removed again within the same session once it became clear it was pure duplication: everything it could show is exactly `Bridge_Issue_Link` filtered to `Link_Type_Name = 'Blocks' AND Direction = 'Inward'`. One general table, filtered per report, beats one table per relationship type.

Gives `Dim_Link_Type` its first real relationship since the original `Bridge_Issue_Link` was deleted a few commits back (previously flagged here as an orphaned placeholder table -- resolved).

Relationships:
- `Issue_Key` -- **active**. The anchor issue (whichever issue that row's own perspective belongs to).
- `Linked_Issue_Key` -- **inactive** (same target table as Issue_Key, only one relationship between the same two tables can be active). The other side of the link.
- `Link_Type_Key` -- active, to `Dim_Link_Type`.

`Fact_Test` also has two real relationships to `Dim_Issue` now (previously only text columns, `Test_Code`/`Parent_Issue_Code`):
- `Test_Issue_Key` -- **active**. The test issue itself. Chains straight through to `Bridge_Issue_Link` (both hops active), no DAX needed for "what's linked to this test".
- `Parent_Issue_Key` -- **inactive**. The Requirement/Story the test set covers, in case a future report needs the parent's own links instead of the test's.

Confirmed by the client's own screenshot that the Blocked Tests requirement anchors on the TEST issue (it has its own Test Runs tab showing the BLOCKED status), not a parent Requirement -- `Test_Issue_Key` is the right active path.

Three measures on `Fact_Test` for the actual worklist the client described (a lot of tests are Blocked; only some have the `is blocked by` link recorded yet -- they need to go through and add the missing ones IN JIRA, which Power BI can't do; this model's job is to surface which ones still need it):
- `[Blocking Issue Codes]` -- comma-joined list of blocking issue codes, `Bridge_Issue_Link` filtered to `Link_Type_Name = 'Blocks' AND Direction = 'Inward'`.
- `[Has Blocking Link]` -- boolean, whether any blocking issue is recorded at all.
- `[Blocked Tests Missing a Blocking Link]` -- count of Blocked tests with NO `is blocked by` link yet. This is the number that matters for the worklist.

**Watch for double-counting**: because both sides of every link are separate rows, a naive `COUNTROWS('Bridge_Issue_Link')` over an unfiltered context counts every relationship twice. Always filter to a specific `Link_Type_Name` + `Direction` (or a single issue) before counting.

## Measures included

A starter set per fact (Total Issues, Pass Rate %, Coverage %, Headcount, etc.) -- open each table in `definition/tables/*.tmdl` to see them, or the Model view's Fields pane once open. Not exhaustive; add report-specific measures as the actual pages get built.
