# Programme_Reports semantic model

Import-mode TMDL definition for the Gold star schema in `sources/Gold`. 13 dimensions generated directly from each notebook's `TableSchema` (so the column list matches what actually gets written to `Gold.gold.*`) plus one hand-added role-playing dimension (`Dim_Due_Date`, see below), 7 facts, 43 relationships.

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

## Known model gaps (not fixed here -- flagging for a decision)

- **Dim_Link_Type has no relationships** -- nothing consumes it since `Bridge_Issue_Link` was deleted. It's in this model as a placeholder; delete the table here (and the notebook) if issue-link traceability reporting isn't coming back.
- **`Fact_Test[Parent_Issue_Code]`** is a plain text column, not a relationship -- there's no `Test_Issue_Key`-style FK from Fact_Test to the parent issue in Dim_Issue. Add one (join on `Issue_Code`) if the test report needs to slice by the parent requirement's own attributes beyond the code string.

## Measures included

A starter set per fact (Total Issues, Pass Rate %, Coverage %, Headcount, etc.) -- open each table in `definition/tables/*.tmdl` to see them, or the Model view's Fields pane once open. Not exhaustive; add report-specific measures as the actual pages get built.
