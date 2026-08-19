# Programme_Reports semantic model

Import-mode TMDL definition for the Gold star schema in `sources/Gold`. 13 dimensions generated directly from each notebook's `TableSchema` (so the column list matches what actually gets written to `Gold.gold.*`) plus one hand-added role-playing dimension (`Dim_Due_Date`, see below), 6 facts, 40 relationships.

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

## Known model gaps (not fixed here -- flagging for a decision)

- **Dim_Link_Type has no relationships** -- nothing consumes it since `Bridge_Issue_Link` was deleted. It's in this model as a placeholder; delete the table here (and the notebook) if issue-link traceability reporting isn't coming back.
- **`Fact_Test[Parent_Issue_Code]`** is a plain text column, not a relationship -- there's no `Test_Issue_Key`-style FK from Fact_Test to the parent issue in Dim_Issue. Add one (join on `Issue_Code`) if the test report needs to slice by the parent requirement's own attributes beyond the code string.
- **No week-start column on Dim_Date** -- if the Resource report needs weekly buckets (like a Jira Team Timeline view), the date dimension needs a `week_start_date` column added to `build_date_dimension`.
- **Resource-day-level capacity/conflict detection isn't modelled yet.** `Dim_Resource[Daily_Capacity_Hours]` exists, but there's no fact at the (resource, day) grain to compare allocated hours against it -- needed for an over-allocation/"conflicts" measure like the Jira Team Timeline view.

Resolved since the last update: **Fact_Test and Fact_Test_Coverage now have Project_Key/Team_Key**, resolved via the Requirement (Fact_Test goes through its Parent_Issue_Code first, since a test set's own project/team isn't necessarily the delivery project; Fact_Test_Coverage resolves directly since Requirement is its own anchor issue).

## Measures included

A starter set per fact (Total Issues, Pass Rate %, Coverage %, Headcount, etc.) -- open each table in `definition/tables/*.tmdl` to see them, or the Model view's Fields pane once open. Not exhaustive; add report-specific measures as the actual pages get built.
