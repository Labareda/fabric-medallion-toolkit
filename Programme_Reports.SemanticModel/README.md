# Programme_Reports semantic model

Import-mode TMDL definition for the Gold star schema in `sources/Gold`. 13 dimensions, 7 facts, 40 relationships -- generated directly from each notebook's `TableSchema` so the column list matches what actually gets written to `Gold.gold.*`.

## Before opening

Every table's M partition points at a placeholder connection string. Find-and-replace `<YOUR_GOLD_SQL_ENDPOINT>` across `definition/tables/*.tmdl` with the Gold lakehouse's SQL analytics endpoint (Fabric workspace -> Gold lakehouse -> Settings -> SQL analytics endpoint -> copy the server hostname). All 20 tables use the same value.

## Opening it

- **Power BI Desktop**: File -> Open -> point at this folder (Desktop's TMDL view opens `.SemanticModel` folders directly), or open via Tabular Editor 3.
- **Fabric workspace**: connect the workspace to this Git repo (Workspace settings -> Git integration) and sync -- the workspace will pick up `Programme_Reports.SemanticModel` as a Semantic Model item.

## After opening, two manual steps (deliberately not hand-authored into the TMDL -- see below)

1. **Mark `Dim_Date` as a date table** on the `date` column: Model view -> right-click `Dim_Date` -> Mark as date table -> `date`. This flips internal metadata that isn't safe to hand-write from outside Desktop.
2. **Refresh** once connections are set, to confirm every partition resolves.

## Date relationships (role-playing)

`Dim_Date` has 14 relationships into it across the model (every date column on every fact). Only one per fact is **active** by default -- the rest need `USERELATIONSHIP()` in any measure that wants that particular date role:

| Fact | Active relationship | Inactive (use USERELATIONSHIP) |
|---|---|---|
| Fact_Issue | Rollup_Start_Date | Rollup_End_Date, Planned_Start_Date, Planned_End_Date, Actual_Start_Date, Actual_End_Date, Created_Date |
| Fact_Issue_History | Valid_From_Date | Valid_To_Date |
| Fact_Test_Run | Started_Date | Finished_Date |
| Fact_Test | Latest_Run_Date | -- |
| Fact_Worklog | Started_Date | -- |

Example for a measure that needs Created_Date instead of the active Rollup_Start_Date:

```
Issues Created =
CALCULATE(
    [Total Issues],
    USERELATIONSHIP('Fact_Issue'[Created_Date], 'Dim_Date'[date])
)
```

`Rollup_Start_Date` was chosen as Fact_Issue's active relationship because the Gantt/timeline report is the primary consumer and the notebook's own comment says to point visuals at the rollup columns, not the raw ones.

## Known model gaps (not fixed here -- flagging for a decision)

- **Fact_Test and Fact_Test_Coverage have no Project_Key/Team_Key.** They were removed while fixing broken SQL that referenced columns which no longer existed after a design revert. Slicing the test report by project or team currently requires going through `Parent_Issue_Code`/`Requirement_Key` to `Dim_Issue`, then a second hop to `Fact_Issue` for `Project_Key` -- which doesn't work as a Power BI relationship (facts don't relate to facts here). If you want project/team slicing on the test report, that needs `Project_Id`/`Team_Name` re-added to those two notebooks' source queries.
- **Dim_Link_Type has no relationships** -- nothing consumes it since `Bridge_Issue_Link` was deleted. It's in this model as a placeholder; delete the table here (and the notebook) if issue-link traceability reporting isn't coming back.
- **`Fact_Test[Parent_Issue_Code]`** is a plain text column, not a relationship -- there's no `Test_Issue_Key`-style FK from Fact_Test to the parent issue in Dim_Issue. Add one (join on `Issue_Code`) if the test report needs to slice by the parent requirement's own attributes beyond the code string.

## Measures included

A starter set per fact (Total Issues, Pass Rate %, Coverage %, Headcount, etc.) -- open each table in `definition/tables/*.tmdl` to see them, or the Model view's Fields pane once open. Not exhaustive; add report-specific measures as the actual pages get built.
