# Testing Model — Design & Build Guide

## What changed and why

The previous model used `Bridge_Test_Set_Test` to handle the many-to-many between tests and test sets. 142 tests belong to 2 or 3 sets. That meant:

- A bridge table with bidirectional cross-filtering in Power BI
- Complex DAX to avoid double-counting at the totals level
- Two separate measure sets (per-set and programme-level) that visibly disagree
- The client maintaining all of that

**The new model has no bridge table.** Instead, `Fact_Test` uses the test-set membership itself as the grain — one row per test per set it belongs to. A test in two sets has two rows. This means:

- The Power BI matrix is a plain two-level expand (Test Set → Test) on FK columns — no bridge, no bidirectional filter, no special DAX
- Set-level totals are just `SUM` and `COUNT` within the filtered set
- Programme-level totals use `DISTINCTCOUNT(Test_Code)` — one measure, obvious logic
- Shared tests are visible rather than hidden, which is the honest picture

It also matches the original screenshot exactly: Issue → Test Set → Test, with status per test.

---

## Tables

### Dimensions (2)

**`Dim_Test_Set`** — one row per test set. Carries workstream, release, status and lead directly, so the test report needs no relationship to `Dim_Issue` at all. Simple, self-contained.

**`Dim_Test_Status`** — Xray statuses plus one synthetic row: **"NOT RUN"**. This is not in Xray — it is the absence of a run. Adding it as a proper dimension member means the matrix shows "NOT RUN" as a real value with a count, rather than a blank that nobody notices before go-live.

### Facts (3)

**`Fact_Test`** — the spine. One row per test-set membership. Latest status denormalised directly onto each row — the matrix needs no join to run history to show current state.

**`Fact_Test_Run_History`** — every run ever. For trend charts, first-time pass rate, and cycle time. Kept separate from `Fact_Test` because the grain is different and mixing them confuses every visual.

**`Fact_Test_Coverage`** — one row per requirement-to-test-set link, plus one row per uncovered requirement (with `Is_Covered = False`). This is how "requirements with no tests" works without any DAX: filter `Is_Covered = False` and you have the list. Both the summary card and the drill-through table use the same filter.

### What was removed

- `Bridge_Test_Set_Test` — no longer needed. The many-to-many is the grain of `Fact_Test`.
- `Fact_Test_Run` — replaced by `Fact_Test_Run_History` (same data, clearer name and purpose).
- The "Shared Tests Warning" DAX — no longer needed. A shared test appearing in two set rows is correct and expected; the user sees it rather than hiding it.

---

## Relationships

All one-to-many, single direction, unless noted. No bidirectional relationships needed in this sub-model.

| From (1) | Column | To (*) | Column | Active |
|---|---|---|---|---|
| `Dim_Test_Set[Test_Set_Key]` | | `Fact_Test[Test_Set_Key]` | | ✅ |
| `Dim_Test_Status[Test_Status_Key]` | | `Fact_Test[Test_Status_Key]` | | ✅ |
| `Dim_Resource[Resource_Key]` | | `Fact_Test[Executor_Key]` | | ✅ |
| `Dim_Test_Status[Test_Status_Key]` | | `Fact_Test_Run_History[Test_Status_Key]` | | ✅ |
| `Dim_Resource[Resource_Key]` | | `Fact_Test_Run_History[Executor_Key]` | | ✅ |
| `Dim_Date[date]` | | `Fact_Test_Run_History[Started_Date]` | | ✅ |
| `Dim_Date[date]` | | `Fact_Test_Run_History[Finished_Date]` | | inactive |
| `Dim_Test_Set[Test_Set_Key]` | | `Fact_Test_Coverage[Test_Set_Key]` | | ✅ |
| `Dim_Issue[Issue_Key]` | | `Fact_Test_Coverage[Requirement_Key]` | | ✅ |

**`Dim_Test_Set` is the hub.** It connects `Fact_Test` and `Fact_Test_Coverage`. A test-set slicer filters both at once, automatically.

**`Dim_Issue` connects only to `Fact_Test_Coverage`**, not to `Fact_Test`. The test matrix doesn't need `Dim_Issue` — all the context is already in `Fact_Test` and `Dim_Test_Set`. This keeps the matrix simple.

**No relationship between `Fact_Test` and `Fact_Test_Run_History`.** They share `Test_Code` as a natural key but are not related in the model. Use `TREATAS` or `CROSSFILTER` in a measure if you ever need to pull run history into the same visual as test status. In practice, the two live on separate report pages.

---

## Sort-by columns

| Table | Column | Sort by |
|---|---|---|
| `Dim_Test_Status` | `Test_Status_Name` | `Test_Status_Sort_Order` |

Set this in the semantic model. The sort order is: PASSED (1) → FAILED (2) → BLOCKED (3) → EXECUTING (4) → TO DO (5) → NOT RUN (6). This gives the matrix a natural reading order — green at top, problems immediately visible.

---

## The matrix visual

The screenshot shows:

```
▼ TRAN-267: IS - US3 - Application Duration - System Admin   [Test Set]
    Test Key   Summary                                        Status
    TRAN-168   SysAdmin can reset the deadline days...        PASSED
    TRAN-169   SysAdmin have updated deadline and all...      PASSED
```

Build this with a **Matrix visual**:

| Field well | Value |
|---|---|
| Rows | `Dim_Test_Set[Test_Set_Label]` |
| Rows (nested) | `Fact_Test[Test_Code]` |
| Columns | *(none — flat)* |
| Values | `Fact_Test[Test_Name]`, `[Latest Status]`, `[Acceptance Criteria]` |

For the "Acceptance Criteria" column, `Fact_Test[Acceptance_Criteria]` is a plain text column — add it as a value with "Don't summarise". Same for `Test_Name` and `Latest_Status`.

**Conditional formatting on Latest_Status:**

| Value | Colour |
|---|---|
| PASSED | #27AE60 (green) |
| FAILED | #C0392B (red) |
| BLOCKED | #2C3E50 (black) |
| EXECUTING | #F39C12 (amber) |
| NOT RUN | #95A5A6 (grey) |

Apply as background colour on the Latest_Status cell. The client sees the RAG without a separate column.

---

## Measures

All simple. No bridge DAX, no ALLEXCEPT gymnastics.

```dax
-- Matrix counts
Tests = COUNTROWS ( Fact_Test )
Tests Passed  = CALCULATE ( [Tests], Fact_Test[Is_Pass]     = TRUE () )
Tests Failed  = CALCULATE ( [Tests], Fact_Test[Is_Fail]     = TRUE () )
Tests Blocked = CALCULATE ( [Tests], Fact_Test[Is_Blocked]  = TRUE () )
Tests Not Run = CALCULATE ( [Tests], Fact_Test[Is_Not_Run]  = TRUE () )
Tests Executed = CALCULATE ( [Tests], Fact_Test[Is_Executed] = TRUE () )

-- Rates
Pass Rate % =
    DIVIDE ( [Tests Passed], [Tests] )

Pass Rate (Executed) % =
    DIVIDE ( [Tests Passed], [Tests Executed] )

Execution Progress % =
    DIVIDE ( [Tests Executed], [Tests] )

-- Programme total (deduplicated across sets)
Tests Unique =
    DISTINCTCOUNT ( Fact_Test[Test_Code] )

Tests Passed (Unique) =
CALCULATE (
    DISTINCTCOUNT ( Fact_Test[Test_Code] ),
    Fact_Test[Is_Pass] = TRUE ()
)

Programme Pass Rate % =
    DIVIDE ( [Tests Passed (Unique)], [Tests Unique] )

-- RAG per set (use in a card or conditional format)
Test Set RAG =
SWITCH (
    TRUE (),
    [Tests] = 0,                   "Grey",
    [Tests Failed] > 0,            "Red",
    [Tests Not Run] > 0,           "Amber",
    [Pass Rate %] = 1,             "Green",
    "Amber"
)

-- Trend (uses Fact_Test_Run_History + Dim_Date)
Runs per Day =
    COUNTROWS ( Fact_Test_Run_History )

Pass Rate Trend % =
    DIVIDE (
        CALCULATE ( COUNTROWS ( Fact_Test_Run_History ),
                    Fact_Test_Run_History[Is_Pass] = TRUE () ),
        COUNTROWS ( Fact_Test_Run_History )
    )

First Time Pass Rate % =
    DIVIDE (
        CALCULATE ( COUNTROWS ( Fact_Test_Run_History ),
                    Fact_Test_Run_History[Passed_First_Time] = TRUE () ),
        CALCULATE ( COUNTROWS ( Fact_Test_Run_History ),
                    Fact_Test_Run_History[Is_First_Run] = TRUE () )
    )

-- Coverage (uses Fact_Test_Coverage)
Requirements Total =
    DISTINCTCOUNT ( Fact_Test_Coverage[Requirement_Code] )

Requirements Covered =
CALCULATE (
    DISTINCTCOUNT ( Fact_Test_Coverage[Requirement_Code] ),
    Fact_Test_Coverage[Is_Covered] = TRUE ()
)

Requirements Uncovered =
    [Requirements Total] - [Requirements Covered]

Coverage % =
    DIVIDE ( [Requirements Covered], [Requirements Total] )
```

---

## Page layout

**Page 1 — Test Sets**

Header cards (left to right):
`Tests` · `Tests Passed` · `Tests Failed` · `Tests Not Run` · `Pass Rate %` · `Execution Progress %`

Main matrix (parent issue → test set → test):

The matrix has three levels using nested rows:
1. `Fact_Test[Parent_Issue_Code]` + `[Parent_Issue_Name]`  ← the requirement
2. `Dim_Test_Set[Test_Set_Label]`  ← the test set
3. `Fact_Test[Test_Code]` + `[Test_Name]` + `[Acceptance_Criteria]` + `[Latest_Status]`

Conditional formatting on `Latest_Status` using the colour table above.

Slicers: `Dim_Test_Set[Workstream]` · `Dim_Test_Set[Release]` · `Dim_Test_Status[Test_Status_Name]`

**Page 2 — Trend**

Line chart: `Dim_Date[date]` on axis · `Pass Rate Trend %` · slicer by `Dim_Test_Set[Test_Set_Label]`

Stacked bar: `Dim_Date[date]` · Passed / Failed / Blocked / Not Run counts from `Fact_Test_Run_History`

Cards: `First Time Pass Rate %` · average `Duration_Mins`

**Page 3 — Coverage**

Card: `Coverage %` · `Requirements Uncovered`

Table of uncovered requirements (filter `Fact_Test_Coverage[Is_Covered] = False`):
Workstream · Release · Requirement_Code · Requirement_Name · Requirement_Category

Table of covered requirements: all the above + Tests_In_Set · Tests_Passed · Tests_Not_Run

---

## Build order

`Dim_Test_Status` and `Dim_Test_Set` must exist before the facts that look them up:

1. `Dim_Test_Status`
2. `Dim_Test_Set` *(depends on `Dim_Issue`, `Fact_Issue`, `Dim_Status` already existing)*
3. `Fact_Test`
4. `Fact_Test_Coverage` *(depends on `Fact_Test` for set stats)*
5. `Fact_Test_Run_History`

Add to orchestration after the existing Gold dimensions, before `Bridge_Issue_Link`.

---

## What to remove from the existing model

- `Bridge_Test_Set_Test` — replaced by the grain of `Fact_Test`
- `Fact_Test_Run` — replaced by `Fact_Test_Run_History`
- The two `Dim_Issue` role-playing copies (`Dim_Test_Set`, `Dim_Test_Execution`) — no longer needed
- All relationships involving `Bridge_Test_Set_Test` and `Fact_Test_Run`
