/* ============================================================================
   GOLD LAYER v3 — DELTA ONLY
   Changed and new objects. Everything else from gold_layer_build.sql stands.

   In this file:
     NEW      Config_Hierarchy
     NEW      Bridge_IssueAncestry
     REPLACE  Dim_Issue          (hierarchy + lead/involved columns)
     NEW      Dim_Role, Config_ResourceRole
     REPLACE  Fact_ResourceAllocation
     RENAME   Dim_User -> Dim_Person
     REPLACE  Fact_IssueDaily insert  (release/workstream keys, sprint optional)
     DROP     Dim_Workstream, Config_ReleaseTranche
     CHECKS   (f)-(i)
   ============================================================================ */


/* ============================================================================
   NEW — Config_Hierarchy
   Levels come from HERE, not from Jira's hierarchyLevel numbers.
   A renamed type or a new tier is a row edit, nothing more.
   ============================================================================ */
CREATE TABLE Gold.gold.Config_Hierarchy (
    Issue_Type_Name varchar(100) NOT NULL PRIMARY KEY,
    Level_Number    int          NOT NULL,   -- 2..8 (level 1 = Project, not an issue)
    Level_Name      varchar(50)  NOT NULL,
    Is_Leaf_Level   bit          NOT NULL DEFAULT 0
);

INSERT INTO Gold.gold.Config_Hierarchy VALUES
    ('Programme',  2, 'Programme',  0),
    ('Release',    3, 'Release',    0),
    ('Initiative', 4, 'Initiative', 0),
    ('Workstream', 5, 'Workstream', 0),
    ('Epic',       6, 'Epic',       0),
    ('Story',      7, 'Task',       0),
    ('Task',       7, 'Task',       0),
    ('Bug',        7, 'Task',       0),
    ('Risk',       7, 'Task',       0),
    ('Sub-task',   8, 'Sub-task',   1),
    ('Subtask',    8, 'Sub-task',   1);
-- Add Test Set / Test / Test Execution here only if they belong in the
-- delivery hierarchy. They are better kept OUT of it — the testing hierarchy
-- is a separate structure via Bridge_TestSetTest.
GO


/* ============================================================================
   NEW — Bridge_IssueAncestry
   One row per issue per ancestor, at every level.
   Built with 8 bounded self-joins rather than recursion: Spark SQL has no
   recursive CTE, and the hierarchy depth caps the joins anyway.
   ============================================================================ */
CREATE VIEW Gold.gold.Bridge_IssueAncestry AS
WITH chain AS (
    SELECT
        TRY_CAST(i0.[id] AS bigint)  AS Issue_ID,
        TRY_CAST(i0.[id] AS bigint)  AS A0, TRY_CAST(i1.[id] AS bigint) AS A1,
        TRY_CAST(i2.[id] AS bigint)  AS A2, TRY_CAST(i3.[id] AS bigint) AS A3,
        TRY_CAST(i4.[id] AS bigint)  AS A4, TRY_CAST(i5.[id] AS bigint) AS A5,
        TRY_CAST(i6.[id] AS bigint)  AS A6, TRY_CAST(i7.[id] AS bigint) AS A7
    FROM [jira].[issues] i0
    LEFT JOIN [jira].[issues] i1 ON TRY_CAST(i1.[id] AS bigint) = TRY_CAST(i0.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i2 ON TRY_CAST(i2.[id] AS bigint) = TRY_CAST(i1.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i3 ON TRY_CAST(i3.[id] AS bigint) = TRY_CAST(i2.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i4 ON TRY_CAST(i4.[id] AS bigint) = TRY_CAST(i3.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i5 ON TRY_CAST(i5.[id] AS bigint) = TRY_CAST(i4.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i6 ON TRY_CAST(i6.[id] AS bigint) = TRY_CAST(i5.fields_parent_id AS bigint)
    LEFT JOIN [jira].[issues] i7 ON TRY_CAST(i7.[id] AS bigint) = TRY_CAST(i6.fields_parent_id AS bigint)
),
unpivoted AS (
    SELECT Issue_ID, A0 AS Ancestor_ID, 0 AS Steps_Up FROM chain WHERE A0 IS NOT NULL
    UNION ALL SELECT Issue_ID, A1, 1 FROM chain WHERE A1 IS NOT NULL
    UNION ALL SELECT Issue_ID, A2, 2 FROM chain WHERE A2 IS NOT NULL
    UNION ALL SELECT Issue_ID, A3, 3 FROM chain WHERE A3 IS NOT NULL
    UNION ALL SELECT Issue_ID, A4, 4 FROM chain WHERE A4 IS NOT NULL
    UNION ALL SELECT Issue_ID, A5, 5 FROM chain WHERE A5 IS NOT NULL
    UNION ALL SELECT Issue_ID, A6, 6 FROM chain WHERE A6 IS NOT NULL
    UNION ALL SELECT Issue_ID, A7, 7 FROM chain WHERE A7 IS NOT NULL
)
SELECT
    u.Issue_ID,
    u.Ancestor_ID                       AS Ancestor_Issue_ID,
    a.[key]                             AS Ancestor_Key,
    a.fields_summary                    AS Ancestor_Summary,
    a.fields_issuetype_name             AS Ancestor_Issue_Type,
    COALESCE(h.Level_Name, 'Unmapped')  AS Ancestor_Level_Name,
    COALESCE(h.Level_Number, 99)        AS Ancestor_Level_Number,
    u.Steps_Up,
    CASE WHEN u.Steps_Up = 0 THEN 1 ELSE 0 END AS Is_Self
FROM unpivoted u
JOIN [jira].[issues] a ON TRY_CAST(a.[id] AS bigint) = u.Ancestor_ID
LEFT JOIN Gold.gold.Config_Hierarchy h ON h.Issue_Type_Name = a.fields_issuetype_name;
GO


/* ============================================================================
   REPLACE — Dim_Issue
   Adds: typed ancestor columns (for filtering), dense Level_1..8 (for the
   Gantt only), and Lead / Involved denormalised columns.
   All other columns from gold_layer_build.sql section 2 still apply — this
   shows the hierarchy and resource additions in context.
   ============================================================================ */
CREATE OR ALTER VIEW Gold.gold.Dim_Issue AS
WITH anc AS (
    SELECT Issue_ID,
           /* ---- TYPED: sparse but correct. Use for slicers and measures. ---- */
           MAX(CASE WHEN Ancestor_Level_Name = 'Programme'  THEN Ancestor_Key END)     AS Programme_Key,
           MAX(CASE WHEN Ancestor_Level_Name = 'Programme'  THEN Ancestor_Summary END) AS Programme_Name,
           MAX(CASE WHEN Ancestor_Level_Name = 'Release'    THEN Ancestor_Key END)     AS Release_Key,
           MAX(CASE WHEN Ancestor_Level_Name = 'Release'    THEN Ancestor_Summary END) AS Release_Name,
           MAX(CASE WHEN Ancestor_Level_Name = 'Initiative' THEN Ancestor_Key END)     AS Initiative_Key,
           MAX(CASE WHEN Ancestor_Level_Name = 'Initiative' THEN Ancestor_Summary END) AS Initiative_Name,
           MAX(CASE WHEN Ancestor_Level_Name = 'Workstream' THEN Ancestor_Key END)     AS Workstream_Key,
           MAX(CASE WHEN Ancestor_Level_Name = 'Workstream' THEN Ancestor_Summary END) AS Workstream_Name,
           MAX(CASE WHEN Ancestor_Level_Name = 'Epic'       THEN Ancestor_Key END)     AS Epic_Issue_Key,
           MAX(CASE WHEN Ancestor_Level_Name = 'Epic'       THEN Ancestor_Summary END) AS Epic_Issue_Name,
           MIN(Ancestor_Level_Number)                                                   AS Top_Level_Number,
           MAX(Steps_Up)                                                                AS Depth_From_Root
    FROM Gold.gold.Bridge_IssueAncestry
    GROUP BY Issue_ID
),
dense AS (
    /* ---- DENSE: compacted upward, no gaps. Gantt display ONLY.
            Never use these as filters — a skipped tier shifts everything up
            and would silently reassign issues to the wrong level. ---- */
    SELECT Issue_ID,
           MAX(CASE WHEN rn = 1 THEN Ancestor_Summary END) AS Level_1,
           MAX(CASE WHEN rn = 2 THEN Ancestor_Summary END) AS Level_2,
           MAX(CASE WHEN rn = 3 THEN Ancestor_Summary END) AS Level_3,
           MAX(CASE WHEN rn = 4 THEN Ancestor_Summary END) AS Level_4,
           MAX(CASE WHEN rn = 5 THEN Ancestor_Summary END) AS Level_5,
           MAX(CASE WHEN rn = 6 THEN Ancestor_Summary END) AS Level_6,
           MAX(CASE WHEN rn = 7 THEN Ancestor_Summary END) AS Level_7,
           MAX(CASE WHEN rn = 8 THEN Ancestor_Summary END) AS Level_8
    FROM (
        SELECT Issue_ID, Ancestor_Summary,
               ROW_NUMBER() OVER (PARTITION BY Issue_ID ORDER BY Steps_Up DESC) AS rn
        FROM Gold.gold.Bridge_IssueAncestry
    ) x
    GROUP BY Issue_ID
),
lead_involved AS (
    SELECT r.Issue_ID,
           MAX(CASE WHEN r.Allocation_Role = 'Lead' THEN p.Person_Name END) AS Lead_Name,
           COUNT(CASE WHEN r.Allocation_Role = 'Involved' THEN 1 END)       AS Involved_Count,
           /* Spark equivalent: concat_ws(', ', collect_list(...)) */
           STRING_AGG(CASE WHEN r.Allocation_Role = 'Involved'
                           THEN p.Person_Name END, ', ')                    AS Involved_Names
    FROM Gold.gold.Fact_ResourceAllocation r
    JOIN Gold.gold.Dim_Person p ON p.Account_ID = r.Account_ID
    GROUP BY r.Issue_ID
)
SELECT
    i.[id]                                  AS Issue_ID,
    i.[key]                                 AS Issue_Key,
    i.fields_summary                        AS Summary,
    i.fields_issuetype_name                 AS Issue_Type,
    i.fields_project_key                    AS Project_Key,

    /* ---- hierarchy: typed (filter with these) ---- */
    COALESCE(h.Level_Name, 'Unmapped')      AS Hierarchy_Level_Name,
    COALESCE(h.Level_Number, 99)            AS Hierarchy_Level_Number,
    a.Programme_Key,  a.Programme_Name,
    a.Release_Key,    a.Release_Name,
    a.Initiative_Key, a.Initiative_Name,
    a.Workstream_Key, a.Workstream_Name,
    a.Epic_Issue_Key, a.Epic_Issue_Name,
    a.Depth_From_Root,
    CASE WHEN a.Programme_Key IS NULL THEN 1 ELSE 0 END AS Is_Orphan,  -- doesn't reach a Programme

    /* ---- hierarchy: dense (Gantt display only) ---- */
    d.Level_1, d.Level_2, d.Level_3, d.Level_4,
    d.Level_5, d.Level_6, d.Level_7, d.Level_8,

    /* ---- resourcing: readable columns for the timeline ---- */
    li.Lead_Name,
    li.Involved_Names,
    COALESCE(li.Involved_Count, 0)          AS Involved_Count,
    CASE WHEN li.Lead_Name IS NULL THEN 1 ELSE 0 END AS Has_No_Lead,

    /* ---- fallbacks: text fields superseded by the hierarchy above.
            Keep for reconciliation; do not use as the source of truth. ---- */
    i.fields_workstream                     AS Workstream_Text,
    i.fields_release                        AS Release_Text,

    /* ---- parent / ordering ---- */
    TRY_CAST(i.fields_parent_id AS bigint)  AS Parent_Issue_ID,
    i.fields_parent_key                     AS Parent_Key,
    i.fields_rank                           AS Lexo_Rank,

    /* ---- testing hierarchy (separate structure — see Bridge_TestSetTest) ---- */
    CASE
        WHEN i.fields_issuetype_name LIKE '%Test Plan%'      THEN 'Test Plan'
        WHEN i.fields_issuetype_name LIKE '%Test Set%'       THEN 'Test Set'
        WHEN i.fields_issuetype_name LIKE '%Test Execution%' THEN 'Test Execution'
        WHEN i.fields_issuetype_name LIKE '%Test%'           THEN 'Test'
    END                                     AS Test_Level,

    'https://esshtransform.atlassian.net/browse/' + i.[key] AS Issue_URL

    /* + all remaining attribute columns from gold_layer_build.sql section 2:
         MoSCoW, Complexity, Severity, Category, Source, Supplier, Environment,
         Description, Acceptance_Criteria, User_Story, Mitigation, etc. */

FROM [jira].[issues] i
LEFT JOIN Gold.gold.Config_Hierarchy h ON h.Issue_Type_Name = i.fields_issuetype_name
LEFT JOIN anc            a  ON a.Issue_ID  = TRY_CAST(i.[id] AS bigint)
LEFT JOIN dense          d  ON d.Issue_ID  = TRY_CAST(i.[id] AS bigint)
LEFT JOIN lead_involved  li ON li.Issue_ID = TRY_CAST(i.[id] AS bigint);
GO


/* ============================================================================
   RENAME — Dim_User becomes Dim_Person (thin supporting dimension)
   ============================================================================ */
CREATE OR ALTER VIEW Gold.gold.Dim_Person AS
SELECT u.accountId    AS Account_ID,
       u.displayName  AS Person_Name,
       u.emailAddress AS Email,
       CASE WHEN u.active = 1 THEN 1 ELSE 0 END AS Is_Active
FROM [jira].[users] u
UNION
/* People appearing only in issue_people_involved and not in jira.users */
SELECT DISTINCT p.person_account_id, p.person_name, p.person_email,
       CASE WHEN p.person_active = 1 THEN 1 ELSE 0 END
FROM [jira].[issue_people_involved] p
WHERE p.person_account_id NOT IN (SELECT accountId FROM [jira].[users]);
GO


/* ============================================================================
   NEW — Dim_Role and Config_ResourceRole
   Role is the analytical axis; Dim_Person is supporting cast.
   ============================================================================ */
CREATE TABLE Gold.gold.Dim_Role (
    Role_Name             varchar(50) NOT NULL PRIMARY KEY,
    Role_Description      varchar(200) NULL,
    Is_Lead               bit NOT NULL DEFAULT 0,
    Contributes_To_Effort bit NOT NULL DEFAULT 0,
    Display_Order         int NULL
);

INSERT INTO Gold.gold.Dim_Role VALUES
    ('Lead',     'Owns and is accountable for the item', 1, 1, 1),
    ('Involved', 'Contributes to the item',              0, 0, 2),
    ('Reporter', 'Raised the item',                      0, 0, 3);

/* Where each role comes from. Change the source field HERE if the client
   means Business Owner rather than assignee — no rebuild required. */
CREATE TABLE Gold.gold.Config_ResourceRole (
    Role_Name          varchar(50)  NOT NULL,
    Source_Field       varchar(100) NOT NULL,
    Allocation_Weight  decimal(5,2) NOT NULL,
    Is_Active          bit NOT NULL DEFAULT 1
);

INSERT INTO Gold.gold.Config_ResourceRole VALUES
    ('Lead',     'fields_assignee_accountId',  1.00, 1),
    ('Involved', 'issue_people_involved',      0.00, 1),
    ('Reporter', 'fields_reporter_accountId',  0.00, 0);
-- Candidate alternatives for Lead, if the client means one of these instead:
--   fields_business_owner_value / fields_technical_contact_value / fields_business_contact_value
GO


/* ============================================================================
   REPLACE — Fact_ResourceAllocation
   Grain: issue x person x role.
   Allocation_Weight stops Involved people multiplying effort totals.
   ============================================================================ */
CREATE OR ALTER VIEW Gold.gold.Fact_ResourceAllocation AS
SELECT TRY_CAST(i.[id] AS bigint)   AS Issue_ID,
       i.fields_assignee_accountId  AS Account_ID,
       'Lead'                       AS Allocation_Role,
       1.00                         AS Allocation_Weight
FROM [jira].[issues] i
WHERE i.fields_assignee_accountId IS NOT NULL
  AND i.fields_assignee_accountId <> 'NULL'

UNION ALL

SELECT TRY_CAST(p.issue_id AS bigint),
       p.person_account_id,
       'Involved',
       0.00
FROM [jira].[issue_people_involved] p
WHERE p.person_account_id IS NOT NULL
  AND NOT EXISTS (          -- a Lead is not also counted as Involved
      SELECT 1 FROM [jira].[issues] i2
      WHERE TRY_CAST(i2.[id] AS bigint) = TRY_CAST(p.issue_id AS bigint)
        AND i2.fields_assignee_accountId = p.person_account_id
  );
GO


/* ============================================================================
   REPLACE — Fact_IssueDaily insert
   Sprint now optional; hierarchy keys added so trends roll up Release /
   Workstream / Programme instead of sprints.
   PRIORITY: start this early. Without sprints it is the only trend source,
   and you cannot backfill what was never snapshotted.
   ============================================================================ */
INSERT INTO Gold.gold.Fact_IssueDaily (
    Snapshot_Date_Key, Snapshot_Date, Issue_ID, Status_ID,
    Programme_Key, Release_Key, Workstream_Key,
    Sprint_ID, Lead_Account_ID,
    Story_Points, Remaining_Estimate_Secs, Risk_Total_Score,
    Is_Open, Is_Blocked, In_Scope_Flag
)
SELECT
    CONVERT(int, FORMAT(CAST(GETDATE() AS date), 'yyyyMMdd')),
    CAST(GETDATE() AS date),
    f.Issue_ID,
    f.Status_ID,
    di.Programme_Key,
    di.Release_Key,
    di.Workstream_Key,
    bs.Sprint_ID,                       -- nullable; retained for future use
    f.Assignee_Account_ID,
    f.Story_Points,
    f.Remaining_Estimate_Secs,
    f.Risk_Total_Score,
    f.Is_Open,
    ds.Is_Blocked,
    1
FROM Gold.gold.Fact_Issue f
JOIN Gold.gold.Dim_Issue  di ON di.Issue_ID  = f.Issue_ID
JOIN Gold.gold.Dim_Status ds ON ds.Status_ID = f.Status_ID
OUTER APPLY (
    SELECT TOP 1 Sprint_ID FROM Gold.gold.Bridge_IssueSprint b
    WHERE b.Issue_ID = f.Issue_ID ORDER BY b.Sprint_ID DESC
) bs
WHERE f.Is_Open = 1
   OR CAST(GETDATE() AS date) = EOMONTH(GETDATE());
GO


/* ============================================================================
   DROP — superseded by the issue-type hierarchy
   ============================================================================ */
DROP TABLE IF EXISTS Gold.gold.Dim_Workstream;        -- Workstream is now an issue
DROP TABLE IF EXISTS Gold.gold.Config_ReleaseTranche; -- Release is now an issue
GO


/* ============================================================================
   ADDITIONAL PRE-BUILD CHECKS
   ============================================================================ */

-- (f) Confirm the hierarchy. Are Release and Workstream issue types?
SELECT [name], hierarchyLevel, subtask, [description]
FROM [jira].[issuetypes]
ORDER BY hierarchyLevel DESC, [name];

-- (g) Orphans: issues whose parent chain never reaches a Programme.
--     These fall out of every rolled-up total silently.
SELECT di.Project_Key, di.Issue_Type, COUNT(*) AS orphan_count
FROM Gold.gold.Dim_Issue di
WHERE di.Is_Orphan = 1
GROUP BY di.Project_Key, di.Issue_Type
ORDER BY orphan_count DESC;

-- (h) Actual depth in the data vs the 8 levels assumed.
--     If max_depth > 7, add joins A8+ to Bridge_IssueAncestry.
SELECT MAX(Steps_Up) AS max_depth FROM Gold.gold.Bridge_IssueAncestry;

-- (i) Lead coverage: how much work has no owner, by level?
SELECT Hierarchy_Level_Name,
       COUNT(*)                     AS issues,
       SUM(Has_No_Lead)             AS no_lead,
       SUM(CASE WHEN Involved_Count = 0 THEN 1 ELSE 0 END) AS no_one_involved
FROM Gold.gold.Dim_Issue
GROUP BY Hierarchy_Level_Name
ORDER BY MIN(Hierarchy_Level_Number);

-- (j) Do the text fields agree with the hierarchy? Expect drift.
SELECT Workstream_Name AS from_hierarchy, Workstream_Text AS from_field, COUNT(*) AS issues
FROM Gold.gold.Dim_Issue
WHERE Workstream_Name IS NOT NULL OR Workstream_Text IS NOT NULL
GROUP BY Workstream_Name, Workstream_Text
ORDER BY issues DESC;
