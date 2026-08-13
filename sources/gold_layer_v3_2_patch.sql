/* ============================================================================
   GOLD LAYER v3.2 — PATCH
   Change: the type -> level map is now PROJECT-SCOPED, and Workstream is
           resolved from a per-project flag rather than a global level.

   Why: if the workstream sits at a different level depending on the project,
        a single global Config_Hierarchy is wrong — the same issue type would
        resolve to different levels in different projects and every rolled-up
        total would be quietly inconsistent.

   Correction to v2: I said jira.project_issue_types was governance-only and
   could be dropped. That was wrong given this. It is now the seed for the
   hierarchy config.

   Supersedes: v3.1 section 1 and section 5 (the "where does workstream live"
   discovery queries — answered).
   ============================================================================ */


/* ----------------------------------------------------------------------------
   1. Config_Hierarchy — now keyed on (Project_Key, Issue_Type_Name)
   ---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS Gold.gold.Config_Hierarchy;

CREATE TABLE Gold.gold.Config_Hierarchy (
    Project_Key         varchar(20)  NOT NULL,   -- '*' = default for all projects
    Issue_Type_Name     varchar(100) NOT NULL,
    Level_Number        int          NOT NULL,
    Level_Name          varchar(50)  NOT NULL,
    Is_Workstream_Level bit          NOT NULL DEFAULT 0,
    Is_Leaf_Level       bit          NOT NULL DEFAULT 0,
    CONSTRAINT PK_Config_Hierarchy PRIMARY KEY (Project_Key, Issue_Type_Name)
);
GO

/* Seed from Jira's own per-project type configuration, then adjust.
   hierarchy_level here is Jira's: 5=Programme .. 1=Epic, 0=Task, -1=Sub-task */
INSERT INTO Gold.gold.Config_Hierarchy
    (Project_Key, Issue_Type_Name, Level_Number, Level_Name, Is_Workstream_Level, Is_Leaf_Level)
SELECT DISTINCT
    pit.project_key,
    pit.issue_type_name,
    /* map Jira's descending hierarchy_level to our ascending Level_Number */
    CASE pit.hierarchy_level
        WHEN  5 THEN 2      -- Programme
        WHEN  4 THEN 3      -- Release
        WHEN  3 THEN 4      -- Initiative
        WHEN  2 THEN 5
        WHEN  1 THEN 6      -- Epic
        WHEN  0 THEN 7      -- Task / Story / Bug / Risk
        WHEN -1 THEN 8      -- Sub-task
        ELSE 99
    END,
    CASE pit.hierarchy_level
        WHEN  5 THEN 'Programme'
        WHEN  4 THEN 'Release'
        WHEN  3 THEN 'Initiative'
        WHEN  2 THEN 'Level 2'
        WHEN  1 THEN 'Epic'
        WHEN  0 THEN 'Task'
        WHEN -1 THEN 'Sub-task'
        ELSE 'Unmapped'
    END,
    0,                                                    -- set below
    CASE WHEN pit.hierarchy_level = -1 THEN 1 ELSE 0 END
FROM [jira].[project_issue_types] pit;
GO

/* --- Flag the workstream level, per project. ---
   Populate from the discovery query in section 4.
   Exactly ONE row per project should carry Is_Workstream_Level = 1;
   the constraint check in section 4 enforces it. */

-- Example shape (replace with real values once discovery is run):
-- UPDATE Gold.gold.Config_Hierarchy SET Is_Workstream_Level = 1
-- WHERE (Project_Key = 'PSP'  AND Issue_Type_Name = '<type at that project''s workstream level>')
--    OR (Project_Key = 'DGPR' AND Issue_Type_Name = '<type at that project''s workstream level>')
--    OR (Project_Key = 'TRAN' AND Issue_Type_Name = '<...>');
GO


/* ----------------------------------------------------------------------------
   2. Bridge_IssueAncestry — level lookup now joins on project + type

   Each ancestor is resolved using ITS OWN project key, not the descendant's.
   This matters: hierarchies here cross projects (a PSP Programme can parent
   DGPR epics), so using the child's project would mislabel the ancestor.
   ---------------------------------------------------------------------------- */
CREATE OR ALTER VIEW Gold.gold.Bridge_IssueAncestry AS
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
    u.Ancestor_ID                           AS Ancestor_Issue_ID,
    a.[key]                                 AS Ancestor_Key,
    a.fields_summary                        AS Ancestor_Summary,
    a.fields_issuetype_name                 AS Ancestor_Issue_Type,
    a.fields_project_key                    AS Ancestor_Project_Key,
    COALESCE(h.Level_Name, hd.Level_Name, 'Unmapped')    AS Ancestor_Level_Name,
    COALESCE(h.Level_Number, hd.Level_Number, 99)        AS Ancestor_Level_Number,
    COALESCE(h.Is_Workstream_Level, hd.Is_Workstream_Level, 0) AS Is_Workstream_Level,
    u.Steps_Up,
    CASE WHEN u.Steps_Up = 0 THEN 1 ELSE 0 END           AS Is_Self
FROM unpivoted u
JOIN [jira].[issues] a ON TRY_CAST(a.[id] AS bigint) = u.Ancestor_ID
/* project-specific mapping first... */
LEFT JOIN Gold.gold.Config_Hierarchy h
       ON h.Project_Key     = a.fields_project_key
      AND h.Issue_Type_Name = a.fields_issuetype_name
/* ...then the '*' default */
LEFT JOIN Gold.gold.Config_Hierarchy hd
       ON hd.Project_Key     = '*'
      AND hd.Issue_Type_Name = a.fields_issuetype_name;
GO


/* ----------------------------------------------------------------------------
   3. Dim_Issue — Workstream resolved from the flagged ancestor

   Reinstate these two lines in the anc CTE (they were removed in v3.1):
   ---------------------------------------------------------------------------- */
-- In the anc CTE, ADD:
--     MAX(CASE WHEN Is_Workstream_Level = 1 THEN Ancestor_Key END)     AS Workstream_Key,
--     MAX(CASE WHEN Is_Workstream_Level = 1 THEN Ancestor_Summary END) AS Workstream_Name,

-- In the SELECT list, ADD:
--     a.Workstream_Key, a.Workstream_Name,
--     CASE WHEN a.Workstream_Key IS NULL THEN 1 ELSE 0 END AS Has_No_Workstream,

/* Note this is now level-agnostic. It picks whichever ancestor is flagged for
   that ancestor's project, so a workstream at level 1 in one project and
   level 2 in another both resolve correctly into the same column. Dashboard 2
   groups on Workstream_Name and never needs to know the difference.

   fields_workstream stays as Workstream_Text for reconciliation only —
   check (j) in the v3 delta compares the two. */
GO


/* ----------------------------------------------------------------------------
   4. Reinstate Workstream_Key on Fact_IssueDaily
   (v3.1 dropped it; it comes back now)
   ---------------------------------------------------------------------------- */
ALTER TABLE Gold.gold.Fact_IssueDaily ADD Workstream_Key varchar(50) NULL;
GO
-- And restore in the daily INSERT: column list `Workstream_Key,`
--                                  SELECT list `di.Workstream_Key,`


/* ============================================================================
   5. DISCOVERY — run these to populate Is_Workstream_Level
   ============================================================================ */

-- (p) What issue types exist at each level, per project?
--     This is the map you are flagging against.
SELECT project_key, hierarchy_level, issue_type_name, is_subtask
FROM [jira].[project_issue_types]
ORDER BY project_key, hierarchy_level DESC, issue_type_name;

-- (q) What actually sits at the top of each project's tree, and does the
--     depth differ by project? This is the empirical version of (p) —
--     trust it over the configuration if the two disagree.
SELECT i.fields_project_key,
       i.fields_issuetype_name,
       i.fields_issuetype_hierarchyLevel,
       COUNT(*)                                                   AS issues,
       SUM(CASE WHEN i.fields_parent_id IS NULL
                  OR i.fields_parent_id = 'NULL' THEN 1 ELSE 0 END) AS root_issues
FROM [jira].[issues] i
GROUP BY i.fields_project_key, i.fields_issuetype_name, i.fields_issuetype_hierarchyLevel
ORDER BY i.fields_project_key, i.fields_issuetype_hierarchyLevel DESC;

-- (r) Candidate workstreams: the distinct issues at each project's
--     top two levels. Compare against the nine named workstreams
--     (BC, Business Change, Data, Development, DGP Development,
--      Information Governance, Policy, Requirements, Testing).
SELECT i.fields_project_key, i.fields_issuetype_name,
       i.[key], i.fields_summary
FROM [jira].[issues] i
WHERE TRY_CAST(i.fields_issuetype_hierarchyLevel AS int) IN (1, 2)
ORDER BY i.fields_project_key, i.fields_summary;

-- (s) VALIDATION — exactly one workstream level per project.
--     Any row returned means a project will double-count or lose issues.
SELECT Project_Key, COUNT(*) AS flagged_levels
FROM Gold.gold.Config_Hierarchy
WHERE Is_Workstream_Level = 1
GROUP BY Project_Key
HAVING COUNT(*) <> 1;

-- (t) VALIDATION — issues that resolve to no workstream at all.
--     Expect some (Programme and Release sit ABOVE the workstream level,
--     so they legitimately have none). Investigate anything at Epic or below.
SELECT Hierarchy_Level_Name, COUNT(*) AS issues
FROM Gold.gold.Dim_Issue
WHERE Workstream_Key IS NULL
GROUP BY Hierarchy_Level_Name
ORDER BY issues DESC;
