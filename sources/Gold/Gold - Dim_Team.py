# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Team
# Built from the team fields on the issues themselves -- there is no
# Silver.jira.teams entity, because Jira's Team field is an Atlassian
# platform object rather than a Jira one and is not in the REST extract.
# A SELECT DISTINCT off the issue rows is therefore the only source, and is
# legitimate HERE in a way it was not for Complexity/Severity: Team is a real
# organisational entity that a report groups and slices by, not just a value
# on an issue.
#
# MERGED ON Team_Name, not Team_Id. Some issues carry a name with no id
# (the field was set before the team object existed, or the team was later
# deleted from the platform), and keying on the id would drop those rows
# entirely. Name is what the report groups by anyway. Team_Id is carried as
# an attribute for tracing back to Jira.
#
# NOTE Team is NOT the same as Workstream. A team can serve several
# workstreams and a workstream can draw on several teams -- keep them as
# separate slicers rather than assuming one implies the other.
#
# EXPECT LOW COVERAGE. The team field was sparsely populated in the sample.
# Check how many issues actually resolve to a team before building a
# team-based dashboard:
#
#   SELECT COUNT(*) AS issues,
#          COUNT(fields_team_name) AS with_team
#   FROM Silver.jira.issues
#
# If with_team is a small fraction, a "delivery by team" visual will look
# broken rather than sparse -- Dim_Resource + Dim_Role is the better
# grouping until the field is filled in.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_team",
    table_type="dim",
    key_column="Team_Key",
    columns={
        "Team_Name":  {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Team_Id":    {"type": "string", "default": "Unknown"},
        "Team_Title": {"type": "string", "default": "Unknown"},
    },
)

# CELL ********************
# MIN(team_id) rather than a plain DISTINCT on both columns: if the same team
# name has ever appeared against two ids, a two-column DISTINCT would emit two
# rows for one team and the merge would fail its uniqueness check on Team_Name.
df = spark.sql("""
    SELECT
        i.fields_team_name              AS Team_Name,
        MIN(i.fields_team_id)           AS Team_Id,
        MIN(COALESCE(i.fields_team_title, i.fields_team_name)) AS Team_Title
    FROM Silver.jira.issues i
    WHERE i.fields_team_name IS NOT NULL
      AND TRIM(i.fields_team_name) <> ''
    GROUP BY i.fields_team_name
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Team built successfully")
