# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Date -- one calendar, eight roles
# Built by the wheel, then given the Unknown/sentinel row so fact rows with a
# null date still join. EVERY date relationship in the model points here:
# Created, Planned Start, Planned End, Rollup Start, Rollup End, Actual End,
# Snapshot, Worklog Started. One active, the rest inactive + USERELATIONSHIP.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
# Range starts before the earliest Jira created date and runs well past the
# latest target end -- widen if the programme extends. Fiscal year = calendar
# year here; set fiscal_year_start_month=4 if the client reports on an April FY.
fmt.build_date_dimension(
    spark,
    fmt.DateDimensionConfig(
        table_name=f"{GOLD_SCHEMA}.dim_date",
        start_date="2020-01-01",
        end_date="2032-12-31",
        fiscal_year_start_month=1,
    ),
)

# CELL ********************
# Sentinel row. Fact_Issue deliberately keeps NULL dates (see its notes), so
# this is for the few facts that DO need a guaranteed match rather than a blank.
fmt.add_date_dimension_sentinel(spark, f"{GOLD_SCHEMA}.dim_date", sentinel_date="1900-01-01")

# CELL ********************
# --- UK (England & Wales) bank-holiday + working-day flags ------------------
# The wheel builds Dim_Date without a holiday calendar, so add one here. This
# makes "working days" (excl. weekends AND bank holidays) available downstream
# -- e.g. Fact_Issue_History.Working_Days_In_Period. Holidays are COMPUTED for
# the whole range rather than hard-coded: the fixed dates (with the weekend
# "substitute day" rule), the moveable Easter pair via Computus, the
# fixed-Monday holidays, plus the known one-offs (Jubilee/Funeral/Coronation).
# These are England & Wales dates; Scotland/NI differ -- change the list below
# if the programme reports on a different nation's calendar.
from datetime import date, timedelta
from pyspark.sql import functions as F

def _easter(y):
    a = y % 19; b = y // 100; c = y % 100; d = b // 4; e = b % 4
    f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i = c // 4; k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7; m = (a + 11 * h + 22 * l) // 451
    mo = (h + l - 7 * m + 114) // 31; da = ((h + l - 7 * m + 114) % 31) + 1
    return date(y, mo, da)

def _first_monday(y, month):
    d = date(y, month, 1)
    return d + timedelta((0 - d.weekday()) % 7)

def _last_monday(y, month):
    nxt = date(y + 1, 1, 1) if month == 12 else date(y, month + 1, 1)
    d = nxt - timedelta(1)
    return d - timedelta((d.weekday() - 0) % 7)

ONE_OFFS = {date(2022, 6, 2), date(2022, 6, 3),   # Platinum Jubilee
            date(2022, 9, 19),                     # State Funeral
            date(2023, 5, 8)}                      # Coronation

def uk_holidays(y0, y1):
    hols = set()
    for y in range(y0, y1 + 1):
        taken = set()
        def observe(dt):
            while dt.weekday() >= 5 or dt in taken:  # bump weekend/clash to next weekday
                dt += timedelta(1)
            taken.add(dt); return dt
        hols.add(observe(date(y, 1, 1)))    # New Year's Day
        hols.add(observe(date(y, 12, 25)))  # Christmas Day
        hols.add(observe(date(y, 12, 26)))  # Boxing Day
        e = _easter(y)
        hols.add(e - timedelta(2))          # Good Friday
        hols.add(e + timedelta(1))          # Easter Monday
        hols.add(_first_monday(y, 5))       # Early May
        hols.add(_last_monday(y, 5))        # Spring
        hols.add(_last_monday(y, 8))        # Summer (Eng & Wales)
    return sorted(hols | ONE_OFFS)

hol_df = (spark.createDataFrame([(d.isoformat(),) for d in uk_holidays(2020, 2032)], ["hol"])
          .select(F.to_date("hol").alias("hol_date")))

dd = spark.table(f"{GOLD_SCHEMA}.dim_date")
dd = (dd.join(F.broadcast(hol_df), dd["date"] == F.col("hol_date"), "left")
        .withColumn("is_holiday", F.col("hol_date").isNotNull())
        .withColumn("is_working_day", (~F.col("is_weekend")) & F.col("hol_date").isNull())
        .drop("hol_date"))
# Spark forbids overwriting a Delta table you're also reading, so stage via a
# temp table (which has no lineage back to dim_date) and swap it in.
_tmp = f"{GOLD_SCHEMA}.dim_date_holidays_tmp"
dd.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_tmp)
(spark.table(_tmp).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{GOLD_SCHEMA}.dim_date"))
spark.sql(f"DROP TABLE IF EXISTS {_tmp}")
print("Dim_Date: added is_holiday / is_working_day")

# CELL ********************
print("Dim_Date built successfully")
