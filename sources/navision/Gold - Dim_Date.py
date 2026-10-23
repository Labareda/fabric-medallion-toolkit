# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Date -- Navision calendar
#
# A standalone date dimension for the Navision Gold layer, SEPARATE from the Jira
# one. Jira's Dim_Date runs 2020-01-01 .. 2032-12-31 (the programme timeline);
# Navision's benefits / finance data spans a different, usually wider range -- so
# set START_DATE / END_DATE below to cover every date the Navision facts reference
# (transaction / posting / service dates, etc.), NOT the Jira end date.
#
# Built with the toolkit's date-dimension builder (same as Jira), then given the
# Unknown / sentinel row so a fact with a null or unmatched date still joins.

# CELL ********************

import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.navision"   # set to your Navision Gold lakehouse/schema

# Cover the full span of dates Navision data references -- widen freely, a date
# dimension is cheap (~365 rows/year). Deliberately NOT the Jira end date.
# Jersey's financial year is the calendar year; set fiscal_year_start_month=4
# if you instead report on an April fiscal year.
START_DATE = "1990-01-01"
END_DATE   = "2035-12-31"

# MARKDOWN ********************

# ##### Build the calendar

# CELL ********************

fmt.build_date_dimension(
    spark,
    fmt.DateDimensionConfig(
        table_name=f"{GOLD_SCHEMA}.dim_date",
        start_date=START_DATE,
        end_date=END_DATE,
        fiscal_year_start_month=1,
    ),
)

# MARKDOWN ********************

# ##### Sentinel row (for null / unmatched fact dates)

# CELL ********************

fmt.add_date_dimension_sentinel(spark, f"{GOLD_SCHEMA}.dim_date", sentinel_date="1900-01-01")
print("Dim_Date (Navision) built successfully")

# MARKDOWN ********************

# ##### Jersey bank-holiday + working-day flags
#
# Adds is_holiday / is_working_day (working = not weekend AND not a bank holiday).
# JERSEY (Channel Islands) holidays: the England & Wales set PLUS Liberation Day
# (9 May, Jersey-only), with the weekend "substitute day" rule, the moveable
# Easter pair via Computus, and the Crown-wide one-offs (Jubilee / Funeral /
# Coronation). Edit the list if the calendar changes.

# CELL ********************

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
    d = date(y, month, 1); return d + timedelta((0 - d.weekday()) % 7)

def _last_monday(y, month):
    nxt = date(y + 1, 1, 1) if month == 12 else date(y, month + 1, 1)
    d = nxt - timedelta(1); return d - timedelta((d.weekday() - 0) % 7)

ONE_OFFS = {date(2022, 6, 2), date(2022, 6, 3),   # Platinum Jubilee
            date(2022, 9, 19),                     # State Funeral
            date(2023, 5, 8)}                      # Coronation

def jersey_holidays(y0, y1):
    hols = set()
    for y in range(y0, y1 + 1):
        taken = set()
        def observe(dt):
            while dt.weekday() >= 5 or dt in taken:   # bump weekend/clash to next weekday
                dt += timedelta(1)
            taken.add(dt); return dt
        hols.add(observe(date(y, 1, 1)))    # New Year's Day
        hols.add(observe(date(y, 12, 25)))  # Christmas Day
        hols.add(observe(date(y, 12, 26)))  # Boxing Day
        e = _easter(y)
        hols.add(e - timedelta(2))          # Good Friday
        hols.add(e + timedelta(1))          # Easter Monday
        hols.add(_first_monday(y, 5))       # Early May
        hols.add(observe(date(y, 5, 9)))    # Liberation Day (Jersey)
        hols.add(_last_monday(y, 5))        # Spring
        hols.add(_last_monday(y, 8))        # Summer
    return sorted(hols | ONE_OFFS)

_y0, _y1 = int(START_DATE[:4]), int(END_DATE[:4])
hol_df = (spark.createDataFrame([(d.isoformat(),) for d in jersey_holidays(_y0, _y1)], ["hol"])
          .select(F.to_date("hol").alias("hol_date")))

dd = spark.table(f"{GOLD_SCHEMA}.dim_date")
dd = (dd.join(F.broadcast(hol_df), dd["date"] == F.col("hol_date"), "left")
        .withColumn("is_holiday", F.col("hol_date").isNotNull())
        .withColumn("is_working_day", (~F.col("is_weekend")) & F.col("hol_date").isNull())
        .drop("hol_date"))
# Spark forbids overwriting a Delta table you're also reading, so stage via a
# temp table and swap it in (same pattern as the Jira Dim_Date).
_tmp = f"{GOLD_SCHEMA}.dim_date_holidays_tmp"
dd.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_tmp)
(spark.table(_tmp).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{GOLD_SCHEMA}.dim_date"))
spark.sql(f"DROP TABLE IF EXISTS {_tmp}")
print("Dim_Date (Navision): added is_holiday / is_working_day (Jersey)")
