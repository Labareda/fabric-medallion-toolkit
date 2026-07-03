# fabric-medallion-toolkit

Reusable Bronze/Silver/Gold medallion toolkit for Microsoft Fabric. Source-agnostic
wheel package + copy-paste notebook templates for onboarding new sources and
building Gold tables.

## Structure

- **`wheel/`** — the Python package. Build it (see below), upload the resulting
  `.whl` into Fabric.
- **`templates/`** — generic starting points for a new source or a new Gold
  table. Copy, fill in the `REPLACE_*` placeholders, paste into a new Fabric
  notebook.
- **`sources/jira/`** — the first real source, and a worked example of what a
  filled-in template looks like.

## Building the wheel (in a Codespace — no local install needed)

1. This repo → **Code** button → **Codespaces** tab → **Create codespace on main**.
2. In the Codespace terminal:
   ```bash
   cd wheel
   pip install build
   python -m build
   ```
3. In the file explorer (left sidebar), find `wheel/dist/fabric_medallion_toolkit-<version>-py3-none-any.whl`,
   right-click → **Download**.
4. Upload that `.whl` into Fabric: either `Files/libs/` on a Lakehouse (with
   a `%pip install` cell in each notebook), or as a Custom Library on a
   Fabric Environment (recommended — no `%pip install` cell needed anywhere).

## Adding a new source

1. Copy `templates/SOURCE_CONFIG_TEMPLATE.json` → `sources/<name>/<name>.json`, fill in.
2. Copy `templates/B2S_TEMPLATE.py` → new Fabric notebook `B2S - <Name>`, fill in `SOURCE_NAME`.
3. For each Gold table this source feeds: copy the matching `S2G_*_TEMPLATE.py`
   → new Fabric notebook `S2G - <table_name>`, fill in the SQL and key fields.
4. `dim_date` is shared across all sources — only create it once, from
   `templates/S2G_DATE_DIMENSION.py`, regardless of how many sources you add.

## Architecture notes

- **4 Lakehouses**: `Bronze`, `Silver`, `Gold`, `Config`. Bronze/Silver get one
  schema per source (e.g. `jira`); Gold is one shared `gold` schema.
- **Bronze** is append-only (full history). **Silver** is a full recompute
  every run (stateless, since Bronze already has everything). **Gold** uses
  `MERGE`, with a deterministic GUID key derived from each table's
  `merge_fields` — same inputs always produce the same key, no key registry
  needed.
- **SCD2** (`merge_scd2`) is opt-in per dimension — use it where a fact should
  reflect what a record's attributes *were* at the time, not what they are
  today.
