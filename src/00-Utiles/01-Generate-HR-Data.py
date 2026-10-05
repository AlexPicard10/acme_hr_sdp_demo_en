# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Synthetic HR data generator — notebook ("HR 360" SDP demo)
# MAGIC
# MAGIC Synthetic HR data generator, designed to run **directly in the workspace** (Git folder) —
# MAGIC training material.
# MAGIC
# MAGIC - Configuration **widgets** (`entity`, `mode`, `employees`, `catalog`, `schema`, `bad_records`).
# MAGIC - **Two subsidiaries**: `entity = FR` (ACME France) writes to the `landing_fr` Volume, `entity = BE`
# MAGIC   (ACME Belgium) to `landing_be`. Same file structure, distinct identifiers
# MAGIC   (`EMPFR…` / `EMPBE…`). Run the notebook once per subsidiary (e.g. FR 50,000, BE 10,000 employees).
# MAGIC - Writes the JSONL extracts **directly to the UC Volume** (`.../landing_xx/employees|absences/`).
# MAGIC - Persists **each subsidiary's roster in its Volume** (`.../landing_xx/_state/roster.json`) to keep
# MAGIC   identifiers stable across runs → required for CDC / SCD (`increment` mode).
# MAGIC - Each extract contains a few **intra-file duplicates**: ~2% of employees have 2 or 3 rows
# MAGIC   (intermediate versions corrected during the day, timestamped by `updated_at`). This feeds the
# MAGIC   "latest row per employee" example (`ROW_NUMBER` window function).
# MAGIC
# MAGIC > Prerequisite: the schema + the `landing_fr` / `landing_be` Volumes exist (see `00-MasterData-build.py`).
# MAGIC > The pipelines read `employees/` and `absences/` from each Volume; the `_state/` folder is not ingested.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters (widgets)

# COMMAND ----------

dbutils.widgets.dropdown("entity", "FR", ["FR", "BE"], "Subsidiary (FR = ACME France, BE = ACME Belgium)")
dbutils.widgets.dropdown("mode", "seed", ["seed", "increment"], "Mode")
dbutils.widgets.text("employees", "50000", "Number of employees (seed)")
dbutils.widgets.text("catalog", "alp_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "acme_hr_en", "Schema")
# Inject a few deliberately non-compliant records to showcase the silver Expectations
# (on by default for the demo; set to "no" for clean extracts).
dbutils.widgets.dropdown("bad_records", "yes", ["no", "yes"], "Inject non-compliant records?")

entity = dbutils.widgets.get("entity")
mode = dbutils.widgets.get("mode")
n_employees = int(dbutils.widgets.get("employees"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
inject_bad = dbutils.widgets.get("bad_records") == "yes"

# Each subsidiary has its own "bucket" (UC Volume) and its own identifier prefix.
VOLUME_ROOT = f"/Volumes/{catalog}/{schema}/landing_{entity.lower()}"
STATE_PATH = f"{VOLUME_ROOT}/_state/roster.json"
ID_PREFIX = f"EMP{entity}"  # EMPFR0000001 / EMPBE0000001
print(f"entity={entity} | mode={mode} | employees={n_employees} | target={VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md ## Generation logic (Faker with a local locale per subsidiary, fixed seed)

# COMMAND ----------

import datetime as dt
import json
import os
import random

from faker import Faker

DEPARTMENT_IDS = [f"D{n:03d}" for n in range(1, 13)]  # D001..D012 (see 00-MasterData-build.py)
CONTRACT_TYPES = ["Permanent", "Permanent", "Permanent", "Fixed-term", "Apprentice", "Intern", "Temp"]  # Permanent over-weighted
JOB_TITLES = [
    "Technician", "Engineer", "Project Manager", "Analyst", "Manager",
    "Account Manager", "Administrator", "Director", "Consultant", "Operator",
]
WORK_LOCATIONS = {
    "FR": ["Paris La Défense", "Lyon", "Lille", "Bordeaux", "Marseille", "Strasbourg", "Toulouse"],
    "BE": ["Brussels", "Antwerp", "Liège", "Ghent", "Namur"],
}[entity]
# Share of employees appearing several times in the same extract (intra-day corrections).
INTRA_FILE_DUP_RATE = 0.02
ABSENCE_TYPES = [
    ("Paid leave", 30), ("Sick leave", 25), ("Time off in lieu", 20),
    ("Training", 12), ("Maternity leave", 5), ("Unpaid leave", 8),
]
# Free-text note entered by the employee / manager on ~30% of absences (unstructured text).
ABSENCE_COMMENT_RATE = 0.3
ABSENCE_COMMENTS = {
    "Sick leave": [
        "Flu, sick note from the doctor", "Stomach bug", "Throat infection with fever",
        "Severe migraine", "Positive Covid test, isolating", "Scheduled knee surgery",
        "Back pain after a day of manual handling on site",
        "Fell down the stairs in building B during a shift",
        "Burnout, 2 weeks off", "Stress due to workload",
    ],
    "Paid leave": [
        "Family holiday", "Trip abroad", "Sister's wedding",
        "Moving house", "Long weekend", "School holidays with the kids",
    ],
    "Time off in lieu": [
        "Appointment with the notary", "Childcare, school closed", "Admin errands",
        "Recovery after weekend on-call duty", "Medical appointment",
    ],
    "Training": [
        "Electrical safety certification", "Fire safety training", "Databricks certification",
        "Team management training", "Annual leadership seminar", "Business English course",
    ],
    "Maternity leave": ["Baby due in June", "Maternity leave", "Prenatal leave"],
    "Unpaid leave": [
        "Personal round-the-world trip", "Caring for a sick parent",
        "Starting a business", "Sabbatical year",
    ],
}

# Person names stay realistic for each country; all other values are in English.
fake = Faker({"FR": "fr_FR", "BE": "fr_BE"}[entity])
SEED = {"FR": 42, "BE": 43}[entity]  # different seed per subsidiary: distinct data
Faker.seed(SEED)
random.seed(SEED)


def _birth_hire_dates():
    birth = fake.date_of_birth(minimum_age=18, maximum_age=63)
    earliest_hire = birth + dt.timedelta(days=18 * 365)
    hire = fake.date_between(start_date=earliest_hire, end_date="today")
    return birth.isoformat(), hire.isoformat()


def build_roster(n):
    roster = []
    for i in range(1, n + 1):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, hire_date = _birth_hire_dates()
        roster.append({
            "employee_id": f"{ID_PREFIX}{i:07d}",
            "first_name": first, "last_name": last, "gender": gender,
            "birth_date": birth_date, "hire_date": hire_date,
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_id": f"{ID_PREFIX}{random.randint(1, max(1, n // 20)):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": random.choice([1.0, 1.0, 1.0, 0.8, 0.5]),
            "status": "Active",
        })
    return roster


def mutate_roster(roster):
    """Small changes for the incremental extract (showcases CDC/SCD)."""
    for emp in random.sample(roster, k=max(1, len(roster) * 3 // 100)):        # ~3% transfers
        emp["department_id"] = random.choice(DEPARTMENT_IDS)
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):            # ~1% promotions
        emp["job_title"] = random.choice(JOB_TITLES)
    for emp in random.sample(roster, k=max(1, len(roster) // 200)):            # ~0.5% leavers
        emp["status"] = "Inactive"
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):            # Fixed-term/Temp -> Permanent
        if emp["contract_type"] in ("Fixed-term", "Temp"):
            emp["contract_type"] = "Permanent"
    next_id = max(int(e["employee_id"][len(ID_PREFIX):]) for e in roster) + 1  # new hires
    for j in range(random.randint(5, 20)):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, _ = _birth_hire_dates()
        roster.append({
            "employee_id": f"{ID_PREFIX}{next_id + j:07d}",
            "first_name": first, "last_name": last, "gender": gender,
            "birth_date": birth_date, "hire_date": dt.date.today().isoformat(),
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_id": f"{ID_PREFIX}{random.randint(1, len(roster) // 20):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": 1.0, "status": "Active",
        })
    return roster


def employee_extract(roster, extract_date, extract_ts):
    """One master record per employee, plus a few intra-file duplicates.

    - `extract_date`: business day of the extract (DATE, for display).
    - `extract_ts`  : exact extraction instant (TIMESTAMP), identical for the whole file.
      It orders extracts (two extracts produced on the SAME day stay ordered).
    - `updated_at`  : when the row was modified in the HRIS. It orders the versions of one
      employee WITHIN a file. CDC sequence key = STRUCT(extract_ts, updated_at).

    ~2% of employees get 1 or 2 intermediate versions (wrong job title or department, corrected
    later in the day) before their final version, which has the latest `updated_at` and matches
    the roster.
    """
    base_ts = dt.datetime.fromisoformat(extract_ts)
    records = []
    for emp in roster:
        final = dict(emp, extract_date=extract_date, extract_ts=extract_ts, updated_at=extract_ts)
        if random.random() < INTRA_FILE_DUP_RATE:
            for hours_before in range(random.randint(1, 2), 0, -1):  # oldest first
                draft = dict(final, updated_at=(base_ts - dt.timedelta(hours=hours_before))
                             .isoformat(timespec="microseconds"))
                if random.random() < 0.5:
                    draft["department_id"] = random.choice([d for d in DEPARTMENT_IDS if d != emp["department_id"]])
                else:
                    draft["job_title"] = random.choice([j for j in JOB_TITLES if j != emp["job_title"]])
                records.append(draft)
        records.append(final)
    return records


def absence_events(roster, start, end, n_events, run_tag):
    events, active = [], [e for e in roster if e["status"] == "Active"]
    labels = [t for t, _ in ABSENCE_TYPES]
    weights = [w for _, w in ABSENCE_TYPES]
    for k in range(n_events):
        emp = random.choice(active)
        s = fake.date_between(start_date=start, end_date=end)
        atype = random.choices(labels, weights=weights)[0]
        days = {
            "Paid leave": random.randint(1, 15), "Sick leave": random.randint(1, 10),
            "Time off in lieu": 1, "Training": random.randint(1, 5),
            "Maternity leave": random.randint(60, 120), "Unpaid leave": random.randint(1, 20),
        }[atype]
        e = s + dt.timedelta(days=days - 1)
        events.append({
            "absence_id": f"ABS-{entity}-{run_tag}-{k:07d}",  # unique per subsidiary and per run
            "employee_id": emp["employee_id"],
            "absence_type": atype,
            "start_date": s.isoformat(),
            "end_date": e.isoformat(),
            "days": days,
            "event_timestamp": dt.datetime.combine(s, dt.time(8, 0)).isoformat(),
            # Free-text note on ~30% of absences (otherwise None).
            "comment": (random.choice(ABSENCE_COMMENTS[atype])
                        if random.random() < ABSENCE_COMMENT_RATE else None),
        })
    return events


# COMMAND ----------

# MAGIC %md ## Non-compliant records (to showcase Expectations)
# MAGIC Each row violates **one** silver constraint, so the data quality metrics fill up
# MAGIC (`DROP ROW` rows discarded, `warn` constraints kept but counted).
# MAGIC
# MAGIC > We **do not** inject violations of the `FAIL UPDATE` constraints (`valid_employee_id` for
# MAGIC > employees, `valid_absence_id` for absences): they would **fail the whole pipeline**, which is
# MAGIC > not the goal here. To demo a blocking failure, inject one deliberately and separately.

# COMMAND ----------

def bad_employees(extract_date, extract_ts):
    """6 deliberately non-compliant employees (one violated constraint per row)."""
    today = dt.date.today()
    base = {
        "first_name": "Test", "last_name": "Quality", "gender": "F",
        "department_id": "D001", "job_title": "Analyst", "contract_type": "Permanent",
        "work_location": WORK_LOCATIONS[0], "manager_id": f"{ID_PREFIX}0000001",
        "email": "bad.record@acme-demo.com", "fte": 1.0, "status": "Active",
        "extract_date": extract_date, "extract_ts": extract_ts, "updated_at": extract_ts,
    }
    return [
        # plausible_age (too young, ~10 years old) -> DROP ROW
        {**base, "employee_id": f"{ID_PREFIX}9990001",
         "birth_date": (today - dt.timedelta(days=10 * 365)).isoformat(), "hire_date": today.isoformat()},
        # plausible_age (too old, ~81; born in 1945 so valid_birth_date stays OK) -> DROP ROW
        {**base, "employee_id": f"{ID_PREFIX}9990002", "birth_date": "1945-01-01", "hire_date": "1965-01-01"},
        # valid_birth_date (birth_date NULL) -> DROP ROW
        # (a date before 1940 would ALSO trigger plausible_age; NULL isolates the constraint)
        {**base, "employee_id": f"{ID_PREFIX}9990003", "birth_date": None, "hire_date": "1955-01-01"},
        # valid_hire_date (hired before being born) -> DROP ROW
        {**base, "employee_id": f"{ID_PREFIX}9990004", "birth_date": "1990-01-01", "hire_date": "1985-01-01"},
        # known_department (unknown department -> department_name NULL after the join) -> WARN
        {**base, "employee_id": f"{ID_PREFIX}9990005", "birth_date": "1985-01-01", "hire_date": "2010-01-01",
         "department_id": "D999"},
        # valid_contract (contract type outside the list) -> WARN
        {**base, "employee_id": f"{ID_PREFIX}9990006", "birth_date": "1985-01-01", "hire_date": "2010-01-01",
         "contract_type": "Freelance"},
    ]


def bad_absences():
    """4 deliberately non-compliant absences (except valid_absence_id, which is FAIL UPDATE)."""
    y = dt.date.today().year
    base = {"employee_id": f"{ID_PREFIX}0000001", "absence_type": "Sick leave",
            "event_timestamp": dt.datetime.now().isoformat()}
    return [
        # valid_employee (employee_id NULL) -> DROP ROW
        {**base, "absence_id": f"ABS-BAD-{entity}-{y}-00000001", "employee_id": None,
         "start_date": f"{y}-01-10", "end_date": f"{y}-01-12", "days": 3},
        # valid_dates (end_date before start_date) -> DROP ROW
        {**base, "absence_id": f"ABS-BAD-{entity}-{y}-00000002",
         "start_date": f"{y}-01-20", "end_date": f"{y}-01-10", "days": 3},
        # positive_days (days = 0) -> DROP ROW
        {**base, "absence_id": f"ABS-BAD-{entity}-{y}-00000003",
         "start_date": f"{y}-01-10", "end_date": f"{y}-01-10", "days": 0},
        # known_type (absence_type NULL) -> WARN
        {**base, "absence_id": f"ABS-BAD-{entity}-{y}-00000004", "absence_type": None,
         "start_date": f"{y}-01-10", "end_date": f"{y}-01-12", "days": 3},
    ]


# COMMAND ----------

# MAGIC %md ## I/O — direct write to the UC Volume (no SDK upload)

# COMMAND ----------

def write_jsonl(records, dataset, filename):
    target_dir = f"{VOLUME_ROOT}/{dataset}"
    os.makedirs(target_dir, exist_ok=True)  # UC Volumes are writable (FUSE)
    path = f"{target_dir}/{filename}"
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"  ✓ {len(records):>6} rows -> {path}")
    return path


def save_state(roster):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(roster, f)


def load_state():
    if not os.path.exists(STATE_PATH):
        raise SystemExit("No roster in the Volume. Run the notebook in 'seed' mode first.")
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
today = dt.date.today().isoformat()
extract_ts = dt.datetime.now().isoformat(timespec="microseconds")  # CDC sequence key (monotonic)

if mode == "seed":
    print(f"[SEED] {n_employees} employees + absence history (12 months)…")
    roster = build_roster(n_employees)
    save_state(roster)
    emp_records = employee_extract(roster, today, extract_ts)
    abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=365), dt.date.today(),
                                 n_employees * 3, ts)
else:  # increment
    print("[INCREMENT] New employee extract (changes) + new absences…")
    roster = mutate_roster(load_state())
    save_state(roster)
    emp_records = employee_extract(roster, today, extract_ts)
    abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=14), dt.date.today(), 200, ts)

# Optional: a few non-compliant records to showcase the silver Expectations.
if inject_bad:
    bad_emp, bad_abs = bad_employees(today, extract_ts), bad_absences()
    emp_records += bad_emp
    abs_records += bad_abs
    print(f"[BAD RECORDS] +{len(bad_emp)} employees and +{len(bad_abs)} absences injected (non-compliant).")

# Consistent file names: <dataset>_<mode>_<ts>.json (ts already contains the date).
write_jsonl(emp_records, "employees", f"employees_{mode}_{ts}.json")
write_jsonl(abs_records, "absences", f"absences_{mode}_{ts}.json")

n_dups = len(emp_records) - len({r["employee_id"] for r in emp_records})
print(f"[{entity}] {n_dups} employee rows are intra-file duplicates (intermediate versions).")
print("\nDone. Re-run the pipelines to ingest the new files (Auto Loader).")

# COMMAND ----------

# MAGIC %md ## Quick check of the Volume content

# COMMAND ----------

for ds in ("employees", "absences"):
    print(f"== {ds} ==")
    for f in dbutils.fs.ls(f"{VOLUME_ROOT}/{ds}"):
        print(f"  {f.name}\t{f.size} bytes")
