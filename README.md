# Lab 3 -- Big Data Processing

Apache Airflow, locally, on Docker Compose.

## Layout

```
lab3_student/
├── docker-compose.yaml      # Airflow stack: scheduler, webserver, worker, triggerer, postgres, redis
├── Dockerfile               # Custom Airflow image with duckdb + pandas
├── requirements.txt         # Extra Python deps (used by Dockerfile)
├── .env.example             # Copy to .env before first run
├── dags/                    # Your DAGs go here (already mounted into Airflow)
│   ├── example_hello.py     # Read in Part 1 -- do not modify
│   └── broken_dag.py        # Hunt the 5 anti-patterns in Part 4 -- do not run, just read
├── include/                 # Business logic separated from DAGs
│   ├── __init__.py
│   ├── duck.py              # CSV <-> Parquet helpers (use these from your tasks)
│   └── transform.py         # Larger transformation -- used by q4_fixed
├── scripts/                 # Run from your host (NOT inside Docker)
│   └── vendor_drop.py       # Simulates an upstream vendor dropping CSVs
├── data/                    # Bind-mounted into every Airflow container at /opt/airflow/data
│   ├── incoming/            # Vendor drops CSVs here
│   ├── raw/                 # DAGs write per-day Parquet here
│   └── agg/                 # DAGs write daily summaries here
└── tests/                   # Optional, used by bonus task B4
```

## Quick start

```bash
# 1. Copy env template
cp .env.example .env       # Linux / Mac
copy .env.example .env     # Windows PowerShell

# 1b. Linux / macOS only: set AIRFLOW_UID to your numeric uid (Compose does NOT expand $(id -u) inside .env)
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env          # Linux (GNU sed)
# sed -i '' "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env       # macOS — uncomment if needed

# 2. Pull public images and build the lab Airflow image (adds duckdb — required for include/duck.py)
docker compose pull postgres redis
docker compose build

# 3. First-time DB init (only needed once)
docker compose up airflow-init

# 4. Start everything
docker compose up -d

# 5. Open http://localhost:8080  (user: airflow / pass: airflow)
```

## Useful commands

```bash
# Inspect running services
docker compose ps

# Stream logs of one service
docker compose logs -f airflow-scheduler

# Open a shell inside the worker (where your tasks actually run)
docker compose exec airflow-worker bash

# Trigger a backfill (used in Part 3)
docker compose exec airflow-scheduler \
    airflow dags backfill q3_daily -s 2026-05-12 -e 2026-05-16 --reset-dagruns

# Stop everything (keeps data and DB volume)
docker compose stop

# Nuke everything (deletes postgres volume!)
docker compose down -v
```

## Helpers

Helper modules in `include/` are already importable from your tasks as
`from include.duck import csv_to_parquet`.

## Vendor simulator

A small script on the host side simulates an upstream system dropping CSVs:

```bash
python scripts/vendor_drop.py --date 2026-05-19
python scripts/vendor_drop.py --range 2026-05-10:2026-05-19
python scripts/vendor_drop.py --date 2026-05-18 --corrupt
python scripts/vendor_drop.py --date 2026-05-21 --split 3
python scripts/vendor_drop.py --date 2026-05-19 --delay 60   # waits 60s before dropping
python scripts/vendor_drop.py --date 2026-05-23 --slow       # writes in 3 chunks, 10s pauses (Q2.7)
python scripts/vendor_drop.py --date 2026-05-24 --rows 30    # tiny day (Q5.2 branching)
```

The script needs no Airflow knowledge and runs with stdlib only -- no pip install required.

## Troubleshooting

| Symptom | Try this |
|---|---|
| Web UI never loads | `docker compose logs airflow-webserver` -- check for migration errors |
| DAGs do not appear in UI | `docker compose logs airflow-scheduler` -- look for import errors |
| `ModuleNotFoundError: No module named 'duckdb'` | Run `docker compose build` then `docker compose up -d --force-recreate`. Do not set `AIRFLOW_IMAGE_NAME` to plain `apache/airflow` without building. |
| `The conn_id fs_default isn't defined` (FileSensor) | Recreate containers with the current `docker-compose.yaml` (defines `AIRFLOW_CONN_FS_DEFAULT`). Your `filepath` must be relative to that base, e.g. `incoming/transactions_{{ ds }}.csv`. |
| Port 8080 in use | Edit `AIRFLOW_UI_PORT` in `.env`, `docker compose up -d` |
| File-not-found error in a task | Remember: tasks see `/opt/airflow/data`, not your host path |
| Bind-mount empty on Windows | Avoid OneDrive / spaces. Move repo to `C:\dev\lab3` |

Detailed installation instructions in `../Sujet/Setup_TP3.md`.
