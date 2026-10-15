# Campus Analytics Using PySpark

## Structure
- campus_analytics.py   - full pipeline (Tasks 1-8)
- data/                 - students.csv, attendance.csv, department_budget.json
- output/student_analytics_parquet/ - Parquet output (regenerated on each run)
- requirements.txt

## Run
Requires Java 8+ and Python 3.8+.

    pip install -r requirements.txt
    python campus_analytics.py

The script prints schemas, joins, categories, aggregations, SQL results,
window-function rankings, then writes and verifies the Parquet output.
