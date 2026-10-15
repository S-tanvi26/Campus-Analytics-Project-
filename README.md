# Campus Analytics Using PySpark
A complete PySpark data pipeline that integrates student, attendance and department budget data, analyses student performance, ranks students within departments, and saves the processed result in Parquet format.

## Problem Statement

A university stores student performance, attendance and departmental budgets in three separate files. This project integrates the datasets, analyses performance, ranks students within each department and saves the processed output as Parquet.

## Project Structure

```
campus_analytics_project/
├── campus_analytics.py                 # Full pipeline (Tasks 1-8)
├── data/
│   ├── students.csv                    # student_id, name, department, city, marks
│   ├── attendance.csv                  # student_id, attendance
│   └── department_budget.json          # department, department_budget
├── output/
│   └── student_analytics_parquet/      # Parquet output (regenerated each run)
├── run_log.txt                         # Console output of a successful run
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.8+
- Java 8 or higher (required by Spark)
- PySpark

## How to Run

```bash
pip install -r requirements.txt
python campus_analytics.py
```

The script prints the result of every task to the console and writes the Parquet output to `output/student_analytics_parquet/`. Running it again is safe because the output is written in `overwrite` mode.

## Configuration

Input paths and column names are defined as constants at the top of `campus_analytics.py`. If your data uses different column names, change them there:

| Constant | Value |
|---|---|
| `STUDENT_ID` | `student_id` |
| `DEPARTMENT` | `department` |
| `MARKS` | `marks` |
| `ATTENDANCE_COL` | `attendance` |
| `BUDGET_COL` | `department_budget` |
| `NAME_COL` | `name` |

---

## Tasks

### Task 1: Load Files and Inspect Schemas (10 marks)

- Created a `SparkSession` (`local[*]`).
- Loaded `students.csv` and `attendance.csv` with `header=True, inferSchema=True`.
- Loaded `department_budget.json` with `multiLine=True` because the file is a JSON array.
- Inspected each DataFrame using `printSchema()`, `count()` and `show()`.
- Cleaned column names (trim and lower-case) so joins do not fail on stray spaces or case differences.

### Task 2: Join Student, Attendance and Budget Datasets (15 marks)

- Joined `students` with `attendance` on `student_id`.
- Joined the result with `department_budget` on `department`.
- Used **left joins** so no student is dropped if a match is missing.
- Averaged attendance per student before joining, so multiple attendance records cannot duplicate student rows.
- **Verification:** an `assert` checks that the row count is unchanged after joining (20 = 20), and unmatched budget/attendance rows are counted (both 0).

### Task 3: Performance Categories and Filtering (15 marks)

Students are classified using `when().otherwise()`:

| Marks | Category |
|---|---|
| 90 and above | Outstanding |
| 80 - 89 | Excellent |
| 70 - 79 | Good |
| Below 70 | Needs Improvement |

Result: Outstanding 4, Excellent 8, Good 6, Needs Improvement 2.
Outstanding and Excellent students are then filtered and displayed.

### Task 4: Department-wise Aggregations (15 marks)

`groupBy("department")` with average marks, maximum marks, student count and average attendance.

| Department | Avg Marks | Max Marks | Students | Avg Attendance |
|---|---|---|---|---|
| CSE | 87.0 | 95 | 8 | 92.88 |
| IT | 82.0 | 94 | 6 | 89.5 |
| ECE | 74.17 | 86 | 6 | 82.83 |

### Task 5: Temporary SQL View and Queries (10 marks)

Created the temporary view `student_analytics` with `createOrReplaceTempView()` and ran three Spark SQL queries:

1. Students scoring at least 85 (`WHERE marks >= 85`): 8 students.
2. Department averages (`GROUP BY department`, `AVG(marks)`).
3. Three highest-scoring students (`ORDER BY marks DESC LIMIT 3`): Advik (95), Navya (94), Ira (93).

### Task 6: Rank Students Using Window Functions (15 marks)

```python
dept_window = Window.partitionBy("department").orderBy(F.col("marks").desc())
ranked_df = categorised_df.withColumn("dept_rank", F.dense_rank().over(dept_window))
```

- `dense_rank()` is used so tied scores share a rank and no student is cut off.
- A `dept_row_number` column is also added for a strict one-row-per-position ranking.
- Top two students per department:

| Department | Rank 1 | Rank 2 |
|---|---|---|
| CSE | Advik (95) | Ira (93) |
| IT | Navya (94) | Aadhya (89) |
| ECE | Anvi (86) | Dhruv (80) |

### Task 7: Save and Verify Output in Parquet (10 marks)

- Saved the processed DataFrame to `output/student_analytics_parquet/` using `write.mode("overwrite").parquet(...)`.
- Read it back with `spark.read.parquet(...)`.
- **Verification:** `assert` statements confirm that the row count (20 written, 20 read) and the columns match.

### Task 8: Code Quality and Verification (10 marks)

- Meaningful variable names: `integrated_df`, `categorised_df`, `department_stats_df`, `ranked_df`.
- No hardcoded analytical answers: every result is calculated from the data.
- Configuration constants at the top of the file.
- Modular structure with `main()` and `if __name__ == "__main__":`.
- Built-in checks: join row-count assertion and Parquet round-trip assertions.
- A successful run is saved in `run_log.txt`.

---

## Pipeline Flow

```
Load 3 files -> Join -> Performance category -> Department aggregations
             -> Spark SQL -> Window ranking -> Save Parquet -> Verify
```

## Technologies Used

- Python
- Apache Spark (PySpark): DataFrame API, Spark SQL, Window functions
- Parquet
