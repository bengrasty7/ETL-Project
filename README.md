# Robot Data Analysis Pipeline

## Data Access

First, install the required dependencies found in requirements.txt

There are two ways to access the data using DuckDB:

### 1. Using the DuckDB CLI Executable

This method allows you to interact with the database directly from the command line.

1. Open a terminal and navigate to the root directory of the project.

2. Run the DuckDB executable with the database file:
```bash
./bin/duckdb data/robot_data.db
```
3. You will now have a DuckDB prompt where you can interact with the database directly. For example:
```sql
SHOW tables;
SELECT * FROM your_table LIMIT 5;
```

To exit the DuckDB prompt, type .quit

### 2. Using DuckDB in Python
This method allows you to access the database within your Python scripts.

1. Install DuckDB if you haven't already:
pip install duckdb

2. In your Python script, use the following code to connect to the database:
```python
import duckdb

db = duckdb.connect('data/robot_data.db')
```
Execute queries on the database:
```python
result = db.execute("SELECT * FROM your_table LIMIT 5")
print(result.fetchall())
```
Close the connection when you're done:
```python
db.close()
```

Both methods allow you to interact with the same database file, so you can choose the approach that best fits your workflow.
## Repository Structure
MACHINA ETL PROJECT
├── bin
│   └── duckdb
├── data
│   ├── data_investigation.py
│   ├── robot_data.db
│   └── sample.parquet
├── src
│   ├── __init__.py
│   ├── A_data_cleaning.py
│   ├── B_wide_formatting.py
│   ├── C_engineer_features.py
│   ├── D_calculate_run_statistics.py
│   ├── etl_pipeline.py
│   └── utils.py
├── README.md
└── requirements.txt
## Pipeline Flow
The pipeline follows a sequential flow from A to D:

- Step 0: Investigate the data (see in data/data_investigation.py)
- Step A: Validate the data and clean
- Step B: Format data in wide format
- Step C: Engineer features, like velocity and acceleration
- Step D: Calculate run statistics for each run_uuid

## Time Rounding Strategy
In this project, round time values to the nearest 0.01 seconds (10 milliseconds) when grouping data. This decision was made based on the understanding that for these robots, I can't confidently say that position/force values occur simultaneously unless they're within a reasonable time frame.

### Pros of this approach:
- Ensures sensor values are near-simultaneous
- Processing speed
- Simplifies data analysis by creating discrete time bins, ensures we don't join after long breaks

### Cons of this approach:
- May lose some temporal precision
- Could potentially group unrelated measurements if the time window is too large
- May miss out on some data quantity if multiple detections per 10 millisecond

Note: I initially attempted to use interpolation, but found it to be computationally expensive and slow for our dataset size.

### Data Exclusion
Run UUIDs with all null values for any dimension I excluded from feature engineering. This ensures that we're only working with meaningful, complete data sets. Without deeper knowledge of sensors and robots, I can't assume a specific coordinate / force if it's not given

### Data Investigation
The data_investigation.py file contains exploratory data analysis and preliminary processing steps. It's recommended to review this file to understand the characteristics and quirks of the dataset before proceeding with the main pipeline.
