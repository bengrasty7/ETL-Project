# Robot Data Analysis Pipeline

## Data Access

First, install the required dependencies found in requirements.txt

There are two ways to access the data using DuckDB. DuckDB is an in-process SQL OLAP database management system. Similar to SQLite but for OLAP queries.
https://duckdb.org/docs/ 


### 1. Using the DuckDB CLI Executable
1. Download DuckDB CLI
Go to the DuckDB Releases Page:
Visit the DuckDB releases page on GitHub.

2. Extract the ZIP File
3. Move the CLI Binary to a Directory in Your PATH
```bash
sudo mv duckdb /usr/local/bin
```
4. Verify Installation
Open Your Command Line Interface:
```bash
duckdb --version
```
You should see the version number of DuckDB if it was installed correctly.
5. Open a DuckDB Database File
Navigate to Your Project Directory:
```bash
duckdb data/robot_data.db
```
This will open the DuckDB CLI with mydatabase.db as the active database.
6. You will now have a DuckDB prompt where you can interact with the database directly. For example:
```sql
SHOW tables;
SELECT * FROM your_table LIMIT 5;
```
There are 2 tables: features, statistics

To exit the DuckDB prompt, type 
```bash
.quit
```
If the executable does not work, install duckdb on your machine, and configure the path such that it has access to the database file

### 2. Using DuckDB in Python
This method allows you to access the database within your Python scripts.

1. Install DuckDB if you haven't already:
```bash
pip install duckdb
```
2. In your Python script, use the following code to connect to the database:
```python
import duckdb

db = duckdb.connect('data/robot_data.db')
```
3. Execute queries on the database:
```python
result = db.execute("SELECT * FROM your_table LIMIT 5")
print(result.fetchall())
```
Close the connection when you're done:
```python
db.close()
```

Both methods allow you to interact with the same database file, so you can choose the approach that best fits your workflow.

### 3. Using Pandas
If you are having trouble with duckdb, each of the 2 tables is located in a csv file in the data/ directory. Simply read in with pandas (pd.read_csv(path)) to explore data

## Pipeline Flow
The pipeline follows a sequential flow from A to D. etl_pipeline.py orchestrates the pipeline.

- Step 0: Investigate the data (see in data/data_investigation.py)
- Step A: Validate the data and clean
- Step B: Format data in wide format
- Step C: Engineer features, like velocity and acceleration
- Step D: Calculate run statistics for each run_uuid

## Time Rounding Strategy
In this project, round time values to the nearest 0.01 seconds (10 milliseconds) when grouping data. This decision was made based on investigation of the time series data for each run_uuid. For the run_uuid's with all fields present, I noticed that sensors would pick up x,y,z values for the encoder and fx,fy,fz for the load cell sequentially, and that generally the 4 sets of values would be populated every 10 milliseconds.

### Pros of this approach:
- Ensures sensor values are near-simultaneous, rather than a lag approach that would assume that the previous value is close in time, where that may not be guaranteed
- Processing speed
- Simplifies data analysis by creating discrete time bins, ensures we don't join after long breaks

### Cons of this approach:
- May lose some temporal precision
- Could potentially group unrelated measurements if the time window is too large
- May miss out on some data quantity if multiple detections per 10 millisecond

Note: I initially attempted to use interpolation, but found it to be computationally expensive and slow for our dataset size. I also attempted binning, but found it much simpler in code to effectively "bin" by rounding.

### Data Exclusion
Run UUIDs with all null values for any relevant dimension for velocity/acceleration I excluded from feature engineering. This ensures that we're only working with meaningful, complete data sets. Without deeper knowledge of sensors and robots, I can't assume a specific coordinate / force if it's not given

### Data Investigation
The data_investigation.py file contains exploratory data analysis and preliminary processing steps. It's recommended to review this file to understand the characteristics and quirks of the dataset before proceeding with the main pipeline.
