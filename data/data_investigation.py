import pandas as pd
import sys
import numpy as np

df = pd.read_parquet('/Users/bengrasty/Documents/Machina ETL Project/data/sample.parquet')

print("Dataframe shape: ", df.shape)

# Check for null values
num_nulls_time = df['time'].isnull().sum()
num_nulls_value = df['value'].isnull().sum()
num_nulls_field = df['field'].isnull().sum()
num_nulls_robot_id = df['robot_id'].isnull().sum()
num_nulls_run_uuid = df['run_uuid'].isnull().sum()
num_nulls_sensor_type = df['sensor_type'].isnull().sum()

print(f'Number of null values in time: {num_nulls_time}')
print(f'Number of null values in value: {num_nulls_value}')
print(f'Number of null values in field: {num_nulls_field}')
print(f'Number of null values in robot_id: {num_nulls_robot_id}')
print(f'Number of null values in run_uuid: {num_nulls_run_uuid}')
print(f'Number of null values in sensor_type: {num_nulls_sensor_type}')


# Check unique values
unique_values_field = df['field'].unique()
unique_values_robot_id = df['robot_id'].unique()
unique_values_run_uuid = df['run_uuid'].unique()
unique_values_sensor_type = df['sensor_type'].unique()

print(f'Unique values in field: {unique_values_field}')
print(f'Unique values in robot_id: {unique_values_robot_id}')
print(f'Unique values in run_uuid: {unique_values_run_uuid}')
print(f'Unique values in sensor_type: {unique_values_sensor_type}')

# Check for outliers (any value greater than 3 standard deviations from the mean)
encoder_df = df[df['sensor_type'] == 'encoder']
encoder_mean = encoder_df['value'].mean()
encoder_std = encoder_df['value'].std()
encoder_max = encoder_df['value'].max()
encoder_min = encoder_df['value'].min()
encoder_outliers = encoder_df[(encoder_df['value'] > encoder_mean + 3 * encoder_std) | (encoder_df['value'] < encoder_mean - 3 * encoder_std)]
print(f'Encoder mean: {encoder_mean}')
print(f'Encoder std: {encoder_std}')
print(f'Encoder max: {encoder_max}')
print(f'Encoder min: {encoder_min}')
print(f'Outliers in encoder sensor type: {encoder_outliers}')

load_cell_df = df[df['sensor_type'] == 'load_cell']
load_cell_mean = load_cell_df['value'].mean()
load_cell_std = load_cell_df['value'].std()
load_cell_max = load_cell_df['value'].max()
load_cell_min = load_cell_df['value'].min()
load_cell_outliers = load_cell_df[(load_cell_df['value'] > load_cell_mean + 3 * load_cell_std) | (load_cell_df['value'] < load_cell_mean - 3 * load_cell_std)]

print(f'Load cell mean: {load_cell_mean}')
print(f'Load cell std: {load_cell_std}')
print(f'Load cell max: {load_cell_max}')
print(f'Load cell min: {load_cell_min}')
print(f'Outliers in load cell sensor type: {load_cell_outliers}')

# Check for outliers specifically in the encoder sensor type for field x
encoder_x_df = encoder_df[encoder_df['field'] == 'x']
encoder_x_mean = encoder_x_df['value'].mean()
encoder_x_std = encoder_x_df['value'].std()
encoder_x_max = encoder_x_df['value'].max()
encoder_x_min = encoder_x_df['value'].min()
encoder_x_outliers = encoder_x_df[(encoder_x_df['value'] > encoder_x_mean + 3 * encoder_x_std) | (encoder_x_df['value'] < encoder_x_mean - 3 * encoder_x_std)]

print(f'Encoder x mean: {encoder_x_mean}')
print(f'Encoder x std: {encoder_x_std}')
print(f'Encoder x max: {encoder_x_max}')
print(f'Encoder x min: {encoder_x_min}')
print(f'Outliers in encoder x field: {encoder_x_outliers}')

encoder_y_df = encoder_df[encoder_df['field'] == 'y']
encoder_y_mean = encoder_y_df['value'].mean()
encoder_y_std = encoder_y_df['value'].std()
encoder_y_max = encoder_y_df['value'].max()
encoder_y_min = encoder_y_df['value'].min()
encoder_y_outliers = encoder_y_df[(encoder_y_df['value'] > encoder_y_mean + 3 * encoder_y_std) | (encoder_y_df['value'] < encoder_y_mean - 3 * encoder_y_std)]

print(f'Encoder y mean: {encoder_y_mean}')
print(f'Encoder y std: {encoder_y_std}')
print(f'Encoder y max: {encoder_y_max}')
print(f'Encoder y min: {encoder_y_min}')
print(f'Outliers in encoder y field: {encoder_y_outliers}')

encoder_z_df = encoder_df[encoder_df['field'] == 'z']
encoder_z_mean = encoder_z_df['value'].mean()
encoder_z_std = encoder_z_df['value'].std()
encoder_z_max = encoder_z_df['value'].max()
encoder_z_min = encoder_z_df['value'].min()
encoder_z_outliers = encoder_z_df[(encoder_z_df['value'] > encoder_z_mean + 3 * encoder_z_std) | (encoder_z_df['value'] < encoder_z_mean - 3 * encoder_z_std)]

print(f'Encoder z mean: {encoder_z_mean}')
print(f'Encoder z std: {encoder_z_std}')
print(f'Encoder z max: {encoder_z_max}')
print(f'Encoder z min: {encoder_z_min}')
print(f'Outliers in encoder z field: {encoder_z_outliers}')


# Check for outliers specifically in the load cell sensor type for field fx
load_cell_fx_df = load_cell_df[load_cell_df['field'] == 'fx']
load_cell_fx_mean = load_cell_fx_df['value'].mean()
load_cell_fx_std = load_cell_fx_df['value'].std()
load_cell_fx_max = load_cell_fx_df['value'].max()
load_cell_fx_min = load_cell_fx_df['value'].min()
load_cell_fx_outliers = load_cell_fx_df[(load_cell_fx_df['value'] > load_cell_fx_mean + 3 * load_cell_fx_std) | (load_cell_fx_df['value'] < load_cell_fx_mean - 3 * load_cell_fx_std)]

print(f'Load cell fx mean: {load_cell_fx_mean}')
print(f'Load cell fx std: {load_cell_fx_std}')
print(f'Load cell fx max: {load_cell_fx_max}')
print(f'Load cell fx min: {load_cell_fx_min}')
print(f'Outliers in load cell fx field: {load_cell_fx_outliers}')

load_cell_fy_df = load_cell_df[load_cell_df['field'] == 'fy']
load_cell_fy_mean = load_cell_fy_df['value'].mean()
load_cell_fy_std = load_cell_fy_df['value'].std()
load_cell_fy_max = load_cell_fy_df['value'].max()
load_cell_fy_min = load_cell_fy_df['value'].min()
load_cell_fy_outliers = load_cell_fy_df[(load_cell_fy_df['value'] > load_cell_fy_mean + 3 * load_cell_fy_std) | (load_cell_fy_df['value'] < load_cell_fy_mean - 3 * load_cell_fy_std)]

print(f'Load cell fy mean: {load_cell_fy_mean}')
print(f'Load cell fy std: {load_cell_fy_std}')
print(f'Load cell fy max: {load_cell_fy_max}')
print(f'Load cell fy min: {load_cell_fy_min}')
print(f'Outliers in load cell fy field: {load_cell_fy_outliers}')

load_cell_fz_df = load_cell_df[load_cell_df['field'] == 'fz']
load_cell_fz_mean = load_cell_fz_df['value'].mean()
load_cell_fz_std = load_cell_fz_df['value'].std()
load_cell_fz_max = load_cell_fz_df['value'].max()
load_cell_fz_min = load_cell_fz_df['value'].min()
load_cell_fz_outliers = load_cell_fz_df[(load_cell_fz_df['value'] > load_cell_fz_mean + 3 * load_cell_fz_std) | (load_cell_fz_df['value'] < load_cell_fz_mean - 3 * load_cell_fz_std)]

print(f'Load cell fz mean: {load_cell_fz_mean}')
print(f'Load cell fz std: {load_cell_fz_std}')
print(f'Load cell fz max: {load_cell_fz_max}')
print(f'Load cell fz min: {load_cell_fz_min}')
print(f'Outliers in load cell fz field: {load_cell_fz_outliers}')

# Check for valid times
valid_times = pd.to_datetime(df['time'], errors='coerce')
num_invalid_times = valid_times.isnull().sum()
print(f'Number of invalid times: {num_invalid_times}')

# show the first 5 invalid times
invalid_times = df[valid_times.isnull()]
print(invalid_times.head())

# show the distinct lenghs of the invalid times
invalid_times_unique_lengths = invalid_times['time'].apply(lambda x: len(str(x))).unique()
print(f'Invalid times lengths: {invalid_times_unique_lengths}') # only length 20

# show 5 valid times
valid_times = df[valid_times.notnull()]
print(valid_times.head())

# print the first 5 run_uuids to check for proper formatting
print(df['run_uuid'].head())

# print min and max dates, extracting the time from the datetime object
# min_date = [pd.to_datetime(dt).dt.date for dt in df['time'].sort_values()][0]
# max_date = [pd.to_datetime(dt).dt.date for dt in df['time'].sort_values()][-1]
df['time'] = pd.to_datetime(df['time'], errors='coerce')
valid_times_df = df[df['time'].notnull()]
min_date = valid_times_df['time'].dt.date.min()
max_date = valid_times_df['time'].dt.date.max()
print(f'Min date: {min_date}')
print(f'Max date: {max_date}')

# sort time values and check average time between values
sorted_times = valid_times_df['time'].sort_values()
time_diffs = sorted_times.diff().dt.total_seconds()
avg_time_diff = time_diffs.mean()
print(f'Average time difference between values: {avg_time_diff}')
print(f'average time difference in milliseconds: {avg_time_diff * 1000}')

# group valid times by unique timestamp and keep unique values
time_groups = valid_times_df.groupby('time').nunique()

# print average time diff between unique timestamps
time_diffs = time_groups.index.to_series().diff().dt.total_seconds()
avg_time_diff = time_diffs.mean()
print(f'Average time difference between unique timestamps: {avg_time_diff}')

# print standard deviation of time differences
std_time_diff = time_diffs.std()
print(f'Standard deviation of time differences: {std_time_diff}')

# print average time diff between timestamps for each run_uuid
time_groups = valid_times_df.groupby(['run_uuid', 'time']).nunique()

time_groups = time_groups.reset_index()
# Sort values by 'run_uuid' and 'time'
time_groups = time_groups.sort_values(by=['run_uuid', 'time'])

# Calculate time differences
time_groups['time_diff'] = time_groups.groupby('run_uuid')['time'].diff()
# Compute the average time difference per run_uuid
average_time_diff = time_groups.groupby('run_uuid')['time_diff'].mean()

# Display the result
print(average_time_diff)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# view 50 rows for each run_uuid
for run_uuid in df['run_uuid'].unique():
    run_uuid_df = df[df['run_uuid'] == run_uuid]
    print(f'Run UUID: {run_uuid}')
    # print all unique field values
    print(f'Unique field values: {run_uuid_df["field"].unique()}')

    # print all unique robot_id values
    print(f'Unique robot_id values: {run_uuid_df["robot_id"].unique()}')

    # look at robot 2
    robot_2_df = run_uuid_df[run_uuid_df['robot_id'] == 2]
    robot_2_df_z = robot_2_df[robot_2_df['field'] == 'z']
    print(robot_2_df_z['value'].unique()) 

    # head
    pivoted_df = run_uuid_df[['time', 'value', 'field', 'run_uuid']].pivot_table(values='value', index='time', columns='field', aggfunc='first')
    print(pivoted_df.head(50))