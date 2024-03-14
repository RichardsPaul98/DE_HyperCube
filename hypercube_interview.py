import pandas as pd
import json

# Read the bmrs_wind_forecast_pair via pandas
bmrs_df = pd.read_csv("bmrs_wind_forecast_pair.csv")
# bmrs_df = bmrs_df.apply(lambda row: row['EFA'] + '' + row['Unnamed: 12'] if pd.notnull(row['Unnamed: 12']) else row['EFA'], axis=1)

# 1. Assessing the data quality of and cleaning ``bmrs_wind_forecast_pair.csv`` and imputing ``NULL`` values for numerical columns
bmrs_df['EFA'].fillna(bmrs_df['Unnamed: 12'], inplace=True)
bmrs_df.drop(["Unnamed: 12", "Column1"], axis=1, inplace=True)
bmrs_df["fuelTypeGeneration"].fillna(0, inplace=True)
bmrs_df["initialForecastSpnGeneration"] = pd.to_numeric(bmrs_df["initialForecastSpnGeneration"], errors='coerce')

# columns_with_number_datatype = bmrs_df.select_dtypes(include=['number']).columns
columns_with_number_datatype = ["initialForecastSpnGeneration", "latestForecastSpnGeneration"]
print(columns_with_number_datatype)

bmrs_df.interpolate(method='nearest', axis=0, limit_direction='both', subset=columns_with_number_datatype, inplace=True)

# Displaying the result after data cleaning
print(bmrs_df)
print(bmrs_df["initialForecastSpnGeneration"].isnull().any().any())
print(bmrs_df["latestForecastSpnGeneration"].isnull().any().any())
print(bmrs_df.isnull().any())
print(bmrs_df[bmrs_df["initialForecastSpnGeneration"].isnull()]["initialForecastSpnGeneration"])
print(bmrs_df)

# 3. Build the feature engineering transformation for a simple time series forecasting model:
#    - for the `initialForecastSpnGeneration` and `ExecutedVolume` fields calculate rolling medians over a 6-hour window
#    - build a daily and weekly aggregate view of the data
# Load the JSON data
with open('linear_orders_raw.json', 'r') as file:
    linear_order_data = json.load(file)

linear_order_data_df = pd.DataFrame(linear_order_data["result"]["records"])
print(linear_order_data_df)

#rolling medians over a 6-hour window
linear_order_data_df['OrderEntryTime'] = pd.to_datetime(linear_order_data_df['OrderEntryTime'])
linear_order_data_df.set_index('OrderEntryTime', inplace=True)
rolling_median = linear_order_data_df['ExecutedVolume'].rolling(window=360).median()
print(rolling_median)

#Daily and weekly aggregate view of the data
daily_agg = linear_order_data_df['ExecutedVolume'].resample('D').sum()  # Daily sum
weekly_agg = linear_order_data_df['ExecutedVolume'].resample('W').sum() 
print(daily_agg)
print(weekly_agg)

#rolling medians over a 6-hour window
bmrs_df['startTimeOfHalfHrPeriod'] = pd.to_datetime(bmrs_df['startTimeOfHalfHrPeriod'])
bmrs_df.set_index('startTimeOfHalfHrPeriod', inplace=True)
rolling_median_bmrs_df = bmrs_df['initialForecastSpnGeneration'].rolling(window=360).median()
print(rolling_median_bmrs_df)

#Daily and weekly aggregate view of the data
daily_agg_bmrs_df = bmrs_df['initialForecastSpnGeneration'].resample('D').sum()  # Daily sum
weekly_agg_bmrs_df = bmrs_df['initialForecastSpnGeneration'].resample('W').sum() 
print(daily_agg_bmrs_df)
print(weekly_agg_bmrs_df)

# 2. Joining bmrs_wind_forecast_pair.csv to ``linear_orders_raw.json`` on ``DeliveryStart``
bmrs_df['outTurnPublishingPeriodCommencingTime'] = pd.to_datetime(bmrs_df['outTurnPublishingPeriodCommencingTime'], format='%d/%m/%Y %H:%M')
linear_order_data_df['DeliveryStart'] = pd.to_datetime(linear_order_data_df['DeliveryStart'])
merged_df = pd.merge(bmrs_df, linear_order_data_df, left_on='outTurnPublishingPeriodCommencingTime', right_on='DeliveryStart')
print(merged_df)

# Automated the SQL DDL Query Generation for ER Diagram.
print(merged_df.dtypes)
columns = []
for col, dtype in merged_df.dtypes.iteritems():
    if dtype == 'object':
        col_type = 'VARCHAR(255)'
    elif dtype == 'int64':
        col_type = 'INT'
    elif dtype == 'float64':
        col_type = 'FLOAT'
    elif dtype == 'datetime64[ns]':
        col_type = 'TIMESTAMP'
    else:
        col_type = 'VARCHAR(255)'  # Default to VARCHAR for unknown types
    columns.append(f'{col} {col_type}')
columns_str = ',\n'.join(columns)
sql = f'CREATE TABLE base_joined_table (\n{columns_str}\n);'
print(sql)