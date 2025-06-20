import pandas as pd
import sys
import os
import gzip
#import glob

# ログ出力用の関数
def log_message(message):
    with open("09_log.txt", "a") as log_file:
        log_file.write(message + "\n")


#input_folder = 'data_10/'

# Input directory and output folder
output_M = "./out/monthly"
output_D = "./out/daily"

# List of input files from command-line arguments
input_files = sys.argv[1:]
#input_files = glob.glob(input_folder + "*.csv.gz")
log_message("inputファイル")

#foldername
folder_name = os.path.basename(os.path.dirname(input_files[0]))
log_message("folder_name")

#filename
filename = os.path.splitext(os.path.splitext(os.path.basename(input_files[0]))[0])[0]


# Load and concatenate all files
df = pd.concat([pd.read_csv(file, compression='gzip', parse_dates=['datetime']) for file in input_files])
log_message("dataFrame")

# Sort the data by datetime
df = df.sort_values('datetime')
log_message("sort dataFrame")
# Extract month information
df['year_month'] = df['datetime'].dt.to_period('M')

# Count the number of records for each month
monthly_counts = df['year_month'].value_counts().sort_index()

# Convert the monthly result to a DataFrame and save to CSV
monthly_counts_df = monthly_counts.reset_index()
monthly_counts_df.columns = ['Year_Month', 'Record_Count']
monthly_counts_df.to_csv(f"{output_M}/{filename}_{folder_name}_monthly.csv", index=False)
log_message("monthlyファイル出力")
# Extract day information
df['year_day'] = df['datetime'].dt.to_period('D')

# Count the number of records for each day
daily_counts = df['year_day'].value_counts().sort_index()

# Convert the daily result to a DataFrame and save to CSV
daily_counts_df = daily_counts.reset_index()
daily_counts_df.columns = ['Year_Day', 'Record_Count']
daily_counts_df.to_csv(f"{output_D}/{filename}_{folder_name}_daily.csv", index=False)
log_message("dailyファイル出力")
print("Monthly and daily record counts have been saved with folder name in filenames.")

