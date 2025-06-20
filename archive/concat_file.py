import pandas as pd
import glob
import sys

# フォルダパスを指定
folder_path = './out/monthly/'

# フォルダ内の全てのCSVファイルを読み込み、リストに追加
#all_files = glob.glob(folder_path + "*.csv")

all_files = sys.argv[1:]

# データフレームのリストを作成し、各ファイルを読み込んでリストに追加
dataframes = []
for file in all_files:
    df = pd.read_csv(file)
    dataframes.append(df)

# 全てのデータフレームを結合
combined_df = pd.concat(dataframes)

# 同じ Year_Day を持つ行ごとに Record_Count を合計
result_df = combined_df.groupby('Year_Month', as_index=False)['Record_Count'].sum()

# 結果を新しいファイルに出力
result_df.to_csv(f"{folder_path}"+'all_nagasaki_2019_monthly.csv', index=False)

