import os
import pandas as pd
def count_files_in_directory(directory_path):
    # 指定されたディレクトリ内の全てのファイルをリストし、ファイル数をカウントする
    return sum(
        1 for entry in os.listdir(directory_path)
        if os.path.isfile(os.path.join(directory_path, entry))
    )

# 使用例
directory_path = './../Common/BLWSakigake/03_tokyo_2019b/'  # 調べたいフォルダのパスに変更してください

print(f"Number of files in the directory: {count_files_in_directory(directory_path)}")

