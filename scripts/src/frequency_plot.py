import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#plt.rcParams['font.family'] = 'Meiryo'

# CSVファイルを読み込む
df = pd.read_csv(
    '/home/data/fukui/interim/user_counts_weekly/sample_data/output/68users_freq.csv', 
    names=['ID', 'frequency'], 
    header=0
    )


def ccdf(df: pd.DataFrame) -> np.ndarray:
    # print(df.head())
    freq_array = np.array(df['frequency'].value_counts())
    # print(freq_array)
    p_list = []
    cumsum = 0.0
    s = float(freq_array.sum())
    # print(s)
    for i in range(len(freq_array)):
        if i == 0:
            p_list.append(0)
        else:
            p = freq_array[i-1]/s
            cumsum += p
            p_list.append(cumsum)

    # print(p_list)
    ccdf_array = 1 - np.array(p_list)
    if ccdf_array[0] == 0:
        ccdf_array[0] = 1.0
    return ccdf_array

# サブプロットを作成 (1行3列の配置)
fig, ax = plt.subplots(figsize=(8, 6))

ccdf_array = ccdf(df)
frequency_count = df['frequency'].value_counts().sort_index()
ax.scatter(frequency_count.index, ccdf_array, marker='o', color='r', s=5)
ax.set_xlabel('Frequency', fontsize=13)
ax.set_ylabel('Complementary cumulative probability')
ax.set_xscale('log')  # x軸を対数スケールに設定
ax.set_yscale('log')  # y軸を対数スケールに設定
ax.set_xlim(left=10**0)
ax.set_ylim(top=10**0)
ax.grid(True)  # グリッドの表示
ax.set_title('Complementary cumulative probability', fontsize=17)

# レイアウト調整
plt.tight_layout()

# グラフをPNGファイルとして保存
plt.savefig('/home/data/fukui/outputs/figures/nagasaki_2019_68users_ombined_scatter_plots.png')
plt.show()  # 現在の図を閉じる

