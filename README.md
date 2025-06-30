# README of TravelModeEstimation

## 概要
- `/home/data/Common/` 配下にある、データについて～
- 前処理して得られたデータから、`列名1`、`列名2`を用いて交通手段を推定する。

## ロードマップ
<!-- もう少しfeatureブランチを細かく切った方が良い気もしています -->
- [ ] 記述統計による欠損値などの把握と処理方針の決定 (feature/00001-scaffold-clean-core)
    - [ ] 日時列の記述統計、可視化を行う関数の実装
- [ ] 

## 進捗
### 2025-06-05
- [x] `/home/data/fukui/interim/`の配下を`agg_before_filter`,`filter`,`filtered`に設定した
- [x] shell scriptを使ってlogsの配下に指定するフォルダを作れた

### 2025-06-04
- [] `/home/data/fukui/interim`配下の`old_user_counts_weekly`と`user_counts_weekly`の競合
- [x] `/home/data/fukui/`配下の`out`を消したい（実行中のため実行が終わり次第）
- [] `/home/data/fukui/interim/user_counts_weekly/sampledata`配下の整理

<details>
  <summary>←をクリックして以前のログを表示</summary>

### 2025-06-03
- [x] `/home/data/fukui/codefile`から`/home/fukui/workspace/TravelModeEstimation`へのコード移植、コードのリファクタ
- [x] `/home/data/fukui/` 配下のデータ見直し
  - [x] `{value}_{monthly|weekly|daily}/`という命名規則に従って`interim`を整理

<details>
<summary>←をクリックして詳細を表示</summary>
- 対応が必要なこと
  - `/home/data/fukui/interim`配下の`old_user_counts_weekly`と`user_counts_weekly`の競合
  - `/home/data/fukui/interim/user_counts_weekly/sampledata`配下の整理
</details>

</details>


---

## 方針

### コーディング規約

- データがそんなに大きくないので、`メモリ効率` より `可読性` の高い処理を優先する。
- `メモリ効率`の高い処理は、文字通り可読性よりもメモリ効率を優先した処理を指し、例えばpandasでは再帰代入（recursive query/function）が挙げられる。
- `可読性`の高い処理は、メモリ効率よりも可読性を優先した処理を指し、例えばpandasではquery methodとmethod chainingが挙げられる。
- なお、どちらの処理においても余計なコピーや参照エラーが発生しやすく可読性の低いBoolean Indexingや推奨されないchaining(chained indexing/assignment、あるいはhidden chaining)、mutatingといった処理は極力使わない。

## 構成
<details>
<summary>←をクリックしてツリーを展開</summary>
<pre>
├───notebooks
│       01_descriptive_statics.py
│
├───src
│   ├───clean_core
│   │       __init__.py
│   │
│   └───descriptive_statics
│       │   describe.py
│       │   missing.py
│       │   plot_missing.py
│       │   query_patterns.py
│       │   __init__.py
│       │
│       └───__pycache__
│
└───tests
        test_n01_descriptive_statics.py
</pre>
</details>

## 環境

| 言語・フレームワーク | バージョン |
| -------------------- | ---------- |
| Python               | 3.12.10     |

## 