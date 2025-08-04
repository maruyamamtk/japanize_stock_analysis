# 日本株分析統合システム v3.0

## 概要

J-Quants APIを使用した日本株のデータ収集・分析システムです。
Polarsベースの高速データ処理と銘柄変動のLINE通知機能を搭載した統合システムです。

## ファイル構造

```
日本株分析/
├── core/                           # コアモジュール
│   ├── __init__.py                 # モジュール初期化
│   ├── config.py                   # 統合設定管理
│   ├── utilities.py                # 共通ユーティリティ
│   ├── data_manager.py             # データ管理エンジン
│   └── analysis_engine.py          # 日本株分析エンジン
├── unified_main.py                 # メインアプリケーション
├── line_notifier.py                # LINE通知システム
├── line_test_simple.py             # LINE通知テスト
├── run_unified.bat                 # 実行スクリプト
├── requirements_unified.txt        # 依存関係
├── config.json                     # 設定ファイル
├── agg_data/                       # 分析結果データ
│   └── {日付}/                     # 日付別データ
│       ├── consolidated_target_metrics.csv
│       ├── eps_annual.csv
│       ├── eps_quarter.csv
│       ├── netsales_annual.csv
│       ├── netsales_quarter.csv
│       └── roe_annual.csv
├── agg_data_windows/               # Windows用データ
└── data/                           # 基本データディレクトリ
    ├── listed_companies.csv        # 上場企業一覧
    ├── stock_price/                # 株価データ
    └── finance/                    # 財務データ
```

## 主要機能

### 1. データ収集機能
- **株価データ取得**: J-Quants APIから株価データを取得
- **財務データ取得**: 企業の財務情報を取得
- **営業日判定**: 日本の祝日を考慮した自動判定
- **差分・一括取得**: 効率的なデータ更新

### 2. 分析機能
- **年次・四半期業績データ処理**: 決算データの抽出と加工
- **EPS成長率計算**: 年次・四半期EPSの成長率算出
- **売上高成長率計算**: 年次・四半期売上高の成長率算出
- **ROE計算**: 自己資本利益率の算出
- **銘柄フィルタリング**: 複数条件による優良銘柄抽出

### 3. LINE通知機能
- **銘柄変動通知**: 新規追加・削除された銘柄をLINE通知
- **日次比較**: 前日との銘柄リスト比較
- **詳細情報**: 銘柄コード、企業名、セクター、分類を通知

## 設定ファイル（config.json）

```json
{
  "output_directory": "データ出力先ディレクトリ",
  "api_settings": {
    "base_url": "https://api.jquants.com/v1",
    "rate_limit_delay": 0.1,
    "retry_attempts": 3
  },
  "data_processing": {
    "skip_weekends": true,
    "include_adjustment_data": true,
    "default_encoding": "utf-8-sig",
    "use_compression": false
  },
  "notification": {
    "line_enabled": true,
    "line_channel_access_token": "LINEチャネルアクセストークン",
    "line_user_id": "LINEユーザーID",
    "notify_no_changes": false,
    "notification_schedule": {
      "enabled": true,
      "time": "09:00",
      "weekdays_only": true
    }
  }
}
```

## 使用方法

### 1. 環境準備

```bash
# 依存パッケージのインストール
pip install -r requirements_unified.txt
```

### 2. 実行方法

#### バッチファイルでの実行（推奨）
```bash
run_unified.bat
```

実行メニュー:
1. データ収集（差分取得）
2. データ収集（一括取得）
3. 株価データ取得のみ（差分）
4. 株価データ取得のみ（一括）
5. 財務データ取得のみ（一括）
6. 分析のみ（現在無効）
7. **LINE通知テスト**
8. **銘柄変動通知実行**
9. カスタム実行

#### コマンドラインでの実行
```bash
# データ収集（差分取得）
python unified_main.py --mode data --data-mode incremental-stock

# データ収集（一括取得）
python unified_main.py --mode data --data-mode all

# 分析エンジンの実行
python unified_main.py --mode analysis --top-n 50

# LINE通知テスト
python line_test_simple.py

# 銘柄変動通知（今日の日付で実行）
python line_notifier.py

# 銘柄変動通知（指定日で実行）
python line_notifier.py 2025-06-22
```

## データモード

| データモード | 説明 |
|-------------|------|
| `incremental-stock` | 株価データ差分取得（営業日判定付き） |
| `bulk-stock` | 株価データ一括取得 |
| `bulk-finance` | 財務データ一括取得 |
| `all` | 全データ一括取得 |

## LINE通知の設定

### 1. LINE Developersでの設定
1. [LINE Developers Console](https://developers.line.biz/console/) でMessaging APIチャンネル作成
2. Channel Access Token発行
3. 作成したBotを友達追加してUser IDを取得

### 2. config.jsonの設定
```json
{
  "notification": {
    "line_enabled": true,
    "line_channel_access_token": "あなたのアクセストークン",
    "line_user_id": "あなたのユーザーID"
  }
}
```

### 3. 通知内容例
```
📊 日本株分析 - 銘柄変動通知

📅 2025-06-21 → 2025-06-22

🆕 新規追加 (1銘柄):
• 97090 ＮＣＳ＆Ａ
  [情報通信・サービスその他] EPS条件のみ

🕐 2025-06-22 14:30:15
```

### 分析エンジンの処理内容
1. **年次業績データの抽出**: 連結年次決算データの抽出・処理
2. **四半期業績データの抽出**: 四半期決算データの抽出・処理
3. **EPS成長率の計算**: 年次・四半期EPSの前期比成長率計算
4. **売上高成長率の計算**: 年次・四半期売上高の前期比成長率計算
5. **ROE計算**: 自己資本利益率の算出
6. **優良銘柄の抽出**: 複数条件による銘柄フィルタリング
7. **結果保存**: CSV形式での分析結果出力

### 4. 出力されるファイル
分析実行後、以下のファイルが生成されます：

## 分析結果データ

### 出力ファイル（./agg_data/{日付}/）
- `consolidated_target_metrics.csv`: 統合指標データ（注目銘柄・EPS条件のみ銘柄）
- `eps_annual.csv`: 年次EPS成長率データ
- `eps_quarter.csv`: 四半期EPS成長率データ
- `netsales_annual.csv`: 年次売上高成長率データ
- `netsales_quarter.csv`: 四半期売上高成長率データ
- `roe_annual.csv`: ROEデータ

### 出力ファイル（./agg_data_windows/{日付}/）
- Windows互換の文字エンコーディング（Shift_JIS BOM付き）で同様のファイル

### 基本データファイル（./data/）
- `listed_companies.csv`: 上場企業一覧
- `stock_price/stock_data.csv`: 株価データ
- `finance/finance_data.csv`: 財務データ

## 技術仕様

### 使用技術
- **Python 3.8+**
- **Polars**: 高速データ処理
- **Requests**: HTTP通信
- **LINE Bot SDK**: LINE通知
- **J-Quants API**: データソース

### 特徴
- **Polarsベース**: 高速データ処理とメモリ効率向上
- **複数エンコーディング対応**: 日本語CSV読み込みの高い互換性
- **営業日判定**: 日本の祝日を考慮した自動判定
- **LINE通知**: 銘柄変動の自動通知
- **エラー安全性**: 堅牢なCSV読み込みとデータ型処理

## 銘柄フィルタリング条件

### EPS条件
- 直近3年の年次EPSがプラス
- 各年25%以上の成長率
- 直近3四半期のEPSがプラス、25%以上成長、単調増加

### 売上条件
- 直近3四半期の売上成長率が増加
- または直近四半期で25%以上成長

### ROE条件
- 直近1年のROEが15%超

### 注目銘柄
- 全条件を満たす銘柄: 「注目銘柄」
- EPS条件のみ: 「EPS条件のみ」

## ログとエラー処理

- 実行ログは`app.log`に記録
- エラー情報の詳細記録
- CSV読み込みエラーの自動回復
- LINE通知エラーの適切な処理

## 注意事項

- J-Quants APIの利用には認証が必要
- LINE通知には適切なトークンとユーザーIDが必要
- データ処理には十分なメモリ容量が必要
- 営業日以外の実行では最新データが取得できない場合があります

---

**日本株分析統合システム v3.0**  
データ収集・分析・通知統合版