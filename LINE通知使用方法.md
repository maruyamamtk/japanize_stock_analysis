# LINE通知システム 使用方法

## 概要
日本株分析の銘柄変動をLINEで通知するシンプルなシステムです。

## セットアップ

### 1. LINE Bot作成
1. [LINE Developers Console](https://developers.line.biz/console/) でMessaging APIチャンネル作成
2. Channel Access Token発行
3. 作成したBotを友達追加してUser IDを取得

### 2. 設定ファイル更新
`config.json`を以下のように設定：
```json
{
  "notification": {
    "line_enabled": true,
    "line_channel_access_token": "あなたのアクセストークン",
    "line_user_id": "あなたのユーザーID"
  }
}
```

### 3. ライブラリインストール
```bash
pip install line-bot-sdk>=3.0.0
```

## 使用方法

### バッチファイルから実行
```bash
run_unified.bat
```
- **7. LINE通知テスト**: 設定確認とテスト送信
- **8. 銘柄変動通知実行**: 手動で変動チェック

### 直接実行
```bash
# テスト送信
python line_test_simple.py

# 銘柄変動通知
python line_notifier.py

# 特定日指定
python line_notifier.py 2025-06-22
```

## 通知内容
- 新規追加された銘柄の情報
- 削除された銘柄の情報
- 銘柄コード、企業名、セクター、分類

## ファイル構成
```
├── line_notifier.py          # メイン通知システム
├── line_test_simple.py       # テスト用スクリプト
└── config.json              # 設定ファイル
```

## 注意事項
- LINE Bot SDKが必要
- Channel Access TokenとUser IDの両方が必要
- 無料プランは月1,000通まで