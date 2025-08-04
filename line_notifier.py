#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
シンプルなLINE通知システム
日本株分析の銘柄変動通知専用
"""

import json
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

try:
    # v2 APIを試す（安定版）
    from linebot import LineBotApi
    from linebot.models import TextSendMessage
    from linebot.exceptions import LineBotApiError
    USE_V3_API = False
    print("LINE Bot SDK v2 API を使用します")
except ImportError:
    try:
        # v3 APIにフォールバック
        from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, TextMessage, PushMessageRequest
        from linebot.v3.exceptions import ApiException
        USE_V3_API = True
        print("LINE Bot SDK v3 API を使用します")
    except ImportError:
        print("エラー: line-bot-sdk がインストールされていません")
        print("pip install line-bot-sdk>=3.0.0 でインストールしてください")
        exit(1)


class LineNotifier:
    """シンプルなLINE通知クラス"""
    
    def __init__(self, config_path: str = "config.json"):
        """初期化"""
        self.config = self._load_config(config_path)
        
        # LINE設定
        notification_config = self.config.get('notification', {})
        self.channel_access_token = notification_config.get('line_channel_access_token')
        self.user_id = notification_config.get('line_user_id')
        self.enabled = notification_config.get('line_enabled', False)
        
        # LINE Bot API初期化
        self.messaging_api = None
        self.line_bot_api = None
        
        if self.enabled and self.channel_access_token:
            if USE_V3_API:
                # v3 API使用
                configuration = Configuration(access_token=self.channel_access_token)
                api_client = ApiClient(configuration)
                self.messaging_api = MessagingApi(api_client)
            else:
                # v2 API使用
                self.line_bot_api = LineBotApi(self.channel_access_token)
    
    def _load_config(self, config_path: str) -> dict:
        """設定ファイル読み込み"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"設定ファイル読み込みエラー: {e}")
            return {}
    
    def send_message(self, message: str) -> bool:
        """メッセージ送信"""
        if not self.enabled:
            print("LINE通知が無効です")
            return False
        
        if not self.user_id:
            print("LINE User IDが設定されていません")
            return False
        
        if not self.messaging_api and not self.line_bot_api:
            print("LINE Bot APIが初期化されていません")
            return False
        
        try:
            # 長すぎるメッセージは切り詰める
            if len(message) > 2000:
                message = message[:1900] + "\n\n（メッセージが長いため省略されました）"
            
            if USE_V3_API and self.messaging_api:
                # v3 APIでメッセージ送信
                text_message = TextMessage(text=message)
                push_message_request = PushMessageRequest(
                    to=self.user_id,
                    messages=[text_message]
                )
                self.messaging_api.push_message(push_message_request)
                
            else:
                # v2 APIでメッセージ送信
                text_message = TextSendMessage(text=message)
                self.line_bot_api.push_message(self.user_id, text_message)
            
            print("✅ LINE通知送信成功")
            return True
            
        except Exception as e:
            if 'ApiException' in str(type(e)) or 'LineBotApiError' in str(type(e)):
                if hasattr(e, 'status'):
                    print(f"❌ LINE API エラー: {e.status} - {e.reason}")
                else:
                    print(f"❌ LINE API エラー: {e}")
            else:
                print(f"❌ 送信エラー: {e}")
            return False


class StockChangeChecker:
    """銘柄変動チェッククラス"""
    
    def __init__(self):
        """初期化"""
        self.agg_data_dir = Path("./agg_data")
    
    def get_available_dates(self) -> List[str]:
        """利用可能な日付リスト取得"""
        if not self.agg_data_dir.exists():
            return []
        
        dates = []
        for date_dir in self.agg_data_dir.iterdir():
            if date_dir.is_dir() and self._is_valid_date(date_dir.name):
                dates.append(date_dir.name)
        
        return sorted(dates)
    
    def _is_valid_date(self, date_str: str) -> bool:
        """日付フォーマット検証"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def load_target_metrics(self, date: str) -> Optional[pl.DataFrame]:
        """指定日のCSV読み込み（複数エンコーディング対応）"""
        file_path = self.agg_data_dir / date / "consolidated_target_metrics.csv"
        
        if not file_path.exists():
            print(f"ファイルが存在しません: {file_path}")
            return None
        
        # 複数のエンコーディングを試す
        encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                return pl.read_csv(
                    file_path, 
                    schema_overrides={'LocalCode': pl.Utf8}, 
                    ignore_errors=True,
                    encoding=encoding
                )
            except Exception as e:
                continue
        
        print(f"CSV読み込みエラー ({date}): 全てのエンコーディングで失敗")
        return None
    
    def compare_stocks(self, current_date: str, previous_date: str) -> tuple[List[Dict], List[Dict]]:
        """銘柄リスト比較"""
        # ファイルパスを出力
        current_file_path = self.agg_data_dir / current_date / "consolidated_target_metrics.csv"
        previous_file_path = self.agg_data_dir / previous_date / "consolidated_target_metrics.csv"
        print(f"[読み込み] 当日: {current_file_path}")
        print(f"[読み込み] 前日: {previous_file_path}")
        current_df = self.load_target_metrics(current_date)
        previous_df = self.load_target_metrics(previous_date)
        
        if current_df is None or previous_df is None:
            print("比較対象ファイルの読み込み失敗")
            return [], []
        
        # LocalCodeセット取得
        current_codes = set(current_df['LocalCode'].to_list())
        previous_codes = set(previous_df['LocalCode'].to_list())
        
        # デバッグ情報
        print(f"📊 {current_date}: {len(current_codes)}銘柄")
        print(f"📊 {previous_date}: {len(previous_codes)}銘柄")
        
        # 97090銘柄の存在確認
        target_code = "97090"
        if target_code in current_codes:
            print(f"✅ {target_code}は{current_date}に存在")
        if target_code in previous_codes:
            print(f"✅ {target_code}は{previous_date}に存在")
        
        # 新規追加・削除銘柄
        new_codes = current_codes - previous_codes
        removed_codes = previous_codes - current_codes
        
        print(f"🆕 新規追加: {len(new_codes)}銘柄")
        print(f"❌ 削除: {len(removed_codes)}銘柄")
        
        new_stocks = []
        if new_codes:
            # 利用可能なカラムをチェック
            available_columns = current_df.columns
            select_columns = ['LocalCode', 'CompanyName']
            if 'Sector17CodeName' in available_columns:
                select_columns.append('Sector17CodeName')
            if '分類' in available_columns:
                select_columns.append('分類')
            
            new_stocks_df = current_df.filter(pl.col('LocalCode').is_in(list(new_codes)))
            new_stocks = new_stocks_df.select(select_columns).to_dicts()
        
        removed_stocks = []
        if removed_codes:
            # 利用可能なカラムをチェック
            available_columns = previous_df.columns
            select_columns = ['LocalCode', 'CompanyName']
            if 'Sector17CodeName' in available_columns:
                select_columns.append('Sector17CodeName')
            if '分類' in available_columns:
                select_columns.append('分類')
            
            removed_stocks_df = previous_df.filter(pl.col('LocalCode').is_in(list(removed_codes)))
            removed_stocks = removed_stocks_df.select(select_columns).to_dicts()
        
        return new_stocks, removed_stocks
    
    def format_message(self, new_stocks: List[Dict], removed_stocks: List[Dict], 
                      current_date: str, previous_date: str) -> str:
        """通知メッセージ作成"""
        lines = [
            "📊 日本株分析 - 銘柄変動通知",
            "",
            f"📅 {previous_date} → {current_date}",
            ""
        ]
        
        if new_stocks:
            lines.append(f"🆕 新規追加 ({len(new_stocks)}銘柄):")
            for stock in new_stocks:
                company_name = stock.get('CompanyName', 'N/A')
                sector = stock.get('Sector17CodeName', 'N/A')
                category = stock.get('分類', 'N/A')
                lines.append(f"• {stock['LocalCode']} {company_name}")
                lines.append(f"  [{sector}] {category}")
            lines.append("")
        
        if removed_stocks:
            lines.append(f"❌ 削除 ({len(removed_stocks)}銘柄):")
            for stock in removed_stocks:
                company_name = stock.get('CompanyName', 'N/A')
                sector = stock.get('Sector17CodeName', 'N/A')
                category = stock.get('分類', 'N/A')
                lines.append(f"• {stock['LocalCode']} {company_name}")
                lines.append(f"  [{sector}] {category}")
            lines.append("")
        
        if not new_stocks and not removed_stocks:
            lines.append("✅ 銘柄の変動はありませんでした")
            lines.append("")
        
        lines.append(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "\n".join(lines)


def check_and_notify(target_date: Optional[str] = None) -> bool:
    """銘柄変動チェックと通知実行"""
    
    # 日付設定
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    target_dt = datetime.strptime(target_date, '%Y-%m-%d')
    previous_dt = target_dt - timedelta(days=1)
    previous_date = previous_dt.strftime('%Y-%m-%d')
    
    # チェッカーと通知システム初期化
    checker = StockChangeChecker()
    notifier = LineNotifier()
    
    # 利用可能日付確認
    available_dates = checker.get_available_dates()
    
    if target_date not in available_dates:
        print(f"対象日のデータなし: {target_date}")
        return False
    
    if previous_date not in available_dates:
        print(f"前日のデータなし: {previous_date}")
        return False
    
    # 銘柄比較
    new_stocks, removed_stocks = checker.compare_stocks(target_date, previous_date)
    
    # 変動なしの場合
    if not new_stocks and not removed_stocks:
        print(f"銘柄変動なし: {previous_date} → {target_date}")
        return True
    
    # 通知メッセージ作成・送信
    message = checker.format_message(new_stocks, removed_stocks, target_date, previous_date)
    return notifier.send_message(message)


def main():
    """メイン関数"""
    import sys
    
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = None
    
    print("=" * 50)
    print("日本株分析 - 銘柄変動通知")
    print("=" * 50)
    
    success = check_and_notify(target_date)
    
    if success:
        print("✅ 処理完了")
    else:
        print("❌ 処理失敗")
        sys.exit(1)


if __name__ == "__main__":
    main()