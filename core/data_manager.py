"""
統合データ管理モジュール
J-Quants APIからのデータ取得・管理をPolarsベースで実装
"""

import requests
import polars as pl
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

from core.config import ConfigurationManager, ApiConfig
from core.utilities import LoggingManager, FileOperations, DataProcessor, TimeStampGenerator, BusinessDayChecker


class JQuantsAPIConnector:
    """J-Quants API接続クラス"""
    
    def __init__(self, api_config: ApiConfig):
        self.config = api_config
        self.logger = LoggingManager.setup_logger("JQuantsAPI")
        self.refresh_token = None
        self.id_token = None
        self.headers = None
        self._authenticate()
    
    def _authenticate(self):
        """認証処理"""
        try:
            # リフレッシュトークン取得
            auth_data = {
                "mailaddress": self.config.mail_address,
                "password": self.config.password
            }
            
            response = requests.post(
                f"{self.config.base_url}/token/auth_user",
                data=json.dumps(auth_data)
            )
            response.raise_for_status()
            
            self.refresh_token = response.json()['refreshToken']
            self.logger.info("リフレッシュトークン取得成功")
            
            # IDトークン取得
            response = requests.post(
                f"{self.config.base_url}/token/auth_refresh",
                params={"refreshtoken": self.refresh_token}
            )
            response.raise_for_status()
            
            self.id_token = response.json()['idToken']
            self.headers = {'Authorization': f'Bearer {self.id_token}'}
            self.logger.info("認証完了")
            
        except Exception as e:
            self.logger.error(f"認証エラー: {e}")
            raise
    
    def fetch_listed_companies(self) -> pl.DataFrame:
        """上場企業一覧の取得"""
        try:
            response = requests.get(
                f"{self.config.base_url}/listed/info",
                headers=self.headers
            )
            response.raise_for_status()
            
            data = response.json()['info']
            df = pl.DataFrame(data)
            
            self.logger.info(f"上場企業一覧取得: {len(df)} 社")
            return df
            
        except Exception as e:
            self.logger.error(f"上場企業一覧取得エラー: {e}")
            return pl.DataFrame()
    
    def fetch_stock_prices_by_code(self, code: str) -> pl.DataFrame:
        """銘柄コード指定での株価取得"""
        try:
            response = requests.get(
                f"{self.config.base_url}/prices/daily_quotes",
                headers=self.headers,
                params={"code": code}
            )
            response.raise_for_status()
            
            data = response.json().get('daily_quotes', [])
            if data:
                return pl.DataFrame(data)
            else:
                return pl.DataFrame()
                
        except Exception as e:
            self.logger.error(f"株価取得エラー (Code: {code}): {e}")
            return pl.DataFrame()
    
    def fetch_financial_data_by_code(self, code: str) -> pl.DataFrame:
        """銘柄コード指定での財務データ取得"""
        try:
            response = requests.get(
                f"{self.config.base_url}/fins/statements",
                headers=self.headers,
                params={"code": code}
            )
            response.raise_for_status()
            
            data = response.json().get('statements', [])
            if data:
                return pl.DataFrame(data)
            else:
                return pl.DataFrame()
                
        except Exception as e:
            self.logger.error(f"財務データ取得エラー (Code: {code}): {e}")
            return pl.DataFrame()


class UnifiedDataManager:
    """統合データ管理クラス"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config = ConfigurationManager(config_path)
        self.api_connector = JQuantsAPIConnector(self.config.api)
        self.business_day_checker = BusinessDayChecker()
        self.logger = LoggingManager.setup_logger("DataManager")
        
        # ディレクトリ確保
        FileOperations.ensure_directory(self.config.paths.output_directory)
        FileOperations.ensure_directory(self.config.paths.stock_price_file.parent)
        FileOperations.ensure_directory(self.config.paths.finance_file.parent)
    
    def fetch_and_save_listed_companies(self) -> pl.DataFrame:
        """上場企業一覧の取得・保存"""
        self.logger.info("上場企業一覧を取得中...")
        
        df = self.api_connector.fetch_listed_companies()
        if not df.is_empty():
            FileOperations.write_csv_safe(df, Path(self.config.paths.listed_info_file))
            self.logger.info(f"上場企業一覧保存完了: {len(df)} 社")
        
        return df
    
    def bulk_fetch_stock_data(self) -> pl.DataFrame:
        """全銘柄の株価データ一括取得"""
        self.logger.info("株価データ一括取得開始")
        
        # 上場企業一覧を取得
        listed_df = self.fetch_and_save_listed_companies()
        if listed_df.is_empty():
            return pl.DataFrame()
        
        stock_codes = listed_df['Code'].unique().to_list()
        all_stock_data = []
        
        for i, code in enumerate(stock_codes, 1):
            df_stock = self.api_connector.fetch_stock_prices_by_code(str(code))
            
            if not df_stock.is_empty():
                # データ型最適化
                df_stock = DataProcessor.optimize_data_types(df_stock)
                all_stock_data.append(df_stock)
            
            # 進捗表示
            if i % 100 == 0 or i == len(stock_codes):
                self.logger.info(f"株価データ取得進捗: {i}/{len(stock_codes)}")
            
            # レート制限
            time.sleep(self.config.api.rate_limit_delay)
        
        # 全データを統合
        if all_stock_data:
            combined_df = pl.concat(all_stock_data, how="vertical")
            FileOperations.write_csv_safe(combined_df, Path(self.config.paths.stock_price_file))
            self.logger.info(f"株価データ一括取得完了: {len(combined_df)} レコード")
            return combined_df
        else:
            return pl.DataFrame()
    
    def incremental_fetch_stock_data(self) -> pl.DataFrame:
        """差分株価データ取得（営業日チェック付き）"""
        self.logger.info("差分株価データ取得開始")
        
        # 既存データから銘柄ごと最終日を取得
        existing_df = FileOperations.read_csv_safe(Path(self.config.paths.stock_price_file))
        if existing_df.is_empty():
            self.logger.warning("既存データなし。一括取得を実行してください。")
            return pl.DataFrame()
        
        # 営業日チェック: 最新営業日を取得
        latest_business_day = self.business_day_checker.get_latest_business_day()
        latest_business_day_str = latest_business_day.strftime('%Y-%m-%d')
        self.logger.info(f"最新営業日: {latest_business_day_str}")
        
        # 既存データの最新日付を取得
        existing_df_with_date = existing_df.with_columns(
            pl.col('Date').str.strptime(pl.Date, format='%Y-%m-%d')
        )
        data_latest_date = existing_df_with_date['Date'].max()
        data_latest_date_str = data_latest_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"既存データの最新日付: {data_latest_date_str}")
        
        # 既存データが最新営業日以降の場合、更新不要
        if data_latest_date >= latest_business_day.date():
            self.logger.info(
                f"データは最新状態です。"
                f"データ最新日: {data_latest_date_str}, "
                f"営業日最新日: {latest_business_day_str}"
            )
            return pl.DataFrame()  # 空のDatFrameを返して処理終了
        
        self.logger.info(
            f"データ更新が必要です。"
            f"データ最新日: {data_latest_date_str}, "
            f"営業日最新日: {latest_business_day_str}"
        )
        
        # 銘柄ごとの最終日付を計算
        last_dates = (existing_df
                     .with_columns(pl.col('Date').str.strptime(pl.Date, format='%Y-%m-%d'))
                     .group_by('Code')
                     .agg(pl.col('Date').max().alias('LastDate'))
                     .with_columns(pl.col('LastDate').dt.strftime('%Y-%m-%d'))
                     )
        
        all_new_data = []
        processed_count = 0
        
        for row in last_dates.iter_rows(named=True):
            code = str(row['Code'])
            last_date = row['LastDate']
            
            # 最終日以降のデータを取得
            df_all = self.api_connector.fetch_stock_prices_by_code(code)
            
            if not df_all.is_empty():
                # 最終日以降のデータをフィルタ
                df_filtered = (df_all
                              .with_columns(pl.col('Date').str.strptime(pl.Date, format='%Y-%m-%d'))
                              .filter(pl.col('Date') > pl.lit(last_date).str.strptime(pl.Date, format='%Y-%m-%d'))
                              .with_columns(pl.col('Date').dt.strftime('%Y-%m-%d'))
                              )
                
                if not df_filtered.is_empty():
                    df_filtered = DataProcessor.optimize_data_types(df_filtered)
                    all_new_data.append(df_filtered)
                    self.logger.info(f"銘柄 {code}: {len(df_filtered)} 件の新規データ")
            
            processed_count += 1
            if processed_count % 100 == 0:
                self.logger.info(f"差分取得進捗: {processed_count}/{len(last_dates)}")
            
            time.sleep(self.config.api.rate_limit_delay)
        
        # 新規データを統合・保存
        if all_new_data:
            new_df = pl.concat(all_new_data, how="vertical")
            
            # 既存データと結合
            updated_df = pl.concat([existing_df, new_df], how="vertical")
            
            # 重複除去・ソート
            updated_df = (updated_df
                         .unique(subset=['Date', 'Code'])
                         .with_columns(pl.col('Date').str.strptime(pl.Date, format='%Y-%m-%d'))
                         .sort(['Date', 'Code'])
                         .with_columns(pl.col('Date').dt.strftime('%Y-%m-%d'))
                         )
            
            FileOperations.write_csv_safe(updated_df, Path(self.config.paths.stock_price_file))
            self.logger.info(f"差分取得完了: {len(new_df)} 件の新規データを追加")
            return new_df
        else:
            self.logger.info("新規データなし")
            return pl.DataFrame()
    
    def bulk_fetch_financial_data(self) -> pl.DataFrame:
        """全銘柄の財務データ一括取得"""
        self.logger.info("財務データ一括取得開始")
        
        # 上場企業一覧を取得
        listed_df = FileOperations.read_csv_safe(Path(self.config.paths.listed_info_file))
        if listed_df.is_empty():
            listed_df = self.fetch_and_save_listed_companies()
        
        if listed_df.is_empty():
            return pl.DataFrame()
        
        stock_codes = listed_df['Code'].unique().to_list()
        all_financial_data = []
        
        for i, code in enumerate(stock_codes, 1):
            df_finance = self.api_connector.fetch_financial_data_by_code(str(code))
            
            if not df_finance.is_empty():
                all_financial_data.append(df_finance)
            
            # 進捗表示
            if i % 100 == 0 or i == len(stock_codes):
                self.logger.info(f"財務データ取得進捗: {i}/{len(stock_codes)}")
            
            time.sleep(self.config.api.rate_limit_delay)
        
        # 全データを統合
        if all_financial_data:
            combined_df = pl.concat(all_financial_data, how="vertical")
            FileOperations.write_csv_safe(combined_df, Path(self.config.paths.finance_file))
            self.logger.info(f"財務データ一括取得完了: {len(combined_df)} レコード")
            return combined_df
        else:
            return pl.DataFrame()
