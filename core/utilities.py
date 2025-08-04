"""
統合ユーティリティモジュール
共通機能とPolars対応のデータ処理を提供（エラーハンドリング強化版）
"""

import os
import logging
import polars as pl
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path


class LoggingManager:
    """ログ管理クラス"""
    
    @staticmethod
    def setup_logger(name: str, log_file: str = "app.log") -> logging.Logger:
        """ロガーのセットアップ"""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        if logger.handlers:
            return logger
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger


class FileOperations:
    """ファイル操作ユーティリティ"""
    
    @staticmethod
    def ensure_directory(path: Path) -> None:
        """ディレクトリの確保"""
        path.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def is_gzip_file(filepath: Path) -> bool:
        """gzipファイルかどうかの判定"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(2)
                return header == b'\\x1f\\x8b'
        except:
            return False
    
    @staticmethod
    def read_csv_safe(filepath: Path) -> pl.DataFrame:
        """安全なCSV読み込み（gzip自動判定・スキーマ問題対応強化版）"""
        logger = LoggingManager.setup_logger("FileOperations")
        
        if not filepath.exists():
            logger.warning(f"ファイルが存在しません: {filepath}")
            return pl.DataFrame()
        
        logger.info(f"CSV読み込み開始: {filepath}")
        
        try:
            if FileOperations.is_gzip_file(filepath):
                # gzipファイルの場合、pandas経由で読み込み
                logger.info("gzipファイルを検出、pandas経由で読み込み")
                import pandas as pd
                df_pd = pd.read_csv(filepath, compression='gzip', encoding='utf-8', dtype=str)
                df = pl.from_pandas(df_pd)
                logger.info(f"gzipファイル読み込み完了: {len(df)} 行")
                return df
            else:
                # 通常のCSVファイルの場合
                logger.info("通常のCSVファイルをPolarsで読み込み")
                
                # まず小さなサンプルでスキーマを確認
                try:
                    sample_df = pl.read_csv(
                        filepath, 
                        n_rows=100,
                        infer_schema_length=1000,
                        schema_overrides={
                            'Code': pl.Utf8,
                            'LocalCode': pl.Utf8,
                            'Date': pl.Utf8
                        }
                    )
                    logger.info(f"サンプル読み込み成功: {sample_df.columns}")
                except Exception as sample_e:
                    logger.warning(f"サンプル読み込み失敗: {sample_e}")
                
                # 強化されたパラメータで読み込み - 全ての可能なコードフィールドを文字列として扱う
                df = pl.read_csv(
                    filepath,
                    infer_schema_length=100000,    # より多くの行でスキーマ推論
                    ignore_errors=True,            # パースエラーを無視
                    null_values=["", "NULL", "null", "N/A", "n/a", "NaN"],  # null値の指定
                    schema_overrides={
                        'Code': pl.Utf8,                   # 銘柄コードは文字列
                        'LocalCode': pl.Utf8,              # LocalCodeも文字列
                        'Date': pl.Utf8,                   # 日付も文字列
                        'DisclosedDate': pl.Utf8,          # 開示日も文字列
                        'DisclosedTime': pl.Utf8,          # 開示時刻も文字列
                        # 数値フィールドも念のため安全な型を指定
                        'AdjustmentOpen': pl.Float64,
                        'AdjustmentHigh': pl.Float64,
                        'AdjustmentLow': pl.Float64,
                        'AdjustmentClose': pl.Float64,
                        'AdjustmentVolume': pl.Float64,
                        'Open': pl.Float64,
                        'High': pl.Float64,
                        'Low': pl.Float64,
                        'Close': pl.Float64,
                        'Volume': pl.Float64
                    }
                )
                
                logger.info(f"CSV読み込み完了: {len(df)} 行, {len(df.columns)} 列")
                logger.debug(f"読み込み列: {df.columns}")
                return df
                
        except Exception as e:
            logger.error(f"CSV読み込みエラー ({filepath}): {e}")
            logger.error(f"エラータイプ: {type(e).__name__}")
            
            # フォールバック: pandas経由で読み込み（全て文字列として）
            try:
                logger.info("フォールバック: pandas経由での読み込みを試行（全て文字列として）")
                import pandas as pd
                df_pd = pd.read_csv(filepath, encoding='utf-8', dtype=str)  # 全て文字列として読み込み
                df = pl.from_pandas(df_pd)
                
                # 必要な列を数値型に変換
                numeric_columns = [
                    'AdjustmentOpen', 'AdjustmentHigh', 'AdjustmentLow', 'AdjustmentClose', 'AdjustmentVolume',
                    'Open', 'High', 'Low', 'Close', 'Volume'
                ]
                
                for col in numeric_columns:
                    if col in df.columns:
                        try:
                            df = df.with_columns(
                                pl.col(col).str.replace(',', '').cast(pl.Float64, strict=False).alias(col)
                            )
                        except:
                            logger.warning(f"列 {col} の数値変換に失敗")
                
                logger.info(f"pandas経由での読み込み成功: {len(df)} 行")
                return df
            except Exception as fallback_e:
                logger.error(f"pandas経由での読み込みも失敗: {fallback_e}")
                return pl.DataFrame()
    
    @staticmethod
    def write_csv_safe(df: pl.DataFrame, filepath: Path, create_dir: bool = True) -> None:
        """安全なCSV保存"""
        logger = LoggingManager.setup_logger("FileOperations")
        
        if create_dir:
            FileOperations.ensure_directory(filepath.parent)
        
        try:
            # Polarsの場合、encodingパラメータは使用できないため、デフォルトのUTF-8で保存
            df.write_csv(filepath)
            logger.info(f"CSV保存完了: {filepath} ({len(df)} 行)")
        except Exception as e:
            logger.error(f"CSV保存エラー: {e}")
            raise


class DataProcessor:
    """Polarsベースのデータ処理ユーティリティ"""
    
    @staticmethod
    def optimize_data_types(df: pl.DataFrame) -> pl.DataFrame:
        """データ型の最適化（エラー安全版）"""
        for col in df.columns:
            try:
                if df[col].dtype == pl.Int64:
                    col_max = df[col].max()
                    col_min = df[col].min()
                    
                    if col_max is not None and col_min is not None:
                        if col_max <= 127 and col_min >= -128:
                            df = df.with_columns(df[col].cast(pl.Int8).alias(col))
                        elif col_max <= 32767 and col_min >= -32768:
                            df = df.with_columns(df[col].cast(pl.Int16).alias(col))
                        elif col_max <= 2147483647 and col_min >= -2147483648:
                            df = df.with_columns(df[col].cast(pl.Int32).alias(col))
            except Exception:
                # データ型変換に失敗した場合はスキップ
                continue
        
        return df
    
    @staticmethod
    def calculate_technical_indicators(df: pl.DataFrame, 
                                     price_col: str = 'AdjustmentClose') -> pl.DataFrame:
        """テクニカル指標の計算（エラー安全版）"""
        try:
            return df.with_columns([
                # 移動平均
                df[price_col].rolling_mean(5).alias('MA5'),
                df[price_col].rolling_mean(25).alias('MA25'),
                df[price_col].rolling_mean(75).alias('MA75'),
                
                # 価格変化率
                (df[price_col].pct_change(1)).alias('PriceChange1D'),
                (df[price_col].pct_change(5)).alias('PriceChange5D'),
                (df[price_col].pct_change(25)).alias('PriceChange25D'),
                
                # ボラティリティ
                df[price_col].rolling_std(20).alias('Volatility'),
                
                # RSI
                DataProcessor._calculate_rsi(df[price_col]).alias('RSI')
            ])
        except Exception as e:
            logger = LoggingManager.setup_logger("DataProcessor")
            logger.error(f"テクニカル指標計算エラー: {e}")
            return df
    
    @staticmethod
    def _calculate_rsi(prices: pl.Series, window: int = 14) -> pl.Series:
        """RSI計算（Polars版・エラー安全版）"""
        try:
            delta = prices.diff()
            
            gain = delta.map_elements(lambda x: max(x, 0) if x is not None else 0, return_dtype=pl.Float64)
            loss = delta.map_elements(lambda x: abs(min(x, 0)) if x is not None else 0, return_dtype=pl.Float64)
            
            avg_gain = gain.rolling_mean(window)
            avg_loss = loss.rolling_mean(window)
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
        except Exception:
            # RSI計算に失敗した場合は50のシリーズを返す
            return pl.Series([50.0] * len(prices))
    
    @staticmethod
    def calculate_bollinger_bands(prices: pl.Series, window: int = 25, num_std: int = 2) -> Tuple[pl.Series, pl.Series]:
        """ボリンジャーバンド計算（エラー安全版）"""
        try:
            ma = prices.rolling_mean(window)
            std = prices.rolling_std(window)
            upper = ma + (std * num_std)
            lower = ma - (std * num_std)
            return upper, lower
        except Exception:
            # 計算に失敗した場合は価格と同じシリーズを返す
            return prices, prices
    
    @staticmethod
    def calculate_financial_ratios(df: pl.DataFrame) -> pl.DataFrame:
        """財務比率の計算（エラー安全版）"""
        try:
            return df.with_columns([
                # 収益性指標（ゼロ除算対策）
                pl.when(pl.col('Equity') != 0)
                .then((pl.col('Profit') / pl.col('Equity')) * 100)
                .otherwise(None)
                .alias('ROE'),
                
                pl.when(pl.col('TotalAssets') != 0)
                .then((pl.col('Profit') / pl.col('TotalAssets')) * 100)
                .otherwise(None)
                .alias('ROA'),
                
                pl.when(pl.col('NetSales') != 0)
                .then((pl.col('OperatingProfit') / pl.col('NetSales')) * 100)
                .otherwise(None)
                .alias('OperatingMargin'),
                
                pl.when(pl.col('NetSales') != 0)
                .then((pl.col('Profit') / pl.col('NetSales')) * 100)
                .otherwise(None)
                .alias('NetMargin'),
                
                # 安全性指標
                pl.when(pl.col('TotalAssets') != 0)
                .then((pl.col('Equity') / pl.col('TotalAssets')) * 100)
                .otherwise(None)
                .alias('EquityRatio'),
                
                # 効率性指標
                pl.when(pl.col('TotalAssets') != 0)
                .then(pl.col('NetSales') / pl.col('TotalAssets'))
                .otherwise(None)
                .alias('AssetTurnover')
            ])
        except Exception as e:
            logger = LoggingManager.setup_logger("DataProcessor")
            logger.error(f"財務比率計算エラー: {e}")
            return df


class TimeStampGenerator:
    """タイムスタンプ生成ユーティリティ"""
    
    @staticmethod
    def get_timestamp() -> str:
        """現在時刻のタイムスタンプを取得"""
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    @staticmethod
    def get_date_string() -> str:
        """現在日付の文字列を取得"""
        return datetime.now().strftime('%Y-%m-%d')


class BusinessDayChecker:
    """営業日判定クラス"""
    
    def __init__(self):
        self.logger = LoggingManager.setup_logger("BusinessDayChecker")
        # 日本の休日（2024-2025年）
        self.japanese_holidays = {
            # 2024年
            '2024-01-01': '元日',
            '2024-01-08': '成人の日',
            '2024-02-11': '建国記念の日',
            '2024-02-12': '建国記念の日 振替休日',
            '2024-02-23': '天皇誕生日',
            '2024-03-20': '春分の日',
            '2024-04-29': '昭和の日',
            '2024-05-03': '憲法記念日',
            '2024-05-04': 'みどりの日',
            '2024-05-05': 'こどもの日',
            '2024-05-06': 'こどもの日 振替休日',
            '2024-07-15': '海の日',
            '2024-08-11': '山の日',
            '2024-08-12': '山の日 振替休日',
            '2024-09-16': '敬老の日',
            '2024-09-22': '秋分の日',
            '2024-09-23': '秋分の日 振替休日',
            '2024-10-14': 'スポーツの日',
            '2024-11-03': '文化の日',
            '2024-11-04': '文化の日 振替休日',
            '2024-11-23': '勤労感謝の日',
            '2024-12-31': '年末休日',
            
            # 2025年
            '2025-01-01': '元日',
            '2025-01-02': '年始休日',
            '2025-01-03': '年始休日',
            '2025-01-13': '成人の日',
            '2025-02-11': '建国記念の日',
            '2025-02-23': '天皇誕生日',
            '2025-02-24': '天皇誕生日 振替休日',
            '2025-03-20': '春分の日',
            '2025-04-29': '昭和の日',
            '2025-05-03': '憲法記念日',
            '2025-05-04': 'みどりの日',
            '2025-05-05': 'こどもの日',
            '2025-05-06': 'こどもの日 振替休日',
            '2025-07-21': '海の日',
            '2025-08-11': '山の日',
            '2025-09-15': '敬老の日',
            '2025-09-23': '秋分の日',
            '2025-10-13': 'スポーツの日',
            '2025-11-03': '文化の日',
            '2025-11-23': '勤労感謝の日',
            '2025-11-24': '勤労感謝の日 振替休日',
            '2025-12-31': '年末休日',
        }
    
    def is_business_day(self, date: datetime) -> bool:
        """指定された日付が東証の営業日かどうかを判定"""
        # 土日は休日
        if date.weekday() >= 5:  # 5=土曜日, 6=日曜日
            return False
        
        # 祝日は休日
        date_str = date.strftime('%Y-%m-%d')
        if date_str in self.japanese_holidays:
            return False
        
        return True
    
    def get_latest_business_day(self, reference_date: datetime = None) -> datetime:
        """指定日（デフォルトは今日）以前の最新営業日を取得"""
        if reference_date is None:
            reference_date = datetime.now()
        
        current_date = reference_date
        
        # 最大30日前まで遡って営業日を探す
        for _ in range(30):
            if self.is_business_day(current_date):
                return current_date
            current_date = current_date - timedelta(days=1)
        
        # 営業日が見つからない場合は元の日付を返す
        self.logger.warning(f"営業日が見つかりませんでした: {reference_date}")
        return reference_date
