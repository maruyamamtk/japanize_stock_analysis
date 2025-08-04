#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日本株データ分析エンジン - Polars実装版
"""

import polars as pl
import os
from datetime import datetime, timedelta
from pathlib import Path


class JapanStockAnalysisEngine:
    """日本株データ分析エンジン"""
    
    def __init__(self, data_dir: str = "C:\\Users\\michika\\Desktop\\日本株分析\\data"):
        """
        初期化
        
        Args:
            data_dir: データディレクトリのパス
        """
        self.data_dir = Path(data_dir)
        self.finance_path = self.data_dir / "finance" / "finance_data.csv"
        self.listed_path = self.data_dir / "listed_companies.csv"
        
        # データの読み込み
        self.df_finance_all = self._load_finance_data()
        self.df_listed_info = self._load_listed_data()
        
        # 出力ディレクトリの作成
        self.today_str = datetime.today().strftime('%Y-%m-%d')
        self.output_dir = Path(f'./agg_data/{self.today_str}')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Windows用出力ディレクトリの作成
        self.output_dir_windows = Path(f'./agg_data_windows/{self.today_str}')
        self.output_dir_windows.mkdir(parents=True, exist_ok=True)
    
    def _load_finance_data(self) -> pl.DataFrame:
        """財務データの読み込み"""
        return pl.read_csv(
            self.finance_path, 
            dtypes={'LocalCode': pl.Utf8},
            infer_schema_length=10000,
            ignore_errors=True
        )
    
    def _load_listed_data(self) -> pl.DataFrame:
        """上場企業データの読み込み"""
        return pl.read_csv(
            self.listed_path,
            dtypes={'Code': pl.Utf8},
            infer_schema_length=10000,
            ignore_errors=True
        )
    
    def analyze_annual_performance(self) -> pl.DataFrame:
        """年次業績データの抽出と処理"""
        # 年度ごとの業績を抽出
        df_finance_annual = self.df_finance_all.filter(
            pl.col('TypeOfDocument') == 'FYFinancialStatements_Consolidated_JP'
        )
        
        # 業績修正が入ることがあるため、最新のレコードのみを抽出
        df_finance_annual = (
            df_finance_annual
            .with_columns(
                pl.col('DisclosureNumber')
                .rank(method='dense', descending=True)
                .over(['LocalCode', 'CurrentPeriodEndDate'])
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
        )
        
        return df_finance_annual
    
    def calculate_annual_eps_growth(self, df_finance_annual: pl.DataFrame) -> pl.DataFrame:
        """年次EPSの成長率を計算"""
        eps_annual = (
            df_finance_annual
            .select([
                'DisclosedDate', 'LocalCode',
                'CurrentPeriodStartDate', 'CurrentPeriodEndDate', 
                'CurrentFiscalYearStartDate', 'EarningsPerShare'
            ])
            .rename({'EarningsPerShare': 'eps'})
            .with_columns([
                pl.col('eps').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            .sort(['LocalCode', 'CurrentPeriodEndDate'])
        )
        
        # 前期のEPSと成長率を計算
        eps_annual = eps_annual.with_columns([
            pl.col('eps').shift(1).over('LocalCode').alias('eps_before'),
        ]).with_columns([
            ((pl.col('eps') / pl.col('eps_before')) - 1).alias('eps_growth_percent'),
            (pl.col('eps') - pl.col('eps_before')).alias('eps_growth_value')
        ])
        
        return eps_annual
    
    def analyze_quarterly_performance(self) -> pl.DataFrame:
        """四半期業績データの抽出と処理"""
        # 四半期ごとの業績を抽出
        df_finance_quarter = self.df_finance_all.filter(
            pl.col('TypeOfDocument').str.contains('FinancialStatements_Consolidated_JP')
        )
        
        # 最新のレコードのみを抽出
        df_finance_quarter = (
            df_finance_quarter
            .with_columns(
                pl.col('DisclosureNumber')
                .rank(method='dense', descending=True)
                .over(['LocalCode', 'CurrentPeriodEndDate'])
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
        )
        
        return df_finance_quarter
    
    def calculate_quarterly_eps_growth(self, df_finance_quarter: pl.DataFrame) -> pl.DataFrame:
        """四半期EPSの成長率を計算"""
        eps_quarter = (
            df_finance_quarter
            .select([
                'DisclosedDate', 'LocalCode',
                'CurrentPeriodStartDate', 'CurrentPeriodEndDate', 
                'CurrentFiscalYearStartDate', 'EarningsPerShare'
            ])
            .rename({'EarningsPerShare': 'eps'})
            .with_columns([
                pl.col('eps').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            .sort(['LocalCode', 'CurrentPeriodEndDate'])
        )
        
        # 前期のEPSと成長率を計算
        eps_quarter = eps_quarter.with_columns([
            pl.col('eps').shift(1).over('LocalCode').alias('eps_before'),
        ]).with_columns([
            ((pl.col('eps') / pl.col('eps_before')) - 1).alias('eps_growth_percent'),
            (pl.col('eps') - pl.col('eps_before')).alias('eps_growth_value')
        ])
        
        return eps_quarter
    
    def calculate_annual_netsales_growth(self, df_finance_annual: pl.DataFrame) -> pl.DataFrame:
        """年次売上高の成長率を計算"""
        netsales_annual = (
            df_finance_annual
            .select([
                'DisclosedDate', 'LocalCode',
                'CurrentPeriodStartDate', 'CurrentPeriodEndDate',
                'CurrentFiscalYearStartDate', 'NetSales'
            ])
            .rename({'NetSales': 'netsales'})
            .with_columns([
                pl.col('netsales').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            .sort(['LocalCode', 'CurrentPeriodEndDate'])
        )
        
        # 前期の売上高と成長率を計算
        netsales_annual = netsales_annual.with_columns([
            pl.col('netsales').shift(1).over('LocalCode').alias('netsales_before'),
        ]).with_columns([
            ((pl.col('netsales') / pl.col('netsales_before')) - 1).alias('netsales_growth_percent'),
            (pl.col('netsales') - pl.col('netsales_before')).alias('netsales_growth_value')
        ])
        
        return netsales_annual
    
    def calculate_quarterly_netsales_growth(self, df_finance_quarter: pl.DataFrame) -> pl.DataFrame:
        """四半期売上高の成長率を計算"""
        netsales_quarter = (
            df_finance_quarter
            .select([
                'DisclosedDate', 'LocalCode', 'TypeOfCurrentPeriod',
                'CurrentPeriodStartDate', 'CurrentPeriodEndDate',
                'CurrentFiscalYearStartDate', 'NetSales'
            ])
            .rename({'NetSales': 'netsales_cumsum'})
            .with_columns([
                pl.col('netsales_cumsum').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            .sort(['LocalCode', 'CurrentPeriodEndDate'])
        )
        
        # 累積の売上高の差分を算出
        netsales_quarter = netsales_quarter.with_columns([
            (pl.col('netsales_cumsum') - 
             pl.col('netsales_cumsum').shift(1).over('LocalCode')).alias('netsales_diff')
        ])
        
        # Qに応じて売上高を算出
        netsales_quarter = netsales_quarter.with_columns([
            pl.when(pl.col('TypeOfCurrentPeriod') == '1Q')
            .then(pl.col('netsales_cumsum'))
            .otherwise(pl.col('netsales_diff'))
            .alias('netsales')
        ])
        
        # 前期の売上高と成長率を計算
        netsales_quarter = netsales_quarter.with_columns([
            pl.col('netsales').shift(1).over('LocalCode').alias('netsales_before'),
        ]).with_columns([
            ((pl.col('netsales') / pl.col('netsales_before')) - 1).alias('netsales_growth_percent'),
            (pl.col('netsales') - pl.col('netsales_before')).alias('netsales_growth_value')
        ])
        
        return netsales_quarter
    
    def calculate_roe(self, df_finance_annual: pl.DataFrame) -> pl.DataFrame:
        """ROEを計算"""
        roe_annual = (
            df_finance_annual
            .select([
                'DisclosedDate', 'LocalCode',
                'CurrentPeriodStartDate', 'CurrentPeriodEndDate',
                'Profit', 'Equity'
            ])
            .with_columns([
                pl.col('Profit').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0),
                pl.col('Equity').str.replace_all("", "0").str.replace_all("nan", "0").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            .with_columns([
                (pl.col('Profit') / pl.col('Equity')).alias('roe')
            ])
        )
        
        return roe_annual
    
    def filter_eps_annual_stocks(self, eps_annual: pl.DataFrame) -> list:
        """年次EPSベースで優良銘柄をフィルタリング
        - 直近3年のEPSがプラス
        - 直近3年のEPS成長率が最低でも各25%以上
        """
        # 直近3年のデータのみをフィルタリング
        eps_annual_filter = (
            eps_annual
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
        )
        
        # 直近3年のEPSがプラス(0より大きい)ティッカーのみを抽出
        min_eps = (
            eps_annual_filter
            .group_by('LocalCode')
            .agg(pl.col('eps').min())
            .filter(pl.col('eps') > 0)
        )
        eps_annual_filter = eps_annual_filter.filter(
            pl.col('LocalCode').is_in(min_eps['LocalCode'])
        )
        
        # 年ごとの成長率が最低25%以上のものを抽出
        min_growth = (
            eps_annual_filter
            .group_by('LocalCode')
            .agg(pl.col('eps_growth_percent').min())
            .filter(pl.col('eps_growth_percent') > 0.25)
        )
        eps_annual_filter = eps_annual_filter.filter(
            pl.col('LocalCode').is_in(min_growth['LocalCode'])
        )
        
        return eps_annual_filter['LocalCode'].unique().to_list()
    
    def filter_eps_quarterly_stocks(self, eps_quarter: pl.DataFrame) -> list:
        """四半期EPSベースで優良銘柄をフィルタリング
        - 直近3クォーターのEPSがプラス
        - 直近3クォーターのEPS成長率が最低でも各25%以上
        - 直近3クォーターのEPS成長率(or EPS成長差分)が単調増加している
        """
        # 直近3クォーターのデータのみをフィルタリング
        eps_quarter_filter = (
            eps_quarter
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
        )
        
        # 直近3クォーターのEPSがプラス
        min_eps = (
            eps_quarter_filter
            .group_by('LocalCode')
            .agg(pl.col('eps').min())
            .filter(pl.col('eps') > 0)
        )
        eps_quarter_filter = eps_quarter_filter.filter(
            pl.col('LocalCode').is_in(min_eps['LocalCode'])
        )
        
        # クォーターごとの成長率が最低25%以上
        min_growth = (
            eps_quarter_filter
            .group_by('LocalCode')
            .agg(pl.col('eps_growth_percent').min())
            .filter(pl.col('eps_growth_percent') > 0.25)
        )
        eps_quarter_filter = eps_quarter_filter.filter(
            pl.col('LocalCode').is_in(min_growth['LocalCode'])
        )
        
        # EPS成長が単調増加しているものを絞り込み
        eps_quarter_filter = eps_quarter_filter.with_columns([
            pl.col('eps_growth_value')
            .rank(method='dense', descending=True)
            .over('LocalCode')
            .alias('growth_rank')
        ])
        
        # クォーター順と成長率順の一致性をチェック
        eps_quarter_filter = eps_quarter_filter.with_columns([
            pl.when(pl.col('growth_rank') == pl.col('rank'))
            .then(1)
            .otherwise(0)
            .alias('growth_flg')
        ])
        
        # 一致しているティッカーのみを抽出
        consistent_growth = (
            eps_quarter_filter
            .group_by('LocalCode')
            .agg(pl.col('growth_flg').min())
            .filter(pl.col('growth_flg') == 1)
        )
        
        return consistent_growth['LocalCode'].to_list()
    
    def filter_netsales_quarterly_stocks(self, netsales_quarter: pl.DataFrame) -> list:
        """売上高データに基づくフィルタリング
        - 直近3四半期の売上成長率が増加している
        - または、直近の四半期の売上が25%以上成長している
        """
        # 直近3クォーターのデータのみをフィルタリング
        netsales_quarter_filter = (
            netsales_quarter
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
        )
        
        # クォーターごとの売上成長率がプラスのものを抽出
        positive_growth = (
            netsales_quarter_filter
            .group_by('LocalCode')
            .agg(pl.col('netsales_growth_percent').min())
            .filter(pl.col('netsales_growth_percent') > 0)
        )
        target_symbol1 = positive_growth['LocalCode'].to_list()
        
        # 直近の売上成長率が25％以上のものを抽出
        high_recent_growth = (
            netsales_quarter_filter
            .filter((pl.col('rank') == 1) & (pl.col('netsales_growth_percent') > 0.25))
        )
        target_symbol2 = high_recent_growth['LocalCode'].to_list()
        
        # 両方の条件のORを取る
        target_symbols = sorted(list(set(target_symbol1) | set(target_symbol2)))
        
        return target_symbols
    
    def filter_roe_stocks(self, roe_annual: pl.DataFrame) -> list:
        """ROEデータに基づくフィルタリング
        - 直近1年のROEが15%を超える
        """
        # 直近1年のデータのみをフィルタリング
        roe_annual_filter = (
            roe_annual
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .filter(pl.col('roe') > 0.15)
        )
        
        return roe_annual_filter['LocalCode'].to_list()
    
    def _save_target_metrics(self, target_codes: list, eps_annual: pl.DataFrame, 
                           eps_quarter: pl.DataFrame, netsales_annual: pl.DataFrame,
                           netsales_quarter: pl.DataFrame, roe_annual: pl.DataFrame, 
                           suffix: str):
        """対象銘柄の指標値をCSVファイルに保存"""
        
        # 対象銘柄の年次EPS
        target_eps_annual = eps_annual.filter(
            pl.col('LocalCode').is_in(target_codes)
        ).sort(['LocalCode', 'CurrentPeriodEndDate'])
        
        # 対象銘柄の四半期EPS
        target_eps_quarter = eps_quarter.filter(
            pl.col('LocalCode').is_in(target_codes)
        ).sort(['LocalCode', 'CurrentPeriodEndDate'])
        
        # 対象銘柄の年次売上高
        target_netsales_annual = netsales_annual.filter(
            pl.col('LocalCode').is_in(target_codes)
        ).sort(['LocalCode', 'CurrentPeriodEndDate'])
        
        # 対象銘柄の四半期売上高
        target_netsales_quarter = netsales_quarter.filter(
            pl.col('LocalCode').is_in(target_codes)
        ).sort(['LocalCode', 'CurrentPeriodEndDate'])
        
        # 対象銘柄のROE
        target_roe_annual = roe_annual.filter(
            pl.col('LocalCode').is_in(target_codes)
        ).sort(['LocalCode', 'CurrentPeriodEndDate'])
        
        # 企業情報も含めて統合データを作成
        target_listed_info = self.df_listed_info.filter(
            pl.col('Code').is_in(target_codes)
        ).select(['Code', 'CompanyName', 'Sector17CodeName', 'MarketCode'])
        
        # 各指標の最新値を取得
        latest_eps_annual = (
            target_eps_annual
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .select(['LocalCode', 'eps', 'eps_growth_percent', 'CurrentPeriodEndDate'])
            .rename({
                'eps': 'latest_annual_eps',
                'eps_growth_percent': 'latest_annual_eps_growth',
                'CurrentPeriodEndDate': 'latest_annual_date'
            })
        )
        
        latest_eps_quarter = (
            target_eps_quarter
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .select(['LocalCode', 'eps', 'eps_growth_percent', 'CurrentPeriodEndDate'])
            .rename({
                'eps': 'latest_quarter_eps',
                'eps_growth_percent': 'latest_quarter_eps_growth',
                'CurrentPeriodEndDate': 'latest_quarter_date'
            })
        )
        
        latest_netsales_annual = (
            target_netsales_annual
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .select(['LocalCode', 'netsales', 'netsales_growth_percent'])
            .rename({
                'netsales': 'latest_annual_netsales',
                'netsales_growth_percent': 'latest_annual_netsales_growth'
            })
        )
        
        latest_netsales_quarter = (
            target_netsales_quarter
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .select(['LocalCode', 'netsales', 'netsales_growth_percent'])
            .rename({
                'netsales': 'latest_quarter_netsales',
                'netsales_growth_percent': 'latest_quarter_netsales_growth'
            })
        )
        
        latest_roe = (
            target_roe_annual
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') == 1)
            .select(['LocalCode', 'roe'])
            .rename({'roe': 'latest_roe'})
        )
        
        # 全データをマージして統合データを作成
        comprehensive_metrics = (
            target_listed_info
            .rename({'Code': 'LocalCode'})
            .join(latest_eps_annual, on='LocalCode', how='left')
            .join(latest_eps_quarter, on='LocalCode', how='left')
            .join(latest_netsales_annual, on='LocalCode', how='left')
            .join(latest_netsales_quarter, on='LocalCode', how='left')
            .join(latest_roe, on='LocalCode', how='left')
        )
        
        # CSVファイルに保存
        comprehensive_metrics.write_csv(self.output_dir / f'target_metrics_{suffix}.csv')
        
        # 各指標の詳細データも保存
        target_eps_annual.write_csv(self.output_dir / f'target_eps_annual_{suffix}.csv')
        target_eps_quarter.write_csv(self.output_dir / f'target_eps_quarter_{suffix}.csv')
        target_netsales_annual.write_csv(self.output_dir / f'target_netsales_annual_{suffix}.csv')
        target_netsales_quarter.write_csv(self.output_dir / f'target_netsales_quarter_{suffix}.csv')
        target_roe_annual.write_csv(self.output_dir / f'target_roe_annual_{suffix}.csv')
        
        print(f"指標値データを保存しました: target_metrics_{suffix}.csv")
    
    def _save_consolidated_target_metrics(self, all_condition_codes: list, eps_only_codes: list,
                                        eps_annual: pl.DataFrame, eps_quarter: pl.DataFrame,
                                        netsales_annual: pl.DataFrame, netsales_quarter: pl.DataFrame,
                                        roe_annual: pl.DataFrame):
        """全ての対象銘柄の指標値を1つのCSVファイルに統合して保存"""
        
        # 全ての対象銘柄を統合
        all_target_codes = list(set(all_condition_codes + eps_only_codes))
        
        if not all_target_codes:
            print("対象銘柄がないため、統合ファイルは作成されませんでした")
            return
        
        # 企業基本情報
        consolidated_info = self.df_listed_info.filter(
            pl.col('Code').is_in(all_target_codes)
        ).select(['Code', 'CompanyName', 'Sector17CodeName', 'MarketCode']).rename({'Code': 'LocalCode'})
        
        # 分類フラグを追加
        consolidated_info = consolidated_info.with_columns([
            pl.when(pl.col('LocalCode').is_in(all_condition_codes))
            .then(pl.lit("全条件"))
            .when(pl.col('LocalCode').is_in(eps_only_codes))
            .then(pl.lit("EPS条件のみ"))
            .otherwise(pl.lit("その他"))
            .alias('分類')
        ])
        
        # 各指標の最新値を取得する関数
        def get_latest_metrics(df, code_col, metrics_cols, prefix):
            return (
                df.filter(pl.col(code_col).is_in(all_target_codes))
                .with_columns(
                    pl.col('CurrentPeriodEndDate')
                    .rank(method='dense', descending=True)
                    .over(code_col)
                    .alias('rank')
                )
                .filter(pl.col('rank') == 1)
                .select([code_col, 'CurrentPeriodEndDate'] + metrics_cols)
                .rename({
                    **{col: f'{prefix}_{col}' for col in metrics_cols},
                    'CurrentPeriodEndDate': f'{prefix}_date'
                })
            )
        
        # 各指標の最新データを取得
        latest_eps_annual = get_latest_metrics(
            eps_annual, 'LocalCode', ['eps', 'eps_growth_percent'], '年次EPS'
        )
        
        latest_eps_quarter = get_latest_metrics(
            eps_quarter, 'LocalCode', ['eps', 'eps_growth_percent'], '四半期EPS'
        )
        
        latest_netsales_annual = get_latest_metrics(
            netsales_annual, 'LocalCode', ['netsales', 'netsales_growth_percent'], '年次売上高'
        )
        
        latest_netsales_quarter = get_latest_metrics(
            netsales_quarter, 'LocalCode', ['netsales', 'netsales_growth_percent'], '四半期売上高'
        )
        
        latest_roe = get_latest_metrics(
            roe_annual, 'LocalCode', ['roe'], 'ROE'
        )
        
        # 直近3年のEPS成長率の統計を計算
        eps_annual_stats = (
            eps_annual.filter(pl.col('LocalCode').is_in(all_target_codes))
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
            .group_by('LocalCode')
            .agg([
                pl.col('eps_growth_percent').min().alias('年次EPS成長率_最小'),
                pl.col('eps_growth_percent').mean().alias('年次EPS成長率_平均'),
                pl.col('eps_growth_percent').max().alias('年次EPS成長率_最大')
            ])
        )
        
        # 直近3四半期のEPS成長率の統計を計算
        eps_quarter_stats = (
            eps_quarter.filter(pl.col('LocalCode').is_in(all_target_codes))
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
            .group_by('LocalCode')
            .agg([
                pl.col('eps_growth_percent').min().alias('四半期EPS成長率_最小'),
                pl.col('eps_growth_percent').mean().alias('四半期EPS成長率_平均'),
                pl.col('eps_growth_percent').max().alias('四半期EPS成長率_最大')
            ])
        )
        
        # 直近3四半期の売上成長率の統計を計算
        netsales_quarter_stats = (
            netsales_quarter.filter(pl.col('LocalCode').is_in(all_target_codes))
            .with_columns(
                pl.col('CurrentPeriodEndDate')
                .rank(method='dense', descending=True)
                .over('LocalCode')
                .alias('rank')
            )
            .filter(pl.col('rank') <= 3)
            .group_by('LocalCode')
            .agg([
                pl.col('netsales_growth_percent').min().alias('四半期売上成長率_最小'),
                pl.col('netsales_growth_percent').mean().alias('四半期売上成長率_平均'),
                pl.col('netsales_growth_percent').max().alias('四半期売上成長率_最大')
            ])
        )
        
        # 全データを統合
        consolidated_metrics = (
            consolidated_info
            .join(latest_eps_annual, on='LocalCode', how='left')
            .join(latest_eps_quarter, on='LocalCode', how='left')
            .join(latest_netsales_annual, on='LocalCode', how='left')
            .join(latest_netsales_quarter, on='LocalCode', how='left')
            .join(latest_roe, on='LocalCode', how='left')
            .join(eps_annual_stats, on='LocalCode', how='left')
            .join(eps_quarter_stats, on='LocalCode', how='left')
            .join(netsales_quarter_stats, on='LocalCode', how='left')
        )
        
        # 列の順序を整理
        column_order = [
            'LocalCode', 'CompanyName', 'Sector17CodeName', 'MarketCode', '分類',
            '年次EPS_eps', '年次EPS_eps_growth_percent', '年次EPS_date',
            '年次EPS成長率_最小', '年次EPS成長率_平均', '年次EPS成長率_最大',
            '四半期EPS_eps', '四半期EPS_eps_growth_percent', '四半期EPS_date',
            '四半期EPS成長率_最小', '四半期EPS成長率_平均', '四半期EPS成長率_最大',
            '年次売上高_netsales', '年次売上高_netsales_growth_percent', '年次売上高_date',
            '四半期売上高_netsales', '四半期売上高_netsales_growth_percent', '四半期売上高_date',
            '四半期売上成長率_最小', '四半期売上成長率_平均', '四半期売上成長率_最大',
            'ROE_roe', 'ROE_date'
        ]
        
        # 存在する列のみを選択
        available_columns = [col for col in column_order if col in consolidated_metrics.columns]
        consolidated_metrics = consolidated_metrics.select(available_columns)
        
        # ソート（分類順、LocalCode順）
        consolidated_metrics = consolidated_metrics.sort(['分類', 'LocalCode'])
        
        # CSVファイルに保存
        consolidated_metrics.write_csv(self.output_dir / 'consolidated_target_metrics.csv')
        
        print(f"統合指標データを保存しました: consolidated_target_metrics.csv")
        print(f"対象銘柄数: {len(all_target_codes)} 銘柄")
        print(f"- 全条件満たす銘柄: {len(all_condition_codes)} 銘柄")
        print(f"- EPS条件のみ満たす銘柄: {len(eps_only_codes)} 銘柄")
        
        return consolidated_metrics
    
    def _save_windows_compatible_files(self, eps_annual: pl.DataFrame, eps_quarter: pl.DataFrame,
                                     netsales_annual: pl.DataFrame, netsales_quarter: pl.DataFrame,
                                     roe_annual: pl.DataFrame, consolidated_metrics: pl.DataFrame):
        """Windows互換形式（Shift_JIS + BOM）でファイルを保存"""
        import csv
        from pathlib import Path
        
        def save_with_sjis_bom(df: pl.DataFrame, filepath: Path):
            """Shift_JIS + BOMでCSVファイルを保存"""
            # DataFrameをリスト形式に変換
            columns = df.columns
            data = df.to_numpy().tolist()
            
            # Shift_JIS + BOMで保存
            with open(filepath, 'w', encoding='shift_jis', newline='', errors='replace') as f:
                # BOMを書き込み
                f.write('\ufeff')
                writer = csv.writer(f)
                # ヘッダーを書き込み
                writer.writerow(columns)
                # データを書き込み
                for row in data:
                    writer.writerow(row)
        
        print("Windows互換形式でファイルを保存中...")
        
        try:
            # 各ファイルをWindows互換形式で保存
            save_with_sjis_bom(eps_annual, self.output_dir_windows / 'eps_annual.csv')
            save_with_sjis_bom(eps_quarter, self.output_dir_windows / 'eps_quarter.csv')
            save_with_sjis_bom(netsales_annual, self.output_dir_windows / 'netsales_annual.csv')
            save_with_sjis_bom(netsales_quarter, self.output_dir_windows / 'netsales_quarter.csv')
            save_with_sjis_bom(roe_annual, self.output_dir_windows / 'roe_annual.csv')
            save_with_sjis_bom(consolidated_metrics, self.output_dir_windows / 'consolidated_target_metrics.csv')
            
            print(f"Windows互換ファイルを保存しました: {self.output_dir_windows}")
            
        except Exception as e:
            print(f"Windows互換ファイル保存中にエラーが発生しました: {e}")
    
    
    def run_analysis(self):
        """分析の実行"""
        print("年次業績データの抽出...")
        df_finance_annual = self.analyze_annual_performance()
        
        print("四半期業績データの抽出...")
        df_finance_quarter = self.analyze_quarterly_performance()
        
        print("EPS成長率の計算...")
        eps_annual = self.calculate_annual_eps_growth(df_finance_annual)
        eps_quarter = self.calculate_quarterly_eps_growth(df_finance_quarter)
        
        print("売上高成長率の計算...")
        netsales_annual = self.calculate_annual_netsales_growth(df_finance_annual)
        netsales_quarter = self.calculate_quarterly_netsales_growth(df_finance_quarter)
        
        print("ROEの計算...")
        roe_annual = self.calculate_roe(df_finance_annual)
        
        # 優良銘柄の抽出（フィルタリング用）
        print("優良銘柄の抽出...")
        eps_annual_filter_list = self.filter_eps_annual_stocks(eps_annual)
        eps_quarter_filter_list = self.filter_eps_quarterly_stocks(eps_quarter)
        netsales_quarter_list = self.filter_netsales_quarterly_stocks(netsales_quarter)
        roe_annual_filter_list = self.filter_roe_stocks(roe_annual)
        
        # 文字列型に変換
        roe_annual_filter_list = [str(x) for x in roe_annual_filter_list]
        eps_annual_filter_list = [str(x) for x in eps_annual_filter_list]
        eps_quarter_filter_list = [str(x) for x in eps_quarter_filter_list]
        netsales_quarter_list = [str(x) for x in netsales_quarter_list]
        
        # 全条件を満たす注目銘柄
        eps_target_list = list(
            set(roe_annual_filter_list) &
            set(netsales_quarter_list) &
            set(eps_quarter_filter_list) &
            set(eps_annual_filter_list)
        )
        
        # EPSの条件だけで抽出
        temp_eps = set(eps_quarter_filter_list) & set(eps_annual_filter_list)
        temp_eps = [str(x) for x in temp_eps]
        
        # 全ての対象銘柄を統合（フィルタリング用）
        all_target_codes = list(set(eps_target_list + temp_eps))
        
        # フィルタリング後のデータを保存
        print(f"フィルタリング後のデータを{self.output_dir}に保存...")
        
        if all_target_codes:
            # 対象銘柄のみのデータを抽出して保存
            filtered_eps_annual = eps_annual.filter(pl.col('LocalCode').is_in(all_target_codes))
            filtered_eps_quarter = eps_quarter.filter(pl.col('LocalCode').is_in(all_target_codes))
            filtered_netsales_annual = netsales_annual.filter(pl.col('LocalCode').is_in(all_target_codes))
            filtered_netsales_quarter = netsales_quarter.filter(pl.col('LocalCode').is_in(all_target_codes))
            filtered_roe_annual = roe_annual.filter(pl.col('LocalCode').is_in(all_target_codes))
            
            # フィルタリング後のデータを保存
            filtered_eps_annual.write_csv(self.output_dir / 'eps_annual.csv')
            filtered_eps_quarter.write_csv(self.output_dir / 'eps_quarter.csv')
            filtered_netsales_annual.write_csv(self.output_dir / 'netsales_annual.csv')
            filtered_netsales_quarter.write_csv(self.output_dir / 'netsales_quarter.csv')
            filtered_roe_annual.write_csv(self.output_dir / 'roe_annual.csv')
            
            print(f"フィルタリング完了: {len(all_target_codes)}銘柄のデータを保存")
        else:
            print("フィルタリング条件を満たす銘柄がないため、空のファイルを保存します")
            # 空のDataFrameを保存（ヘッダーのみ）
            eps_annual.head(0).write_csv(self.output_dir / 'eps_annual.csv')
            eps_quarter.head(0).write_csv(self.output_dir / 'eps_quarter.csv')
            netsales_annual.head(0).write_csv(self.output_dir / 'netsales_annual.csv')
            netsales_quarter.head(0).write_csv(self.output_dir / 'netsales_quarter.csv')
            roe_annual.head(0).write_csv(self.output_dir / 'roe_annual.csv')
        
        print(f"全条件を満たす注目銘柄: {eps_target_list}")
        
        # 対象銘柄の情報を抽出
        if eps_target_list:
            target_listed_info = self.df_listed_info.filter(
                pl.col('Code').is_in(eps_target_list)
            )
            target_listed_info.write_csv(self.output_dir / 'target_listed_info.csv')
            print(f"注目銘柄の情報を保存しました")
            print(target_listed_info)
        
        print(f"\nEPS条件のみで抽出した注目銘柄: {temp_eps}")
        
        if temp_eps:
            eps_only_info = self.df_listed_info.filter(
                pl.col('Code').is_in(temp_eps)
            )
            print(eps_only_info)
        
        # 統合指標データを作成・保存
        consolidated_metrics = self._save_consolidated_target_metrics(eps_target_list, temp_eps, eps_annual, eps_quarter,
                                             netsales_annual, netsales_quarter, roe_annual)
        
        # Windows互換形式でファイルを保存（フィルタリング後のデータ）
        if all_target_codes:
            self._save_windows_compatible_files(filtered_eps_annual, filtered_eps_quarter, filtered_netsales_annual, 
                                              filtered_netsales_quarter, filtered_roe_annual, consolidated_metrics)
        else:
            # 対象銘柄がない場合は空のデータフレームで保存
            self._save_windows_compatible_files(eps_annual.head(0), eps_quarter.head(0), netsales_annual.head(0), 
                                              netsales_quarter.head(0), roe_annual.head(0), consolidated_metrics)
        
        
        return {
            'eps_annual': filtered_eps_annual if all_target_codes else eps_annual.head(0),
            'eps_quarter': filtered_eps_quarter if all_target_codes else eps_quarter.head(0),
            'netsales_annual': filtered_netsales_annual if all_target_codes else netsales_annual.head(0),
            'netsales_quarter': filtered_netsales_quarter if all_target_codes else netsales_quarter.head(0),
            'roe_annual': filtered_roe_annual if all_target_codes else roe_annual.head(0),
            'target_stocks': eps_target_list,
            'eps_only_stocks': temp_eps
        }


# 使用例
if __name__ == "__main__":
    # エンジンの初期化と分析の実行
    engine = JapanStockAnalysisEngine()
    results = engine.run_analysis()
    
    print("\n分析完了！")
    print(f"結果は ./agg_data/{engine.today_str}/ に保存されました")