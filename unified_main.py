"""
統合日本株分析システム
リファクタリング後のメインアプリケーション
"""

import argparse
import sys
from pathlib import Path

# 新しい統合モジュールをインポート
from core.config import ConfigurationManager
from core.data_manager import UnifiedDataManager
from core.utilities import LoggingManager
from core.analysis_engine import JapanStockAnalysisEngine


class JapanStockAnalysisSystem:
    """日本株分析統合システム（リファクタリング版）"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config = ConfigurationManager(config_path)
        self.logger = LoggingManager.setup_logger("JapanStockSystem")
        
        # 設定検証
        if not self.config.validate_configuration():
            self.logger.error("設定が無効です。config.jsonを確認してください。")
            sys.exit(1)
        
        # データマネージャーを初期化
        self.data_manager = UnifiedDataManager(config_path)
        
        # 分析エンジンを初期化
        data_dir = self.config.get('data_directory', 'C:\\Users\\michika\\Desktop\\日本株分析\\data')
        self.analysis_engine = JapanStockAnalysisEngine(data_dir)
    
    def execute_data_collection(self, mode: str):
        """データ収集の実行"""
        self.logger.info(f"データ収集開始: {mode}")
        
        if mode == "bulk-stock":
            # 株価データ一括取得
            return self.data_manager.bulk_fetch_stock_data()
        elif mode == "incremental-stock":
            # 株価データ差分取得
            return self.data_manager.incremental_fetch_stock_data()
        elif mode == "bulk-finance":
            # 財務データ一括取得
            return self.data_manager.bulk_fetch_financial_data()
        elif mode == "all":
            # 全データ一括取得
            self.data_manager.bulk_fetch_stock_data()
            self.data_manager.bulk_fetch_financial_data()
        else:
            raise ValueError(f"不正なデータ収集モード: {mode}")
        
        self.logger.info("データ収集完了")
    
    def execute_analysis(self, top_n: int = 50):
        """分析の実行"""
        self.logger.info(f"分析開始（上位{top_n}銘柄を抽出）")
        
        try:
            # 分析エンジンを使用して分析を実行
            results = self.analysis_engine.run_analysis()
            
            # 結果から上位N銘柄を抽出
            if results['target_stocks']:
                target_codes = results['target_stocks'][:top_n]
                
                # 銘柄情報を取得
                target_info = self.analysis_engine.df_listed_info.filter(
                    self.analysis_engine.df_listed_info['Code'].is_in(target_codes)
                )
                
                self.logger.info(f"分析完了: {len(target_codes)}銘柄を抽出")
                return target_info
            else:
                self.logger.warning("抽出された銘柄がありませんでした")
                return None
                
        except Exception as e:
            self.logger.error(f"分析中にエラーが発生しました: {e}")
            return None
    
    def execute_full_pipeline(self, top_n: int = 50, data_mode: str = "incremental-stock"):
        """完全パイプラインの実行"""
        self.logger.info("===== 完全パイプライン開始 =====")
        
        # データ収集
        self.execute_data_collection(data_mode)
        
        # 分析実行
        result_df = self.execute_analysis(top_n)
        
        self.logger.info("===== 完全パイプライン完了 =====")
        return result_df


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='日本株分析統合システム（リファクタリング版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
  実行例:
  # 分析のみ
  python unified_main.py --mode analysis --top-n 50
  
  # データ取得
  # python unified_main.py --mode data --data-mode bulk-stock
  
  # 完全パイプライン
  # python unified_main.py --mode pipeline --data-mode incremental-stock --top-n 30
        """
    )
    
    parser.add_argument(
        '--mode', 
        choices=['data', 'analysis', 'pipeline'], 
        default='pipeline',
        help='実行モード (data: データ取得, analysis: 分析, pipeline: 全実行)'
    )
    
    parser.add_argument(
        '--data-mode',
        choices=['bulk-stock', 'incremental-stock', 'bulk-finance', 'all'],
        default='incremental-stock',
        help='データ取得モード'
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=50,
        help='分析で抽出する上位銘柄数'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='設定ファイルのパス'
    )
    
    
    args = parser.parse_args()
    
    try:
        system = JapanStockAnalysisSystem(args.config)
        
        if args.mode == 'data':
            print(f"データ取得を開始（{args.data_mode}）...")
            system.execute_data_collection(args.data_mode)
            print("データ取得完了")
            
        elif args.mode == 'analysis':
            print(f"分析を開始（上位{args.top_n}銘柄）...")
            result = system.execute_analysis(args.top_n)
            
            if result is not None:
                print(f"分析完了: {len(result)} 銘柄を抽出")
                
                # 上位5銘柄を表示
                if len(result) >= 5:
                    display_cols = ['Code', 'CompanyName', 'CompositeScore']
                    available_cols = [col for col in display_cols if col in result.columns]
                    print("\n上位5銘柄:")
                    print(result.select(available_cols).head(5))
            else:
                print("抽出された銘柄がありませんでした")
            
        elif args.mode == 'pipeline':
            print(f"完全パイプラインを開始（{args.data_mode} + 上位{args.top_n}銘柄）...")
            result = system.execute_full_pipeline(args.top_n, args.data_mode)
            
            if result is not None:
                print(f"パイプライン完了: {len(result)} 銘柄を抽出")
                
                # 上位10銘柄を表示
                if len(result) >= 10:
                    display_cols = ['Code', 'CompanyName', 'Sector17CodeName', 'CompositeScore']
                    available_cols = [col for col in display_cols if col in result.columns]
                    print("\n上位10銘柄:")
                    print(result.select(available_cols).head(10))
            else:
                print("抽出された銘柄がありませんでした")
        
        print("\n処理が完了しました。")
        print("結果は data/analysis_results フォルダに保存されています。")
        
    except KeyboardInterrupt:
        print("\n処理が中断されました。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
