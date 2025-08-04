"""
統合設定管理モジュール
アプリケーション全体の設定を一元管理
"""

import json
import os
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ApiConfig:
    """API設定"""
    mail_address: str
    password: str
    base_url: str = "https://api.jquants.com/v1"
    rate_limit_delay: float = 0.1
    retry_attempts: int = 3


@dataclass
class PathConfig:
    """パス設定"""
    output_directory: Path
    stock_price_file: Path
    finance_file: Path
    listed_info_file: Path
    analysis_results_dir: Path


class ConfigurationManager:
    """統合設定管理クラス"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self._api_config = None
        self._path_config = None
        self._config_data = None
        self._load_configuration()
    
    def _load_configuration(self):
        """設定ファイルの読み込み"""
        if not self.config_path.exists():
            self._create_default_configuration()
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # 設定データを保存
        self._config_data = config_data
        
        # API設定
        self._api_config = ApiConfig(
            mail_address=config_data.get('mail_address', ''),
            password=config_data.get('password', ''),
            base_url=config_data.get('api_settings', {}).get('base_url', 'https://api.jquants.com/v1'),
            rate_limit_delay=config_data.get('api_settings', {}).get('rate_limit_delay', 0.1),
            retry_attempts=config_data.get('api_settings', {}).get('retry_attempts', 3)
        )
        
        # パス設定
        base_dir = Path(config_data.get('output_directory', 'C:/Users/michika/Desktop/日本株分析/data'))
        self._path_config = PathConfig(
            output_directory=base_dir,
            stock_price_file=base_dir / "stock_price" / "stock_data.csv",
            finance_file=base_dir / "finance" / "finance_data.csv",
            listed_info_file=base_dir / "listed_companies.csv",
            analysis_results_dir=base_dir / "analysis_results"
        )
    
    def _create_default_configuration(self):
        """デフォルト設定ファイルの作成"""
        default_config = {
            "mail_address": "YOUR_EMAIL_HERE",
            "password": "YOUR_PASSWORD_HERE",
            "output_directory": "C:/Users/michika/Desktop/日本株分析/data",
            "api_settings": {
                "base_url": "https://api.jquants.com/v1",
                "rate_limit_delay": 0.1,
                "retry_attempts": 3
            }
        }
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
    
    @property
    def api(self) -> ApiConfig:
        """API設定の取得"""
        return self._api_config
    
    @property
    def paths(self) -> PathConfig:
        """パス設定の取得"""
        return self._path_config
    
    def get(self, key: str, default=None):
        """設定値の取得（辞書スタイル）"""
        return self._config_data.get(key, default)
    
    def validate_configuration(self) -> bool:
        """設定の妥当性チェック"""
        if self.api.mail_address == "YOUR_EMAIL_HERE":
            return False
        if self.api.password == "YOUR_PASSWORD_HERE":
            return False
        return True
