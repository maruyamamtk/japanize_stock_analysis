"""
統合日本株分析システム - Coreモジュール
リファクタリング後の統合機能を提供
"""

from .config import ConfigurationManager, ApiConfig, PathConfig
from .data_manager import UnifiedDataManager, JQuantsAPIConnector
from .utilities import LoggingManager, FileOperations, DataProcessor, TimeStampGenerator, BusinessDayChecker

__version__ = "3.0.0"
__author__ = "Japan Stock Analysis System"

__all__ = [
    # 設定管理
    'ConfigurationManager',
    'ApiConfig',
    'PathConfig',
    
    # データ管理
    'UnifiedDataManager',
    'JQuantsAPIConnector',
    
    # ユーティリティ
    'LoggingManager',
    'FileOperations',
    'DataProcessor',
    'TimeStampGenerator',
    'BusinessDayChecker'
]
