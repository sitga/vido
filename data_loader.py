#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据加载模块
支持生成模拟数据和读取外部CSV/Excel文件
"""

from typing import Optional, Union, List
from pathlib import Path
import warnings

import pandas as pd
import numpy as np

from config import config, DataConfig
from utils import logger, validate_file_path, handle_exceptions


class DataLoader:
    """
    数据加载器类
    支持模拟数据生成和外部文件读取
    """
    
    # 标准列名映射（支持中英文）
    COLUMN_MAPPING = {
        # 中文列名 -> 标准列名
        '用户ID': 'user_id',
        '视频ID': 'video_id',
        '观看时长（秒）': 'watch_duration',
        '观看时长': 'watch_duration',
        '完播状态': 'completion_status',
        '发布时间': 'publish_time',
        '视频类别': 'video_category',
        '类别': 'video_category',
        '视频时长（秒）': 'video_duration',
        '视频时长': 'video_duration',
        # 英文列名 -> 标准列名
        'user_id': 'user_id',
        'video_id': 'video_id',
        'watch_duration': 'watch_duration',
        'completion_status': 'completion_status',
        'publish_time': 'publish_time',
        'video_category': 'video_category',
        'video_duration': 'video_duration'
    }
    
    # 必需列
    REQUIRED_COLUMNS = ['user_id', 'video_id', 'watch_duration', 
                        'video_duration', 'video_category']
    
    def __init__(self, data_config: Optional[DataConfig] = None):
        """
        初始化数据加载器
        
        Args:
            data_config: 数据配置对象，默认使用全局配置
        """
        self.config = data_config or config.data
        self._video_duration_cache: Optional[dict] = None
    
    def generate_mock_data(self, n_records: Optional[int] = None, 
                          random_seed: Optional[int] = None) -> pd.DataFrame:
        """
        生成模拟的短视频观看数据
        
        Args:
            n_records: 数据条数，默认使用配置值
            random_seed: 随机种子，默认使用配置值
            
        Returns:
            生成的模拟数据DataFrame
        """
        n_records = n_records or self.config.default_record_count
        random_seed = random_seed or self.config.random_seed
        
        logger.info(f"开始生成模拟数据，记录数: {n_records}")
        
        np.random.seed(random_seed)
        
        # 用户ID和视频ID
        user_ids = np.random.randint(
            self.config.user_id_min, 
            self.config.user_id_max, 
            size=n_records
        )
        video_ids = np.random.randint(
            self.config.video_id_min, 
            self.config.video_id_max, 
            size=n_records
        )
        
        # 视频类别
        video_categories = np.random.choice(
            self.config.video_categories, 
            size=n_records
        )
        
        # 发布时间（近30天）
        publish_dates = pd.date_range(
            start=self.config.start_date, 
            end=self.config.end_date, 
            periods=n_records
        )
        publish_dates = np.random.choice(publish_dates, size=n_records)
        
        # 观看时长（秒）
        watch_durations = self._generate_watch_durations(n_records)
        
        # 视频时长（缓存优化）
        video_durations = self._generate_video_durations(video_ids)
        
        # 完播状态
        completion_status = watch_durations >= (
            video_durations * self.config.completion_threshold
        )
        
        # 构建DataFrame
        df = pd.DataFrame({
            'user_id': user_ids,
            'video_id': video_ids,
            'watch_duration': watch_durations,
            'completion_status': completion_status,
            'publish_time': publish_dates,
            'video_category': video_categories,
            'video_duration': video_durations
        })
        
        logger.info(f"模拟数据生成完成，共 {len(df)} 条记录")
        return df
    
    def _generate_watch_durations(self, n_records: int) -> np.ndarray:
        """
        生成观看时长数据
        
        Args:
            n_records: 记录数
            
        Returns:
            观看时长数组
        """
        dist = self.config.watch_duration_distribution
        ranges = self.config.watch_duration_ranges
        
        durations = []
        
        # 无效观看
        count = int(n_records * dist['invalid'])
        durations.append(np.random.randint(
            ranges['invalid'][0], ranges['invalid'][1], size=count
        ))
        
        # 短观看
        count = int(n_records * dist['short'])
        durations.append(np.random.randint(
            ranges['short'][0], ranges['short'][1], size=count
        ))
        
        # 中等观看
        count = int(n_records * dist['medium'])
        durations.append(np.random.randint(
            ranges['medium'][0], ranges['medium'][1], size=count
        ))
        
        # 长观看
        count = n_records - sum(len(d) for d in durations)
        durations.append(np.random.randint(
            ranges['long'][0], ranges['long'][1], size=count
        ))
        
        watch_durations = np.concatenate(durations)
        np.random.shuffle(watch_durations)
        
        return watch_durations
    
    def _generate_video_durations(self, video_ids: np.ndarray) -> np.ndarray:
        """
        生成视频时长数据（使用缓存优化）
        
        Args:
            video_ids: 视频ID数组
            
        Returns:
            视频时长数组
        """
        if self._video_duration_cache is None:
            unique_videos = np.unique(video_ids)
            self._video_duration_cache = {
                vid: np.random.randint(
                    self.config.video_duration_min, 
                    self.config.video_duration_max + 1
                ) 
                for vid in unique_videos
            }
        
        return np.array([self._video_duration_cache[vid] for vid in video_ids])
    
    @handle_exceptions(default_return=None, log_level='error')
    def load_from_csv(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """
        从CSV文件加载数据
        
        Args:
            file_path: CSV文件路径
            **kwargs: 传递给pd.read_csv的额外参数
            
        Returns:
            加载的DataFrame
        """
        file_path = validate_file_path(
            file_path, 
            must_exist=True, 
            allowed_extensions=['csv']
        )
        
        logger.info(f"从CSV加载数据: {file_path}")
        
        default_kwargs = {
            'encoding': config.file.default_encoding,
            'sep': config.file.csv_separator
        }
        default_kwargs.update(kwargs)
        
        df = pd.read_csv(file_path, **default_kwargs)
        df = self._standardize_columns(df)
        
        logger.info(f"CSV数据加载完成，共 {len(df)} 条记录")
        return df
    
    @handle_exceptions(default_return=None, log_level='error')
    def load_from_excel(self, file_path: Union[str, Path], 
                        sheet_name: Optional[Union[str, int]] = None,
                        **kwargs) -> pd.DataFrame:
        """
        从Excel文件加载数据
        
        Args:
            file_path: Excel文件路径
            sheet_name: 工作表名称或索引
            **kwargs: 传递给pd.read_excel的额外参数
            
        Returns:
            加载的DataFrame
        """
        file_path = validate_file_path(
            file_path, 
            must_exist=True, 
            allowed_extensions=['xlsx', 'xls']
        )
        
        logger.info(f"从Excel加载数据: {file_path}")
        
        if sheet_name is None:
            sheet_name = config.file.excel_sheet_name
        
        df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        df = self._standardize_columns(df)
        
        logger.info(f"Excel数据加载完成，共 {len(df)} 条记录")
        return df
    
    def load_data(self, source: Optional[Union[str, Path]] = None,
                  data_type: str = 'mock',
                  n_records: Optional[int] = None) -> pd.DataFrame:
        """
        统一数据加载接口
        
        Args:
            source: 数据源（文件路径或None）
            data_type: 数据类型 ('mock', 'csv', 'excel')
            n_records: 模拟数据记录数
            
        Returns:
            加载的DataFrame
            
        Raises:
            ValueError: 不支持的数据类型
        """
        if data_type == 'mock':
            return self.generate_mock_data(n_records)
        elif data_type == 'csv':
            if source is None:
                raise ValueError("CSV数据源需要提供文件路径")
            return self.load_from_csv(source)
        elif data_type in ['excel', 'xlsx', 'xls']:
            if source is None:
                raise ValueError("Excel数据源需要提供文件路径")
            return self.load_from_excel(source)
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化列名（中英文映射）
        
        Args:
            df: 原始DataFrame
            
        Returns:
            列名标准化后的DataFrame
        """
        # 创建列名映射
        rename_map = {}
        for col in df.columns:
            if col in self.COLUMN_MAPPING:
                rename_map[col] = self.COLUMN_MAPPING[col]
        
        if rename_map:
            df = df.rename(columns=rename_map)
            logger.debug(f"列名标准化: {rename_map}")
        
        # 检查必需列
        missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            logger.warning(f"缺少推荐列: {missing_cols}")
        
        # 数据类型转换
        df = self._convert_data_types(df)
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        转换数据类型
        
        Args:
            df: 原始DataFrame
            
        Returns:
            类型转换后的DataFrame
        """
        # 时间列转换
        if 'publish_time' in df.columns:
            df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
        
        # 布尔列转换
        if 'completion_status' in df.columns:
            if df['completion_status'].dtype != bool:
                df['completion_status'] = df['completion_status'].astype(bool)
        
        return df
    
    def validate_data(self, df: pd.DataFrame, 
                     raise_on_error: bool = True) -> bool:
        """
        验证数据完整性
        
        Args:
            df: 待验证的DataFrame
            raise_on_error: 验证失败时是否抛出异常
            
        Returns:
            验证是否通过
        """
        try:
            if df is None or df.empty:
                raise ValueError("数据为空")
            
            # 检查必需列
            missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
            if missing_cols:
                raise ValueError(f"缺少必需列: {missing_cols}")
            
            # 检查数据有效性
            if (df['watch_duration'] < 0).any():
                raise ValueError("观看时长存在负值")
            
            if (df['video_duration'] <= 0).any():
                raise ValueError("视频时长必须大于0")
            
            logger.info("数据验证通过")
            return True
            
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            if raise_on_error:
                raise
            return False


# 便捷函数
def load_mock_data(n_records: Optional[int] = None, 
                   random_seed: Optional[int] = None) -> pd.DataFrame:
    """
    快速生成模拟数据
    
    Args:
        n_records: 记录数
        random_seed: 随机种子
        
    Returns:
        模拟数据DataFrame
    """
    loader = DataLoader()
    return loader.generate_mock_data(n_records, random_seed)


def load_from_file(file_path: Union[str, Path], 
                   **kwargs) -> pd.DataFrame:
    """
    从文件加载数据（自动识别格式）
    
    Args:
        file_path: 文件路径
        **kwargs: 额外参数
        
    Returns:
        加载的DataFrame
    """
    loader = DataLoader()
    
    path = Path(file_path)
    ext = path.suffix.lower()
    
    if ext == '.csv':
        return loader.load_from_csv(file_path, **kwargs)
    elif ext in ['.xlsx', '.xls']:
        return loader.load_from_excel(file_path, **kwargs)
    else:
        raise ValueError(f"不支持的文件格式: {ext}")
