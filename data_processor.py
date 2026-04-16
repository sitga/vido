#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理模块
优化groupby聚合逻辑，提供高效的数据处理功能
"""

from typing import Optional, Dict, Any, List, Tuple
from functools import lru_cache

import pandas as pd
import numpy as np

from config import config, DataConfig
from utils import logger, validate_dataframe, memoize


class DataProcessor:
    """
    数据处理器类
    提供数据清洗、过滤、聚合等功能
    """
    
    def __init__(self, data_config: Optional[DataConfig] = None):
        """
        初始化数据处理器
        
        Args:
            data_config: 数据配置对象，默认使用全局配置
        """
        self.config = data_config or config.data
        self._filtered_df: Optional[pd.DataFrame] = None
        self._category_stats: Optional[pd.DataFrame] = None
    
    def filter_invalid_watch(self, df: pd.DataFrame, 
                            min_duration: Optional[int] = None,
                            inplace: bool = False) -> pd.DataFrame:
        """
        过滤无效观看记录（观看时长过短）
        
        Args:
            df: 原始数据DataFrame
            min_duration: 最小有效观看时长，默认使用配置值
            inplace: 是否原地修改
            
        Returns:
            过滤后的DataFrame
        """
        min_duration = min_duration or self.config.min_watch_duration
        
        initial_count = len(df)
        
        if inplace:
            df = df[df['watch_duration'] >= min_duration]
        else:
            # 使用视图而非复制，提高性能
            mask = df['watch_duration'] >= min_duration
            df = df[mask]
        
        filtered_count = len(df)
        filter_ratio = (1 - filtered_count / initial_count) * 100
        
        logger.info(f"数据过滤: 原始 {initial_count} 条 -> 过滤后 {filtered_count} 条 "
                   f"(过滤比例: {filter_ratio:.2f}%)")
        
        self._filtered_df = df
        return df
    
    def calculate_category_stats(self, df: Optional[pd.DataFrame] = None,
                                 use_pivot: bool = True) -> pd.DataFrame:
        """
        按视频类别计算统计数据
        
        使用pd.pivot_table优化groupby性能
        
        Args:
            df: 输入数据，默认使用已过滤的数据
            use_pivot: 是否使用pivot_table（性能更优）
            
        Returns:
            类别统计DataFrame
        """
        if df is None:
            df = self._filtered_df
        
        if df is None or df.empty:
            raise ValueError("输入数据为空")
        
        logger.info("开始计算类别统计数据...")
        
        if use_pivot:
            # 使用pivot_table优化性能
            category_stats = self._calculate_with_pivot(df)
        else:
            # 使用传统groupby
            category_stats = self._calculate_with_groupby(df)
        
        # 排序
        category_stats = category_stats.sort_values('completion_rate', ascending=False)
        
        self._category_stats = category_stats
        
        logger.info(f"类别统计完成，共 {len(category_stats)} 个类别")
        return category_stats
    
    def _calculate_with_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        使用pivot_table计算类别统计（性能优化版）
        
        Args:
            df: 输入数据
            
        Returns:
            类别统计DataFrame
        """
        # 使用pivot_table一次性计算多个聚合指标
        pivot = pd.pivot_table(
            df,
            values=['completion_status', 'watch_duration', 'video_duration'],
            index='video_category',
            aggfunc={
                'completion_status': ['count', 'mean'],
                'watch_duration': 'mean',
                'video_duration': 'mean'
            }
        )
        
        # 展平多级列名
        pivot.columns = ['_'.join(col).strip() if col[1] else col[0] 
                        for col in pivot.columns.values]
        
        # 重命名列
        column_map = {
            'completion_status_count': 'watch_count',
            'completion_status_mean': 'completion_rate',
            'watch_duration_mean': 'avg_watch_duration',
            'video_duration_mean': 'avg_video_duration'
        }
        
        category_stats = pivot.rename(columns=column_map)
        
        # 转换完播率为百分比
        category_stats['completion_rate'] = category_stats['completion_rate'] * 100
        
        # 四舍五入
        decimal_places = config.analysis.decimal_places
        category_stats = category_stats.round(decimal_places)
        
        return category_stats
    
    def _calculate_with_groupby(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        使用groupby计算类别统计（兼容版）
        
        Args:
            df: 输入数据
            
        Returns:
            类别统计DataFrame
        """
        stats = df.groupby('video_category').agg({
            'completion_status': ['count', 'mean'],
            'watch_duration': 'mean',
            'video_duration': 'mean'
        })
        
        # 展平列名
        stats.columns = ['watch_count', 'completion_rate', 
                        'avg_watch_duration', 'avg_video_duration']
        
        # 转换完播率为百分比
        stats['completion_rate'] = stats['completion_rate'] * 100
        
        # 四舍五入
        decimal_places = config.analysis.decimal_places
        stats = stats.round(decimal_places)
        
        return stats
    
    def add_time_features(self, df: pd.DataFrame, 
                         time_col: str = 'publish_time') -> pd.DataFrame:
        """
        添加时间特征列
        
        Args:
            df: 输入数据
            time_col: 时间列名
            
        Returns:
            添加了时间特征的DataFrame
        """
        if time_col not in df.columns:
            logger.warning(f"时间列 '{time_col}' 不存在")
            return df
        
        df = df.copy()
        
        # 提取日期
        df['watch_date'] = df[time_col].dt.date
        
        # 提取年份和周数
        df['year'] = df[time_col].dt.year
        df['week'] = df[time_col].dt.isocalendar().week
        
        # 提取月份和季度
        df['month'] = df[time_col].dt.month
        df['quarter'] = df[time_col].dt.quarter
        
        # 提取星期几
        df['day_of_week'] = df[time_col].dt.dayofweek  # 0=周一, 6=周日
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        # 提取小时（如果有）
        if df[time_col].dt.hour.nunique() > 1:
            df['hour'] = df[time_col].dt.hour
        
        logger.debug("时间特征添加完成")
        return df
    
    def calculate_completion_rate(self, df: pd.DataFrame,
                                  group_by: Optional[str] = None) -> pd.DataFrame:
        """
        计算完播率
        
        Args:
            df: 输入数据
            group_by: 分组列名，None则计算整体完播率
            
        Returns:
            完播率统计DataFrame
        """
        if group_by is None:
            # 整体完播率
            total = len(df)
            completed = df['completion_status'].sum()
            rate = (completed / total * 100) if total > 0 else 0
            
            return pd.DataFrame({
                'total_count': [total],
                'completed_count': [completed],
                'completion_rate': [round(rate, config.analysis.decimal_places)]
            })
        else:
            # 分组完播率
            result = df.groupby(group_by).agg({
                'completion_status': ['count', 'sum', 'mean']
            })
            
            result.columns = ['total_count', 'completed_count', 'completion_rate']
            result['completion_rate'] = result['completion_rate'] * 100
            result = result.round(config.analysis.decimal_places)
            
            return result
    
    def get_duration_segments(self, df: pd.DataFrame,
                             duration_col: str = 'video_duration') -> pd.DataFrame:
        """
        按视频时长分段
        
        Args:
            df: 输入数据
            duration_col: 时长列名
            
        Returns:
            添加了时长分段的DataFrame
        """
        bins = config.analysis.duration_bins
        labels = config.analysis.duration_labels
        
        df = df.copy()
        df['duration_segment'] = pd.cut(
            df[duration_col], 
            bins=bins, 
            labels=labels[:len(bins)-1],
            include_lowest=True
        )
        
        return df
    
    def get_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        获取数据摘要统计
        
        Args:
            df: 输入数据
            
        Returns:
            摘要统计字典
        """
        stats = {
            'total_records': len(df),
            'unique_users': df['user_id'].nunique(),
            'unique_videos': df['video_id'].nunique(),
            'categories': df['video_category'].nunique(),
            'date_range': {
                'start': df['publish_time'].min().strftime('%Y-%m-%d'),
                'end': df['publish_time'].max().strftime('%Y-%m-%d')
            },
            'watch_duration': {
                'mean': round(df['watch_duration'].mean(), 2),
                'median': round(df['watch_duration'].median(), 2),
                'min': int(df['watch_duration'].min()),
                'max': int(df['watch_duration'].max())
            },
            'video_duration': {
                'mean': round(df['video_duration'].mean(), 2),
                'median': round(df['video_duration'].median(), 2),
                'min': int(df['video_duration'].min()),
                'max': int(df['video_duration'].max())
            },
            'overall_completion_rate': round(
                df['completion_status'].mean() * 100, 2
            )
        }
        
        return stats
    
    def process_pipeline(self, df: pd.DataFrame,
                        filter_invalid: bool = True,
                        add_time_features: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        执行完整的数据处理流程
        
        Args:
            df: 原始数据
            filter_invalid: 是否过滤无效观看
            add_time_features: 是否添加时间特征
            
        Returns:
            (处理后的数据, 类别统计) 元组
        """
        logger.info("开始数据处理流程...")
        
        # 1. 过滤无效观看
        if filter_invalid:
            df = self.filter_invalid_watch(df)
        
        # 2. 添加时间特征
        if add_time_features:
            df = self.add_time_features(df)
        
        # 3. 计算类别统计
        category_stats = self.calculate_category_stats(df)
        
        logger.info("数据处理流程完成")
        return df, category_stats


# 便捷函数
def process_data(df: pd.DataFrame, 
                min_watch_duration: Optional[int] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    快速处理数据
    
    Args:
        df: 原始数据
        min_watch_duration: 最小有效观看时长
        
    Returns:
        (处理后的数据, 类别统计) 元组
    """
    processor = DataProcessor()
    return processor.process_pipeline(df, filter_invalid=True, add_time_features=True)
