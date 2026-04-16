#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统计分析模块
提供多维度统计分析功能，包括时间序列分析、相关性分析、用户分层等
"""

from typing import Optional, Dict, Any, List, Tuple, Union
from dataclasses import dataclass

import pandas as pd
import numpy as np

from config import config, AnalysisConfig
from utils import logger, calculate_percent_change, format_number


@dataclass
class TrendAnalysisResult:
    """趋势分析结果"""
    daily_stats: pd.DataFrame
    weekly_stats: pd.DataFrame
    monthly_stats: Optional[pd.DataFrame]
    trend_summary: Dict[str, Any]


@dataclass
class CorrelationResult:
    """相关性分析结果"""
    correlation_coefficient: float
    p_value: Optional[float]
    interpretation: str
    duration_segment_analysis: Optional[pd.DataFrame]


@dataclass
class UserSegmentResult:
    """用户分层分析结果"""
    segment_stats: pd.DataFrame
    segment_distribution: pd.DataFrame
    insights: List[str]


class VideoAnalyzer:
    """
    视频数据分析器类
    提供全面的统计分析功能
    """
    
    def __init__(self, analysis_config: Optional[AnalysisConfig] = None):
        """
        初始化分析器
        
        Args:
            analysis_config: 分析配置对象，默认使用全局配置
        """
        self.config = analysis_config or config.analysis
        self._decimal_places = self.config.decimal_places
    
    def analyze_trends(self, df: pd.DataFrame,
                      date_col: str = 'watch_date',
                      time_col: str = 'publish_time') -> TrendAnalysisResult:
        """
        分析播放量与完播率趋势
        
        Args:
            df: 输入数据
            date_col: 日期列名
            time_col: 时间列名
            
        Returns:
            趋势分析结果
        """
        logger.info("开始趋势分析...")
        
        # 日统计
        daily_stats = self._calculate_daily_stats(df, date_col)
        
        # 周统计
        weekly_stats = self._calculate_weekly_stats(df, time_col)
        
        # 月统计（如果有跨月数据）
        monthly_stats = self._calculate_monthly_stats(df, time_col)
        
        # 趋势摘要
        trend_summary = self._generate_trend_summary(daily_stats, weekly_stats)
        
        logger.info("趋势分析完成")
        
        return TrendAnalysisResult(
            daily_stats=daily_stats,
            weekly_stats=weekly_stats,
            monthly_stats=monthly_stats,
            trend_summary=trend_summary
        )
    
    def _calculate_daily_stats(self, df: pd.DataFrame, 
                               date_col: str) -> pd.DataFrame:
        """
        计算每日统计
        
        Args:
            df: 输入数据
            date_col: 日期列名
            
        Returns:
            每日统计DataFrame
        """
        if date_col not in df.columns:
            df = df.copy()
            df[date_col] = df['publish_time'].dt.date
        
        daily = df.groupby(date_col).agg({
            'video_id': 'count',
            'completion_status': 'mean',
            'watch_duration': 'mean',
            'user_id': 'nunique'
        }).rename(columns={
            'video_id': 'daily_plays',
            'completion_status': 'daily_completion_rate',
            'watch_duration': 'avg_watch_duration',
            'user_id': 'unique_users'
        })
        
        # 转换为百分比
        daily['daily_completion_rate'] = daily['daily_completion_rate'] * 100
        
        # 计算日环比
        daily['plays_change_pct'] = daily['daily_plays'].pct_change() * 100
        daily['completion_change_pct'] = daily['daily_completion_rate'].pct_change() * 100
        
        # 四舍五入
        daily = daily.round(self._decimal_places)
        
        return daily
    
    def _calculate_weekly_stats(self, df: pd.DataFrame,
                                time_col: str) -> pd.DataFrame:
        """
        计算周统计
        
        Args:
            df: 输入数据
            time_col: 时间列名
            
        Returns:
            周统计DataFrame
        """
        # 确保有年份和周数列
        if 'year' not in df.columns or 'week' not in df.columns:
            df = df.copy()
            df['year'] = df[time_col].dt.year
            df['week'] = df[time_col].dt.isocalendar().week
        
        weekly = df.groupby(['year', 'week']).agg({
            'video_id': 'count',
            'completion_status': 'mean',
            'watch_duration': 'mean',
            'user_id': 'nunique'
        }).rename(columns={
            'video_id': 'weekly_plays',
            'completion_status': 'weekly_completion_rate',
            'watch_duration': 'avg_watch_duration',
            'user_id': 'unique_users'
        })
        
        # 转换为百分比
        weekly['weekly_completion_rate'] = weekly['weekly_completion_rate'] * 100
        
        # 计算周环比
        weekly['plays_change_pct'] = weekly['weekly_plays'].pct_change() * 100
        weekly['completion_change_pct'] = weekly['weekly_completion_rate'].pct_change() * 100
        
        # 四舍五入
        weekly = weekly.round(self._decimal_places)
        
        return weekly
    
    def _calculate_monthly_stats(self, df: pd.DataFrame,
                                 time_col: str) -> Optional[pd.DataFrame]:
        """
        计算月统计
        
        Args:
            df: 输入数据
            time_col: 时间列名
            
        Returns:
            月统计DataFrame，如果数据不足一个月则返回None
        """
        df = df.copy()
        df['month'] = df[time_col].dt.to_period('M')
        
        # 检查是否有跨月数据
        if df['month'].nunique() < 2:
            return None
        
        monthly = df.groupby('month').agg({
            'video_id': 'count',
            'completion_status': 'mean',
            'watch_duration': 'mean',
            'user_id': 'nunique'
        }).rename(columns={
            'video_id': 'monthly_plays',
            'completion_status': 'monthly_completion_rate',
            'watch_duration': 'avg_watch_duration',
            'user_id': 'unique_users'
        })
        
        monthly['monthly_completion_rate'] = monthly['monthly_completion_rate'] * 100
        monthly['plays_change_pct'] = monthly['monthly_plays'].pct_change() * 100
        monthly['completion_change_pct'] = monthly['monthly_completion_rate'].pct_change() * 100
        
        return monthly.round(self._decimal_places)
    
    def _generate_trend_summary(self, daily: pd.DataFrame,
                                weekly: pd.DataFrame) -> Dict[str, Any]:
        """
        生成趋势摘要
        
        Args:
            daily: 日统计数据
            weekly: 周统计数据
            
        Returns:
            趋势摘要字典
        """
        summary = {
            'daily': {
                'avg_plays': round(daily['daily_plays'].mean(), self._decimal_places),
                'max_plays': int(daily['daily_plays'].max()),
                'min_plays': int(daily['daily_plays'].min()),
                'avg_completion_rate': round(daily['daily_completion_rate'].mean(), self._decimal_places),
                'trend_direction': 'up' if daily['daily_plays'].iloc[-1] > daily['daily_plays'].iloc[0] else 'down'
            },
            'weekly': {
                'avg_plays': round(weekly['weekly_plays'].mean(), self._decimal_places),
                'avg_completion_rate': round(weekly['weekly_completion_rate'].mean(), self._decimal_places)
            }
        }
        
        return summary
    
    def analyze_correlation(self, df: pd.DataFrame,
                           x_col: str = 'video_duration',
                           y_col: str = 'completion_status',
                           by_segment: bool = True) -> CorrelationResult:
        """
        分析视频时长与完播率的相关性
        
        Args:
            df: 输入数据
            x_col: 自变量列名
            y_col: 因变量列名
            by_segment: 是否按时长分段分析
            
        Returns:
            相关性分析结果
        """
        logger.info(f"开始相关性分析: {x_col} vs {y_col}")
        
        # 计算相关系数
        # 按视频ID聚合，避免同一视频的多次观看影响
        video_stats = df.groupby('video_id').agg({
            x_col: 'first',
            y_col: 'mean'
        })
        
        correlation = video_stats[x_col].corr(video_stats[y_col])
        
        # 解释相关性
        interpretation = self._interpret_correlation(correlation)
        
        # 按时长分段分析
        duration_segment_analysis = None
        if by_segment:
            duration_segment_analysis = self._analyze_by_duration_segment(df)
        
        logger.info(f"相关性分析完成，相关系数: {correlation:.4f}")
        
        return CorrelationResult(
            correlation_coefficient=round(correlation, 4),
            p_value=None,  # 可扩展添加显著性检验
            interpretation=interpretation,
            duration_segment_analysis=duration_segment_analysis
        )
    
    def _interpret_correlation(self, correlation: float) -> str:
        """
        解释相关系数
        
        Args:
            correlation: 相关系数值
            
        Returns:
            解释文本
        """
        abs_corr = abs(correlation)
        direction = "正相关" if correlation > 0 else "负相关"
        
        if abs_corr < self.config.correlation_threshold_weak:
            return "相关关系不显著"
        elif abs_corr < self.config.correlation_threshold_strong:
            return f"存在较弱的{direction}关系"
        else:
            return f"存在较强的{direction}关系"
    
    def _analyze_by_duration_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按视频时长分段分析
        
        Args:
            df: 输入数据
            
        Returns:
            分段分析结果
        """
        bins = self.config.duration_bins
        labels = self.config.duration_labels[:len(bins)-1]
        
        df = df.copy()
        df['duration_segment'] = pd.cut(
            df['video_duration'],
            bins=bins,
            labels=labels,
            include_lowest=True
        )
        
        segment_analysis = df.groupby('duration_segment').agg({
            'completion_status': ['count', 'mean'],
            'watch_duration': 'mean',
            'video_id': 'nunique'
        })
        
        segment_analysis.columns = ['watch_count', 'completion_rate', 
                                    'avg_watch_duration', 'unique_videos']
        segment_analysis['completion_rate'] = segment_analysis['completion_rate'] * 100
        
        return segment_analysis.round(self._decimal_places)
    
    def analyze_user_segments(self, df: pd.DataFrame,
                             segment_by: str = 'activity') -> UserSegmentResult:
        """
        用户分层分析
        
        Args:
            df: 输入数据
            segment_by: 分层维度 ('activity', 'engagement', 'loyalty')
            
        Returns:
            用户分层分析结果
        """
        logger.info(f"开始用户分层分析，维度: {segment_by}")
        
        if segment_by == 'activity':
            segment_stats = self._segment_by_activity(df)
        elif segment_by == 'engagement':
            segment_stats = self._segment_by_engagement(df)
        elif segment_by == 'loyalty':
            segment_stats = self._segment_by_loyalty(df)
        else:
            raise ValueError(f"未知的分层维度: {segment_by}")
        
        # 计算分布
        total_users = segment_stats['user_count'].sum()
        segment_distribution = segment_stats.copy()
        segment_distribution['percentage'] = (
            segment_distribution['user_count'] / total_users * 100
        ).round(self._decimal_places)
        
        # 生成洞察
        insights = self._generate_segment_insights(segment_stats, segment_by)
        
        logger.info("用户分层分析完成")
        
        return UserSegmentResult(
            segment_stats=segment_stats,
            segment_distribution=segment_distribution,
            insights=insights
        )
    
    def _segment_by_activity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按活跃度分层
        
        Args:
            df: 输入数据
            
        Returns:
            分层统计DataFrame
        """
        # 计算每个用户的观看次数
        user_activity = df.groupby('user_id').agg({
            'video_id': 'count',
            'watch_duration': 'sum',
            'completion_status': 'mean'
        }).rename(columns={
            'video_id': 'watch_count',
            'watch_duration': 'total_watch_time',
            'completion_status': 'avg_completion_rate'
        })
        
        # 定义分层阈值
        q33 = user_activity['watch_count'].quantile(0.33)
        q67 = user_activity['watch_count'].quantile(0.67)
        
        def classify_activity(count):
            if count <= q33:
                return '低活跃'
            elif count <= q67:
                return '中活跃'
            else:
                return '高活跃'
        
        user_activity['segment'] = user_activity['watch_count'].apply(classify_activity)
        
        # 聚合统计
        segment_stats = user_activity.groupby('segment').agg({
            'watch_count': ['count', 'mean'],
            'total_watch_time': 'mean',
            'avg_completion_rate': 'mean'
        })
        
        segment_stats.columns = ['user_count', 'avg_watches', 
                                'avg_watch_time', 'avg_completion_rate']
        segment_stats['avg_completion_rate'] = segment_stats['avg_completion_rate'] * 100
        
        return segment_stats.round(self._decimal_places)
    
    def _segment_by_engagement(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按参与度分层（基于完播率）
        
        Args:
            df: 输入数据
            
        Returns:
            分层统计DataFrame
        """
        user_engagement = df.groupby('user_id').agg({
            'completion_status': 'mean',
            'watch_duration': 'mean',
            'video_id': 'count'
        }).rename(columns={
            'completion_status': 'avg_completion_rate',
            'watch_duration': 'avg_watch_duration',
            'video_id': 'watch_count'
        })
        
        # 按完播率分层
        def classify_engagement(rate):
            if rate < 0.3:
                return '低参与'
            elif rate < 0.7:
                return '中参与'
            else:
                return '高参与'
        
        user_engagement['segment'] = user_engagement['avg_completion_rate'].apply(classify_engagement)
        
        segment_stats = user_engagement.groupby('segment').agg({
            'avg_completion_rate': ['count', 'mean'],
            'avg_watch_duration': 'mean',
            'watch_count': 'mean'
        })
        
        segment_stats.columns = ['user_count', 'avg_completion_rate', 
                                'avg_watch_duration', 'avg_watches']
        segment_stats['avg_completion_rate'] = segment_stats['avg_completion_rate'] * 100
        
        return segment_stats.round(self._decimal_places)
    
    def _segment_by_loyalty(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按忠诚度分层（基于观看天数）
        
        Args:
            df: 输入数据
            
        Returns:
            分层统计DataFrame
        """
        df = df.copy()
        df['watch_date'] = df['publish_time'].dt.date
        
        user_loyalty = df.groupby('user_id').agg({
            'watch_date': 'nunique',
            'video_id': 'count',
            'completion_status': 'mean'
        }).rename(columns={
            'watch_date': 'active_days',
            'video_id': 'watch_count',
            'completion_status': 'avg_completion_rate'
        })
        
        # 按活跃天数分层
        def classify_loyalty(days):
            if days == 1:
                return '新用户'
            elif days <= 3:
                return '偶尔访问'
            elif days <= 7:
                return '常规用户'
            else:
                return '忠实用户'
        
        user_loyalty['segment'] = user_loyalty['active_days'].apply(classify_loyalty)
        
        segment_stats = user_loyalty.groupby('segment').agg({
            'active_days': ['count', 'mean'],
            'watch_count': 'mean',
            'avg_completion_rate': 'mean'
        })
        
        segment_stats.columns = ['user_count', 'avg_active_days', 
                                'avg_watches', 'avg_completion_rate']
        segment_stats['avg_completion_rate'] = segment_stats['avg_completion_rate'] * 100
        
        return segment_stats.round(self._decimal_places)
    
    def _generate_segment_insights(self, segment_stats: pd.DataFrame,
                                   segment_by: str) -> List[str]:
        """
        生成分层洞察
        
        Args:
            segment_stats: 分层统计
            segment_by: 分层维度
            
        Returns:
            洞察列表
        """
        insights = []
        
        # 找出最大群体
        max_segment = segment_stats['user_count'].idxmax()
        max_pct = segment_stats.loc[max_segment, 'user_count'] / segment_stats['user_count'].sum() * 100
        insights.append(f"最大用户群体为'{max_segment}'，占比 {max_pct:.1f}%")
        
        # 完播率对比
        if 'avg_completion_rate' in segment_stats.columns:
            best_segment = segment_stats['avg_completion_rate'].idxmax()
            worst_segment = segment_stats['avg_completion_rate'].idxmin()
            best_rate = segment_stats.loc[best_segment, 'avg_completion_rate']
            worst_rate = segment_stats.loc[worst_segment, 'avg_completion_rate']
            insights.append(f"完播率最高的是'{best_segment}'({best_rate:.1f}%)，"
                          f"最低的是'{worst_segment}'({worst_rate:.1f}%)")
        
        return insights
    
    def compare_dimensions(self, df: pd.DataFrame,
                          dimensions: List[str]) -> Dict[str, pd.DataFrame]:
        """
        多维度对比分析
        
        Args:
            df: 输入数据
            dimensions: 对比维度列表
            
        Returns:
            各维度分析结果字典
        """
        results = {}
        
        for dim in dimensions:
            if dim not in df.columns:
                logger.warning(f"维度 '{dim}' 不存在于数据中")
                continue
            
            stats = df.groupby(dim).agg({
                'video_id': 'count',
                'completion_status': 'mean',
                'watch_duration': 'mean',
                'user_id': 'nunique'
            }).rename(columns={
                'video_id': 'play_count',
                'completion_status': 'completion_rate',
                'watch_duration': 'avg_watch_duration',
                'user_id': 'unique_users'
            })
            
            stats['completion_rate'] = stats['completion_rate'] * 100
            results[dim] = stats.round(self._decimal_places)
        
        return results


# 便捷函数
def analyze_daily_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    快速分析每日趋势
    
    Args:
        df: 输入数据
        
    Returns:
        每日统计DataFrame
    """
    analyzer = VideoAnalyzer()
    result = analyzer.analyze_trends(df)
    return result.daily_stats


def analyze_correlation(df: pd.DataFrame) -> CorrelationResult:
    """
    快速分析相关性
    
    Args:
        df: 输入数据
        
    Returns:
        相关性分析结果
    """
    analyzer = VideoAnalyzer()
    return analyzer.analyze_correlation(df)
