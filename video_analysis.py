#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
短视频平台用户观看数据综合分析脚本（重构版）

技术栈：Python + Pandas + Matplotlib
功能：数据处理、统计分析、可视化展示

重构要点：
1. 配置化管理：所有配置项抽离为独立配置类
2. 面向对象封装：核心分析逻辑封装为类
3. 性能优化：使用pivot_table替代部分groupby，减少数据拷贝
4. 健壮性增强：完善的异常处理、参数校验、日志记录
5. 类型注解：完整的类型提示和文档字符串
6. 功能扩展：支持外部数据读取、自定义输出路径
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import rcParams

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DataConfig:
    """数据相关配置"""
    random_seed: int = 42
    n_records: int = 10000
    user_id_range: Tuple[int, int] = (10001, 20000)
    video_id_range: Tuple[int, int] = (1001, 3000)
    categories: List[str] = field(
        default_factory=lambda: ['娱乐', '教育', '美食', '旅游', '游戏', '科技', '生活', '美妆']
    )
    date_range: Tuple[str, str] = ('2024-01-01', '2024-01-30')
    video_duration_range: Tuple[int, int] = (15, 300)
    watch_duration_range: Tuple[int, int] = (0, 600)


@dataclass
class FilterConfig:
    """过滤相关配置"""
    min_watch_duration: int = 3
    completion_threshold: float = 0.9
    watch_duration_distribution: Dict[str, float] = field(
        default_factory=lambda: {
            'invalid': 0.15,
            'short': 0.35,
            'medium': 0.35,
            'long': 0.15
        }
    )


@dataclass
class VisualizationConfig:
    """可视化相关配置"""
    figure_size: Tuple[int, int] = (16, 12)
    dpi: int = 300
    output_format: str = 'png'
    output_path: str = 'video_analysis_charts'
    font_family: List[str] = field(
        default_factory=lambda: ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
    )
    font_size: int = 10
    colors: Dict[str, str] = field(
        default_factory=lambda: {
            'complete': '#2ecc71',
            'incomplete': '#e74c3c',
            'play_count': 'b',
            'completion_rate': 'r'
        }
    )
    pad: float = 3.0
    tight_layout_pad: float = 3.0


@dataclass
class AnalysisConfig:
    """分析相关配置"""
    data: DataConfig = field(default_factory=DataConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'AnalysisConfig':
        """从字典创建配置对象
        
        Args:
            config_dict: 配置字典
            
        Returns:
            AnalysisConfig: 配置对象
        """
        data_config = DataConfig(**config_dict.get('data', {}))
        filter_config = FilterConfig(**config_dict.get('filter', {}))
        viz_config = VisualizationConfig(**config_dict.get('visualization', {}))
        return cls(data=data_config, filter=filter_config, visualization=viz_config)


class DataValidator:
    """数据校验工具类"""
    
    REQUIRED_COLUMNS = ['用户ID', '视频ID', '观看时长（秒）', '完播状态', '发布时间', '视频类别', '视频时长（秒）']
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> bool:
        """校验DataFrame是否包含必需字段
        
        Args:
            df: 待校验的DataFrame
            required_columns: 必需的列名列表，默认使用类常量
            
        Returns:
            bool: 校验是否通过
            
        Raises:
            ValueError: 当缺少必需字段时抛出
        """
        if required_columns is None:
            required_columns = DataValidator.REQUIRED_COLUMNS
            
        if df is None or df.empty:
            raise ValueError("数据为空，无法进行分析")
            
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"缺少必需字段: {missing_columns}")
            
        logger.info("数据校验通过，包含所有必需字段")
        return True
    
    @staticmethod
    def validate_data_types(df: pd.DataFrame) -> bool:
        """校验数据类型是否合法
        
        Args:
            df: 待校验的DataFrame
            
        Returns:
            bool: 校验是否通过
            
        Raises:
            TypeError: 当数据类型不合法时抛出
        """
        type_checks = {
            '观看时长（秒）': (np.integer, np.floating),
            '视频时长（秒）': (np.integer, np.floating),
            '完播状态': (bool, np.bool_),
        }
        
        for col, expected_types in type_checks.items():
            if col in df.columns:
                if not df[col].dtype in expected_types:
                    try:
                        if col == '完播状态':
                            df[col] = df[col].astype(bool)
                        else:
                            df[col] = pd.to_numeric(df[col], errors='raise')
                        logger.info(f"字段 '{col}' 类型已自动转换为正确类型")
                    except Exception as e:
                        raise TypeError(f"字段 '{col}' 类型不合法，期望类型: {expected_types}，实际类型: {df[col].dtype}")
        
        if '发布时间' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['发布时间']):
            try:
                df['发布时间'] = pd.to_datetime(df['发布时间'])
                logger.info("字段 '发布时间' 类型已自动转换为datetime类型")
            except Exception as e:
                raise TypeError(f"字段 '发布时间' 无法转换为datetime类型: {e}")
                
        return True
    
    @staticmethod
    def validate_positive_int(value: int, param_name: str) -> bool:
        """校验参数是否为正整数
        
        Args:
            value: 待校验的值
            param_name: 参数名称
            
        Returns:
            bool: 校验是否通过
            
        Raises:
            ValueError: 当值不是正整数时抛出
        """
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"参数 '{param_name}' 必须为正整数，当前值: {value}")
        return True


class PathHandler:
    """路径处理工具类"""
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> Path:
        """确保目录存在，不存在则创建
        
        Args:
            path: 目录路径
            
        Returns:
            Path: 目录路径对象
        """
        path = Path(path)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建目录: {path}")
        return path
    
    @staticmethod
    def get_output_path(
        base_path: Union[str, Path],
        filename: str,
        format: str = 'png'
    ) -> Path:
        """获取完整的输出路径
        
        Args:
            base_path: 基础路径
            filename: 文件名（不含扩展名）
            format: 文件格式
            
        Returns:
            Path: 完整的输出路径
        """
        base_path = Path(base_path)
        if base_path.suffix:
            return base_path
        return base_path.parent / f"{filename}.{format}"
    
    @staticmethod
    def validate_file_path(file_path: Union[str, Path], must_exist: bool = True) -> Path:
        """校验文件路径
        
        Args:
            file_path: 文件路径
            must_exist: 文件是否必须存在
            
        Returns:
            Path: 文件路径对象
            
        Raises:
            FileNotFoundError: 当文件不存在且must_exist为True时抛出
        """
        path = Path(file_path)
        if must_exist and not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")
        return path


class DataGenerator:
    """数据生成器类"""
    
    def __init__(self, config: DataConfig):
        """初始化数据生成器
        
        Args:
            config: 数据配置对象
        """
        self.config = config
        np.random.seed(config.random_seed)
        logger.info(f"数据生成器初始化完成，随机种子: {config.random_seed}")
    
    def generate_watch_durations(self, n_records: int, distribution: Dict[str, float]) -> np.ndarray:
        """生成观看时长数据
        
        Args:
            n_records: 记录数量
            distribution: 观看时长分布配置
            
        Returns:
            np.ndarray: 观看时长数组
        """
        invalid_count = int(n_records * distribution['invalid'])
        short_count = int(n_records * distribution['short'])
        medium_count = int(n_records * distribution['medium'])
        long_count = n_records - invalid_count - short_count - medium_count
        
        watch_durations = np.concatenate([
            np.random.randint(0, 3, size=invalid_count),
            np.random.randint(3, 60, size=short_count),
            np.random.randint(60, 300, size=medium_count),
            np.random.randint(300, 601, size=long_count)
        ])
        np.random.shuffle(watch_durations)
        return watch_durations
    
    def generate_mock_data(self, n_records: Optional[int] = None) -> pd.DataFrame:
        """生成模拟的短视频观看数据
        
        Args:
            n_records: 数据条数，默认使用配置值
            
        Returns:
            pd.DataFrame: 生成的数据
            
        Raises:
            ValueError: 当n_records不是正整数时抛出
        """
        if n_records is None:
            n_records = self.config.n_records
            
        DataValidator.validate_positive_int(n_records, 'n_records')
        logger.info(f"开始生成 {n_records} 条模拟数据")
        
        user_ids = np.random.randint(
            self.config.user_id_range[0],
            self.config.user_id_range[1],
            size=n_records
        )
        
        video_ids = np.random.randint(
            self.config.video_id_range[0],
            self.config.video_id_range[1],
            size=n_records
        )
        
        video_categories = np.random.choice(self.config.categories, size=n_records)
        
        publish_dates = pd.date_range(
            start=self.config.date_range[0],
            end=self.config.date_range[1],
            periods=n_records
        )
        publish_dates = np.random.choice(publish_dates, size=n_records)
        
        distribution = {
            'invalid': 0.15,
            'short': 0.35,
            'medium': 0.35,
            'long': 0.15
        }
        watch_durations = self.generate_watch_durations(n_records, distribution)
        
        video_durations_dict = {
            vid: np.random.randint(
                self.config.video_duration_range[0],
                self.config.video_duration_range[1] + 1
            )
            for vid in np.unique(video_ids)
        }
        video_durations = np.array([video_durations_dict[vid] for vid in video_ids])
        
        completion_status = watch_durations >= (video_durations * 0.9)
        
        df = pd.DataFrame({
            '用户ID': user_ids,
            '视频ID': video_ids,
            '观看时长（秒）': watch_durations,
            '完播状态': completion_status,
            '发布时间': publish_dates,
            '视频类别': video_categories,
            '视频时长（秒）': video_durations
        })
        
        logger.info(f"模拟数据生成完成，共 {len(df)} 条记录")
        return df
    
    @staticmethod
    def load_external_data(
        file_path: Union[str, Path],
        file_type: Optional[str] = None,
        encoding: str = 'utf-8'
    ) -> pd.DataFrame:
        """从外部文件加载数据
        
        Args:
            file_path: 文件路径
            file_type: 文件类型（'csv' 或 'excel'），默认自动检测
            encoding: 文件编码，默认utf-8
            
        Returns:
            pd.DataFrame: 加载的数据
            
        Raises:
            FileNotFoundError: 当文件不存在时抛出
            ValueError: 当文件类型不支持时抛出
        """
        path = PathHandler.validate_file_path(file_path)
        
        if file_type is None:
            file_type = path.suffix.lower().replace('.', '')
        
        logger.info(f"从外部文件加载数据: {path}")
        
        try:
            if file_type in ['csv']:
                df = pd.read_csv(path, encoding=encoding)
            elif file_type in ['xlsx', 'xls', 'excel']:
                df = pd.read_excel(path)
            else:
                raise ValueError(f"不支持的文件类型: {file_type}，支持类型: csv, xlsx, xls")
            
            DataValidator.validate_dataframe(df)
            DataValidator.validate_data_types(df)
            
            logger.info(f"外部数据加载完成，共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"加载外部数据失败: {e}")
            raise


class DataProcessor:
    """数据处理器类"""
    
    def __init__(self, config: FilterConfig):
        """初始化数据处理器
        
        Args:
            config: 过滤配置对象
        """
        self.config = config
        logger.info("数据处理器初始化完成")
    
    def filter_invalid_watches(self, df: pd.DataFrame) -> pd.DataFrame:
        """过滤无效观看记录
        
        Args:
            df: 原始数据
            
        Returns:
            pd.DataFrame: 过滤后的数据
        """
        initial_count = len(df)
        filtered_df = df[df['观看时长（秒）'] >= self.config.min_watch_duration].copy()
        filtered_count = len(filtered_df)
        
        filter_ratio = (1 - filtered_count / initial_count) * 100
        logger.info(
            f"过滤无效观看: 原始 {initial_count} 条 -> 过滤后 {filtered_count} 条 "
            f"(过滤比例: {filter_ratio:.2f}%)"
        )
        
        return filtered_df
    
    def calculate_category_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """按视频类别计算统计数据（优化版：使用pivot_table）
        
        Args:
            df: 过滤后的数据
            
        Returns:
            pd.DataFrame: 类别统计数据
        """
        category_stats = df.pivot_table(
            index='视频类别',
            values=['完播状态', '观看时长（秒）', '视频时长（秒）'],
            aggfunc={
                '完播状态': ['count', 'mean'],
                '观看时长（秒）': 'mean',
                '视频时长（秒）': 'mean'
            }
        ).round(4)
        
        category_stats.columns = ['观看次数', '完播率', '平均观看时长（秒）', '平均视频时长（秒）']
        category_stats['完播率'] = category_stats['完播率'] * 100
        category_stats = category_stats.sort_values('完播率', ascending=False)
        
        logger.info(f"类别统计计算完成，共 {len(category_stats)} 个类别")
        return category_stats
    
    def process(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """执行完整的数据处理流程
        
        Args:
            df: 原始数据
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (过滤后的数据, 类别统计数据)
        """
        logger.info("=" * 60)
        logger.info("数据处理模块")
        logger.info("=" * 60)
        
        filtered_df = self.filter_invalid_watches(df)
        category_stats = self.calculate_category_stats(filtered_df)
        
        logger.info("\n按视频类别统计（过滤无效观看后）：")
        logger.info(f"\n{category_stats.round(2)}")
        
        return filtered_df, category_stats


class StatisticalAnalyzer:
    """统计分析器类"""
    
    def __init__(self):
        """初始化统计分析器"""
        logger.info("统计分析器初始化完成")
    
    def calculate_daily_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算每日统计数据
        
        Args:
            df: 过滤后的数据
            
        Returns:
            pd.DataFrame: 每日统计数据
        """
        df = df.copy()
        df['观看日期'] = df['发布时间'].dt.date
        
        daily_play = df.groupby('观看日期').agg({
            '视频ID': 'count',
            '完播状态': 'mean'
        }).rename(columns={'视频ID': '当日播放量', '完播状态': '日完播率'})
        
        daily_play['日完播率'] = (daily_play['日完播率'] * 100).round(2)
        
        logger.info(f"每日统计计算完成，共 {len(daily_play)} 天")
        return daily_play
    
    def calculate_weekly_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算周统计数据及周环比
        
        Args:
            df: 过滤后的数据
            
        Returns:
            pd.DataFrame: 周统计数据
        """
        df = df.copy()
        df['年份'] = df['发布时间'].dt.year
        df['周数'] = df['发布时间'].dt.isocalendar().week
        
        weekly_stats = df.groupby(['年份', '周数']).agg({
            '视频ID': 'count',
            '完播状态': 'mean'
        }).rename(columns={'视频ID': '周播放量', '完播状态': '周完播率'})
        
        weekly_stats['周完播率'] = (weekly_stats['周完播率'] * 100).round(2)
        weekly_stats['播放量周环比(%)'] = weekly_stats['周播放量'].pct_change() * 100
        weekly_stats['完播率周环比(%)'] = weekly_stats['周完播率'].pct_change() * 100
        
        logger.info(f"周统计计算完成，共 {len(weekly_stats)} 周")
        return weekly_stats
    
    def analyze_completion_correlation(self, df: pd.DataFrame) -> Tuple[float, str]:
        """分析完播率与视频时长的相关性
        
        Args:
            df: 过滤后的数据
            
        Returns:
            Tuple[float, str]: (相关系数, 结论描述)
        """
        corr_data = df.groupby('视频ID').agg({
            '视频时长（秒）': 'first',
            '完播状态': 'mean'
        })
        
        correlation = corr_data['视频时长（秒）'].corr(corr_data['完播状态'])
        
        if correlation < -0.1:
            conclusion = "存在较弱的负相关关系，视频时长越长，完播率倾向于越低"
        elif correlation > 0.1:
            conclusion = "存在较弱的正相关关系"
        else:
            conclusion = "相关关系不显著"
        
        logger.info(f"相关性分析完成，Pearson相关系数: {correlation:.4f}")
        logger.info(f"结论: {conclusion}")
        
        return correlation, conclusion
    
    def analyze_by_time_period(
        self,
        df: pd.DataFrame,
        period: str = 'hour'
    ) -> pd.DataFrame:
        """按时段分析数据
        
        Args:
            df: 过滤后的数据
            period: 时间段类型（'hour' 或 'weekday'）
            
        Returns:
            pd.DataFrame: 时段统计数据
        """
        df = df.copy()
        
        if period == 'hour':
            df['时段'] = df['发布时间'].dt.hour
            period_name = '小时'
        elif period == 'weekday':
            df['时段'] = df['发布时间'].dt.dayofweek
            period_name = '星期'
        else:
            raise ValueError(f"不支持的时段类型: {period}，支持: 'hour', 'weekday'")
        
        period_stats = df.groupby('时段').agg({
            '视频ID': 'count',
            '完播状态': 'mean',
            '观看时长（秒）': 'mean'
        }).rename(columns={
            '视频ID': '播放次数',
            '完播状态': '完播率',
            '观看时长（秒）': '平均观看时长'
        })
        
        period_stats['完播率'] = (period_stats['完播率'] * 100).round(2)
        
        logger.info(f"按时段（{period_name}）分析完成")
        return period_stats
    
    def analyze_by_user_segment(
        self,
        df: pd.DataFrame,
        n_segments: int = 3
    ) -> pd.DataFrame:
        """按用户分层分析数据
        
        Args:
            df: 过滤后的数据
            n_segments: 分层数量
            
        Returns:
            pd.DataFrame: 用户分层统计数据
        """
        user_stats = df.groupby('用户ID').agg({
            '视频ID': 'count',
            '完播状态': 'mean',
            '观看时长（秒）': 'sum'
        }).rename(columns={
            '视频ID': '观看次数',
            '完播状态': '完播率',
            '观看时长（秒）': '总观看时长'
        })
        
        user_stats['用户层级'] = pd.qcut(
            user_stats['观看次数'],
            q=n_segments,
            labels=['低活跃', '中活跃', '高活跃'][:n_segments]
        )
        
        segment_stats = user_stats.groupby('用户层级').agg({
            '观看次数': 'mean',
            '完播率': 'mean',
            '总观看时长': 'mean'
        }).round(2)
        
        segment_stats['完播率'] = (segment_stats['完播率'] * 100).round(2)
        
        logger.info(f"用户分层分析完成，共 {n_segments} 个层级")
        return segment_stats
    
    def analyze(
        self,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, float, str]:
        """执行完整的统计分析流程
        
        Args:
            df: 过滤后的数据
            
        Returns:
            Tuple: (每日统计, 周统计, 相关系数, 相关性结论)
        """
        logger.info("=" * 60)
        logger.info("统计分析模块")
        logger.info("=" * 60)
        
        daily_play = self.calculate_daily_stats(df)
        weekly_stats = self.calculate_weekly_stats(df)
        correlation, conclusion = self.analyze_completion_correlation(df)
        
        logger.info("\n每日播放量统计（前10天）：")
        logger.info(f"\n{daily_play.head(10)}")
        
        logger.info("\n周统计及周环比：")
        logger.info(f"\n{weekly_stats.round(2)}")
        
        return daily_play, weekly_stats, correlation, conclusion


class Visualizer:
    """可视化类"""
    
    def __init__(self, config: VisualizationConfig):
        """初始化可视化器
        
        Args:
            config: 可视化配置对象
        """
        self.config = config
        self._setup_matplotlib()
        logger.info("可视化器初始化完成")
    
    def _setup_matplotlib(self) -> None:
        """配置Matplotlib中文显示"""
        rcParams['font.sans-serif'] = self.config.font_family
        rcParams['axes.unicode_minus'] = False
        rcParams['font.size'] = self.config.font_size
    
    def plot_trend_chart(
        self,
        daily_play: pd.DataFrame,
        ax: Optional[plt.Axes] = None
    ) -> plt.Axes:
        """绘制每日播放量与完播率趋势图
        
        Args:
            daily_play: 每日播放数据
            ax: matplotlib轴对象，默认创建新图
            
        Returns:
            plt.Axes: 绑定数据的轴对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(self.config.figure_size[0], 6))
        
        dates = daily_play.index
        play_count = daily_play['当日播放量']
        completion_rate = daily_play['日完播率']
        
        line1 = ax.plot(
            dates, play_count,
            f'{self.config.colors["play_count"]}-o',
            linewidth=2, markersize=4, label='当日播放量'
        )
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('播放量（次）', fontsize=12, color=self.config.colors['play_count'])
        ax.tick_params(axis='y', labelcolor=self.config.colors['play_count'])
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)
        
        ax_twin = ax.twinx()
        line2 = ax_twin.plot(
            dates, completion_rate,
            f'{self.config.colors["completion_rate"]}-s',
            linewidth=2, markersize=4, label='日完播率(%)'
        )
        ax_twin.set_ylabel('完播率（%）', fontsize=12, color=self.config.colors['completion_rate'])
        ax_twin.tick_params(axis='y', labelcolor=self.config.colors['completion_rate'])
        
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax.legend(lines, labels, loc='upper left')
        
        ax.set_title('图1：每日播放量与完播率趋势图', fontsize=14, pad=20)
        
        logger.info("趋势图绘制完成")
        return ax
    
    def plot_stacked_bar_chart(
        self,
        category_stats: pd.DataFrame,
        ax: Optional[plt.Axes] = None
    ) -> plt.Axes:
        """绘制各视频类别观看次数堆叠柱状图
        
        Args:
            category_stats: 类别统计数据
            ax: matplotlib轴对象，默认创建新图
            
        Returns:
            plt.Axes: 绑定数据的轴对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(self.config.figure_size[0], 6))
        
        categories = category_stats.index.tolist()
        complete_plays = category_stats['观看次数'] * (category_stats['完播率'] / 100)
        incomplete_plays = category_stats['观看次数'] - complete_plays
        
        x = np.arange(len(categories))
        width = 0.6
        
        ax.bar(
            x, complete_plays, width,
            label='完播次数', color=self.config.colors['complete']
        )
        ax.bar(
            x, incomplete_plays, width,
            bottom=complete_plays,
            label='未完播次数', color=self.config.colors['incomplete']
        )
        
        ax.set_xlabel('视频类别', fontsize=12)
        ax.set_ylabel('观看次数（次）', fontsize=12)
        ax.set_title('图2：各视频类别观看次数（按完播状态堆叠）', fontsize=14, pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=45)
        ax.legend()
        
        for i, v in enumerate(category_stats['完播率']):
            total_height = complete_plays.iloc[i] + incomplete_plays.iloc[i]
            ax.text(
                i, total_height + max(category_stats['观看次数']) * 0.01,
                f'{v:.1f}%', ha='center', va='bottom', fontsize=10
            )
        
        logger.info("堆叠柱状图绘制完成")
        return ax
    
    def create_visualization(
        self,
        daily_play: pd.DataFrame,
        category_stats: pd.DataFrame,
        output_path: Optional[Union[str, Path]] = None,
        show: bool = True
    ) -> str:
        """创建完整的可视化图表
        
        Args:
            daily_play: 每日播放数据
            category_stats: 类别统计数据
            output_path: 输出路径，默认使用配置值
            show: 是否显示图表
            
        Returns:
            str: 保存的文件路径
        """
        logger.info("=" * 60)
        logger.info("可视化模块")
        logger.info("=" * 60)
        
        fig = plt.figure(figsize=self.config.figure_size)
        
        ax1 = plt.subplot(2, 1, 1)
        self.plot_trend_chart(daily_play, ax1)
        
        ax2 = plt.subplot(2, 1, 2)
        self.plot_stacked_bar_chart(category_stats, ax2)
        
        plt.tight_layout(pad=self.config.tight_layout_pad)
        
        if output_path is None:
            output_path = f"{self.config.output_path}.{self.config.output_format}"
        
        output_path = Path(output_path)
        PathHandler.ensure_directory(output_path.parent)
        
        try:
            plt.savefig(output_path, dpi=self.config.dpi, bbox_inches='tight')
            logger.info(f"图表已保存: {output_path}")
        except Exception as e:
            logger.error(f"图表保存失败: {e}")
            raise
        
        if show:
            plt.show()
        
        self._print_chart_info()
        
        return str(output_path)
    
    def _print_chart_info(self) -> None:
        """打印图表信息解读"""
        info = """
图表信息解读：

【图1：每日播放量与完播率趋势图 - 折线图】
核心信息：
1. 蓝色折线展示每日播放量的变化趋势，反映平台用户活跃度波动
2. 红色折线展示每日完播率变化，反映内容整体吸引力
3. 双Y轴设计便于同时观察两个指标的协同变化关系
对运营的帮助：
- 识别播放量高峰日，分析当日内容特点进行复刻
- 完播率持续走低时需紧急排查内容质量问题
- 观察两者是否背离（如播放量↑但完播率↓），判断流量是否精准

【图2：各视频类别观看次数（按完播状态堆叠） - 堆叠柱状图】
核心信息：
1. 柱子总高度展示各类别的总观看次数（受欢迎程度）
2. 绿色部分=完播次数，红色部分=未完播次数，直观展示完播结构
3. 柱子顶部标签显示具体完播率数值，便于精准比较
对运营的帮助：
- 识别高观看量但低完播率的类别，作为重点优化对象
- 识别高完播率的类别，作为优质内容方向加大投入
- 为内容创作者提供明确的类别对标参考
"""
        logger.info(info)


class VideoAnalysis:
    """视频数据分析主类"""
    
    def __init__(
        self,
        config: Optional[AnalysisConfig] = None,
        config_dict: Optional[Dict[str, Any]] = None
    ):
        """初始化视频数据分析器
        
        Args:
            config: 分析配置对象
            config_dict: 配置字典，与config二选一
        """
        if config is not None:
            self.config = config
        elif config_dict is not None:
            self.config = AnalysisConfig.from_dict(config_dict)
        else:
            self.config = AnalysisConfig()
        
        self.data_generator = DataGenerator(self.config.data)
        self.data_processor = DataProcessor(self.config.filter)
        self.statistical_analyzer = StatisticalAnalyzer()
        self.visualizer = Visualizer(self.config.visualization)
        
        self.raw_data: Optional[pd.DataFrame] = None
        self.filtered_data: Optional[pd.DataFrame] = None
        self.category_stats: Optional[pd.DataFrame] = None
        self.daily_stats: Optional[pd.DataFrame] = None
        self.weekly_stats: Optional[pd.DataFrame] = None
        
        logger.info("=" * 70)
        logger.info("短视频平台用户观看数据分析系统")
        logger.info("=" * 70)
    
    def load_data(
        self,
        data: Optional[pd.DataFrame] = None,
        file_path: Optional[Union[str, Path]] = None,
        n_records: Optional[int] = None
    ) -> pd.DataFrame:
        """加载数据（支持外部数据或模拟数据）
        
        Args:
            data: 直接传入的DataFrame
            file_path: 外部文件路径
            n_records: 模拟数据条数
            
        Returns:
            pd.DataFrame: 加载的数据
        """
        if data is not None:
            DataValidator.validate_dataframe(data)
            DataValidator.validate_data_types(data)
            self.raw_data = data
            logger.info(f"使用传入的数据，共 {len(data)} 条记录")
        elif file_path is not None:
            self.raw_data = DataGenerator.load_external_data(file_path)
        else:
            self.raw_data = self.data_generator.generate_mock_data(n_records)
        
        return self.raw_data
    
    def process_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """执行数据处理
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (过滤后数据, 类别统计)
        """
        if self.raw_data is None:
            raise ValueError("请先调用 load_data 方法加载数据")
        
        self.filtered_data, self.category_stats = self.data_processor.process(self.raw_data)
        return self.filtered_data, self.category_stats
    
    def analyze_data(
        self
    ) -> Tuple[pd.DataFrame, pd.DataFrame, float, str]:
        """执行统计分析
        
        Returns:
            Tuple: (每日统计, 周统计, 相关系数, 结论)
        """
        if self.filtered_data is None:
            raise ValueError("请先调用 process_data 方法处理数据")
        
        result = self.statistical_analyzer.analyze(self.filtered_data)
        self.daily_stats = result[0]
        self.weekly_stats = result[1]
        return result
    
    def visualize(
        self,
        output_path: Optional[Union[str, Path]] = None,
        show: bool = True
    ) -> str:
        """生成可视化图表
        
        Args:
            output_path: 输出路径
            show: 是否显示图表
            
        Returns:
            str: 保存的文件路径
        """
        if self.daily_stats is None or self.category_stats is None:
            raise ValueError("请先调用 analyze_data 方法分析数据")
        
        return self.visualizer.create_visualization(
            self.daily_stats,
            self.category_stats,
            output_path,
            show
        )
    
    def run_full_analysis(
        self,
        data: Optional[pd.DataFrame] = None,
        file_path: Optional[Union[str, Path]] = None,
        n_records: Optional[int] = None,
        output_path: Optional[Union[str, Path]] = None,
        show: bool = True
    ) -> Dict[str, Any]:
        """执行完整的分析流程
        
        Args:
            data: 直接传入的DataFrame
            file_path: 外部文件路径
            n_records: 模拟数据条数
            output_path: 可视化输出路径
            show: 是否显示图表
            
        Returns:
            Dict[str, Any]: 分析结果字典
        """
        logger.info("[1/4] 加载数据...")
        self.load_data(data, file_path, n_records)
        logger.info(f"数据字段: {self.raw_data.columns.tolist()}")
        logger.info(f"数据样例:\n{self.raw_data.head()}")
        
        logger.info("\n[2/4] 数据处理...")
        self.process_data()
        
        logger.info("\n[3/4] 统计分析...")
        daily, weekly, corr, conclusion = self.analyze_data()
        
        logger.info("\n[4/4] 生成可视化...")
        saved_path = self.visualize(output_path, show)
        
        logger.info("\n" + "=" * 70)
        logger.info("分析完成！")
        logger.info("=" * 70)
        
        return {
            'raw_data': self.raw_data,
            'filtered_data': self.filtered_data,
            'category_stats': self.category_stats,
            'daily_stats': self.daily_stats,
            'weekly_stats': self.weekly_stats,
            'correlation': corr,
            'correlation_conclusion': conclusion,
            'chart_path': saved_path
        }


def main():
    """主函数：执行完整分析流程"""
    config = AnalysisConfig()
    
    analyzer = VideoAnalysis(config)
    
    results = analyzer.run_full_analysis(
        n_records=10000,
        output_path='video_analysis_charts.png',
        show=True
    )
    
    return results


if __name__ == "__main__":
    main()
