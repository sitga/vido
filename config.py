#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置文件模块
集中管理所有配置项，便于维护和修改
"""

from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path


@dataclass
class DataConfig:
    """数据生成与处理配置"""
    # 随机种子
    random_seed: int = 42
    
    # 模拟数据规模
    default_record_count: int = 10000
    
    # 用户ID范围
    user_id_min: int = 10001
    user_id_max: int = 20000
    
    # 视频ID范围
    video_id_min: int = 1001
    video_id_max: int = 3000
    
    # 视频类别
    video_categories: List[str] = field(default_factory=lambda: [
        '娱乐', '教育', '美食', '旅游', '游戏', '科技', '生活', '美妆'
    ])
    
    # 视频时长范围（秒）
    video_duration_min: int = 15
    video_duration_max: int = 300
    
    # 观看时长分布比例
    watch_duration_distribution: Dict[str, float] = field(default_factory=lambda: {
        'invalid': 0.15,    # 无效观看（<3秒）
        'short': 0.35,      # 短观看（3-60秒）
        'medium': 0.35,     # 中等观看（60-300秒）
        'long': 0.15        # 长观看（300-600秒）
    })
    
    # 观看时长范围（秒）
    watch_duration_ranges: Dict[str, Tuple[int, int]] = field(default_factory=lambda: {
        'invalid': (0, 3),
        'short': (3, 60),
        'medium': (60, 300),
        'long': (300, 601)
    })
    
    # 数据过滤阈值
    min_watch_duration: int = 3  # 最小有效观看时长（秒）
    
    # 完播判定阈值（观看时长 >= 视频时长 * completion_threshold 视为完播）
    completion_threshold: float = 0.9
    
    # 日期范围
    start_date: str = '2024-01-01'
    end_date: str = '2024-01-30'


@dataclass
class VisualizationConfig:
    """可视化配置"""
    # 图表尺寸
    figure_size: Tuple[int, int] = (16, 12)
    
    # 子图布局
    subplot_layout: Tuple[int, int] = (2, 1)
    
    # DPI设置
    dpi: int = 300
    
    # 输出格式支持
    supported_formats: List[str] = field(default_factory=lambda: ['png', 'pdf', 'svg', 'jpg'])
    default_format: str = 'png'
    
    # 默认输出路径
    output_dir: Path = field(default_factory=lambda: Path('./output'))
    default_filename: str = 'video_analysis_charts'
    
    # 颜色配置
    colors: Dict[str, str] = field(default_factory=lambda: {
        'play_count': '#3498db',           # 播放量 - 蓝色
        'completion_rate': '#e74c3c',      # 完播率 - 红色
        'complete_plays': '#2ecc71',       # 完播次数 - 绿色
        'incomplete_plays': '#e74c3c',     # 未完播次数 - 红色
        'grid': '#cccccc',                 # 网格线
        'background': '#ffffff'            # 背景色
    })
    
    # 字体配置
    font_family: List[str] = field(default_factory=lambda: [
        'SimHei', 'Microsoft YaHei', 'DejaVu Sans'
    ])
    font_size: int = 10
    title_font_size: int = 14
    label_font_size: int = 12
    
    # 线条样式
    line_styles: Dict[str, Any] = field(default_factory=lambda: {
        'play_count': {'linewidth': 2, 'markersize': 4, 'marker': 'o'},
        'completion_rate': {'linewidth': 2, 'markersize': 4, 'marker': 's'}
    })
    
    # 柱状图配置
    bar_width: float = 0.6
    
    # 布局配置
    tight_layout_pad: float = 3.0
    rotation_angle: int = 45


@dataclass
class AnalysisConfig:
    """统计分析配置"""
    # 相关性分析阈值
    correlation_threshold_weak: float = 0.1
    correlation_threshold_strong: float = 0.5
    
    # 视频时长分组区间（用于相关性分析）
    duration_bins: List[int] = field(default_factory=lambda: [0, 30, 60, 120, 180, 300])
    duration_labels: List[str] = field(default_factory=lambda: [
        '0-30秒', '31-60秒', '61-120秒', '121-180秒', '180秒以上'
    ])
    
    # 统计指标保留小数位
    decimal_places: int = 2


@dataclass
class LoggingConfig:
    """日志配置"""
    # 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
    level: str = 'INFO'
    
    # 日志格式
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format: str = '%Y-%m-%d %H:%M:%S'
    
    # 日志文件配置
    log_to_file: bool = False
    log_file_path: Optional[Path] = None
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class FileConfig:
    """文件路径配置"""
    # 支持的输入文件格式
    supported_input_formats: List[str] = field(default_factory=lambda: ['csv', 'xlsx', 'xls'])
    
    # 默认编码
    default_encoding: str = 'utf-8'
    
    # CSV读取配置
    csv_separator: str = ','
    csv_engine: Optional[str] = None
    
    # Excel读取配置
    excel_sheet_name: Union[str, int] = 0
    excel_engine: Optional[str] = None


# 类型修复
from typing import Union


@dataclass
class AppConfig:
    """应用总配置"""
    data: DataConfig = field(default_factory=DataConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    file: FileConfig = field(default_factory=FileConfig)


# 全局配置实例
config = AppConfig()
