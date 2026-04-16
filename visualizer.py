#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可视化模块
提供图表绘制功能，支持多种输出格式
"""

from typing import Optional, Union, List, Dict, Any, Tuple
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import warnings

from config import config, VisualizationConfig
from utils import logger, ensure_output_dir, handle_exceptions

# 设置中文显示
warnings.filterwarnings('ignore')


@dataclass
class ChartConfig:
    """图表配置"""
    title: str
    xlabel: str
    ylabel: str
    figsize: Tuple[int, int] = (12, 6)


class Visualizer:
    """
    可视化器类
    提供多种图表绘制功能
    """
    
    def __init__(self, viz_config: Optional[VisualizationConfig] = None):
        """
        初始化可视化器
        
        Args:
            viz_config: 可视化配置对象，默认使用全局配置
        """
        self.config = viz_config or config.visualization
        self._setup_fonts()
        self._current_figure: Optional[Figure] = None
    
    def _setup_fonts(self) -> None:
        """配置中文字体"""
        rcParams['font.sans-serif'] = self.config.font_family
        rcParams['axes.unicode_minus'] = False
        rcParams['font.size'] = self.config.font_size
    
    def create_figure(self, figsize: Optional[Tuple[int, int]] = None) -> Figure:
        """
        创建新图表
        
        Args:
            figsize: 图表尺寸
            
        Returns:
            Figure对象
        """
        figsize = figsize or self.config.figure_size
        self._current_figure = plt.figure(figsize=figsize)
        return self._current_figure
    
    def plot_trend_chart(self, daily_stats: pd.DataFrame,
                        ax: Optional[Axes] = None) -> Axes:
        """
        绘制趋势图（播放量与完播率双Y轴）
        
        Args:
            daily_stats: 每日统计数据
            ax: 可选的Axes对象，为None则创建新图
            
        Returns:
            Axes对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(12, 6))
        
        dates = daily_stats.index
        play_count = daily_stats['daily_plays']
        completion_rate = daily_stats['daily_completion_rate']
        
        colors = self.config.colors
        line_styles = self.config.line_styles
        
        # 绘制播放量折线
        line1 = ax.plot(dates, play_count, color=colors['play_count'],
                       label='当日播放量', **line_styles['play_count'])
        ax.set_xlabel('日期', fontsize=self.config.label_font_size)
        ax.set_ylabel('播放量（次）', fontsize=self.config.label_font_size, 
                     color=colors['play_count'])
        ax.tick_params(axis='y', labelcolor=colors['play_count'])
        ax.tick_params(axis='x', rotation=self.config.rotation_angle)
        ax.grid(True, alpha=0.3, color=colors['grid'])
        
        # 创建双Y轴展示完播率
        ax_twin = ax.twinx()
        line2 = ax_twin.plot(dates, completion_rate, color=colors['completion_rate'],
                            label='日完播率(%)', **line_styles['completion_rate'])
        ax_twin.set_ylabel('完播率（%）', fontsize=self.config.label_font_size,
                          color=colors['completion_rate'])
        ax_twin.tick_params(axis='y', labelcolor=colors['completion_rate'])
        
        # 合并图例
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax.legend(lines, labels, loc='upper left')
        
        ax.set_title('每日播放量与完播率趋势图', 
                    fontsize=self.config.title_font_size, pad=20)
        
        return ax
    
    def plot_stacked_bar_chart(self, category_stats: pd.DataFrame,
                              ax: Optional[Axes] = None) -> Axes:
        """
        绘制堆叠柱状图（各类别观看次数与完播情况）
        
        Args:
            category_stats: 类别统计数据
            ax: 可选的Axes对象，为None则创建新图
            
        Returns:
            Axes对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(12, 6))
        
        categories = category_stats.index.tolist()
        
        # 计算完播和未完播次数
        complete_plays = category_stats['watch_count'] * (category_stats['completion_rate'] / 100)
        incomplete_plays = category_stats['watch_count'] - complete_plays
        
        colors = self.config.colors
        x = np.arange(len(categories))
        width = self.config.bar_width
        
        # 绘制堆叠柱状图
        rects1 = ax.bar(x, complete_plays, width, 
                       label='完播次数', color=colors['complete_plays'])
        rects2 = ax.bar(x, incomplete_plays, width, 
                       bottom=complete_plays, label='未完播次数',
                       color=colors['incomplete_plays'])
        
        ax.set_xlabel('视频类别', fontsize=self.config.label_font_size)
        ax.set_ylabel('观看次数（次）', fontsize=self.config.label_font_size)
        ax.set_title('各视频类别观看次数（按完播状态堆叠）',
                    fontsize=self.config.title_font_size, pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=self.config.rotation_angle)
        ax.legend()
        
        # 在柱状图上添加完播率标签
        max_count = category_stats['watch_count'].max()
        for i, v in enumerate(category_stats['completion_rate']):
            total_height = complete_plays.iloc[i] + incomplete_plays.iloc[i]
            ax.text(i, total_height + max_count * 0.01,
                   f'{v:.1f}%', ha='center', va='bottom', fontsize=10)
        
        return ax
    
    def plot_completion_by_category(self, category_stats: pd.DataFrame,
                                    ax: Optional[Axes] = None) -> Axes:
        """
        绘制各类别完播率对比图
        
        Args:
            category_stats: 类别统计数据
            ax: 可选的Axes对象
            
        Returns:
            Axes对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(10, 6))
        
        categories = category_stats.index.tolist()
        completion_rates = category_stats['completion_rate']
        
        colors = plt.cm.RdYlGn(
            completion_rates / completion_rates.max()
        )
        
        bars = ax.barh(categories, completion_rates, color=colors)
        ax.set_xlabel('完播率（%）', fontsize=self.config.label_font_size)
        ax.set_title('各视频类别完播率对比',
                    fontsize=self.config.title_font_size, pad=20)
        
        # 添加数值标签
        for i, (bar, rate) in enumerate(zip(bars, completion_rates)):
            ax.text(rate + 0.5, i, f'{rate:.1f}%',
                   va='center', fontsize=10)
        
        ax.set_xlim(0, completion_rates.max() * 1.15)
        
        return ax
    
    def plot_duration_correlation(self, correlation_data: pd.DataFrame,
                                  ax: Optional[Axes] = None) -> Axes:
        """
        绘制视频时长与完播率关系图
        
        Args:
            correlation_data: 相关性数据
            ax: 可选的Axes对象
            
        Returns:
            Axes对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(10, 6))
        
        # 散点图
        ax.scatter(correlation_data['video_duration'],
                  correlation_data['completion_status'] * 100,
                  alpha=0.5, s=30)
        
        # 添加趋势线
        z = np.polyfit(correlation_data['video_duration'],
                      correlation_data['completion_status'] * 100, 1)
        p = np.poly1d(z)
        x_line = np.linspace(correlation_data['video_duration'].min(),
                            correlation_data['video_duration'].max(), 100)
        ax.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2)
        
        ax.set_xlabel('视频时长（秒）', fontsize=self.config.label_font_size)
        ax.set_ylabel('完播率（%）', fontsize=self.config.label_font_size)
        ax.set_title('视频时长与完播率关系',
                    fontsize=self.config.title_font_size, pad=20)
        ax.grid(True, alpha=0.3)
        
        return ax
    
    def plot_weekly_comparison(self, weekly_stats: pd.DataFrame,
                               ax: Optional[Axes] = None) -> Axes:
        """
        绘制周环比对比图
        
        Args:
            weekly_stats: 周统计数据
            ax: 可选的Axes对象
            
        Returns:
            Axes对象
        """
        if ax is None:
            _, ax = plt.subplots(figsize=(12, 6))
        
        # 创建周标签
        week_labels = [f"{int(row[0])}W{int(row[1]):02d}" 
                      for row in weekly_stats.index]
        
        x = np.arange(len(week_labels))
        width = 0.35
        
        plays = weekly_stats['weekly_plays']
        
        bars = ax.bar(x, plays, width, label='周播放量',
                     color=self.config.colors['play_count'])
        
        ax.set_xlabel('周', fontsize=self.config.label_font_size)
        ax.set_ylabel('播放量', fontsize=self.config.label_font_size)
        ax.set_title('周播放量趋势', fontsize=self.config.title_font_size, pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(week_labels, rotation=self.config.rotation_angle)
        
        # 添加环比变化标注
        for i, (bar, change) in enumerate(zip(bars, 
                                              weekly_stats['plays_change_pct'])):
            if not np.isnan(change):
                height = bar.get_height()
                color = 'green' if change > 0 else 'red'
                symbol = '↑' if change > 0 else '↓'
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{symbol}{abs(change):.1f}%',
                       ha='center', va='bottom', fontsize=8, color=color)
        
        return ax
    
    def create_dashboard(self, daily_stats: pd.DataFrame,
                        category_stats: pd.DataFrame,
                        weekly_stats: Optional[pd.DataFrame] = None) -> Figure:
        """
        创建综合仪表盘
        
        Args:
            daily_stats: 每日统计数据
            category_stats: 类别统计数据
            weekly_stats: 周统计数据（可选）
            
        Returns:
            Figure对象
        """
        fig = self.create_figure()
        
        if weekly_stats is not None:
            # 2x2布局
            gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
            
            ax1 = fig.add_subplot(gs[0, :])
            self.plot_trend_chart(daily_stats, ax=ax1)
            
            ax2 = fig.add_subplot(gs[1, 0])
            self.plot_stacked_bar_chart(category_stats, ax=ax2)
            
            ax3 = fig.add_subplot(gs[1, 1])
            self.plot_weekly_comparison(weekly_stats, ax=ax3)
        else:
            # 2x1布局
            gs = fig.add_gridspec(2, 1, hspace=0.3)
            
            ax1 = fig.add_subplot(gs[0])
            self.plot_trend_chart(daily_stats, ax=ax1)
            
            ax2 = fig.add_subplot(gs[1])
            self.plot_stacked_bar_chart(category_stats, ax=ax2)
        
        plt.tight_layout(pad=self.config.tight_layout_pad)
        
        return fig
    
    @handle_exceptions(default_return=None, log_level='error')
    def save_chart(self, output_path: Optional[Union[str, Path]] = None,
                  fig: Optional[Figure] = None,
                  format: Optional[str] = None,
                  **kwargs) -> Path:
        """
        保存图表
        
        Args:
            output_path: 输出路径，默认使用配置路径
            fig: 要保存的Figure，默认使用当前图表
            format: 输出格式
            **kwargs: 传递给savefig的额外参数
            
        Returns:
            保存的文件路径
        """
        if fig is None:
            fig = self._current_figure
        
        if fig is None:
            raise ValueError("没有可保存的图表")
        
        # 确定输出路径
        if output_path is None:
            output_dir = self.config.output_dir
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{self.config.default_filename}.{format or self.config.default_format}"
        else:
            output_path = Path(output_path)
            ensure_output_dir(output_path)
        
        # 确定格式
        if format is None:
            format = output_path.suffix.lstrip('.') or self.config.default_format
        
        if format not in self.config.supported_formats:
            raise ValueError(f"不支持的格式: {format}")
        
        # 保存参数
        save_kwargs = {
            'dpi': self.config.dpi,
            'bbox_inches': 'tight',
            'facecolor': 'white'
        }
        save_kwargs.update(kwargs)
        
        fig.savefig(output_path, format=format, **save_kwargs)
        logger.info(f"图表已保存: {output_path}")
        
        return output_path
    
    def show(self) -> None:
        """显示图表"""
        plt.show()
    
    def close(self, fig: Optional[Figure] = None) -> None:
        """
        关闭图表
        
        Args:
            fig: 要关闭的Figure，None则关闭当前图表
        """
        if fig is None:
            fig = self._current_figure
        
        if fig is not None:
            plt.close(fig)
            if fig is self._current_figure:
                self._current_figure = None


# 便捷函数
def plot_analysis_results(daily_stats: pd.DataFrame,
                         category_stats: pd.DataFrame,
                         output_path: Optional[str] = None) -> Path:
    """
    快速绘制分析结果图表
    
    Args:
        daily_stats: 每日统计数据
        category_stats: 类别统计数据
        output_path: 输出路径
        
    Returns:
        保存的文件路径
    """
    viz = Visualizer()
    fig = viz.create_dashboard(daily_stats, category_stats)
    
    if output_path:
        return viz.save_chart(output_path)
    else:
        viz.show()
        return None


def plot_category_comparison(category_stats: pd.DataFrame,
                            output_path: Optional[str] = None) -> Path:
    """
    快速绘制类别对比图
    
    Args:
        category_stats: 类别统计数据
        output_path: 输出路径
        
    Returns:
        保存的文件路径
    """
    viz = Visualizer()
    viz.create_figure(figsize=(10, 6))
    viz.plot_completion_by_category(category_stats)
    
    if output_path:
        return viz.save_chart(output_path)
    else:
        viz.show()
        return None
