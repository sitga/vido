#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
短视频平台用户观看数据综合分析系统
技术栈：Python + Pandas + Matplotlib
功能：数据加载、处理、统计分析、可视化展示

重构特性：
- 配置化管理：所有配置项集中管理，便于修改
- 面向对象设计：模块化封装，提高代码复用性
- 性能优化：使用pivot_table优化聚合，减少数据拷贝
- 健壮性增强：完善的异常处理和参数校验
- 类型注解：完整的类型提示，提升可读性
- 日志系统：替代print，使用标准logging模块
- 多数据源支持：支持模拟数据和外部CSV/Excel
- 灵活可视化：支持多种输出格式和自定义样式
"""

from typing import Optional, Union, List, Dict, Any, Tuple
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import numpy as np

# 导入各模块
from config import config, AppConfig, DataConfig, VisualizationConfig
from utils import logger, setup_logger, validate_dataframe, format_number
from data_loader import DataLoader, load_mock_data, load_from_file
from data_processor import DataProcessor, process_data
from analyzer import (
    VideoAnalyzer, TrendAnalysisResult, CorrelationResult, 
    UserSegmentResult, analyze_daily_trends, analyze_correlation
)
from visualizer import Visualizer, plot_analysis_results


@dataclass
class AnalysisReport:
    """分析报告数据类"""
    summary: Dict[str, Any]
    daily_stats: pd.DataFrame
    weekly_stats: pd.DataFrame
    category_stats: pd.DataFrame
    correlation_result: Optional[CorrelationResult]
    user_segments: Optional[UserSegmentResult]
    output_files: List[Path]


class VideoAnalysisSystem:
    """
    视频分析系统主类
    整合数据加载、处理、分析、可视化全流程
    """
    
    def __init__(self, app_config: Optional[AppConfig] = None):
        """
        初始化分析系统
        
        Args:
            app_config: 应用配置对象，默认使用全局配置
        """
        self.config = app_config or config
        
        # 初始化各模块
        self.data_loader = DataLoader(self.config.data)
        self.data_processor = DataProcessor(self.config.data)
        self.analyzer = VideoAnalyzer(self.config.analysis)
        self.visualizer = Visualizer(self.config.visualization)
        
        # 存储中间结果
        self._raw_data: Optional[pd.DataFrame] = None
        self._processed_data: Optional[pd.DataFrame] = None
        self._category_stats: Optional[pd.DataFrame] = None
        self._trend_result: Optional[TrendAnalysisResult] = None
        
        logger.info("视频分析系统初始化完成")
    
    def load_data(self, source: Optional[Union[str, Path]] = None,
                  data_type: str = 'mock',
                  n_records: Optional[int] = None) -> pd.DataFrame:
        """
        加载数据
        
        Args:
            source: 数据源（文件路径或None）
            data_type: 数据类型 ('mock', 'csv', 'excel')
            n_records: 模拟数据记录数
            
        Returns:
            加载的DataFrame
        """
        logger.info(f"加载数据: type={data_type}, source={source}")
        
        self._raw_data = self.data_loader.load_data(source, data_type, n_records)
        
        # 验证数据
        self.data_loader.validate_data(self._raw_data)
        
        logger.info(f"数据加载完成，共 {len(self._raw_data)} 条记录")
        return self._raw_data
    
    def process_data(self, filter_invalid: bool = True,
                    add_time_features: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        处理数据
        
        Args:
            filter_invalid: 是否过滤无效观看
            add_time_features: 是否添加时间特征
            
        Returns:
            (处理后的数据, 类别统计) 元组
        """
        if self._raw_data is None:
            raise ValueError("请先加载数据")
        
        logger.info("开始数据处理...")
        
        self._processed_data, self._category_stats = self.data_processor.process_pipeline(
            self._raw_data,
            filter_invalid=filter_invalid,
            add_time_features=add_time_features
        )
        
        return self._processed_data, self._category_stats
    
    def analyze(self, include_correlation: bool = True,
               include_user_segments: bool = True) -> Dict[str, Any]:
        """
        执行分析
        
        Args:
            include_correlation: 是否包含相关性分析
            include_user_segments: 是否包含用户分层分析
            
        Returns:
            分析结果字典
        """
        if self._processed_data is None:
            raise ValueError("请先处理数据")
        
        logger.info("开始数据分析...")
        
        results = {}
        
        # 1. 趋势分析
        self._trend_result = self.analyzer.analyze_trends(self._processed_data)
        results['trend'] = self._trend_result
        
        # 2. 相关性分析
        if include_correlation:
            correlation = self.analyzer.analyze_correlation(self._processed_data)
            results['correlation'] = correlation
        
        # 3. 用户分层分析
        if include_user_segments:
            user_segments = self.analyzer.analyze_user_segments(
                self._processed_data, segment_by='activity'
            )
            results['user_segments'] = user_segments
        
        logger.info("数据分析完成")
        return results
    
    def visualize(self, output_path: Optional[Union[str, Path]] = None,
                 show_chart: bool = False) -> Path:
        """
        生成可视化图表
        
        Args:
            output_path: 输出路径
            show_chart: 是否显示图表
            
        Returns:
            保存的文件路径
        """
        if self._trend_result is None or self._category_stats is None:
            raise ValueError("请先执行分析")
        
        logger.info("生成可视化图表...")
        
        fig = self.visualizer.create_dashboard(
            self._trend_result.daily_stats,
            self._category_stats,
            self._trend_result.weekly_stats
        )
        
        if output_path:
            saved_path = self.visualizer.save_chart(output_path, fig)
        else:
            saved_path = self.visualizer.save_chart(fig=fig)
        
        if show_chart:
            self.visualizer.show()
        
        self.visualizer.close(fig)
        
        return saved_path
    
    def generate_report(self, output_dir: Optional[Union[str, Path]] = None) -> AnalysisReport:
        """
        生成完整分析报告
        
        Args:
            output_dir: 输出目录
            
        Returns:
            分析报告对象
        """
        if self._processed_data is None:
            raise ValueError("请先完成数据处理")
        
        logger.info("生成分析报告...")
        
        # 数据摘要
        summary = self.data_processor.get_summary_stats(self._processed_data)
        
        # 执行分析
        analysis_results = self.analyze()
        
        # 生成可视化
        output_files = []
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 主图表
            main_chart_path = output_dir / 'analysis_dashboard.png'
            self.visualize(main_chart_path)
            output_files.append(main_chart_path)
            
            # 类别对比图
            from visualizer import plot_category_comparison
            category_chart_path = output_dir / 'category_comparison.png'
            plot_category_comparison(self._category_stats, category_chart_path)
            output_files.append(category_chart_path)
        
        report = AnalysisReport(
            summary=summary,
            daily_stats=self._trend_result.daily_stats if self._trend_result else pd.DataFrame(),
            weekly_stats=self._trend_result.weekly_stats if self._trend_result else pd.DataFrame(),
            category_stats=self._category_stats,
            correlation_result=analysis_results.get('correlation'),
            user_segments=analysis_results.get('user_segments'),
            output_files=output_files
        )
        
        logger.info("分析报告生成完成")
        return report
    
    def run_full_pipeline(self, source: Optional[Union[str, Path]] = None,
                         data_type: str = 'mock',
                         n_records: Optional[int] = None,
                         output_dir: Optional[Union[str, Path]] = None) -> AnalysisReport:
        """
        执行完整分析流程
        
        Args:
            source: 数据源
            data_type: 数据类型
            n_records: 模拟数据记录数
            output_dir: 输出目录
            
        Returns:
            分析报告对象
        """
        logger.info("=" * 60)
        logger.info("开始执行完整分析流程")
        logger.info("=" * 60)
        
        # 1. 加载数据
        self.load_data(source, data_type, n_records)
        
        # 2. 处理数据
        self.process_data()
        
        # 3. 生成报告
        report = self.generate_report(output_dir)
        
        logger.info("=" * 60)
        logger.info("分析流程执行完成")
        logger.info("=" * 60)
        
        return report
    
    def print_report(self, report: AnalysisReport) -> None:
        """
        打印分析报告到控制台
        
        Args:
            report: 分析报告对象
        """
        print("\n" + "=" * 70)
        print("短视频平台用户观看数据分析报告")
        print("=" * 70)
        
        # 数据摘要
        print("\n【数据摘要】")
        print(f"总记录数: {report.summary['total_records']:,}")
        print(f"独立用户数: {report.summary['unique_users']:,}")
        print(f"独立视频数: {report.summary['unique_videos']:,}")
        print(f"视频类别数: {report.summary['categories']}")
        print(f"数据时间范围: {report.summary['date_range']['start']} 至 {report.summary['date_range']['end']}")
        print(f"整体完播率: {report.summary['overall_completion_rate']:.2f}%")
        
        # 类别统计
        print("\n【视频类别统计】")
        print(report.category_stats.to_string())
        
        # 趋势摘要
        if hasattr(report, 'daily_stats') and not report.daily_stats.empty:
            print("\n【每日趋势摘要】")
            daily_summary = {
                '平均日播放量': format_number(report.daily_stats['daily_plays'].mean()),
                '最高日播放量': format_number(report.daily_stats['daily_plays'].max()),
                '平均日完播率': format_number(report.daily_stats['daily_completion_rate'].mean(), suffix='%')
            }
            for key, value in daily_summary.items():
                print(f"  {key}: {value}")
        
        # 相关性分析
        if report.correlation_result:
            print("\n【相关性分析】")
            print(f"视频时长与完播率相关系数: {report.correlation_result.correlation_coefficient}")
            print(f"结论: {report.correlation_result.interpretation}")
        
        # 用户分层
        if report.user_segments:
            print("\n【用户分层分析】")
            print(report.user_segments.segment_distribution.to_string())
            print("\n洞察:")
            for insight in report.user_segments.insights:
                print(f"  • {insight}")
        
        # 输出文件
        if report.output_files:
            print("\n【输出文件】")
            for file_path in report.output_files:
                print(f"  • {file_path}")
        
        print("\n" + "=" * 70)


def main():
    """
    主函数：执行完整分析流程
    
    使用示例:
        python video_analysis.py
    
    使用外部数据:
        python video_analysis.py --data-type csv --source data.csv
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='短视频平台用户观看数据分析')
    parser.add_argument('--data-type', type=str, default='mock',
                       choices=['mock', 'csv', 'excel'],
                       help='数据类型 (默认: mock)')
    parser.add_argument('--source', type=str, default=None,
                       help='数据源文件路径')
    parser.add_argument('--n-records', type=int, default=10000,
                       help='模拟数据记录数 (默认: 10000)')
    parser.add_argument('--output-dir', type=str, default='./output',
                       help='输出目录 (默认: ./output)')
    parser.add_argument('--show-chart', action='store_true',
                       help='显示图表')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='日志级别 (默认: INFO)')
    
    args = parser.parse_args()
    
    # 设置日志级别
    logger.setLevel(args.log_level)
    
    # 创建分析系统
    system = VideoAnalysisSystem()
    
    # 执行分析流程
    report = system.run_full_pipeline(
        source=args.source,
        data_type=args.data_type,
        n_records=args.n_records,
        output_dir=args.output_dir
    )
    
    # 打印报告
    system.print_report(report)
    
    # 显示图表（如果需要）
    if args.show_chart:
        system.visualize(show_chart=True)


if __name__ == "__main__":
    main()
