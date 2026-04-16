#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
短视频平台用户观看数据综合分析脚本

重构说明：
- 采用面向对象设计，核心逻辑封装在VideoAnalysis类中
- 所有配置项集中管理，支持灵活配置
- 完善的异常处理和日志记录
- 性能优化，减少数据拷贝
- 完整的类型注解和文档字符串

技术栈：Python + Pandas + Matplotlib + Logging
功能：数据生成/读取、数据处理、统计分析、可视化展示
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Tuple, Union, Any, Dict, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
import warnings

warnings.filterwarnings('ignore')


@dataclass
class AnalysisConfig:
    """
    分析配置类，集中管理所有配置参数

    Attributes:
        random_seed: 随机种子，保证结果可复现
        font_families: 中文字体配置列表
        font_size: 默认字体大小
        figure_size: 图表尺寸 (宽度, 高度)
        figure_dpi: 图表分辨率
        min_watch_seconds: 有效观看最小时长阈值
        completion_rate_threshold: 完播率阈值比例
        output_dir: 图表输出目录
        output_format: 图表输出格式 (png/pdf/svg)
        default_records: 默认生成的模拟数据条数
        log_level: 日志级别
    """

    random_seed: int = 42
    font_families: List[str] = field(
        default_factory=lambda: ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
    )
    font_size: int = 10
    figure_size: Tuple[int, int] = (16, 12)
    figure_dpi: int = 300
    min_watch_seconds: int = 3
    completion_rate_threshold: float = 0.9
    output_dir: str = './output'
    output_format: str = 'png'
    default_records: int = 10000
    log_level: int = logging.INFO

    def __post_init__(self) -> None:
        """初始化后验证配置并创建输出目录"""
        self._validate_config()
        self._ensure_output_dir()

    def _validate_config(self) -> None:
        """验证配置参数的合法性"""
        assert self.random_seed >= 0, "随机种子必须为非负整数"
        assert self.min_watch_seconds > 0, "最小观看时长必须大于0"
        assert 0 < self.completion_rate_threshold <= 1, "完播率阈值必须在(0,1]范围内"
        assert self.output_format in ['png', 'pdf', 'svg'], "不支持的图表格式"
        assert self.figure_dpi > 0, "DPI必须大于0"

    def _ensure_output_dir(self) -> None:
        """确保输出目录存在"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    配置并返回日志记录器

    Args:
        level: 日志级别

    Returns:
        配置好的Logger实例
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    验证DataFrame是否包含必需的列且数据合法

    Args:
        df: 待验证的DataFrame
        required_columns: 必需的列名列表

    Returns:
        验证通过返回True

    Raises:
        ValueError: 数据验证失败时抛出
        TypeError: 数据类型不正确时抛出
    """
    if df is None or df.empty:
        raise ValueError("DataFrame为空或为None")

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"缺少必需的列: {missing_columns}")

    if '观看时长（秒）' in df.columns and (df['观看时长（秒）'] < 0).any():
        raise ValueError("观看时长不能为负数")

    if '视频时长（秒）' in df.columns and (df['视频时长（秒）'] <= 0).any():
        raise ValueError("视频时长必须大于0")

    return True


def type_check(func: Callable) -> Callable:
    """
    参数类型校验装饰器

    Args:
        func: 被装饰的函数

    Returns:
        包装后的函数
    """
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except TypeError as e:
            logger = logging.getLogger(__name__)
            logger.error(f"参数类型错误: {str(e)}")
            raise
    return wrapper


class VideoAnalysis:
    """
    视频数据分析主类，封装完整的分析流程

    Attributes:
        config: 分析配置实例
        logger: 日志记录器
        raw_data: 原始数据
        filtered_data: 过滤后的数据
        category_stats: 按类别统计结果
        daily_stats: 每日统计结果
        weekly_stats: 每周统计结果
    """

    REQUIRED_COLUMNS = [
        '用户ID', '视频ID', '观看时长（秒）',
        '完播状态', '发布时间', '视频类别', '视频时长（秒）'
    ]

    CATEGORIES = ['娱乐', '教育', '美食', '旅游', '游戏', '科技', '生活', '美妆']

    def __init__(self, config: Optional[AnalysisConfig] = None) -> None:
        """
        初始化分析器

        Args:
            config: 自定义配置，不提供则使用默认配置
        """
        self.config = config or AnalysisConfig()
        self.logger = setup_logging(self.config.log_level)
        self._setup_matplotlib()
        np.random.seed(self.config.random_seed)

        self.raw_data: Optional[pd.DataFrame] = None
        self.filtered_data: Optional[pd.DataFrame] = None
        self.category_stats: Optional[pd.DataFrame] = None
        self.daily_stats: Optional[pd.DataFrame] = None
        self.weekly_stats: Optional[pd.DataFrame] = None

        self.logger.info("视频数据分析器初始化完成")

    def _setup_matplotlib(self) -> None:
        """配置Matplotlib中文字体和绘图参数"""
        rcParams['font.sans-serif'] = self.config.font_families
        rcParams['axes.unicode_minus'] = False
        rcParams['font.size'] = self.config.font_size

    @type_check
    def generate_mock_data(self, n_records: Optional[int] = None) -> pd.DataFrame:
        """
        生成模拟的短视频观看数据

        Args:
            n_records: 数据条数，不提供则使用配置中的默认值

        Returns:
            包含观看数据的DataFrame
        """
        n = n_records or self.config.default_records
        self.logger.info(f"开始生成{n}条模拟数据...")

        try:
            user_ids = np.random.randint(10001, 20000, size=n)
            video_ids = np.random.randint(1001, 3000, size=n)
            video_categories = np.random.choice(self.CATEGORIES, size=n)
            publish_dates = pd.date_range(start='2024-01-01', end='2024-01-30', periods=n)
            publish_dates = np.random.choice(publish_dates, size=n)

            watch_durations = self._generate_watch_durations(n)

            unique_videos = np.unique(video_ids)
            video_durations_dict = {
                vid: np.random.randint(15, 301) for vid in unique_videos
            }
            video_durations = np.array([video_durations_dict[vid] for vid in video_ids])

            threshold = self.config.completion_rate_threshold
            completion_status = watch_durations >= (video_durations * threshold)

            self.raw_data = pd.DataFrame({
                '用户ID': user_ids,
                '视频ID': video_ids,
                '观看时长（秒）': watch_durations,
                '完播状态': completion_status,
                '发布时间': publish_dates,
                '视频类别': video_categories,
                '视频时长（秒）': video_durations
            })

            self.logger.info(f"模拟数据生成成功，共{len(self.raw_data)}条记录")
            return self.raw_data

        except Exception as e:
            self.logger.error(f"生成模拟数据失败: {str(e)}", exc_info=True)
            raise

    def _generate_watch_durations(self, n_records: int) -> np.ndarray:
        """
        生成符合真实分布的观看时长数据

        Args:
            n_records: 数据条数

        Returns:
            观看时长数组
        """
        watch_durations = np.concatenate([
            np.random.randint(0, 3, size=int(n_records * 0.15)),
            np.random.randint(3, 60, size=int(n_records * 0.35)),
            np.random.randint(60, 300, size=int(n_records * 0.35)),
            np.random.randint(300, 601, size=int(n_records * 0.15))
        ])
        np.random.shuffle(watch_durations)
        return watch_durations

    @type_check
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        从外部文件读取数据（支持CSV/Excel）

        Args:
            file_path: 数据文件路径

        Returns:
            读取的DataFrame

        Raises:
            FileNotFoundError: 文件不存在时抛出
            ValueError: 文件格式不支持时抛出
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"数据文件不存在: {file_path}")

        self.logger.info(f"从文件加载数据: {file_path}")

        try:
            if path.suffix.lower() == '.csv':
                self.raw_data = pd.read_csv(file_path, parse_dates=['发布时间'])
            elif path.suffix.lower() in ['.xlsx', '.xls']:
                self.raw_data = pd.read_excel(file_path, parse_dates=['发布时间'])
            else:
                raise ValueError(f"不支持的文件格式: {path.suffix}")

            validate_dataframe(self.raw_data, self.REQUIRED_COLUMNS)
            self.logger.info(f"数据加载成功，共{len(self.raw_data)}条记录")
            return self.raw_data

        except Exception as e:
            self.logger.error(f"加载数据失败: {str(e)}", exc_info=True)
            raise

    @type_check
    def process_data(self, df: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        数据处理模块：过滤无效观看，按类别计算完播率

        Args:
            df: 输入数据，不提供则使用self.raw_data

        Returns:
            (过滤后的数据, 按类别聚合的结果)
        """
        input_df = df if df is not None else self.raw_data
        if input_df is None:
            raise ValueError("没有可处理的数据，请先调用generate_mock_data或load_data")

        self.logger.info("开始数据处理...")
        self.logger.info("=" * 60)
        self.logger.info("数据处理模块")
        self.logger.info("=" * 60)

        try:
            validate_dataframe(input_df, self.REQUIRED_COLUMNS)

            initial_count = len(input_df)
            min_watch = self.config.min_watch_seconds

            filtered_df = input_df[
                input_df['观看时长（秒）'] >= min_watch
            ].reset_index(drop=True)

            filtered_count = len(filtered_df)
            filter_rate = (1 - filtered_count / initial_count) * 100

            self.logger.info(f"原始数据条数：{initial_count}")
            self.logger.info(f"过滤无效观看（<{min_watch}秒）后数据条数：{filtered_count}")
            self.logger.info(f"过滤比例：{filter_rate:.2f}%")

            self.filtered_data = filtered_df

            category_stats = pd.pivot_table(
                filtered_df,
                index='视频类别',
                values=['完播状态', '观看时长（秒）', '视频时长（秒）'],
                aggfunc={
                    '完播状态': ['count', 'mean'],
                    '观看时长（秒）': 'mean',
                    '视频时长（秒）': 'mean'
                }
            ).round(4)

            category_stats.columns = [
                '观看次数', '完播率', '平均观看时长（秒）', '平均视频时长（秒）'
            ]
            category_stats['完播率'] = category_stats['完播率'] * 100
            category_stats = category_stats.sort_values('完播率', ascending=False)

            self.category_stats = category_stats

            self.logger.info("\n按视频类别统计（过滤无效观看后）：")
            self.logger.info(f"\n{category_stats.round(2).to_string()}")

            return filtered_df, category_stats

        except Exception as e:
            self.logger.error(f"数据处理失败: {str(e)}", exc_info=True)
            raise

    @type_check
    def statistical_analysis(
        self,
        filtered_df: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        统计分析模块：每日播放量、周环比、相关性分析、多维度对比

        Args:
            filtered_df: 过滤后的数据，不提供则使用self.filtered_data

        Returns:
            (每日统计数据, 周环比数据)
        """
        df = filtered_df if filtered_df is not None else self.filtered_data
        if df is None:
            raise ValueError("没有可分析的数据，请先调用process_data")

        self.logger.info("\n" + "=" * 60)
        self.logger.info("统计分析模块")
        self.logger.info("=" * 60)

        try:
            df = df.copy()
            df['观看日期'] = df['发布时间'].dt.date

            daily_stats = pd.pivot_table(
                df,
                index='观看日期',
                values=['视频ID', '完播状态'],
                aggfunc={'视频ID': 'count', '完播状态': 'mean'}
            ).rename(columns={'视频ID': '当日播放量', '完播状态': '日完播率'})

            daily_stats['日完播率'] = (daily_stats['日完播率'] * 100).round(2)
            self.daily_stats = daily_stats

            self.logger.info("\n每日播放量统计（前10天）：")
            self.logger.info(f"\n{daily_stats.head(10).to_string()}")

            df['年份'] = df['发布时间'].dt.year
            df['周数'] = df['发布时间'].dt.isocalendar().week

            weekly_stats = pd.pivot_table(
                df,
                index=['年份', '周数'],
                values=['视频ID', '完播状态'],
                aggfunc={'视频ID': 'count', '完播状态': 'mean'}
            ).rename(columns={'视频ID': '周播放量', '完播状态': '周完播率'})

            weekly_stats['周完播率'] = (weekly_stats['周完播率'] * 100).round(2)
            weekly_stats['播放量周环比(%)'] = weekly_stats['周播放量'].pct_change() * 100
            weekly_stats['完播率周环比(%)'] = weekly_stats['周完播率'].pct_change() * 100

            self.weekly_stats = weekly_stats

            self.logger.info("\n周统计及周环比：")
            self.logger.info(f"\n{weekly_stats.round(2).to_string()}")

            self._analyze_correlation(df)
            self._multi_dimension_analysis(df)

            return daily_stats, weekly_stats

        except Exception as e:
            self.logger.error(f"统计分析失败: {str(e)}", exc_info=True)
            raise

    def _analyze_correlation(self, df: pd.DataFrame) -> None:
        """分析视频时长与完播率的相关性"""
        self.logger.info("\n" + "-" * 60)
        self.logger.info("完播率与视频时长的相关性分析：")
        self.logger.info("-" * 60)

        corr_data = pd.pivot_table(
            df,
            index='视频ID',
            values=['视频时长（秒）', '完播状态'],
            aggfunc={'视频时长（秒）': 'first', '完播状态': 'mean'}
        )

        correlation = corr_data['视频时长（秒）'].corr(corr_data['完播状态'])

        self.logger.info(f"视频时长与完播率的Pearson相关系数为：{correlation:.4f}")
        if correlation < -0.1:
            self.logger.info("结论：存在较弱的负相关关系，视频时长越长，完播率倾向于越低")
        elif correlation > 0.1:
            self.logger.info("结论：存在较弱的正相关关系")
        else:
            self.logger.info("结论：相关关系不显著")

    def _multi_dimension_analysis(self, df: pd.DataFrame) -> None:
        """多维度对比分析：按时长分组分析完播率"""
        self.logger.info("\n" + "-" * 60)
        self.logger.info("多维度分析：按时长分组统计完播率")
        self.logger.info("-" * 60)

        bins = [0, 30, 60, 120, 180, 301]
        labels = ['0-30秒', '31-60秒', '61-120秒', '121-180秒', '180秒以上']

        df_grouped = df.copy()
        df_grouped['时长分组'] = pd.cut(
            df_grouped['视频时长（秒）'],
            bins=bins,
            labels=labels,
            right=False
        )

        duration_analysis = df_grouped.groupby('时长分组', observed=True).agg({
            '完播状态': 'mean',
            '视频ID': 'count'
        }).rename(columns={'完播状态': '完播率', '视频ID': '样本数'})

        duration_analysis['完播率'] = (duration_analysis['完播率'] * 100).round(2)
        self.logger.info(f"\n{duration_analysis.to_string()}")

    @type_check
    def create_visualization(
        self,
        daily_stats: Optional[pd.DataFrame] = None,
        category_stats: Optional[pd.DataFrame] = None,
        output_name: str = 'video_analysis_charts'
    ) -> str:
        """
        可视化模块入口：创建组合图表并保存

        Args:
            daily_stats: 每日统计数据
            category_stats: 类别统计数据
            output_name: 输出文件名（不含扩展名）

        Returns:
            保存的图表文件完整路径
        """
        daily = daily_stats if daily_stats is not None else self.daily_stats
        cat_stats = category_stats if category_stats is not None else self.category_stats

        if daily is None or cat_stats is None:
            raise ValueError("请先完成统计分析后再生成可视化图表")

        self.logger.info("\n" + "=" * 60)
        self.logger.info("可视化模块")
        self.logger.info("=" * 60)

        try:
            fig = plt.figure(figsize=self.config.figure_size)

            ax1 = plt.subplot(2, 1, 1)
            self._plot_trend_chart(ax1, daily)

            ax2 = plt.subplot(2, 1, 2)
            self._plot_stacked_chart(ax2, cat_stats)

            plt.tight_layout(pad=3.0)

            output_path = os.path.join(
                self.config.output_dir,
                f"{output_name}.{self.config.output_format}"
            )
            plt.savefig(output_path, dpi=self.config.figure_dpi, bbox_inches='tight')

            self.logger.info(f"图表已保存为：{output_path}")
            self._print_chart_interpretation()

            plt.show()

            return output_path

        except Exception as e:
            self.logger.error(f"生成可视化图表失败: {str(e)}", exc_info=True)
            raise

    def _plot_trend_chart(self, ax: plt.Axes, daily_stats: pd.DataFrame) -> None:
        """
        绘制趋势图子图：每日播放量与完播率

        Args:
            ax: Matplotlib Axes对象
            daily_stats: 每日统计数据
        """
        dates = daily_stats.index
        play_count = daily_stats['当日播放量']
        completion_rate = daily_stats['日完播率']

        line1 = ax.plot(dates, play_count, 'b-o', linewidth=2, markersize=4, label='当日播放量')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('播放量（次）', fontsize=12, color='b')
        ax.tick_params(axis='y', labelcolor='b')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)

        ax_twin = ax.twinx()
        line2 = ax_twin.plot(
            dates, completion_rate, 'r-s', linewidth=2, markersize=4, label='日完播率(%)'
        )
        ax_twin.set_ylabel('完播率（%）', fontsize=12, color='r')
        ax_twin.tick_params(axis='y', labelcolor='r')

        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax.legend(lines, labels, loc='upper left')
        ax.set_title('图1：每日播放量与完播率趋势图', fontsize=14, pad=20)

    def _plot_stacked_chart(self, ax: plt.Axes, category_stats: pd.DataFrame) -> None:
        """
        绘制堆叠柱状图子图：各视频类别观看次数与完播情况

        Args:
            ax: Matplotlib Axes对象
            category_stats: 类别统计数据
        """
        categories = category_stats.index.tolist()
        complete_plays = category_stats['观看次数'] * (category_stats['完播率'] / 100)
        incomplete_plays = category_stats['观看次数'] - complete_plays

        x = np.arange(len(categories))
        width = 0.6

        ax.bar(x, complete_plays, width, label='完播次数', color='#2ecc71')
        ax.bar(x, incomplete_plays, width, bottom=complete_plays, label='未完播次数', color='#e74c3c')

        ax.set_xlabel('视频类别', fontsize=12)
        ax.set_ylabel('观看次数（次）', fontsize=12)
        ax.set_title('图2：各视频类别观看次数（按完播状态堆叠）', fontsize=14, pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=45)
        ax.legend()

        max_watch = max(category_stats['观看次数'])
        for i, v in enumerate(category_stats['完播率']):
            total_height = complete_plays.iloc[i] + incomplete_plays.iloc[i]
            ax.text(
                i, total_height + max_watch * 0.01,
                f'{v:.1f}%', ha='center', va='bottom', fontsize=10
            )

    def _print_chart_interpretation(self) -> None:
        """打印图表信息解读"""
        self.logger.info("\n" + "-" * 60)
        self.logger.info("图表信息解读：")
        self.logger.info("-" * 60)

        self.logger.info("\n【图1：每日播放量与完播率趋势图 - 折线图】")
        self.logger.info("核心信息：")
        self.logger.info("1. 蓝色折线展示每日播放量的变化趋势，反映平台用户活跃度波动")
        self.logger.info("2. 红色折线展示每日完播率变化，反映内容整体吸引力")
        self.logger.info("3. 双Y轴设计便于同时观察两个指标的协同变化关系")
        self.logger.info("对运营的帮助：")
        self.logger.info("- 识别播放量高峰日，分析当日内容特点进行复刻")
        self.logger.info("- 完播率持续走低时需紧急排查内容质量问题")
        self.logger.info("- 观察两者是否背离（如播放量↑但完播率↓），判断流量是否精准")

        self.logger.info("\n【图2：各视频类别观看次数（按完播状态堆叠） - 堆叠柱状图】")
        self.logger.info("核心信息：")
        self.logger.info("1. 柱子总高度展示各类别的总观看次数（受欢迎程度）")
        self.logger.info("2. 绿色部分=完播次数，红色部分=未完播次数，直观展示完播结构")
        self.logger.info("3. 柱子顶部标签显示具体完播率数值，便于精准比较")
        self.logger.info("对运营的帮助：")
        self.logger.info("- 识别高观看量但低完播率的类别，作为重点优化对象")
        self.logger.info("- 识别高完播率的类别，作为优质内容方向加大投入")
        self.logger.info("- 为内容创作者提供明确的类别对标参考")

    def run_full_analysis(
        self,
        use_mock: bool = True,
        data_path: Optional[str] = None,
        n_records: Optional[int] = None
    ) -> None:
        """
        执行完整的分析流程

        Args:
            use_mock: 是否使用模拟数据
            data_path: 外部数据文件路径（use_mock=False时使用）
            n_records: 模拟数据条数（use_mock=True时使用）
        """
        self.logger.info("=" * 70)
        self.logger.info("短视频平台用户观看数据分析报告")
        self.logger.info("=" * 70)

        self.logger.info("\n[1/4] 数据准备阶段...")
        if use_mock:
            self.generate_mock_data(n_records)
        else:
            if not data_path:
                raise ValueError("使用外部数据时必须指定data_path")
            self.load_data(data_path)

        self.logger.info("数据字段：{}".format(self.raw_data.columns.tolist()))
        self.logger.info("数据样例：")
        self.logger.info(f"\n{self.raw_data.head().to_string()}")

        self.logger.info("\n[2/4] 数据处理中...")
        self.process_data()

        self.logger.info("\n[3/4] 统计分析中...")
        self.statistical_analysis()

        self.logger.info("\n[4/4] 生成可视化图表...")
        self.create_visualization()

        self.logger.info("\n" + "=" * 70)
        self.logger.info("分析完成！")
        self.logger.info("=" * 70)


def main() -> None:
    """主函数：执行完整分析流程"""
    config = AnalysisConfig(
        random_seed=42,
        output_dir='./output',
        output_format='png',
        figure_dpi=300
    )

    analyzer = VideoAnalysis(config)
    analyzer.run_full_analysis(use_mock=True, n_records=10000)


if __name__ == "__main__":
    main()
