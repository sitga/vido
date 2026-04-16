#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
短视频平台用户观看数据综合分析脚本
技术栈：Python + Pandas + Matplotlib
功能：数据处理、统计分析、可视化展示
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
import warnings
warnings.filterwarnings('ignore')

# 设置中文显示
rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
rcParams['axes.unicode_minus'] = False
rcParams['font.size'] = 10

def generate_mock_data(n_records=10000):
    """
    生成模拟的短视频观看数据
    :param n_records: 数据条数
    :return: DataFrame
    """
    np.random.seed(42)  # 设置随机种子保证可复现
    
    # 用户ID和视频ID
    user_ids = np.random.randint(10001, 20000, size=n_records)
    video_ids = np.random.randint(1001, 3000, size=n_records)
    
    # 视频类别
    categories = ['娱乐', '教育', '美食', '旅游', '游戏', '科技', '生活', '美妆']
    video_categories = np.random.choice(categories, size=n_records)
    
    # 发布时间（近30天）
    publish_dates = pd.date_range(start='2024-01-01', end='2024-01-30', periods=n_records)
    publish_dates = np.random.choice(publish_dates, size=n_records)
    
    # 观看时长（秒，0-600秒）
    watch_durations = np.concatenate([
        np.random.randint(0, 3, size=int(n_records * 0.15)),  # 无效观看（<3秒）
        np.random.randint(3, 60, size=int(n_records * 0.35)),  # 短观看
        np.random.randint(60, 300, size=int(n_records * 0.35)),  # 中等观看
        np.random.randint(300, 601, size=int(n_records * 0.15))  # 长观看
    ])
    np.random.shuffle(watch_durations)
    
    # 视频时长（假设每个视频固定时长，15-300秒）
    video_durations_dict = {vid: np.random.randint(15, 301) for vid in np.unique(video_ids)}
    video_durations = np.array([video_durations_dict[vid] for vid in video_ids])
    
    # 完播状态：观看时长 >= 视频时长 * 0.9 视为完播
    completion_status = watch_durations >= (video_durations * 0.9)
    
    # 构建DataFrame
    df = pd.DataFrame({
        '用户ID': user_ids,
        '视频ID': video_ids,
        '观看时长（秒）': watch_durations,
        '完播状态': completion_status,
        '发布时间': publish_dates,
        '视频类别': video_categories,
        '视频时长（秒）': video_durations
    })
    
    return df

def data_processing(df):
    """
    数据处理模块：过滤无效观看，按类别计算完播率
    :param df: 原始数据
    :return: 过滤后的数据、按类别聚合的结果
    """
    print("=" * 60)
    print("数据处理模块")
    print("=" * 60)
    
    # 1. 过滤无效观看（观看时长 < 3秒）
    initial_count = len(df)
    df_filtered = df[df['观看时长（秒）'] >= 3].copy()
    filtered_count = len(df_filtered)
    
    print(f"原始数据条数：{initial_count}")
    print(f"过滤无效观看（<3秒）后数据条数：{filtered_count}")
    print(f"过滤比例：{(1 - filtered_count/initial_count)*100:.2f}%")
    
    # 2. 按视频类别聚合计算平均完播率
    category_stats = df_filtered.groupby('视频类别').agg({
        '完播状态': ['count', 'mean'],  # count:观看次数, mean:完播率
        '观看时长（秒）': 'mean',
        '视频时长（秒）': 'mean'
    }).round(4)
    
    # 重命名列
    category_stats.columns = ['观看次数', '完播率', '平均观看时长（秒）', '平均视频时长（秒）']
    category_stats['完播率'] = category_stats['完播率'] * 100  # 转为百分比
    category_stats = category_stats.sort_values('完播率', ascending=False)
    
    print("\n按视频类别统计（过滤无效观看后）：")
    print(category_stats.round(2))
    
    return df_filtered, category_stats

def statistical_analysis(df_filtered):
    """
    统计分析模块：每日播放量、周环比、相关性分析
    :param df_filtered: 过滤后的数据
    :return: 每日统计数据、周环比数据
    """
    print("\n" + "=" * 60)
    print("统计分析模块")
    print("=" * 60)
    
    # 提取日期（去掉时间部分）
    df_filtered['观看日期'] = df_filtered['发布时间'].dt.date
    
    # 1. 计算每日新增播放量
    daily_play = df_filtered.groupby('观看日期').agg({
        '视频ID': 'count',  # 播放次数
        '完播状态': 'mean'  # 日完播率
    }).rename(columns={'视频ID': '当日播放量', '完播状态': '日完播率'})
    
    daily_play['日完播率'] = (daily_play['日完播率'] * 100).round(2)
    
    print("\n每日播放量统计（前10天）：")
    print(daily_play.head(10))
    
    # 2. 计算周环比变化
    # 先按周分组（假设周一为一周开始）
    df_filtered['年份'] = df_filtered['发布时间'].dt.year
    df_filtered['周数'] = df_filtered['发布时间'].dt.isocalendar().week
    
    weekly_stats = df_filtered.groupby(['年份', '周数']).agg({
        '视频ID': 'count',
        '完播状态': 'mean'
    }).rename(columns={'视频ID': '周播放量', '完播状态': '周完播率'})
    
    weekly_stats['周完播率'] = (weekly_stats['周完播率'] * 100).round(2)
    
    # 计算周环比（(本周-上周)/上周 * 100%）
    weekly_stats['播放量周环比(%)'] = weekly_stats['周播放量'].pct_change() * 100
    weekly_stats['完播率周环比(%)'] = weekly_stats['周完播率'].pct_change() * 100
    
    print("\n周统计及周环比：")
    print(weekly_stats.round(2))
    
    # 3. 完播率与视频时长的相关性分析思路说明
    print("\n" + "-" * 60)
    print("完播率与视频时长的相关性分析思路：")
    print("-" * 60)
    print("""
    分析思路：
    1. 数据分组：将视频按时长分为不同区间（如15-30秒、31-60秒、61-120秒等）
    2. 统计指标：计算每个时长区间的平均完播率
    3. 相关性计算：计算视频时长与完播率的Pearson相关系数
    4. 可视化：绘制散点图展示视频时长与完播率的关系
    5. 控制变量：分析不同类别下的时长-完播率关系，排除类别影响
    
    预期发现：
    - 可能存在"黄金时长"区间（如30-60秒）完播率最高
    - 过短（<15秒）或过长（>180秒）的视频完播率可能较低
    - 不同类别的视频可能有不同的最优时长范围
    """)
    
    # 实际计算相关性
    corr_data = df_filtered.groupby('视频ID').agg({
        '视频时长（秒）': 'first',
        '完播状态': 'mean'
    })
    correlation = corr_data['视频时长（秒）'].corr(corr_data['完播状态'])
    
    print(f"实际计算：视频时长与完播率的Pearson相关系数为：{correlation:.4f}")
    if correlation < -0.1:
        print("结论：存在较弱的负相关关系，视频时长越长，完播率倾向于越低")
    elif correlation > 0.1:
        print("结论：存在较弱的正相关关系")
    else:
        print("结论：相关关系不显著")
    
    return daily_play, weekly_stats

def create_visualization(daily_play, category_stats):
    """
    可视化模块：折线图+堆叠柱状图组合展示
    :param daily_play: 每日播放数据
    :param category_stats: 类别统计数据
    """
    print("\n" + "=" * 60)
    print("可视化模块")
    print("=" * 60)
    
    # 创建画布和子图布局
    fig = plt.figure(figsize=(16, 12))
    
    # 子图1：折线图 - 每日播放量与完播率趋势
    ax1 = plt.subplot(2, 1, 1)
    
    # 准备数据
    dates = daily_play.index
    play_count = daily_play['当日播放量']
    completion_rate = daily_play['日完播率']
    
    # 绘制播放量折线
    line1 = ax1.plot(dates, play_count, 'b-o', linewidth=2, markersize=4, label='当日播放量')
    ax1.set_xlabel('日期', fontsize=12)
    ax1.set_ylabel('播放量（次）', fontsize=12, color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # 创建双Y轴展示完播率
    ax1_twin = ax1.twinx()
    line2 = ax1_twin.plot(dates, completion_rate, 'r-s', linewidth=2, markersize=4, label='日完播率(%)')
    ax1_twin.set_ylabel('完播率（%）', fontsize=12, color='r')
    ax1_twin.tick_params(axis='y', labelcolor='r')
    
    # 合并图例
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    ax1.set_title('图1：每日播放量与完播率趋势图', fontsize=14, pad=20)
    
    # 子图2：堆叠柱状图 - 各视频类别观看次数与完播情况
    ax2 = plt.subplot(2, 1, 2)
    
    # 准备堆叠数据
    categories = category_stats.index.tolist()
    # 估算：完播次数 = 观看次数 * 完播率
    complete_plays = category_stats['观看次数'] * (category_stats['完播率'] / 100)
    incomplete_plays = category_stats['观看次数'] - complete_plays
    
    x = np.arange(len(categories))
    width = 0.6
    
    # 绘制堆叠柱状图
    rects1 = ax2.bar(x, complete_plays, width, label='完播次数', color='#2ecc71')
    rects2 = ax2.bar(x, incomplete_plays, width, bottom=complete_plays, label='未完播次数', color='#e74c3c')
    
    ax2.set_xlabel('视频类别', fontsize=12)
    ax2.set_ylabel('观看次数（次）', fontsize=12)
    ax2.set_title('图2：各视频类别观看次数（按完播状态堆叠）', fontsize=14, pad=20)
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories, rotation=45)
    ax2.legend()
    
    # 在柱状图上添加完播率标签
    for i, v in enumerate(category_stats['完播率']):
        total_height = complete_plays.iloc[i] + incomplete_plays.iloc[i]
        ax2.text(i, total_height + max(category_stats['观看次数'])*0.01,
                 f'{v:.1f}%', ha='center', va='bottom', fontsize=10)
    
    # 调整布局
    plt.tight_layout(pad=3.0)
    
    # 保存图片
    plt.savefig('video_analysis_charts.png', dpi=300, bbox_inches='tight')
    print("图表已保存为：video_analysis_charts.png")
    
    # 显示图表说明
    print("\n" + "-" * 60)
    print("图表信息解读：")
    print("-" * 60)
    
    print("\n【图1：每日播放量与完播率趋势图 - 折线图】")
    print("核心信息：")
    print("1. 蓝色折线展示每日播放量的变化趋势，反映平台用户活跃度波动")
    print("2. 红色折线展示每日完播率变化，反映内容整体吸引力")
    print("3. 双Y轴设计便于同时观察两个指标的协同变化关系")
    print("对运营的帮助：")
    print("- 识别播放量高峰日，分析当日内容特点进行复刻")
    print("- 完播率持续走低时需紧急排查内容质量问题")
    print("- 观察两者是否背离（如播放量↑但完播率↓），判断流量是否精准")
    
    print("\n【图2：各视频类别观看次数（按完播状态堆叠） - 堆叠柱状图】")
    print("核心信息：")
    print("1. 柱子总高度展示各类别的总观看次数（受欢迎程度）")
    print("2. 绿色部分=完播次数，红色部分=未完播次数，直观展示完播结构")
    print("3. 柱子顶部标签显示具体完播率数值，便于精准比较")
    print("对运营的帮助：")
    print("- 识别高观看量但低完播率的类别，作为重点优化对象")
    print("- 识别高完播率的类别，作为优质内容方向加大投入")
    print("- 为内容创作者提供明确的类别对标参考")
    
    plt.show()

def main():
    """主函数：执行完整分析流程"""
    print("=" * 70)
    print("短视频平台用户观看数据分析报告")
    print("=" * 70)
    
    # 1. 生成模拟数据
    print("\n[1/4] 生成模拟数据...")
    df = generate_mock_data(10000)
    print("数据字段：", df.columns.tolist())
    print("数据样例：")
    print(df.head())
    
    # 2. 数据处理
    print("\n[2/4] 数据处理中...")
    df_filtered, category_stats = data_processing(df)
    
    # 3. 统计分析
    print("\n[3/4] 统计分析中...")
    daily_play, weekly_stats = statistical_analysis(df_filtered)
    
    # 4. 可视化
    print("\n[4/4] 生成可视化图表...")
    create_visualization(daily_play, category_stats)
    
    print("\n" + "=" * 70)
    print("分析完成！")
    print("=" * 70)

if __name__ == "__main__":
    main()
