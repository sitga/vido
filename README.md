# 短视频平台用户观看数据分析系统

## 项目概述

本项目是一个重构后的短视频平台用户观看数据分析系统，采用模块化、面向对象的设计，提供数据加载、处理、统计分析和可视化展示的全流程功能。

## 重构特性

### 1. 配置化管理
- 所有配置项集中管理在 `config.py` 中
- 使用 `@dataclass` 定义配置类，支持类型提示
- 包含数据配置、可视化配置、分析配置、日志配置、文件配置等

### 2. 模块化设计
```
video_analysis.py    # 主分析类，整合全流程
config.py            # 配置管理
utils.py             # 工具函数（日志、校验、缓存等）
data_loader.py       # 数据加载（模拟数据、CSV、Excel）
data_processor.py    # 数据处理（过滤、聚合、特征工程）
analyzer.py          # 统计分析（趋势、相关性、用户分层）
visualizer.py        # 可视化（多种图表类型、多格式输出）
```

### 3. 性能优化
- 使用 `pd.pivot_table` 替代部分 `groupby` 操作
- 优化数据拷贝，使用视图而非复制
- 视频时长缓存机制，避免重复计算
- 函数结果缓存装饰器 `@memoize`

### 4. 健壮性增强
- 完整的异常处理机制
- 参数类型校验使用 `typing` 模块
- 数据完整性验证
- 文件操作安全处理

### 5. 代码可读性
- 完整的类型注解（参数/返回值）
- 符合 PEP8 规范的命名
- 详细的文档字符串
- 移除魔法数字，使用配置常量

### 6. 日志系统
- 使用标准 `logging` 模块替代 `print`
- 支持文件日志和控制台日志
- 可配置的日志级别

### 7. 多数据源支持
- 模拟数据生成
- CSV 文件读取
- Excel 文件读取
- 自动列名映射（中英文）

### 8. 灵活可视化
- 支持 PNG、PDF、SVG、JPG 等多种输出格式
- 可配置的图表样式
- 拆分超大函数为独立子函数

## 安装依赖

```bash
pip install pandas numpy matplotlib openpyxl
```

## 快速开始

### 基本使用

```python
from video_analysis import VideoAnalysisSystem

# 创建分析系统
system = VideoAnalysisSystem()

# 执行完整分析流程
report = system.run_full_pipeline(
    data_type='mock',      # 使用模拟数据
    n_records=10000,       # 生成10000条记录
    output_dir='./output'  # 输出目录
)

# 打印报告
system.print_report(report)
```

### 使用外部数据

```python
from video_analysis import VideoAnalysisSystem

system = VideoAnalysisSystem()

# 从CSV加载
report = system.run_full_pipeline(
    source='data.csv',
    data_type='csv',
    output_dir='./output'
)

# 从Excel加载
report = system.run_full_pipeline(
    source='data.xlsx',
    data_type='excel',
    output_dir='./output'
)
```

### 分步执行

```python
from video_analysis import VideoAnalysisSystem

system = VideoAnalysisSystem()

# 1. 加载数据
system.load_data(data_type='mock', n_records=10000)

# 2. 处理数据
system.process_data()

# 3. 执行分析
analysis_results = system.analyze()

# 4. 生成可视化
system.visualize(output_path='./output/charts.png')

# 5. 生成完整报告
report = system.generate_report(output_dir='./output')
```

## 命令行使用

```bash
# 使用模拟数据（默认）
python video_analysis.py

# 指定记录数
python video_analysis.py --n-records 5000

# 使用CSV文件
python video_analysis.py --data-type csv --source data.csv

# 使用Excel文件
python video_analysis.py --data-type excel --source data.xlsx

# 指定输出目录
python video_analysis.py --output-dir ./my_output

# 显示图表
python video_analysis.py --show-chart

# 设置日志级别
python video_analysis.py --log-level DEBUG
```

## 配置说明

### 修改配置

```python
from config import config

# 修改数据配置
config.data.random_seed = 123
config.data.default_record_count = 5000

# 修改可视化配置
config.visualization.dpi = 150
config.visualization.figure_size = (12, 8)

# 修改分析配置
config.analysis.decimal_places = 3
```

### 配置项说明

#### DataConfig（数据配置）
- `random_seed`: 随机种子
- `default_record_count`: 默认记录数
- `video_categories`: 视频类别列表
- `min_watch_duration`: 最小有效观看时长
- `completion_threshold`: 完播判定阈值

#### VisualizationConfig（可视化配置）
- `figure_size`: 图表尺寸
- `dpi`: 输出DPI
- `supported_formats`: 支持的输出格式
- `colors`: 颜色配置
- `font_family`: 字体配置

#### AnalysisConfig（分析配置）
- `correlation_threshold_weak`: 弱相关阈值
- `correlation_threshold_strong`: 强相关阈值
- `duration_bins`: 视频时长分组区间
- `decimal_places`: 小数位数

## 模块详细说明

### data_loader.py

数据加载模块，支持多种数据源。

```python
from data_loader import DataLoader

loader = DataLoader()

# 生成模拟数据
df = loader.generate_mock_data(n_records=10000)

# 从CSV加载
df = loader.load_from_csv('data.csv')

# 从Excel加载
df = loader.load_from_excel('data.xlsx')

# 验证数据
loader.validate_data(df)
```

### data_processor.py

数据处理模块，提供清洗、过滤、聚合功能。

```python
from data_processor import DataProcessor

processor = DataProcessor()

# 过滤无效观看
filtered_df = processor.filter_invalid_watch(df)

# 计算类别统计
category_stats = processor.calculate_category_stats(df)

# 添加时间特征
df_with_time = processor.add_time_features(df)

# 执行完整处理流程
processed_df, stats = processor.process_pipeline(df)
```

### analyzer.py

统计分析模块，提供多维度分析功能。

```python
from analyzer import VideoAnalyzer

analyzer = VideoAnalyzer()

# 趋势分析
trend_result = analyzer.analyze_trends(df)

# 相关性分析
correlation = analyzer.analyze_correlation(df)

# 用户分层分析
segments = analyzer.analyze_user_segments(df, segment_by='activity')

# 多维度对比
results = analyzer.compare_dimensions(df, ['video_category', 'duration_segment'])
```

### visualizer.py

可视化模块，提供多种图表类型。

```python
from visualizer import Visualizer

viz = Visualizer()

# 绘制趋势图
viz.plot_trend_chart(daily_stats)

# 绘制堆叠柱状图
viz.plot_stacked_bar_chart(category_stats)

# 绘制类别对比图
viz.plot_completion_by_category(category_stats)

# 创建综合仪表盘
fig = viz.create_dashboard(daily_stats, category_stats, weekly_stats)

# 保存图表
viz.save_chart('output.png')
```

## 数据结构

### 输入数据格式

CSV/Excel 文件应包含以下列：

| 列名（中文） | 列名（英文） | 类型 | 说明 |
|------------|------------|------|------|
| 用户ID | user_id | int | 用户唯一标识 |
| 视频ID | video_id | int | 视频唯一标识 |
| 观看时长（秒） | watch_duration | int | 观看时长（秒） |
| 视频时长（秒） | video_duration | int | 视频总时长（秒） |
| 视频类别 | video_category | str | 视频分类 |
| 发布时间 | publish_time | datetime | 视频发布时间 |
| 完播状态 | completion_status | bool | 是否完播 |

### 输出结果

分析完成后会生成：
- `analysis_dashboard.png`: 综合分析仪表盘
- `category_comparison.png`: 类别对比图

## 项目结构

```
.
├── video_analysis.py    # 主程序
├── config.py            # 配置模块
├── utils.py             # 工具模块
├── data_loader.py       # 数据加载模块
├── data_processor.py    # 数据处理模块
├── analyzer.py          # 统计分析模块
├── visualizer.py        # 可视化模块
├── README.md            # 项目说明
└── output/              # 输出目录（自动生成）
    ├── analysis_dashboard.png
    └── category_comparison.png
```

## 开发规范

### 代码风格
- 遵循 PEP8 规范
- 使用类型注解
- 编写详细的文档字符串
- 使用有意义的变量名

### 命名规范
- 类名：PascalCase（如 `VideoAnalyzer`）
- 函数/变量：snake_case（如 `calculate_stats`）
- 常量：UPPER_SNAKE_CASE（如 `MAX_RECORDS`）
- 私有成员：前导下划线（如 `_internal_var`）

## 许可证

MIT License
