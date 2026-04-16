# 短视频平台用户观看数据分析系统

## 项目简介

本项目是一个短视频平台用户观看数据综合分析脚本，基于 Python + Pandas + Matplotlib 技术栈，提供数据处理、统计分析、可视化展示等完整功能。

## 重构亮点

### 1. 配置化管理
- 所有配置项抽离为独立配置类（`DataConfig`、`FilterConfig`、`VisualizationConfig`、`AnalysisConfig`）
- 支持通过字典动态创建配置对象
- 移除所有魔法数字，统一管理阈值、图表样式等参数

### 2. 面向对象封装
- 核心分析逻辑封装为 `VideoAnalysis` 主类
- 模块化设计：`DataGenerator`、`DataProcessor`、`StatisticalAnalyzer`、`Visualizer`
- 工具类封装：`DataValidator`（数据校验）、`PathHandler`（路径处理）

### 3. 性能优化
- 使用 `pivot_table` 替代部分 `groupby` 聚合，提升大数据量处理效率
- 优化 `df.copy()` 使用场景，减少不必要的数据拷贝
- 缓存计算结果，避免重复计算

### 4. 健壮性增强
- 完善的参数类型校验（使用 `typing` 模块）
- 全面的异常处理（文件读取失败、数据为空、聚合异常等）
- 日志记录替代 `print`（使用 `logging` 模块）
- 数据字段完整性与类型合法性校验

### 5. 可读性提升
- 完整的类型注解（参数/返回值）
- 规范的变量命名（符合 PEP8 规范）
- 详细的文档字符串（参数说明、返回值说明、异常说明）
- 清晰的代码结构与注释

### 6. 功能扩展
- 支持读取外部真实 CSV/Excel 数据
- 可视化模块支持自定义输出路径、图表格式
- 统计分析模块新增多维度对比（按时段/用户分层）

## 项目结构

```
video_analysis.py
├── 配置类
│   ├── DataConfig          # 数据相关配置
│   ├── FilterConfig        # 过滤相关配置
│   ├── VisualizationConfig # 可视化相关配置
│   └── AnalysisConfig      # 分析总配置
├── 工具类
│   ├── DataValidator       # 数据校验工具
│   └── PathHandler         # 路径处理工具
├── 核心模块
│   ├── DataGenerator       # 数据生成器
│   ├── DataProcessor       # 数据处理器
│   ├── StatisticalAnalyzer # 统计分析器
│   └── Visualizer          # 可视化器
└── 主类
    └── VideoAnalysis       # 视频数据分析主类
```

## 快速开始

### 基本使用

```python
from video_analysis import VideoAnalysis

# 使用默认配置
analyzer = VideoAnalysis()
results = analyzer.run_full_analysis(n_records=10000)
```

### 自定义配置

```python
from video_analysis import VideoAnalysis, AnalysisConfig, DataConfig, VisualizationConfig

# 自定义配置
config = AnalysisConfig(
    data=DataConfig(
        random_seed=42,
        n_records=50000,
        categories=['娱乐', '教育', '美食', '科技']
    ),
    visualization=VisualizationConfig(
        figure_size=(20, 14),
        dpi=150,
        output_format='pdf'
    )
)

analyzer = VideoAnalysis(config=config)
results = analyzer.run_full_analysis()
```

### 从字典创建配置

```python
config_dict = {
    'data': {
        'random_seed': 123,
        'n_records': 20000
    },
    'filter': {
        'min_watch_duration': 5,
        'completion_threshold': 0.85
    },
    'visualization': {
        'output_format': 'png',
        'dpi': 300
    }
}

analyzer = VideoAnalysis(config_dict=config_dict)
```

### 加载外部数据

```python
analyzer = VideoAnalysis()

# 从CSV文件加载
results = analyzer.run_full_analysis(file_path='data/watch_data.csv')

# 从Excel文件加载
results = analyzer.run_full_analysis(file_path='data/watch_data.xlsx')
```

### 分步执行分析

```python
analyzer = VideoAnalysis()

# 1. 加载数据
analyzer.load_data(n_records=10000)

# 2. 数据处理
filtered_data, category_stats = analyzer.process_data()

# 3. 统计分析
daily_stats, weekly_stats, correlation, conclusion = analyzer.analyze_data()

# 4. 可视化
chart_path = analyzer.visualize(output_path='output/charts.png', show=True)
```

### 多维度分析

```python
analyzer = VideoAnalysis()
analyzer.load_data(n_records=10000)
analyzer.process_data()

# 按时段分析
hourly_stats = analyzer.statistical_analyzer.analyze_by_time_period(
    analyzer.filtered_data, period='hour'
)

# 按用户分层分析
user_segment_stats = analyzer.statistical_analyzer.analyze_by_user_segment(
    analyzer.filtered_data, n_segments=3
)
```

## 配置项说明

### DataConfig（数据配置）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| random_seed | int | 42 | 随机种子 |
| n_records | int | 10000 | 默认数据条数 |
| user_id_range | Tuple[int, int] | (10001, 20000) | 用户ID范围 |
| video_id_range | Tuple[int, int] | (1001, 3000) | 视频ID范围 |
| categories | List[str] | ['娱乐', '教育', ...] | 视频类别列表 |
| date_range | Tuple[str, str] | ('2024-01-01', '2024-01-30') | 日期范围 |
| video_duration_range | Tuple[int, int] | (15, 300) | 视频时长范围（秒） |
| watch_duration_range | Tuple[int, int] | (0, 600) | 观看时长范围（秒） |

### FilterConfig（过滤配置）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| min_watch_duration | int | 3 | 最小有效观看时长（秒） |
| completion_threshold | float | 0.9 | 完播判定阈值 |

### VisualizationConfig（可视化配置）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| figure_size | Tuple[int, int] | (16, 12) | 图表尺寸 |
| dpi | int | 300 | 图表分辨率 |
| output_format | str | 'png' | 输出格式（png/pdf） |
| output_path | str | 'video_analysis_charts' | 默认输出路径 |
| font_family | List[str] | ['SimHei', ...] | 中文字体列表 |
| font_size | int | 10 | 字体大小 |

## 数据字段要求

外部数据文件需包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| 用户ID | int | 用户唯一标识 |
| 视频ID | int | 视频唯一标识 |
| 观看时长（秒） | int/float | 用户观看时长 |
| 完播状态 | bool | 是否完播 |
| 发布时间 | datetime/str | 视频发布时间 |
| 视频类别 | str | 视频分类 |
| 视频时长（秒） | int/float | 视频总时长 |

## 输出结果

`run_full_analysis()` 方法返回包含以下内容的字典：

```python
{
    'raw_data': pd.DataFrame,           # 原始数据
    'filtered_data': pd.DataFrame,      # 过滤后数据
    'category_stats': pd.DataFrame,     # 类别统计数据
    'daily_stats': pd.DataFrame,        # 每日统计数据
    'weekly_stats': pd.DataFrame,       # 周统计数据
    'correlation': float,               # 相关系数
    'correlation_conclusion': str,      # 相关性结论
    'chart_path': str                   # 图表保存路径
}
```

## 依赖环境

```
Python >= 3.8
pandas >= 1.3.0
numpy >= 1.20.0
matplotlib >= 3.4.0
openpyxl >= 3.0.0  # Excel文件读取支持
```

## 安装依赖

```bash
pip install pandas numpy matplotlib openpyxl
```

## 运行测试

```bash
python video_analysis.py
```

## 代码规范

本项目遵循 PEP8 编码规范，具有以下特点：

- 使用 dataclass 简化配置类定义
- 完整的类型注解
- 规范的文档字符串
- 清晰的模块划分

## 许可证

MIT License
