#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具类模块
提供数据校验、路径处理、日志配置等通用功能
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable
from functools import wraps
import pandas as pd
import numpy as np

from config import config, LoggingConfig


def setup_logger(name: str = "video_analysis", log_config: Optional[LoggingConfig] = None) -> logging.Logger:
    """
    配置并返回日志记录器
    
    Args:
        name: 日志记录器名称
        log_config: 日志配置对象，默认使用全局配置
        
    Returns:
        配置好的日志记录器
    """
    if log_config is None:
        log_config = config.logging
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_config.level.upper()))
    
    # 清除已有处理器
    logger.handlers.clear()
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_config.level.upper()))
    console_formatter = logging.Formatter(log_config.format, datefmt=log_config.date_format)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器（可选）
    if log_config.log_to_file and log_config.log_file_path:
        from logging.handlers import RotatingFileHandler
        
        log_path = Path(log_config.log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=log_config.max_bytes,
            backupCount=log_config.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, log_config.level.upper()))
        file_formatter = logging.Formatter(log_config.format, datefmt=log_config.date_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


# 全局日志记录器
logger = setup_logger()


def validate_dataframe(df: pd.DataFrame, required_columns: List[str], 
                       column_types: Optional[Dict[str, type]] = None) -> None:
    """
    校验DataFrame的字段完整性和数据类型
    
    Args:
        df: 待校验的DataFrame
        required_columns: 必需的列名列表
        column_types: 列名到期望类型的映射字典
        
    Raises:
        ValueError: 数据校验失败时抛出
        TypeError: 数据类型不匹配时抛出
    """
    if df is None or df.empty:
        raise ValueError("输入数据为空或None")
    
    # 检查必需列
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"缺少必需列: {missing_columns}")
    
    # 检查数据类型
    if column_types:
        for col, expected_type in column_types.items():
            if col in df.columns:
                actual_type = df[col].dtype
                # 处理数值类型的兼容性
                if expected_type in (int, float, np.number):
                    if not np.issubdtype(actual_type, np.number):
                        raise TypeError(f"列 '{col}' 期望数值类型，实际为 {actual_type}")
                elif expected_type == bool:
                    if not (df[col].dtype == bool or df[col].dtype == np.bool_):
                        raise TypeError(f"列 '{col}' 期望布尔类型，实际为 {actual_type}")
                elif expected_type == str:
                    if not (df[col].dtype == object or pd.api.types.is_string_dtype(df[col])):
                        raise TypeError(f"列 '{col}' 期望字符串类型，实际为 {actual_type}")


def ensure_output_dir(output_path: Union[str, Path]) -> Path:
    """
    确保输出目录存在，如不存在则创建
    
    Args:
        output_path: 输出文件路径
        
    Returns:
        输出目录的Path对象
        
    Raises:
        OSError: 创建目录失败时抛出
    """
    output_dir = Path(output_path).parent
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    except OSError as e:
        raise OSError(f"无法创建输出目录 {output_dir}: {e}")


def safe_save_file(df: pd.DataFrame, output_path: Union[str, Path], 
                   file_format: Optional[str] = None, **kwargs) -> Path:
    """
    安全地保存DataFrame到文件
    
    Args:
        df: 要保存的DataFrame
        output_path: 输出文件路径
        file_format: 文件格式，自动从后缀推断
        **kwargs: 传递给pandas保存函数的额外参数
        
    Returns:
        保存后的文件路径
        
    Raises:
        ValueError: 不支持的文件格式
        OSError: 保存失败时抛出
    """
    output_path = Path(output_path)
    ensure_output_dir(output_path)
    
    # 自动推断格式
    if file_format is None:
        file_format = output_path.suffix.lower().lstrip('.')
    
    try:
        if file_format in ['csv']:
            df.to_csv(output_path, index=False, encoding=config.file.default_encoding, **kwargs)
        elif file_format in ['xlsx', 'xls']:
            df.to_excel(output_path, index=False, **kwargs)
        elif file_format in ['json']:
            df.to_json(output_path, orient='records', force_ascii=False, **kwargs)
        elif file_format in ['parquet']:
            df.to_parquet(output_path, **kwargs)
        else:
            raise ValueError(f"不支持的文件格式: {file_format}")
        
        logger.info(f"文件已成功保存: {output_path}")
        return output_path
        
    except Exception as e:
        raise OSError(f"保存文件失败 {output_path}: {e}")


def handle_exceptions(default_return: Any = None, 
                     raise_on_error: bool = False,
                     log_level: str = 'error') -> Callable:
    """
    异常处理装饰器工厂
    
    Args:
        default_return: 发生异常时的默认返回值
        raise_on_error: 是否在异常时重新抛出
        log_level: 日志级别 ('debug', 'info', 'warning', 'error')
        
    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_func = getattr(logger, log_level.lower(), logger.error)
                log_func(f"函数 {func.__name__} 执行失败: {e}")
                
                if raise_on_error:
                    raise
                return default_return
        return wrapper
    return decorator


def memoize(func: Callable) -> Callable:
    """
    简单的缓存装饰器，缓存函数结果
    
    Args:
        func: 要缓存的函数
        
    Returns:
        带缓存的函数
    """
    cache = {}
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 创建可哈希的缓存键
        key = str(args) + str(sorted(kwargs.items()))
        
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    
    wrapper.cache = cache
    wrapper.clear_cache = lambda: cache.clear()
    
    return wrapper


def format_number(value: Union[int, float], decimal_places: int = 2, 
                  suffix: str = '') -> str:
    """
    格式化数字显示
    
    Args:
        value: 要格式化的数值
        decimal_places: 小数位数
        suffix: 后缀（如 '%'）
        
    Returns:
        格式化后的字符串
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 'N/A'
    
    format_str = f"{{:,.{decimal_places}f}}{suffix}"
    return format_str.format(value)


def calculate_percent_change(current: float, previous: float) -> Optional[float]:
    """
    计算百分比变化
    
    Args:
        current: 当前值
        previous: 上一期值
        
    Returns:
        百分比变化值，如果previous为0则返回None
    """
    if previous == 0:
        return None
    return ((current - previous) / previous) * 100


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """
    将列表分块
    
    Args:
        lst: 原始列表
        chunk_size: 每块大小
        
    Returns:
        分块后的列表
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def get_project_root() -> Path:
    """
    获取项目根目录
    
    Returns:
        项目根目录的Path对象
    """
    return Path(__file__).parent.absolute()


def validate_file_path(file_path: Union[str, Path], 
                       must_exist: bool = False,
                       allowed_extensions: Optional[List[str]] = None) -> Path:
    """
    验证文件路径
    
    Args:
        file_path: 文件路径
        must_exist: 是否要求文件必须存在
        allowed_extensions: 允许的文件扩展名列表
        
    Returns:
        验证后的Path对象
        
    Raises:
        FileNotFoundError: 文件不存在且must_exist为True时
        ValueError: 文件扩展名不在允许列表中时
    """
    path = Path(file_path)
    
    if must_exist and not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    
    if allowed_extensions:
        ext = path.suffix.lower().lstrip('.')
        if ext not in allowed_extensions:
            raise ValueError(f"不支持的文件格式 '{ext}'，允许格式: {allowed_extensions}")
    
    return path
