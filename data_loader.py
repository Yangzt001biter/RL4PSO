"""
数据加载模块：用于从Excel文件加载各种模拟数据
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def load_excel_data(file_path, sheet_name=0, header=0, columns=None):
    """
    从Excel或CSV文件加载数据
    
    参数:
        file_path (str): 数据文件路径（支持.xlsx和.csv）
        sheet_name: 工作表名称或索引，仅用于Excel文件
        header (int): 指定表头行的位置，None表示没有表头
        columns (list): 指定要读取的列名列表，None表示读取所有列
    
    返回:
        numpy.ndarray: 加载的数据数组
    """
    try:
        # 根据文件扩展名选择加载方法
        if file_path.endswith('.csv'):
            if columns:
                data = pd.read_csv(file_path, header=header, usecols=columns)
            else:
                data = pd.read_csv(file_path, header=header)
        else:
            if columns:
                data = pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=columns, engine='openpyxl')
            else:
                data = pd.read_excel(file_path, sheet_name=sheet_name, header=header, engine='openpyxl')
            
        # 检查数据是否为空
        if data.empty:
            raise ValueError(f"从 {file_path} 加载的数据为空")
        
        # 如果有多列，只保留数值列（排除时间戳列）
        if data.shape[1] > 1:
            # 假设第一列是时间戳，直接使用第二列的数值
            try:
                numeric_data = pd.DataFrame(data.iloc[:, 1])
            except:
                raise ValueError(f"文件 {file_path} 中没有找到可用的数值数据")
        else:
            # 如果只有一列，尝试直接转换为数值
            try:
                numeric_data = pd.DataFrame(pd.to_numeric(data.iloc[:, 0]))
            except:
                raise ValueError(f"文件 {file_path} 中的数据无法转换为数值")
            
        # 返回数据
        return numeric_data.values.flatten()
        
    except Exception as e:
        print(f"加载数据文件 {file_path} 时出错: {str(e)}")
        raise

def load_time_series_data(file_path, date_column=None, value_column=None, start_date=None, end_date=None, sheet_name=0):
    """
    加载时间序列数据，并可以根据日期范围筛选
    
    参数:
        file_path (str): Excel文件路径
        date_column (str): 日期列名
        value_column (str): 值列名
        start_date (str): 开始日期，格式为 'YYYY-MM-DD'
        end_date (str): 结束日期，格式为 'YYYY-MM-DD'
        sheet_name: 工作表名称或索引
    
    返回:
        pandas.DataFrame: 包含日期和对应值的数据框
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        
        # 检查是否需要筛选特定列
        if date_column and value_column:
            df = df[[date_column, value_column]]
            
            # 确保日期列是datetime类型
            df[date_column] = pd.to_datetime(df[date_column])
            
            # 按日期范围筛选
            if start_date:
                start_date = pd.to_datetime(start_date)
                df = df[df[date_column] >= start_date]
            if end_date:
                end_date = pd.to_datetime(end_date)
                df = df[df[date_column] <= end_date]
            
            # 按日期排序
            df = df.sort_values(by=date_column)
            
        return df
    except Exception as e:
        print(f"加载时间序列数据时出错: {str(e)}")
        raise

def load_data(load_data_file, solar_data_file, price_data_file, weather_data_file, 
              load_config=None, solar_config=None, price_config=None, weather_config=None):
    """
    加载所有数据文件并返回一个字典
    
    参数:
        *_data_file (str): 各数据文件路径
        *_config (dict): 各数据文件的加载配置，包含sheet_name、header、columns等参数
    
    返回:
        dict: 包含各类数据的字典
    """
    # 默认配置 - 设置header=0表示第一行是标题
    default_config = {'sheet_name': 0, 'header': 0, 'columns': None}
    
    # 使用提供的配置或默认配置
    load_config = load_config or default_config
    solar_config = solar_config or default_config
    price_config = price_config or default_config
    weather_config = weather_config or default_config
    
    # 加载各类数据
    load_data = load_excel_data(load_data_file, **load_config)
    solar_data = load_excel_data(solar_data_file, **solar_config)
    price_data = load_excel_data(price_data_file, **price_config)
    weather_data = load_excel_data(weather_data_file, **weather_config)
    
    # 确保所有数据长度一致
    min_length = min(len(load_data), len(solar_data), len(price_data), len(weather_data))
    
    if min_length == 0:
        raise ValueError("至少有一个数据集为空")
    
    # 截断数据以确保所有数组长度一致
    load_data = load_data[:min_length]
    solar_data = solar_data[:min_length]
    price_data = price_data[:min_length]
    weather_data = weather_data[:min_length]
    
    print(f"数据加载完成。每个数据集包含 {min_length} 个数据点。")
    
    return {
        'load_data': load_data,
        'solar_data': solar_data,
        'price_data': price_data,
        'weather_data': weather_data
    }

def generate_synthetic_data(n_days=30, time_step_minutes=5, output_file=None):
    """
    生成合成数据用于测试，并可选择导出到Excel文件
    
    参数:
        n_days (int): 天数
        time_step_minutes (int): 时间步长(分钟)
        output_file (str): 输出文件路径，如不提供则不保存

    返回:
        dict: 包含生成的数据的字典
    """
    # 计算总时间步数
    steps_per_day = 24 * 60 // time_step_minutes
    total_steps = n_days * steps_per_day
    
    # 创建时间序列
    start_date = datetime(2023, 1, 1)
    time_steps = [start_date + timedelta(minutes=i*time_step_minutes) for i in range(total_steps)]
    
    # 生成负载数据 (典型的日负载曲线 + 随机噪声)
    load_base = np.zeros(total_steps)
    for i in range(total_steps):
        hour = time_steps[i].hour + time_steps[i].minute / 60.0
        # 模拟日负载曲线
        daily_pattern = 3.0 + 5.0 * np.sin((hour - 6) * np.pi / 12) if 6 <= hour <= 22 else 3.0
        # 添加周末效应
        weekend_factor = 0.8 if time_steps[i].weekday() >= 5 else 1.0
        # 添加随机波动
        noise = np.random.normal(0, 0.5)
        load_base[i] = daily_pattern * weekend_factor + noise
    
    # 生成光伏发电数据 (基于日照时间 + 随机云层影响)
    solar_base = np.zeros(total_steps)
    for i in range(total_steps):
        hour = time_steps[i].hour + time_steps[i].minute / 60.0
        # 模拟日照时间
        if 6 <= hour <= 18:
            # 光伏输出最大值发生在正午
            solar_output = 8.0 * np.sin((hour - 6) * np.pi / 12)
            # 添加季节效应
            month = time_steps[i].month
            seasonal_factor = 1.0 + 0.3 * np.sin((month - 1) * np.pi / 6)  # 夏季增加，冬季减少
            # 添加随机云层影响
            cloud_effect = max(0, np.random.normal(0.9, 0.2))
            solar_base[i] = solar_output * seasonal_factor * cloud_effect
        else:
            solar_base[i] = 0.0  # 夜间无光伏输出
    
    # 生成电价数据 (分时电价 + 季节/周末变化)
    price_base = np.zeros(total_steps)
    for i in range(total_steps):
        hour = time_steps[i].hour
        # 基本分时电价
        if 6 <= hour < 8 or 18 <= hour < 22:  # 早晚高峰
            base_price = 1.2
        elif 8 <= hour < 18:  # 日间平段
            base_price = 0.8
        else:  # 夜间低谷
            base_price = 0.5
        
        # 周末价格降低
        weekend_discount = 0.9 if time_steps[i].weekday() >= 5 else 1.0
        
        # 季节因素 (夏冬高峰)
        month = time_steps[i].month
        seasonal_factor = 1.0
        if month in [6, 7, 8]:  # 夏季
            seasonal_factor = 1.2
        elif month in [12, 1, 2]:  # 冬季
            seasonal_factor = 1.1
        
        # 添加小幅随机波动
        noise = np.random.normal(0, 0.05)
        
        price_base[i] = base_price * weekend_discount * seasonal_factor + noise
    
    # 生成气象数据 (温度，单位：摄氏度)
    weather_base = np.zeros(total_steps)
    for i in range(total_steps):
        hour = time_steps[i].hour + time_steps[i].minute / 60.0
        day = i // steps_per_day
        
        # 日内温度变化 (早晨低，下午高)
        daily_pattern = 20.0 + 5.0 * np.sin((hour - 6) * np.pi / 12)
        
        # 添加天气系统影响 (每3-5天变化一次)
        weather_system = 3.0 * np.sin(day * np.pi / 4.0)
        
        # 添加随机波动
        noise = np.random.normal(0, 1.0)
        
        weather_base[i] = daily_pattern + weather_system + noise
    
    data = {
        'time': time_steps,
        'load_data': load_base,
        'solar_data': solar_base,
        'price_data': price_base,
        'weather_data': weather_base
    }
    
    # 保存到Excel文件
    if output_file:
        df = pd.DataFrame({
            'Time': time_steps,
            'Load (kW)': load_base,
            'Solar (kW)': solar_base,
            'Price ($/kWh)': price_base,
            'Temperature (C)': weather_base
        })
        df.to_excel(output_file, index=False)
        print(f"合成数据已保存到 {output_file}")
    
    return data

if __name__ == "__main__":
    # 示例：生成测试数据
    data = generate_synthetic_data(n_days=7, output_file="synthetic_data.xlsx")
    print("生成的数据示例:")
    for i in range(5):  # 显示前5个数据点
        print(f"时间: {data['time'][i]}, 负载: {data['load_data'][i]:.2f}kW, "
              f"光伏: {data['solar_data'][i]:.2f}kW, 电价: {data['price_data'][i]:.2f}$/kWh, "
              f"温度: {data['weather_data'][i]:.2f}°C")