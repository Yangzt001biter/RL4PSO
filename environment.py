"""
环境模块：定义储能优化的模拟环境
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

class PSO_Environment:
    """储能优化环境"""
    def __init__(self, load_data, solar_data, price_data, weather_data, 
                 time_step=5, max_capacity=60.0, min_capacity=6.6, 
                 efficiency=0.9, max_charge_power=10.0, initial_soc=30.0,
                 use_history=True, history_window=12):
        """
        初始化储能优化环境
        
        参数:
            load_data (numpy.ndarray): 负载数据数组
            solar_data (numpy.ndarray): 光伏发电数据数组
            price_data (numpy.ndarray): 电价数据数组
            weather_data (numpy.ndarray): 气象数据数组
            time_step (int): 时间步长(分钟)
            max_capacity (float): 储能最大容量(kWh)
            min_capacity (float): 储能最小容量(kWh)
            efficiency (float): 充放电效率
            max_charge_power (float): 最大充放电功率(kW)
            initial_soc (float): 初始SOC(kWh)
            use_history (bool): 是否在状态中包含历史数据
            history_window (int): 历史数据窗口大小(时间步数)
        """
        # 环境参数 - 确保所有数值都是标量
        self.C_max = float(max_capacity)  # 储能最大容量 (kWh)
        self.C_min = float(min_capacity)  # 储能最小容量 (kWh)
        self.eff = float(efficiency)      # 充放电效率
        self.max_charge_power = float(max_charge_power)  # 最大充电功率 (kW)
        
        # 状态相关参数
        self.initial_soc = float(initial_soc)  # 初始SOC (kWh)
        self.soc = self.initial_soc
        self.time_step = int(time_step)      # 时间步长 (分钟)
        self.time_step_hours = float(self.time_step) / 60.0  # 时间步长 (小时)
        
        # 加载外部数据 - 确保是numpy数组
        self.load_data = np.asarray(load_data, dtype=np.float32)
        self.solar_data = np.asarray(solar_data, dtype=np.float32)
        self.price_data = np.asarray(price_data, dtype=np.float32)
        self.weather_data = np.asarray(weather_data, dtype=np.float32)
        
        # 初始化状态
        self.t = 0  # 当前时间步
        self.total_steps = len(self.load_data)  # 数据长度作为总时间步数
        
        # 历史数据设置
        self.use_history = bool(use_history)
        self.history_window = int(history_window)
        self.load_history = np.zeros(history_window, dtype=np.float32)
        self.solar_history = np.zeros(history_window, dtype=np.float32)
        self.price_history = np.zeros(history_window, dtype=np.float32)
        self.weather_history = np.zeros(history_window, dtype=np.float32)
        
        # 定义动作空间
        self.n_actions = 21  # 将充放电功率离散化为21个动作
        
        # 定义状态空间维度
        if self.use_history:
            # [负载, 光伏, SOC, 电价, 气象, 时间(小时), 负载历史, 光伏历史, 电价历史, 气象历史]
            self.state_dim = 5 + 1 + 4 * history_window
        else:
            # [负载, 光伏, SOC, 电价, 气象, 时间(小时)]
            self.state_dim = 6
        
        # 创建时间序列（假设每个时间步对应5分钟，从0点开始）
        self.start_datetime = datetime(2023, 1, 1, 0, 0, 0)
        self.time_series = [self.start_datetime + timedelta(minutes=i*self.time_step) for i in range(self.total_steps)]
        
        # 记录性能指标
        self.total_grid_cost = 0.0
        self.total_battery_cycles = 0.0
        self.total_self_consumption = 0.0
        self.energy_not_served = 0.0  # 负载未被满足的能量
        self.charge_discharge_logs = []  # 记录充放电行为

    def reset(self):
        """重置环境状态"""
        self.t = 0
        self.soc = self.initial_soc
        
        # 重置历史数据
        self.load_history = np.zeros(self.history_window)
        self.solar_history = np.zeros(self.history_window)
        self.price_history = np.zeros(self.history_window)
        self.weather_history = np.zeros(self.history_window)
        
        # 重置性能指标
        self.total_grid_cost = 0.0
        self.total_battery_cycles = 0.0
        self.total_self_consumption = 0.0
        self.energy_not_served = 0.0
        self.charge_discharge_logs = []
        
        return self._get_state()

    def _to_scalar(self, value):
        """将任意数值类型转换为Python标量"""
        if hasattr(value, 'item'):  # PyTorch tensor或numpy array
            return value.item()
        elif hasattr(value, 'numpy'):  # PyTorch tensor
            return value.numpy().item()
        elif hasattr(value, '__array__'):  # numpy array
            return value.__array__().item()
        return float(value)  # 其他数值类型

    def _get_state(self):
        """获取当前环境状态"""
        # 基本状态 - 确保获取标量值
        L_t = self._to_scalar(self.load_data[self.t])
        P_solar_t = self._to_scalar(self.solar_data[self.t])
        SOC_t = self._to_scalar(self.soc)
        P_price_t = self._to_scalar(self.price_data[self.t])
        Weather_t = self._to_scalar(self.weather_data[self.t])
        
        # 获取当前时间(小时)，用于学习时间相关模式
        current_hour = float(self.time_series[self.t].hour + self.time_series[self.t].minute / 60.0)
        
        if self.use_history:
            # 确保所有历史数据都是一维数组
            load_history = np.asarray(self.load_history, dtype=np.float32)
            solar_history = np.asarray(self.solar_history, dtype=np.float32)
            price_history = np.asarray(self.price_history, dtype=np.float32)
            weather_history = np.asarray(self.weather_history, dtype=np.float32)
            
            # 创建包含历史数据的状态
            state = np.concatenate([
                np.array([L_t, P_solar_t, SOC_t, P_price_t, Weather_t, current_hour], dtype=np.float32),
                load_history,
                solar_history,
                price_history,
                weather_history
            ])
        else:
            # 不使用历史数据的状态
            state = np.array([L_t, P_solar_t, SOC_t, P_price_t, Weather_t, current_hour], dtype=np.float32)
        
        return state

    def _update_history(self):
        """更新历史数据"""
        if self.use_history:
            # 移动历史数据窗口
            self.load_history = np.roll(self.load_history, -1)
            self.load_history[-1] = self.load_data[self.t]
            
            self.solar_history = np.roll(self.solar_history, -1)
            self.solar_history[-1] = self.solar_data[self.t]
            
            self.price_history = np.roll(self.price_history, -1)
            self.price_history[-1] = self.price_data[self.t]
            
            self.weather_history = np.roll(self.weather_history, -1)
            self.weather_history[-1] = self.weather_data[self.t]

    def _action_to_charge_discharge(self, action):
        """将离散动作转换为充放电功率"""
        # 确保 action 是标量
        action = self._to_scalar(action)
        max_charge_power = self._to_scalar(self.max_charge_power)
        
        if action <= 10:  # 放电 (0-10kW)
            P_discharge = action * (max_charge_power / 10)
            P_charge = 0.0
        elif action == 11:  # 不充不放
            P_discharge = 0.0
            P_charge = 0.0
        else:  # 充电 (0-10kW)
            P_discharge = 0.0
            P_charge = (action - 11) * (max_charge_power / 10)
        
        # 检查SOC约束，确保不会超出容量限制
        soc = self._to_scalar(self.soc)
        c_min = self._to_scalar(self.C_min)
        c_max = self._to_scalar(self.C_max)
        eff = self._to_scalar(self.eff)
        time_step_hours = self._to_scalar(self.time_step_hours)
        
        max_discharge = min(
            self._to_scalar(P_discharge),
            self._to_scalar((soc - c_min) / time_step_hours * eff)
        )
        max_charge = min(
            self._to_scalar(P_charge),
            self._to_scalar((c_max - soc) / time_step_hours / eff)
        )
        
        # 应用约束
        P_discharge = self._to_scalar(max(0, max_discharge))
        P_charge = self._to_scalar(max(0, max_charge))
        
        return P_charge, P_discharge

    def step(self, action):
        """执行一个环境步骤
        
        参数:
            action (int): 动作索引 (0-20)
            
        返回:
            tuple: (next_state, reward, done, info)
        """
        # 获取当前状态的值
        L_t = self._to_scalar(self.load_data[self.t])
        P_solar_t = self._to_scalar(self.solar_data[self.t])
        P_price_t = self._to_scalar(self.price_data[self.t])
        
        # 将动作转换为充放电功率
        P_charge, P_discharge = self._action_to_charge_discharge(action)
        
        # 计算电网交换功率
        P_grid = L_t - P_solar_t + P_charge - P_discharge
        
        # 更新SOC
        delta_soc = (P_charge * self.eff - P_discharge / self.eff) * self.time_step_hours
        self.soc = min(self.C_max, max(self.C_min, self.soc + delta_soc))
        
        # 计算成本和奖励
        grid_cost = P_grid * P_price_t * self.time_step_hours if P_grid > 0 else 0
        self.total_grid_cost += grid_cost
        
        # 计算电池循环次数增量
        cycle_increment = abs(delta_soc) / (2 * (self.C_max - self.C_min))
        self.total_battery_cycles += cycle_increment
        
        # 计算自发自用量
        self_consumption = min(P_solar_t, L_t + P_charge)
        self.total_self_consumption += self_consumption * self.time_step_hours
        
        # 计算未满足负载
        if P_grid < 0:
            self.energy_not_served += abs(P_grid) * self.time_step_hours
        
        # 记录充放电行为
        self.charge_discharge_logs.append({
            'time': self.time_series[self.t],
            'load': L_t,
            'solar': P_solar_t,
            'charge': P_charge,
            'discharge': P_discharge,
            'grid': P_grid,
            'soc': self.soc,
            'price': P_price_t
        })
        
        # 计算奖励
        reward = -(grid_cost + cycle_increment * 0.1)  # 考虑电网成本和电池损耗
        
        # 更新历史数据
        self._update_history()
        
        # 更新时间步
        self.t += 1
        
        # 检查是否结束
        done = self.t >= self.total_steps - 1
        
        # 如果还没结束，获取下一个状态
        if not done:
            next_state = self._get_state()
        else:
            next_state = None
        
        # 返回结果
        info = {
            'grid_cost': grid_cost,
            'cycle_increment': cycle_increment,
            'self_consumption': self_consumption * self.time_step_hours
        }
        
        return next_state, reward, done, info

    def get_performance_metrics(self):
        """获取当前性能指标"""
        return {
            'total_grid_cost': self.total_grid_cost,
            'total_battery_cycles': self.total_battery_cycles,
            'total_self_consumption': self.total_self_consumption,
            'energy_not_served': self.energy_not_served
        }
        
    def get_charge_discharge_logs(self):
        """获取充放电日志"""
        return pd.DataFrame(self.charge_discharge_logs)