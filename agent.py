"""
智能体模块：定义DQN智能体和相关的神经网络、经验回放缓冲区
"""
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
import numpy as np
import random
from collections import namedtuple, deque
import math

# 定义经验回放中的转换
Transition = namedtuple('Transition', ('state', 'action', 'next_state', 'reward', 'done'))

class ReplayBuffer:
    """经验回放缓冲区"""
    def __init__(self, capacity):
        """
        初始化经验回放缓冲区
        
        参数:
            capacity (int): 缓冲区容量
        """
        self.memory = deque(maxlen=capacity)
    
    def push(self, state, action, next_state, reward, done):
        """存储转换"""
        self.memory.append(Transition(state, action, next_state, reward, done))
    
    def sample(self, batch_size):
        """随机采样一批转换"""
        return random.sample(self.memory, batch_size)
    
    def __len__(self):
        """返回缓冲区当前大小"""
        return len(self.memory)

class PrioritizedReplayBuffer:
    """优先级经验回放缓冲区"""
    def __init__(self, capacity, alpha=0.6, beta_start=0.4, beta_frames=100000):
        """
        初始化优先级经验回放缓冲区
        
        参数:
            capacity (int): 缓冲区容量
            alpha (float): 决定优先级影响程度的指数
            beta_start (float): 重要性采样的初始beta值
            beta_frames (int): beta从beta_start到1的过渡帧数
        """
        self.capacity = capacity
        self.memory = []
        self.position = 0
        self.priorities = np.zeros((capacity,), dtype=np.float32)
        self.alpha = alpha
        self.beta_start = beta_start
        self.beta_frames = beta_frames
        self.frame = 1
        self.epsilon = 1e-5  # 小值防止优先级为0
    
    def beta_by_frame(self, frame_idx):
        """随时间线性增加beta值"""
        return min(1.0, self.beta_start + frame_idx * (1.0 - self.beta_start) / self.beta_frames)
    
    def push(self, state, action, next_state, reward, done):
        """存储转换，并计算优先级"""
        max_prio = np.max(self.priorities) if self.memory else 1.0
        
        if len(self.memory) < self.capacity:
            self.memory.append(Transition(state, action, next_state, reward, done))
        else:
            self.memory[self.position] = Transition(state, action, next_state, reward, done)
        
        self.priorities[self.position] = max_prio
        self.position = (self.position + 1) % self.capacity
        self.frame += 1
    
    def sample(self, batch_size):
        """优先级采样一批转换"""
        if len(self.memory) == self.capacity:
            prios = self.priorities
        else:
            prios = self.priorities[:self.position]
        
        # 计算采样概率
        probs = prios ** self.alpha
        probs /= probs.sum()
        
        # 采样索引
        indices = np.random.choice(len(self.memory), batch_size, p=probs)
        samples = [self.memory[idx] for idx in indices]
        
        # 计算重要性权重
        beta = self.beta_by_frame(self.frame)
        weights = (len(self.memory) * probs[indices]) ** (-beta)
        weights /= weights.max()
        
        return samples, indices, weights
    
    def update_priorities(self, indices, priorities):
        """更新优先级"""
        for idx, priority in zip(indices, priorities):
            self.priorities[idx] = priority + self.epsilon
    
    def __len__(self):
        """返回缓冲区当前大小"""
        return len(self.memory)

class QNetwork(nn.Module):
    """DQN的Q网络"""
    def __init__(self, state_dim, n_actions, hidden_dim=128):
        """
        初始化Q网络
        
        参数:
            state_dim (int): 状态维度
            n_actions (int): 动作数量
            hidden_dim (int): 隐藏层维度
        """
        super(QNetwork, self).__init__()
        
        self.feature_layer = nn.Sequential(
            nn.Linear(state_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU()
        )
        
        self.advantage_stream = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, n_actions)
        )
        
        self.value_stream = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1)
        )
    
    def forward(self, x):
        """前向传播"""
        features = self.feature_layer(x)
        
        advantage = self.advantage_stream(features)
        value = self.value_stream(features)
        
        # 计算Q值：V(s) + A(s,a) - 平均A(s,a)
        q_values = value + advantage - advantage.mean(dim=1, keepdim=True)
        
        return q_values

class DQNAgent:
    """DQN智能体"""
    def __init__(self, state_dim, n_actions, use_prioritized_replay=True, use_double_dqn=True, 
                 lr=0.001, gamma=0.99, batch_size=64, buffer_size=10000, 
                 epsilon_start=1.0, epsilon_end=0.01, epsilon_decay=2000, 
                 target_update=10, hidden_dim=128, device="cpu"):
        """
        初始化DQN智能体
        
        参数:
            state_dim (int): 状态维度
            n_actions (int): 动作数量
            use_prioritized_replay (bool): 是否使用优先级经验回放
            use_double_dqn (bool): 是否使用Double DQN
            lr (float): 学习率
            gamma (float): 折扣因子
            batch_size (int): 批次大小
            buffer_size (int): 经验回放缓冲区大小
            epsilon_start (float): 初始探索率
            epsilon_end (float): 最终探索率
            epsilon_decay (int): 探索率衰减参数
            target_update (int): 目标网络更新频率
            hidden_dim (int): 隐藏层维度
            device (str): 运行设备
        """
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.use_prioritized_replay = use_prioritized_replay
        self.use_double_dqn = use_double_dqn
        self.gamma = gamma
        self.batch_size = batch_size
        self.epsilon_start = epsilon_start
        self.epsilon_end = epsilon_end
        self.epsilon_decay = epsilon_decay
        self.target_update = target_update
        self.hidden_dim = hidden_dim
        self.device = device
        
        # 创建Q网络和目标网络
        self.policy_net = QNetwork(state_dim, n_actions, hidden_dim).to(device)
        self.target_net = QNetwork(state_dim, n_actions, hidden_dim).to(device)
        self.target_net.load_state_dict(self.policy_net.state_dict())
        self.target_net.eval()  # 设置为评估模式
        
        # 定义优化器
        self.optimizer = optim.Adam(self.policy_net.parameters(), lr=lr)
        
        # 创建经验回放缓冲区
        if use_prioritized_replay:
            self.memory = PrioritizedReplayBuffer(buffer_size)
        else:
            self.memory = ReplayBuffer(buffer_size)
        
        self.steps_done = 0
        
        # 性能指标
        self.episode_rewards = []
        self.avg_rewards = []
        self.losses = []
        self.epsilons = []
    
    def select_action(self, state, training=True):
        """
        选择动作（训练时使用ε-贪婪策略）
        
        参数:
            state: 当前状态
            training (bool): 是否处于训练模式
            
        返回:
            int: 选择的动作
        """
        # 探索率自适应衰减
        epsilon = self.epsilon_end + (self.epsilon_start - self.epsilon_end) * \
                  math.exp(-1. * self.steps_done / self.epsilon_decay)
        self.steps_done += 1
        
        if training and random.random() < epsilon:
            # 随机探索
            return random.randrange(self.n_actions)
        else:
            # 根据策略选择最优动作
            with torch.no_grad():
                state = torch.FloatTensor(state).unsqueeze(0).to(self.device)
                q_values = self.policy_net(state)
                return q_values.max(1)[1].item()
    
    def get_current_epsilon(self):
        """获取当前的探索率"""
        return self.epsilon_end + (self.epsilon_start - self.epsilon_end) * \
               math.exp(-1. * self.steps_done / self.epsilon_decay)
    
    def optimize_model(self):
        """从经验回放中学习"""
        if len(self.memory) < self.batch_size:
            return None
        
        # 根据是否使用优先级经验回放选择不同的采样方法
        if self.use_prioritized_replay:
            transitions, indices, weights = self.memory.sample(self.batch_size)
            weights = torch.FloatTensor(weights).to(self.device)
        else:
            transitions = self.memory.sample(self.batch_size)
            weights = torch.ones(self.batch_size).to(self.device)
        
        # 将批次转换为单独的分组
        batch = Transition(*zip(*transitions))
        
        # 提取各组数据并预先转换为numpy数组
        non_final_mask = torch.tensor(tuple(map(lambda s: s is not None, batch.next_state)), 
                                     dtype=torch.bool).to(self.device)
        
        # 优化：先转换为numpy数组，再一次性转换为tensor
        non_final_next_states = np.array([s for s in batch.next_state if s is not None])
        non_final_next_states = torch.FloatTensor(non_final_next_states).to(self.device)
        
        state_batch = torch.FloatTensor(np.array(batch.state)).to(self.device)
        action_batch = torch.LongTensor(np.array(batch.action)).unsqueeze(1).to(self.device)
        reward_batch = torch.FloatTensor(np.array(batch.reward)).to(self.device)
        done_batch = torch.FloatTensor(np.array([float(d) for d in batch.done])).to(self.device)
        
        # 计算当前Q值
        state_action_values = self.policy_net(state_batch).gather(1, action_batch)
        
        # 计算下一状态的Q值
        next_state_values = torch.zeros(self.batch_size).to(self.device)
        
        if self.use_double_dqn:
            # Double DQN: 使用策略网络选择动作，使用目标网络估计Q值
            next_action_values = self.policy_net(non_final_next_states).max(1)[1].unsqueeze(1)
            next_state_values[non_final_mask] = self.target_net(non_final_next_states).gather(1, next_action_values).squeeze(1)
        else:
            # 普通DQN
            next_state_values[non_final_mask] = self.target_net(non_final_next_states).max(1)[0].detach()
        
        # 计算期望的Q值
        expected_state_action_values = (next_state_values * (1 - done_batch) * self.gamma) + reward_batch
        
        # 计算Huber损失
        loss = F.smooth_l1_loss(state_action_values, expected_state_action_values.unsqueeze(1))
        
        # 优化模型
        self.optimizer.zero_grad()
        loss.backward()
        
        # 梯度裁剪，防止梯度爆炸
        torch.nn.utils.clip_grad_norm_(self.policy_net.parameters(), 1)
        self.optimizer.step()
        
        return loss.item()
    
    def update_target_network(self):
        """更新目标网络"""
        self.target_net.load_state_dict(self.policy_net.state_dict())
    
    def save_model(self, path):
        """保存模型"""
        torch.save({
            'policy_net_state_dict': self.policy_net.state_dict(),
            'target_net_state_dict': self.target_net.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'steps_done': self.steps_done,
            'episode_rewards': self.episode_rewards,
            'avg_rewards': self.avg_rewards,
            'losses': self.losses,
            'epsilons': self.epsilons
        }, path)
        print(f"模型已保存到 {path}")
    
    def load_model(self, path):
        """加载模型"""
        checkpoint = torch.load(path)
        self.policy_net.load_state_dict(checkpoint['policy_net_state_dict'])
        self.target_net.load_state_dict(checkpoint['target_net_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.steps_done = checkpoint['steps_done']
        self.episode_rewards = checkpoint['episode_rewards']
        self.avg_rewards = checkpoint['avg_rewards']
        self.losses = checkpoint['losses']
        self.epsilons = checkpoint['epsilons']
        print(f"模型已从 {path} 加载")

class AdaptiveEpsilonScheduler:
    """自适应探索率调度器"""
    def __init__(self, start=1.0, end=0.01, decay=0.995, warmup=1000, adaptive_threshold=0.1):
        """
        初始化自适应探索率调度器
        
        参数:
            start (float): 起始探索率
            end (float): 最小探索率
            decay (float): 基础衰减率
            warmup (int): 预热步数
            adaptive_threshold (float): 自适应阈值
        """
        self.epsilon = start
        self.start = start
        self.end = end
        self.decay = decay
        self.warmup = warmup
        self.adaptive_threshold = adaptive_threshold
        self.steps = 0
        self.recent_rewards = deque(maxlen=100)
    
    def get_epsilon(self):
        """获取当前探索率"""
        return self.epsilon
    
    def update(self, reward=None, loss=None):
        """
        更新探索率
        
        参数:
            reward (float): 当前奖励
            loss (float): 当前损失
        """
        self.steps += 1
        
        # 记录奖励
        if reward is not None:
            self.recent_rewards.append(reward)
        
        # 预热阶段使用固定的探索率
        if self.steps < self.warmup:
            return self.epsilon
        
        # 基础衰减
        self.epsilon = max(self.end, self.epsilon * self.decay)
        
        # 自适应调整
        if len(self.recent_rewards) > 10:
            # 计算最近奖励的变化率
            avg_reward = sum(self.recent_rewards) / len(self.recent_rewards)
            if avg_reward < 0:  # 奖励较低，增加探索
                self.epsilon = min(self.start, self.epsilon * 1.05)
            elif avg_reward > self.adaptive_threshold:  # 奖励较高，减少探索
                self.epsilon = max(self.end, self.epsilon * 0.95)
        
        return self.epsilon