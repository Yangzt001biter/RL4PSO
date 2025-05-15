"""
训练模块：定义DQN训练过程和评估函数
"""
import numpy as np
import torch
import matplotlib.pyplot as plt
import time
import pandas as pd
from datetime import datetime
import os

def train(env, agent, num_episodes=200, max_steps_per_episode=None, eval_frequency=10, 
          eval_episodes=3, save_path=None, log_interval=10, adaptive_epsilon=False):
    """
    训练DQN智能体
    
    参数:
        env: 训练环境
        agent: DQN智能体
        num_episodes (int): 训练的总回合数
        max_steps_per_episode (int): 每个回合的最大步数，None表示不限制
        eval_frequency (int): 评估频率（每n个回合评估一次）
        eval_episodes (int): 每次评估的回合数
        save_path (str): 模型保存路径，None表示不保存
        log_interval (int): 日志打印间隔
        adaptive_epsilon (bool): 是否使用自适应探索率
        
    返回:
        dict: 包含训练过程中收集的指标
    """
    # 创建保存目录
    if save_path and not os.path.exists(save_path):
        os.makedirs(save_path)
    
    # 训练指标
    rewards = []
    avg_rewards = []
    losses = []
    epsilons = []
    eval_returns = []
    
    # 自适应探索率调度器
    if adaptive_epsilon:
        from agent import AdaptiveEpsilonScheduler
        scheduler = AdaptiveEpsilonScheduler()
    
    # 开始时间
    start_time = time.time()
    
    for i_episode in range(num_episodes):
        state = env.reset()
        episode_reward = 0
        episode_loss = 0
        steps = 0
        
        # 设置探索率
        if adaptive_epsilon:
            agent.epsilon = scheduler.get_epsilon()
        
        done = False
        while not done:
            # 检查是否达到步数上限
            if max_steps_per_episode and steps >= max_steps_per_episode:
                break
                
            # 选择动作
            action = agent.select_action(state)
            
            # 执行动作
            next_state, reward, done, info = env.step(action)
            
            # 存储转换
            if hasattr(agent.memory, 'push'):
                agent.memory.push(state, action, next_state if not done else None, reward, done)
            
            state = next_state
            
            # 优化模型
            loss = agent.optimize_model()
            if loss is not None:
                episode_loss += loss
            
            # 更新目标网络
            if steps % agent.target_update == 0:
                agent.update_target_network()
            
            episode_reward += reward
            steps += 1
        
        # 记录当前回合的指标
        rewards.append(episode_reward)
        if i_episode >= 99:
            avg_rewards.append(np.mean(rewards[-100:]))
        else:
            avg_rewards.append(np.mean(rewards[:i_episode+1]))
        
        if episode_loss > 0:
            losses.append(episode_loss / steps)
        else:
            losses.append(0)
        
        # 记录当前探索率
        if adaptive_epsilon:
            epsilons.append(scheduler.get_epsilon())
            # 更新探索率
            scheduler.update(reward=episode_reward)
        else:
            epsilons.append(agent.get_current_epsilon())
        
        # 定期评估
        if (i_episode + 1) % eval_frequency == 0:
            eval_reward = evaluate(env, agent, eval_episodes)
            eval_returns.append(eval_reward)
            
            # 保存模型
            if save_path:
                agent.save_model(f"{save_path}/dqn_episode_{i_episode+1}.pt")
        
        # 打印训练进度
        if (i_episode + 1) % log_interval == 0:
            avg_reward = avg_rewards[-1]
            epsilon = epsilons[-1]
            elapsed_time = time.time() - start_time
            print(f"Episode {i_episode+1}/{num_episodes} | " 
                  f"Avg Reward: {avg_reward:.2f} | "
                  f"Epsilon: {epsilon:.4f} | "
                  f"Steps: {steps} | "
                  f"Time: {elapsed_time:.2f}s")
            
            # 预计剩余时间
            avg_time_per_episode = elapsed_time / (i_episode + 1)
            remaining_episodes = num_episodes - (i_episode + 1)
            estimated_remaining_time = avg_time_per_episode * remaining_episodes
            print(f"Estimated time remaining: {estimated_remaining_time:.2f}s")
    
    # 训练结束时间
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Training completed in {total_time:.2f}s")
    
    # 返回训练指标
    return {
        'rewards': rewards,
        'avg_rewards': avg_rewards,
        'losses': losses,
        'epsilons': epsilons,
        'eval_returns': eval_returns,
        'total_time': total_time
    }

def evaluate(env, agent, num_episodes=5):
    """
    评估智能体性能
    
    参数:
        env: 评估环境
        agent: DQN智能体
        num_episodes (int): 评估的回合数
    
    返回:
        float: 平均回报
    """
    episode_returns = []
    
    for _ in range(num_episodes):
        state = env.reset()
        episode_reward = 0
        done = False
        
        while not done:
            # 选择动作（不使用探索）
            action = agent.select_action(state, training=False)
            
            # 执行动作
            next_state, reward, done, _ = env.step(action)
            
            state = next_state
            episode_reward += reward
        
        episode_returns.append(episode_reward)
    
    # 计算平均回报
    avg_return = np.mean(episode_returns)
    print(f"Evaluation over {num_episodes} episodes: {avg_return:.2f}")
    
    return avg_return

def plot_training_results(results, title="DQN Training Results", save_path=None):
    """
    绘制训练结果
    
    参数:
        results (dict): 训练指标
        title (str): 图表标题
        save_path (str): 保存路径，None表示不保存
    """
    # 创建图表
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(title)
    
    # 绘制奖励曲线
    axes[0, 0].plot(results['rewards'], 'b-', alpha=0.3)
    axes[0, 0].plot(results['avg_rewards'], 'r-')
    axes[0, 0].set_title('Rewards')
    axes[0, 0].set_xlabel('Episode')
    axes[0, 0].set_ylabel('Reward')
    axes[0, 0].legend(['Episode Reward', 'Average Reward'])
    axes[0, 0].grid(True)
    
    # 绘制损失曲线
    axes[0, 1].plot(results['losses'])
    axes[0, 1].set_title('Loss')
    axes[0, 1].set_xlabel('Episode')
    axes[0, 1].set_ylabel('Loss')
    axes[0, 1].grid(True)
    
    # 绘制探索率曲线
    axes[1, 0].plot(results['epsilons'])
    axes[1, 0].set_title('Exploration Rate (ε)')
    axes[1, 0].set_xlabel('Episode')
    axes[1, 0].set_ylabel('ε')
    axes[1, 0].grid(True)
    
    # 绘制评估回报曲线
    if 'eval_returns' in results and results['eval_returns']:
        eval_episodes = list(range(0, len(results['rewards']), len(results['rewards']) // len(results['eval_returns'])))
        if len(eval_episodes) > len(results['eval_returns']):
            eval_episodes = eval_episodes[:len(results['eval_returns'])]
        axes[1, 1].plot(eval_episodes, results['eval_returns'])
        axes[1, 1].set_title('Evaluation Returns')
        axes[1, 1].set_xlabel('Episode')
        axes[1, 1].set_ylabel('Average Return')
        axes[1, 1].grid(True)
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.9)
    
    # 保存图表
    if save_path:
        plt.savefig(save_path)
    
    plt.show()

def plot_battery_behavior(env, save_path=None):
    """
    绘制电池行为
    
    参数:
        env: 环境
        save_path (str): 保存路径，None表示不保存
    """
    # 获取充放电日志
    logs = env.get_charge_discharge_logs()
    
    # 创建图表
    fig, axes = plt.subplots(4, 1, figsize=(15, 15), sharex=True)
    
    # 绘制负载和光伏
    axes[0].plot(logs['time'], logs['load'], 'b-', label='Load')
    axes[0].plot(logs['time'], logs['solar'], 'y-', label='Solar')
    axes[0].set_title('Load and Solar Generation')
    axes[0].set_ylabel('Power (kW)')
    axes[0].legend()
    axes[0].grid(True)
    
    # 绘制电池充放电
    axes[1].plot(logs['time'], logs['charge'], 'g-', label='Charge')
    axes[1].plot(logs['time'], logs['discharge'], 'r-', label='Discharge')
    axes[1].set_title('Battery Charge/Discharge')
    axes[1].set_ylabel('Power (kW)')
    axes[1].legend()
    axes[1].grid(True)
    
    # 绘制电池SOC
    axes[2].plot(logs['time'], logs['soc'], 'k-')
    axes[2].set_title('Battery State of Charge')
    axes[2].set_ylabel('SOC (kWh)')
    axes[2].grid(True)
    
    # 绘制电网交互
    axes[3].plot(logs['time'], logs['grid'], 'b-')
    axes[3].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    axes[3].set_title('Grid Power Exchange')
    axes[3].set_xlabel('Time')
    axes[3].set_ylabel('Power (kW)')
    axes[3].grid(True)
    
    plt.tight_layout()
    
    # 保存图表
    if save_path:
        plt.savefig(save_path)
    
    plt.show()

def save_logs_to_excel(env, file_path):
    """
    将环境日志保存到Excel文件
    
    参数:
        env: 环境
        file_path (str): 保存路径
    """
    # 获取充放电日志
    logs = env.get_charge_discharge_logs()
    
    # 转换为pandas DataFrame
    logs_df = pd.DataFrame(logs)
    
    # 保存到Excel
    logs_df.to_excel(file_path, index=False)
    print(f"日志已保存到 {file_path}")