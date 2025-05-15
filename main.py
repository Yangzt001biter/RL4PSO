import torch
import numpy as np
import matplotlib.pyplot as plt
import argparse
import os
from environment import PSO_Environment
from agent import DQNAgent, QNetwork, ReplayBuffer, Transition
from data_loader import load_data

def parse_args():
    """命令行参数解析"""
    # 获取当前脚本所在目录的绝对路径
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    parser = argparse.ArgumentParser(description='储能系统优化的DQN模型')
    parser.add_argument('--load_file', type=str, 
                       default=os.path.join(base_dir, 'data', 'load_data.csv'), 
                       help='负载数据文件路径')
    parser.add_argument('--solar_file', type=str, 
                       default=os.path.join(base_dir, 'data', 'solar_data.csv'), 
                       help='光伏发电数据文件路径')
    parser.add_argument('--price_file', type=str, 
                       default=os.path.join(base_dir, 'data', 'price_data.csv'), 
                       help='电价数据文件路径')
    parser.add_argument('--weather_file', type=str, 
                       default=os.path.join(base_dir, 'data', 'weather_data.csv'), 
                       help='气象数据文件路径')
    parser.add_argument('--episodes', type=int, default=300, help='训练回合数')
    parser.add_argument('--render', action='store_true', help='是否渲染训练过程')
    parser.add_argument('--save_model', action='store_true', help='是否保存训练模型')
    parser.add_argument('--model_path', type=str, 
                       default=os.path.join(base_dir, 'models', 'dqn_model.pth'), 
                       help='模型保存路径')
    parser.add_argument('--seed', type=int, default=42, help='随机种子')
    parser.add_argument('--adaptive_epsilon', action='store_true', default=True, 
                       help='是否使用自适应探索率衰减')
    parser.add_argument('--buffer_size', type=int, default=10000, 
                       help='经验回放缓冲区大小')
    return parser.parse_args()

def adaptive_epsilon_decay(agent, avg_rewards, i_episode, window=10):
    """自适应探索率衰减策略
    
    根据最近几个回合的平均奖励变化来调整探索率
    如果奖励稳定提升，则加速衰减探索率
    如果奖励下降或波动大，则减缓衰减速度
    """
    if i_episode < window + 1:
        # 前几个回合使用标准衰减
        return
    
    # 计算最近window个回合的平均奖励变化率
    recent_rewards = avg_rewards[-(window+1):]
    reward_change = (recent_rewards[-1] - recent_rewards[0]) / abs(recent_rewards[0] + 1e-6)
    
    # 根据奖励变化调整衰减率
    if reward_change > 0.05:  # 奖励明显提升
        agent.epsilon_decay *= 0.99  # 加快衰减
    elif reward_change < -0.02:  # 奖励下降
        agent.epsilon_decay *= 1.01  # 减缓衰减
    else:  # 奖励相对稳定
        agent.epsilon_decay *= 0.995  # 使用标准衰减

def train(env, agent, args):
    """训练DQN智能体"""
    rewards = []
    avg_rewards = []
    losses = []
    best_avg_reward = -float('inf')
    
    print(f"开始训练，共{args.episodes}个回合...")
    for i_episode in range(args.episodes):
        state = env.reset()
        episode_reward = 0
        episode_loss = 0
        steps = 0
        
        done = False
        while not done:
            action = agent.select_action(state)
            next_state, reward, done, _ = env.step(action)
            
            # 存储经验
            if done:
                agent.memory.push(state, action, None, reward, done)
            else:
                agent.memory.push(state, action, next_state, reward, done)
            
            state = next_state
            
            # 优化模型
            loss = agent.optimize_model()
            if loss is not None:
                episode_loss += loss
            
            # 更新目标网络
            if steps % agent.target_update == 0:
                agent.target_net.load_state_dict(agent.policy_net.state_dict())
            
            episode_reward += reward
            steps += 1
        
        # 更新探索率
        if args.adaptive_epsilon:
            rewards.append(episode_reward)
            if i_episode >= 99:
                avg_rewards.append(np.mean(rewards[-100:]))
            else:
                avg_rewards.append(np.mean(rewards))
                
            adaptive_epsilon_decay(agent, avg_rewards, i_episode)
        else:
            rewards.append(episode_reward)
            if i_episode >= 99:
                avg_rewards.append(np.mean(rewards[-100:]))
            else:
                avg_rewards.append(np.mean(rewards))
        
        if episode_loss > 0:
            losses.append(episode_loss / steps)
        
        # 打印训练进度
        if (i_episode + 1) % 10 == 0:
            current_epsilon = agent.get_current_epsilon()
            print(f"回合 {i_episode+1}/{args.episodes}, 平均奖励: {avg_rewards[-1]:.2f}, 探索率: {current_epsilon:.4f}")
        
        # 保存最佳模型
        if args.save_model and avg_rewards[-1] > best_avg_reward:
            best_avg_reward = avg_rewards[-1]
            if not os.path.exists(os.path.dirname(args.model_path)):
                os.makedirs(os.path.dirname(args.model_path))
            torch.save(agent.policy_net.state_dict(), args.model_path)
            print(f"最佳模型已保存，平均奖励: {best_avg_reward:.2f}")
    
    return rewards, avg_rewards, losses

def evaluate(env, agent, num_episodes=10):
    """评估训练好的DQN智能体"""
    print("\n开始评估...")
    rewards = []
    socs = []
    charges = []
    discharges = []
    
    for i in range(num_episodes):
        state = env.reset()
        episode_reward = 0
        episode_socs = []
        episode_charges = []
        episode_discharges = []
        
        done = False
        while not done:
            # 使用贪婪策略（不探索）
            action = agent.select_action(state, training=False)
            next_state, reward, done, _ = env.step(action)
            
            # 记录SOC和充放电功率
            episode_socs.append(state[2])  # SOC
            P_charge, P_discharge = env._action_to_charge_discharge(action)
            episode_charges.append(P_charge)
            episode_discharges.append(P_discharge)
            
            state = next_state
            episode_reward += reward
        
        rewards.append(episode_reward)
        socs.append(episode_socs)
        charges.append(episode_charges)
        discharges.append(episode_discharges)
        
        print(f"评估回合 {i+1}: 累计奖励 = {episode_reward:.2f}")
    
    avg_reward = np.mean(rewards)
    print(f"平均评估奖励: {avg_reward:.2f}")
    
    return {
        'rewards': rewards,
        'socs': socs,
        'charges': charges,
        'discharges': discharges
    }

def plot_training_results(rewards, avg_rewards, losses):
    """绘制训练过程的奖励和损失"""
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    plt.figure(figsize=(12, 10))
    
    # 绘制奖励
    plt.subplot(2, 1, 1)
    plt.plot(rewards, alpha=0.6, label='Episode Reward')
    plt.plot(avg_rewards, label='Average Reward')
    plt.xlabel('Episode')
    plt.ylabel('Reward')
    plt.title('Training Rewards')
    plt.legend()
    plt.grid(True)
    
    # 绘制损失
    plt.subplot(2, 1, 2)
    plt.plot(losses)
    plt.xlabel('Episode')
    plt.ylabel('Loss')
    plt.title('Training Loss')
    plt.grid(True)
    
    # 保存图表
    plt.tight_layout()
    if not os.path.exists('results'):
        os.makedirs('results')
    plt.savefig('results/training_results.png')
    plt.close()  # 关闭图表，避免显示

def plot_evaluation_results(eval_results, env):
    """绘制评估结果"""
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    # 选择第一个评估回合的数据
    socs = eval_results['socs'][0]
    charges = eval_results['charges'][0]
    discharges = eval_results['discharges'][0]
    
    # 获取时间轴
    time_steps = np.arange(len(socs))
    hours = (time_steps * env.time_step / 60) % 24
    
    plt.figure(figsize=(15, 12))
    
    # 绘制SOC曲线
    plt.subplot(3, 1, 1)
    plt.plot(hours, socs)
    plt.axhline(y=env.C_max, color='r', linestyle='--', label='Max Capacity')
    plt.axhline(y=env.C_min, color='r', linestyle='--', label='Min Capacity')
    plt.xlabel('Time (hours)')
    plt.ylabel('SOC (kWh)')
    plt.title('Battery SOC')
    plt.grid(True)
    plt.legend()
    
    # 绘制充电功率
    plt.subplot(3, 1, 2)
    plt.plot(hours, charges, 'g')
    plt.axhline(y=env.max_charge_power, color='r', linestyle='--', label='Max Power')
    plt.xlabel('Time (hours)')
    plt.ylabel('Charge Power (kW)')
    plt.title('Charging Power')
    plt.grid(True)
    plt.legend()
    
    # 绘制放电功率
    plt.subplot(3, 1, 3)
    plt.plot(hours, discharges, 'orange')
    plt.axhline(y=env.max_charge_power, color='r', linestyle='--', label='Max Power')
    plt.xlabel('Time (hours)')
    plt.ylabel('Discharge Power (kW)')
    plt.title('Discharging Power')
    plt.grid(True)
    plt.legend()
    
    # 保存图表
    plt.tight_layout()
    if not os.path.exists('results'):
        os.makedirs('results')
    plt.savefig('results/evaluation_results.png')
    plt.close()  # 关闭图表，避免显示

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    # 设置随机种子
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)
    
    # 获取基础目录
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 创建必要的目录
    for dir_path in ['models', 'results']:
        full_path = os.path.join(base_dir, dir_path)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
            print(f"创建目录: {full_path}")
    
    # 验证数据文件是否存在
    required_files = [args.load_file, args.solar_file, args.price_file, args.weather_file]
    missing_files = [f for f in required_files if not os.path.exists(f)]
    if missing_files:
        raise FileNotFoundError(f"缺少以下数据文件：\n" + "\n".join(missing_files) + 
                              "\n请确保所有数据文件都位于 data 目录下。")
    
    # 初始化环境
    print("初始化环境...")
    # 加载数据
    data = load_data(
        load_data_file=args.load_file,
        solar_data_file=args.solar_file,
        price_data_file=args.price_file,
        weather_data_file=args.weather_file
    )
    
    env = PSO_Environment(
        load_data=data['load_data'],
        solar_data=data['solar_data'],
        price_data=data['price_data'],
        weather_data=data['weather_data']
    )
    
    # 初始化智能体
    print("初始化DQN智能体...")
    agent = DQNAgent(
        state_dim=env.state_dim, 
        n_actions=env.n_actions,
        buffer_size=args.buffer_size
    )
    
    # 训练智能体
    rewards, avg_rewards, losses = train(env, agent, args)
    
    # 绘制训练结果
    plot_training_results(rewards, avg_rewards, losses)
    
    # 评估训练好的智能体
    if args.save_model:
        # 加载最佳模型
        agent.policy_net.load_state_dict(torch.load(args.model_path))
    
    eval_results = evaluate(env, agent)
    
    # 绘制评估结果
    plot_evaluation_results(eval_results, env)
    
    print("训练和评估完成！")

if __name__ == "__main__":
    main()