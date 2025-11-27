import numpy as np
import matplotlib.pyplot as plt

def simulate_martingale(starting_bankroll=20, base_bet=2, target=250, win_prob=18/37):
    """
    Simulate a single Martingale betting session.
    
    Args:
        starting_bankroll: Initial amount of money
        base_bet: Starting bet size
        target: Target bankroll to walk away
        win_prob: Probability of winning a single bet (default 0.5 for fair game)
    
    Returns:
        bankroll_history: List of bankroll values over time
        outcome: 'win' if reached target, 'bust' if went broke
    """
    bankroll = starting_bankroll
    current_bet = base_bet
    bankroll_history = [bankroll]
    
    while bankroll > 0 and bankroll < target:
        # Check if we can afford the current bet
        if current_bet > bankroll:
            current_bet = bankroll  # Bet everything we have left
        
        # Simulate the bet outcome
        if np.random.random() < win_prob:
            # Win
            bankroll += current_bet
            current_bet = base_bet  # Reset to base bet
        else:
            # Lose
            bankroll -= current_bet
            current_bet *= 2  # Double the bet
        
        bankroll_history.append(bankroll)
    
    outcome = 'win' if bankroll >= target else 'bust'
    return bankroll_history, outcome


def run_monte_carlo_simulation(n_trials=10000000, starting_bankroll=20, base_bet=2, target=250):
    """
    Run multiple Martingale betting simulations.
    
    Args:
        n_trials: Number of simulations to run
        starting_bankroll: Initial amount of money
        base_bet: Starting bet size
        target: Target bankroll to walk away
    
    Returns:
        all_histories: List of bankroll histories for each trial
        outcomes: Dictionary with counts of wins and busts
    """
    all_histories = []
    wins = 0
    busts = 0
    
    for i in range(n_trials):
        history, outcome = simulate_martingale(starting_bankroll, base_bet, target)
        all_histories.append(history)
        
        if outcome == 'win':
            wins += 1
        else:
            busts += 1
    
    outcomes = {'wins': wins, 'busts': busts}
    return all_histories, outcomes


def plot_monte_carlo_results(all_histories, outcomes, starting_bankroll=20, target=250):
    """
    Plot the Monte Carlo simulation results as a step graph.
    
    Args:
        all_histories: List of bankroll histories for each trial
        outcomes: Dictionary with counts of wins and busts
        starting_bankroll: Initial amount of money
        target: Target bankroll to walk away
    """
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    # Plot all trials
    for i, history in enumerate(all_histories):
        steps = range(len(history))
        final_value = history[-1]
        
        # Color based on outcome
        if final_value >= target:
            color = 'green'
        else:
            color = 'red'
        
        ax.step(steps, history, where='post', color=color, alpha=0.3, linewidth=0.8)
    
    # Add reference lines
    ax.axhline(y=starting_bankroll, color='blue', linestyle='--', 
                linewidth=2, label=f'Starting Bankroll: ${starting_bankroll}', alpha=0.7)
    ax.axhline(y=target, color='green', linestyle='--', 
                linewidth=2, label=f'Target: ${target}', alpha=0.7)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1)
    
    ax.set_xlabel('Number of Bets', fontsize=12)
    ax.set_ylabel('Bankroll ($)', fontsize=12)
    ax.set_title(f'Monte Carlo Simulation: Martingale Betting Strategy ({len(all_histories)} Trials)', 
                  fontsize=14, fontweight='bold')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(-10, target + 20)
    
    # Add text box with statistics
    total_trials = len(all_histories)
    win_rate = (outcomes['wins'] / total_trials) * 100
    bust_rate = (outcomes['busts'] / total_trials) * 100
    
    stats_text = f"Results:\n"
    stats_text += f"Wins: {outcomes['wins']} ({win_rate:.1f}%)\n"
    stats_text += f"Busts: {outcomes['busts']} ({bust_rate:.1f}%)\n"
    stats_text += f"\nStrategy:\n"
    stats_text += f"Base Bet: $2\n"
    stats_text += f"Win → Reset to base bet\n"
    stats_text += f"Loss → Double bet"
    
    ax.text(0.98, 0.5, stats_text, transform=ax.transAxes,
             fontsize=11, verticalalignment='center',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
             horizontalalignment='right')
    
    plt.tight_layout()
    plt.savefig('gambling_simulation_results.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"\n{'='*60}")
    print(f"MONTE CARLO SIMULATION RESULTS")
    print(f"{'='*60}")
    print(f"Total Trials: {total_trials}")
    print(f"Starting Bankroll: ${starting_bankroll}")
    print(f"Base Bet: $2")
    print(f"Target: ${target}")
    print(f"\nOutcomes:")
    print(f"  Reached Target (${target}): {outcomes['wins']} ({win_rate:.2f}%)")
    print(f"  Went Broke: {outcomes['busts']} ({bust_rate:.2f}%)")
    print(f"{'='*60}")
    
    # Calculate some additional statistics
    final_outcomes = [history[-1] for history in all_histories]
    avg_bets_to_end = np.mean([len(history) - 1 for history in all_histories])
    max_bets = max([len(history) - 1 for history in all_histories])
    min_bets = min([len(history) - 1 for history in all_histories])
    
    print(f"\nAdditional Statistics:")
    print(f"  Average bets until end: {avg_bets_to_end:.1f}")
    print(f"  Maximum bets in a trial: {max_bets}")
    print(f"  Minimum bets in a trial: {min_bets}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Parameters
    N_TRIALS = 1000000
    STARTING_BANKROLL = 20
    BASE_BET = 2
    TARGET = 100
    
    print(f"Starting Monte Carlo Simulation...")
    print(f"Simulating {N_TRIALS} trials of Martingale betting strategy...")
    
    # Run simulation
    all_histories, outcomes = run_monte_carlo_simulation(
        n_trials=N_TRIALS,
        starting_bankroll=STARTING_BANKROLL,
        base_bet=BASE_BET,
        target=TARGET
    )
    
    # Print results
    total_trials = N_TRIALS
    win_rate = (outcomes['wins'] / total_trials) * 100
    bust_rate = (outcomes['busts'] / total_trials) * 100
    
    print(f"\n{'='*60}")
    print(f"MONTE CARLO SIMULATION RESULTS")
    print(f"{'='*60}")
    print(f"Total Trials: {total_trials:,}")
    print(f"Starting Bankroll: ${STARTING_BANKROLL}")
    print(f"Base Bet: ${BASE_BET}")
    print(f"Target: ${TARGET}")
    print(f"\nOutcomes:")
    print(f"  Reached Target (${TARGET}): {outcomes['wins']:,} ({win_rate:.4f}%)")
    print(f"  Went Broke: {outcomes['busts']:,} ({bust_rate:.4f}%)")
    print(f"{'='*60}\n")

