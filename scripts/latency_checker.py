import statistics
import os
import matplotlib.pyplot as plt

def read_latency_data(file_path):
    """Read and parse latency data from a file."""
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' was not found.")
        return None
    
    data = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                clean_line = line.strip()
                if not clean_line:
                    continue
                try:
                    val = float(clean_line)
                    data.append(val)
                except ValueError:
                    continue
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    
    if not data:
        print(f"No valid data found in {file_path}")
        return None
    
    return data

def calculate_stats(data):
    """Calculate statistics for latency data (in seconds)."""
    ms_data = [x for x in data]
    
    stats = {
        'ms_data': ms_data,
        'count': len(data),
        'mean': statistics.mean(ms_data),
        'median': statistics.median(ms_data),
        'min': min(ms_data),
        'max': max(ms_data),
        'stdev': statistics.stdev(ms_data) if len(data) > 1 else 0.0
    }
    return stats

def print_stats(file_path, stats):
    """Print statistics for a single file."""
    print("-" * 40)
    print(f"File:   {file_path}")
    print(f"Count:  {stats['count']} pings")
    print("-" * 40)
    print(f"Mean:   {stats['mean']:.4f} ms")
    print(f"Median: {stats['median']:.4f} ms  <-- MOST IMPORTANT (Typical latency)")
    print(f"Min:    {stats['min']:.4f} ms    <-- BEST CASE (Physical limit)")
    print(f"Max:    {stats['max']:.4f} ms")
    print(f"Jitter: {stats['stdev']:.4f} ms    <-- Lower is better (Stability)")
    print("-" * 40)

def analyze_multiple_files(file_paths):
    """Analyze latency for multiple files and plot histograms with x-axis limited to 10 ms."""
    all_stats = {}
    
    # Read and calculate stats for all files
    for file_path in file_paths:
        data = read_latency_data(file_path)
        if data is not None:
            stats = calculate_stats(data)
            all_stats[file_path] = stats
    
    if not all_stats:
        print("No valid data found in any file.")
        return
    
    # Create subplots for histograms
    num_files = len(all_stats)
    fig, axes = plt.subplots(num_files, 1, figsize=(12, 5 * num_files))
    
    # Handle case of single file (axes won't be an array)
    if num_files == 1:
        axes = [axes]
    
    colors = ['skyblue', 'lightcoral', 'lightgreen', 'lightyellow', 'plum']
    
    # Plot histogram for each file
    for idx, (file_path, stats) in enumerate(all_stats.items()):
        ax = axes[idx]
        # Limit the x-axis to 10 ms for the histogram
        ax.hist(stats['ms_data'], bins=500, color=colors[idx % len(colors)], edgecolor='black', alpha=0.7, range=(0, 10))
        ax.set_title(f'Latency Histogram - {os.path.basename(file_path)}', fontsize=12, fontweight='bold')
        ax.set_xlabel('Latency (ms)')
        ax.set_ylabel('Frequency')
        ax.grid(axis='y', alpha=0.75)
        ax.set_xlim([0, 10])
        
        # Add statistics text box
        stats_text = f"Median: {stats['median']:.2f} ms\nMean: {stats['mean']:.2f} ms\nJitter: {stats['stdev']:.2f} ms"
        ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
                verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    # Print detailed statistics for all files
    print("\n" + "=" * 60)
    print("LATENCY ANALYSIS SUMMARY")
    print("=" * 60 + "\n")
    
    for file_path, stats in all_stats.items():
        print_stats(file_path, stats)
        print()

if __name__ == "__main__":
    # Analyze multiple log files
    file_paths = [
        "scripts/logs1.txt",
        "scripts/logs2.txt",
        "scripts/logs4.txt"
    ]
    
    analyze_multiple_files(file_paths)