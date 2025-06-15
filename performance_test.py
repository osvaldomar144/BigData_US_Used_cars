import argparse
import subprocess
import time
import os
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser()
parser.add_argument("job", type=str, choices=["job-1", "job-2"], help="Job name")
parser.add_argument("master", type=str, choices=["local[*]", "yarn"], help="Master type")
parser.add_argument("--fractions", type=str, help="Fractions of dataset to use (es: 0.01 0.2 0.5 0.7)")
args = parser.parse_args()

tools = ["map-reduce", "spark-core", "spark-sql"]
fraction_values = list(map(float, args.fractions.split()))
fractions = [f"data-{x * 100}%" for x in fraction_values] + ["data_cleaned"]

execution_data = {tool: [] for tool in tools}

for tool in tools:
    for fraction in fractions:
        start = time.time()
        process = subprocess.run(
            ["bash", "run.sh", args.job, fraction, args.master],
            cwd=tool,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        end = time.time()

        exec_time = end - start
        execution_data[tool].append(exec_time)

        print(f"Finished {tool}#{args.job} for dataset \"{fraction}\" in {exec_time:.2f} seconds")

        output_path = os.path.join("logs", tool, args.job)
        os.makedirs(output_path, exist_ok=True)
        with open(os.path.join(output_path, f"stdout-{fraction}.txt"), "wb") as f:
            f.write(process.stdout)

plt.figure(figsize=(10, 6))

x_labels = [f if f == "data_cleaned" else f"data-{f.split('-')[1]}" for f in fractions]
x_pos = list(range(len(fractions)))

colors = {
    "map-reduce": "red",
    "spark-core": "green",
    "spark-sql": "blue"
}

for tool in tools:
    plt.plot(
        x_pos,
        execution_data[tool],
        marker='o',
        linestyle='-',
        label=tool,
        color=colors[tool]
    )

plt.xticks(x_pos, x_labels)
plt.xlabel('Dataset Fraction')
plt.ylabel('Processing Time (seconds)')
plt.title(f'Benchmark Execution Time - {args.job}')
plt.grid(True)
plt.legend()
plt.tight_layout()

output_graph = os.path.join("logs", f"benchmark_{args.job}.png")
os.makedirs("logs", exist_ok=True)
plt.savefig(output_graph, dpi=300)
