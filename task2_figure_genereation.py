import matplotlib.pyplot as plt
import pandas as pd

def plot(file: str):
    data = pd.read_csv(file, sep="\t", header=None, names=["label", "spending"])
    data[["label", "month"]] = data["label"].apply(lambda x: pd.Series(x.split(': ')))
    
    pivoted_data =  data.pivot(index="month", columns="label", values="spending")

    plt.figure(figsize=(10, 6))
    for product in pivoted_data.columns:
        plt.plot(pivoted_data.index, pivoted_data[product], label=product, marker='o')

    plt.title("Monthly Coffee Spending")
    plt.xlabel("Month")
    plt.ylabel("Spending")
    plt.legend(title="Coffee Type")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file.replace(".txt", ".pdf"))

if __name__ == "__main__":
    plot("task2_1_output.txt")

    plot("task2_2_output.txt")