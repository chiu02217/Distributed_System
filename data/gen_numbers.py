import random

nums = random.sample(range(1, 1_000_000), 50_000)
with open("data/server_storage/input/input_dataset_big.txt", "w") as f:
    f.write("\n".join(map(str, nums)))