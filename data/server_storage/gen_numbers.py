import random

# nums = random.sample(range(1, 1_000_000), 50_000)
# with open("data/server_storage/input/input_dataset_big.txt", "w") as f:
#     f.write("\n".join(map(str, nums)))


# create 500w number file for test
with open("data/server_storage/input/input_dataset_biggest.txt", "w") as f:
    for i in range(1, 5_000_001):
        f.write(str(i) + "\n")