
import os
import random

def generate_test_files(output_dir="data/input"):
    os.makedirs(output_dir, exist_ok=True)

    # ---- Small datasets ----
    for i in range(1, 6):
        filename = f"input_dataset_small_{i:03}.txt"
        path = os.path.join(output_dir, filename)
        nums = [str(random.randint(1, 200)) for _ in range(50)]
        with open(path, "w") as f:
            f.write("\n".join(nums))
        print(f"Generated {path}")

    # ---- Medium datasets ----
    for i in range(1, 16):
        filename = f"input_dataset_medium_{i:03}.txt"
        path = os.path.join(output_dir, filename)
        nums = [str(random.randint(1, 10000)) for _ in range(500)]
        with open(path, "w") as f:
            f.write("\n".join(nums))
        print(f"Generated {path}")

    # ---- Large datasets ----
    for i in range(1, 6):
        filename = f"input_dataset_big_{i:03}.txt"
        path = os.path.join(output_dir, filename)
        nums = [str(random.randint(1, 50000)) for _ in range(5000)]
        with open(path, "w") as f:
            f.write("\n".join(nums))
        print(f"Generated {path}")

    # ---- Extra: One extremely large dataset (50,000 unique numbers) ----
    filename = "input_dataset_big.txt"
    path = os.path.join(output_dir, filename)
    nums = random.sample(range(1, 1_000_000), 50_000)
    with open(path, "w") as f:
        f.write("\n".join(map(str, nums)))
    print(f"✅ Generated {path} (50,000 unique numbers)")

if __name__ == "__main__":
    generate_test_files()
