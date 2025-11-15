# generate_files.py
import os
os.makedirs("files", exist_ok=True)
for i in range(1, 21):
    path = os.path.join("files", f"doc_{i:02d}.txt")
    with open(path, "w") as f:
        f.write(f"This is test file number {i}\n" * (i % 5 + 1))
print("Generated 20 files in ./files")