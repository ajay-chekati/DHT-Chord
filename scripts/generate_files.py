# generate_files.py
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

FILES_DIR = os.path.join(ROOT_DIR, "scripts", "files")
os.makedirs(FILES_DIR, exist_ok=True)

for i in range(1, 201):
    path = os.path.join(FILES_DIR, f"doc_{i:02d}.txt")
    with open(path, "w") as f:
        f.write(f"This is test file number {i}\n" * (i % 5 + 1))

print(f"Generated 200 files in {FILES_DIR}")