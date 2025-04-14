import os
import shutil
import tarfile

import kagglehub

# Download latest version
path = kagglehub.dataset_download("atulanandjha/lfwpeople")

print("Path to dataset files:", path)

# Define the destination directory
DESTINATION_DIR = "datasets"

# Ensure the destination directory exists
os.makedirs(DESTINATION_DIR, exist_ok=True)

# Copy all files from the downloaded path to the destination directory
for file_name in os.listdir(path):
    full_file_name = os.path.join(path, file_name)
    if os.path.isfile(full_file_name):
        shutil.copy(full_file_name, DESTINATION_DIR)

print("Files copied to:", DESTINATION_DIR)

# Decompress all .tgz files in the destination directory
for file_name in os.listdir(DESTINATION_DIR):
    if file_name.endswith(".tgz"):
        tgz_file_path = os.path.join(DESTINATION_DIR, file_name)
        with tarfile.open(tgz_file_path, "r:gz") as tar:
            tar.extractall(path=DESTINATION_DIR)

        os.remove(tgz_file_path)  # Remove the .tgz file after decompression

print("Decompressed all .tgz files in:", DESTINATION_DIR)
