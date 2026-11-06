
import subprocess
import time
from pathlib import Path

LOCAL_DIR = Path("data/raw/generated")
HDFS_DIR = "/fraud-detection/transactions/live-input"

processed_files = set()


def upload_file(file_path):
    hdfs_path = f"{HDFS_DIR}/{file_path.name}"

    # Check whether the file already exists in HDFS
    check = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", hdfs_path]
    )

    if check.returncode == 0:
        print(f"Already exists in HDFS: {file_path.name}")
        processed_files.add(file_path)
        return

    try:
        subprocess.run(
            ["hdfs", "dfs", "-put", str(file_path), HDFS_DIR],
            check=True
        )

        print(f"Uploaded: {file_path.name}")
        processed_files.add(file_path)

    except subprocess.CalledProcessError:
        print(f"Upload failed: {file_path.name}")


def main():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR],
        check=True
    )

    print("HDFS batch uploader started.")
    print(f"Monitoring: {LOCAL_DIR}")

    try:
        while True:
            for file_path in sorted(LOCAL_DIR.glob("batch_*.csv")):
                if file_path not in processed_files:
                    upload_file(file_path)

            time.sleep(5)

    except KeyboardInterrupt:
        print("\nUploader stopped.")


if __name__ == "__main__":
    main()

