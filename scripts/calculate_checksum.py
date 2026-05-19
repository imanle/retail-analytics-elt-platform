"""
calculate_checksum.py

Calculates a SHA-256 checksum for a given file.
Used to detect duplicate files before ingestion.
"""

import hashlib
import os


CHUNK_SIZE = 8192  # 8 KB chunks for memory-efficient hashing


def calculate_checksum(file_path: str) -> str:
    """
    Args:
        file_path: path to the file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if os.path.getsize(file_path) == 0:
        raise ValueError(f"File is empty: {file_path}")

    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            sha256.update(chunk)

    return sha256.hexdigest()


def checksums_match(file_path: str, expected_checksum: str) -> bool:
    return calculate_checksum(file_path) == expected_checksum


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 calculate_checksum.py <file_path>")
        sys.exit(1)
    path = sys.argv[1]
    checksum = calculate_checksum(path)
    print(f"SHA-256: {checksum}")
    print(f"File   : {path}")
