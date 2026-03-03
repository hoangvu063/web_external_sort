import struct
import random
import os


def generate_bin_file(start: float, end: float, count: int, filename: str):
    if count <= 0:
        raise ValueError("Số lượng phần tử phải > 0")

    if start > end:
        raise ValueError("Giá trị bắt đầu phải <= giá trị kết thúc")

    # Lấy thư mục chứa file .py
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Tạo thư mục data cùng cấp
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Đường dẫn đầy đủ của file
    file_path = os.path.join(data_dir, filename)

    with open(file_path, "wb") as f:
        for _ in range(count):
            value = random.uniform(start, end)
            f.write(struct.pack("<d", value))  # double 8 byte (little-endian)

    file_size = os.path.getsize(file_path)

    print("\n✔ Đã tạo file:", file_path)
    print("✔ Số phần tử:", count)
    print("✔ Kích thước file:", file_size, "bytes")
    print("✔ Mỗi phần tử: 8 bytes (double)\n")


def main():
    print("=== GENERATE FLOAT .BIN FILE (8-BYTE DOUBLE) ===")

    try:
        start = float(input("Nhập giá trị bắt đầu: "))
        end = float(input("Nhập giá trị kết thúc: "))
        count = int(input("Nhập số lượng phần tử: "))
        filename = input("Nhập tên file output (vd: input.bin): ").strip()

        if not filename.endswith(".bin"):
            filename += ".bin"

        generate_bin_file(start, end, count, filename)

    except Exception as e:
        print("Lỗi:", e)


if __name__ == "__main__":
    main()