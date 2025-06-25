#!/usr/bin/env python3
"""
File kiểm thử chức năng phân vùng cơ sở dữ liệu - Assignment 1 Tester
Chương trình này test các chức năng:
1. Load dữ liệu từ file vào database
2. Tạo phân vùng theo Range hoặc Round Robin
3. Chèn dữ liệu mới vào phân vùng
4. Xác minh tính đúng đắn của dữ liệu
"""

import psycopg2      # Thư viện kết nối PostgreSQL
import traceback     # Để in chi tiết lỗi
import testHelper    # Module chứa các hàm hỗ trợ test
import Interface as MyAssignment  # Module chính chứa logic phân vùng
import time          # Để đo thời gian thực thi

# Các hằng số cấu hình
DATABASE_NAME = 'dds_assgn1'              # Tên database sử dụng cho bài tập
RATINGS_TABLE = 'ratings'                 # Tên bảng chính chứa dữ liệu rating
RANGE_TABLE_PREFIX = 'range_part'         # Tiền tố cho các bảng phân vùng range
RROBIN_TABLE_PREFIX = 'rrobin_part'       # Tiền tố cho các bảng phân vùng round robin
INPUT_FILE_PATH = 'ratings.dat'           # Đường dẫn file dữ liệu đầu vào
ACTUAL_ROWS_IN_INPUT_FILE = 10000054      # Số dòng dự kiến trong file (để validation)

def print_progress(message, indent=0):
    """
    In thông báo tiến trình với timestamp và thụt lề
    Args:
        message: Nội dung thông báo cần in
        indent: Mức độ thụt lề (0 = không thụt, 1 = 2 spaces, 2 = 4 spaces...)
    """
    print(f"[{time.strftime('%H:%M:%S')}] {'  ' * indent}{message}")

def verify_partition_content(conn, prefix, number_of_partitions):
    """
    Xác minh tính đúng đắn của dữ liệu trong các bảng phân vùng
    Kiểm tra xem tổng số dòng trong tất cả phân vùng có bằng số dòng trong bảng gốc không
    Args:
        conn: Kết nối database
        prefix: Tiền tố tên bảng phân vùng (range_part hoặc rrobin_part)
        number_of_partitions: Số lượng phân vùng cần kiểm tra
    """
    cur = conn.cursor()
    total_rows = 0  # Tổng số dòng trong tất cả phân vùng
    print_progress(f"Verifying {prefix} partition tables:")
    
    # Đếm số dòng trong từng phân vùng
    for i in range(number_of_partitions):
        table_name = f"{prefix}{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        total_rows += count
        print_progress(f"- {table_name}: {count:,} rows", indent=1)
    
    # Đếm số dòng trong bảng gốc để so sánh
    cur.execute(f"SELECT COUNT(*) FROM {RATINGS_TABLE}")
    original_count = cur.fetchone()[0]
    
    # So sánh và thông báo kết quả
    print_progress(f"Total rows: partitions={total_rows:,}, original={original_count:,}")
    if total_rows == original_count:
        print_progress("Partition content passed!")  # Dữ liệu đúng
    else:
        print_progress("Partition content failed!")  # Có lỗi dữ liệu
    cur.close()

def main():
    """
    Hàm chính của chương trình test
    Thực hiện tuần tự các bước:
    1. Tạo database và làm sạch bảng
    2. Test chức năng load dữ liệu từ file
    3. Cho phép user chọn loại phân vùng (range hoặc round robin)
    4. Test chức năng tạo phân vùng và chèn dữ liệu
    5. Xác minh tính đúng đắn
    6. Dọn dẹp database
    """
    try:
        print_progress("Starting test...")
        # Tạo database cho bài tập (nếu chưa có)
        testHelper.createdb(DATABASE_NAME)

        # Mở kết nối đến database và thiết lập autocommit
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            # Xóa tất cả bảng public cũ để bắt đầu test sạch
            testHelper.deleteAllPublicTables(conn)

            # BƯỚC 1: Test chức năng load dữ liệu từ file vào database
            print_progress("Testing loadratings...")
            start_time = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            load_time = time.time() - start_time
            print_progress(f"loadratings: {'passed' if result else 'failed'}! ({load_time:.3f} seconds)")

            # BƯỚC 2: Cho phép user chọn loại phân vùng
            partition_choice = input("\nChoose partitioning (range/roundrobin): ").strip().lower()
            start_time = time.time()

            if partition_choice == 'range':
                # Test Range Partitioning (phân vùng theo phạm vi)
                print_progress("Testing RANGE partitioning...")
                print_progress("Creating 5 range partitions...")
                
                # Tạo 5 phân vùng range dựa trên giá trị rating
                [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print_progress("rangepartition passed!")
                    # Xác minh dữ liệu trong các phân vùng
                    verify_partition_content(conn, RANGE_TABLE_PREFIX, 5)
                else:
                    print_progress("rangepartition failed!")

                # Test chức năng chèn dữ liệu mới vào range partition
                print_progress("Testing range insert...")
                # Chèn record: userid=100, movieid=2, rating=3
                [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
                print_progress(f"rangeinsert: {'passed' if result else 'failed'}!")

            elif partition_choice == 'roundrobin':
                # Test Round Robin Partitioning (phân vùng tuần tự)
                print_progress("Testing ROUND ROBIN partitioning...")
                print_progress("Creating 5 roundrobin partitions...")
                
                # Tạo 5 phân vùng round robin
                [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print_progress("roundrobinpartition passed!")
                    # Xác minh dữ liệu trong các phân vùng
                    verify_partition_content(conn, RROBIN_TABLE_PREFIX, 5)
                else:
                    print_progress("roundrobinpartition failed!")

                # Test chức năng chèn dữ liệu mới vào round robin partition
                print_progress("Testing roundrobin insert...")
                # Chèn record: userid=100, movieid=1, rating=3
                [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
                print_progress(f"roundrobininsert: {'passed' if result else 'failed'}!")

            else:
                print_progress("Invalid choice! Choose 'range' or 'roundrobin'.")
                return

            # Hiển thị tổng thời gian thực thi
            elapsed_time = time.time() - start_time
            print_progress(f"Total partitioning + insert time: {elapsed_time:.3f} seconds")

            # BƯỚC 3: Tùy chọn dọn dẹp - xóa tất cả bảng sau khi test
            if input('\nPress enter to delete all tables: ') == '':
                print_progress("Deleting all tables...")
                testHelper.deleteAllPublicTables(conn)
                print_progress("Tables deleted.")

    except Exception:
        # Xử lý lỗi: in stack trace chi tiết để debug
        print_progress("Error occurred:")
        traceback.print_exc()

if __name__ == '__main__':
    # Entry point - chạy hàm main khi file được thực thi trực tiếp
    # Điều này cho phép file có thể được import như module mà không tự động chạy test
    main()