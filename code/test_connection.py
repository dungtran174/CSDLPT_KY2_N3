#!/usr/bin/env python3
"""
Kiểm tra kết nối đến cơ sở dữ liệu PostgreSQL
File này dùng để test kết nối database trước khi chạy các chức năng chính
"""
import psycopg2  # Thư viện kết nối PostgreSQL
from Interface import getopenconnection  # Import hàm kết nối từ module Interface

def test_connection():
    """
    Hàm kiểm tra kết nối PostgreSQL và tạo database cho bài tập
    Returns:
        True nếu kết nối thành công, False nếu có lỗi
    """
    try:
        # Bước 1: Kiểm tra kết nối cơ bản đến PostgreSQL
        conn = getopenconnection()  # Kết nối với database mặc định 'postgres'
        print("PostgreSQL connection successful")
        
        # Bước 2: Kiểm tra và tạo database cho bài tập nếu chưa tồn tại
        # Thiết lập chế độ autocommit để có thể thực thi CREATE DATABASE
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Kiểm tra xem database 'dds_assgn1' đã tồn tại chưa
        cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='dds_assgn1'")
        
        if cur.fetchone()[0] == 0:
            # Database chưa tồn tại -> tạo mới
            cur.execute("CREATE DATABASE dds_assgn1")
            print("Database 'dds_assgn1' created")
        else:
            # Database đã tồn tại
            print("Database 'dds_assgn1' exists")
        
        # Đóng cursor và connection đầu tiên
        cur.close()
        conn.close()
        
        # Bước 3: Kiểm tra kết nối đến database bài tập vừa tạo/đã có
        conn = getopenconnection(dbname='dds_assgn1')
        print("Connected to dds_assgn1 database")
        conn.close()
        
        return True  # Trả về True nếu tất cả các bước thành công
        
    except psycopg2.OperationalError as e:
        # Xử lý lỗi kết nối cụ thể (sai thông tin đăng nhập, server không chạy, etc.)
        print(f"Connection failed: {e}")
        return False
        
    except Exception as e:
        # Xử lý các lỗi khác không mong muốn
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    # Chạy hàm test khi file được thực thi trực tiếp
    # Điều này cho phép file có thể được import như module mà không tự động chạy test
    test_connection()