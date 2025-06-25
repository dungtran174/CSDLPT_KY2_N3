#!/usr/bin/python2.7
#
# Interface for the assignement - Giao diện cho bài tập lớn
#

import psycopg2  # Thư viện để kết nối và thao tác với PostgreSQL
from io import StringIO  # Để tạo buffer trong bộ nhớ cho việc copy dữ liệu


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    """
    Hàm tạo kết nối đến cơ sở dữ liệu PostgreSQL
    Args:
        user: Tên người dùng (mặc định: 'postgres')
        password: Mật khẩu (mặc định: '1234') 
        dbname: Tên cơ sở dữ liệu (mặc định: 'postgres')
    Returns:
        Đối tượng kết nối psycopg2
    """
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Hàm tải dữ liệu từ file vào bảng cơ sở dữ liệu
    Args:
        ratingstablename: Tên bảng để lưu dữ liệu đánh giá
        ratingsfilepath: Đường dẫn file chứa dữ liệu đánh giá
        openconnection: Kết nối database đã mở
    """
    con = openconnection  # Lấy kết nối database
    cur = con.cursor()    # Tạo cursor để thực thi các câu lệnh SQL
    
    # Xóa bảng nếu tồn tại và tạo bảng mới với cấu trúc cần thiết
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename}")
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid integer,      -- ID người dùng
            movieid integer,     -- ID phim
            rating float         -- Điểm đánh giá (0.0 - 5.0)
        )
    """)
    
    # Đọc và xử lý file theo từng chunk để tối ưu hiệu suất với file lớn
    chunk_size = 100000  # Số dòng xử lý mỗi lần
    with open(ratingsfilepath, 'r') as f:
        while True:
            chunk = []  # Danh sách lưu trữ các dòng dữ liệu trong chunk hiện tại
            # Đọc chunk_size dòng từ file
            for _ in range(chunk_size):
                line = f.readline()
                if not line:  # Nếu hết file thì dừng
                    break
                parts = line.strip().split('::')  # File định dạng userID::movieID::rating::timestamp
                if len(parts) >= 3:  # Đảm bảo dòng có đủ thông tin cần thiết
                    userid, movieid, rating = parts[0], parts[1], parts[2]
                    chunk.append(f"{userid}\t{movieid}\t{rating}\n")  # Chuyển sang định dạng tab-delimited cho COPY
            
            if not chunk:  # Nếu không còn dữ liệu thì thoát khỏi vòng lặp
                break
                
            # Tạo buffer trong bộ nhớ cho chunk và sử dụng COPY để insert nhanh
            buffer = StringIO(''.join(chunk))
            cur.copy_from(buffer, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
            con.commit()  # Xác nhận giao dịch cho chunk này
    
    cur.close()  # Đóng cursor

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm tạo phân vùng theo phạm vi (range partitioning) dựa trên điểm rating
    Range Partitioning: Chia dữ liệu dựa trên khoảng giá trị liên tục của một thuộc tính
    Args:
        ratingstablename: Tên bảng chính chứa dữ liệu
        numberofpartitions: Số phân vùng cần tạo
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions  # Tính khoảng cách giữa các phân vùng (rating từ 0-5)
    
    # Tạo tất cả các bảng phân vùng cùng lúc để tối ưu
    # Mỗi bảng có tên range_part0, range_part1, range_part2, ...
    create_tables_sql = '; '.join([
        f"CREATE TABLE IF NOT EXISTS range_part{i} (userid integer, movieid integer, rating float)"
        for i in range(numberofpartitions)
    ])
    cur.execute(create_tables_sql)
    
    # Xóa dữ liệu cũ trong các phân vùng nếu có
    cur.execute('; '.join([f"TRUNCATE TABLE range_part{i}" for i in range(numberofpartitions)]))
    
    # Phân chia dữ liệu vào các phân vùng dựa trên khoảng rating
    for i in range(numberofpartitions):
        minRange = i * delta          # Giá trị rating tối thiểu của phân vùng
        maxRange = minRange + delta   # Giá trị rating tối đa của phân vùng
        
        if i == 0:  # Phân vùng đầu tiên bao gồm cả rating = 0
            cur.execute("""
                INSERT INTO range_part{}
                SELECT userid, movieid, rating 
                FROM {}
                WHERE rating >= {} AND rating <= {}
            """.format(i, ratingstablename, minRange, maxRange))
        else:  # Các phân vùng khác không bao gồm giá trị biên trái để tránh trùng lặp
            cur.execute("""
                INSERT INTO range_part{}
                SELECT userid, movieid, rating 
                FROM {}
                WHERE rating > {} AND rating <= {}
            """.format(i, ratingstablename, minRange, maxRange))
    
    cur.close()
    con.commit()  # Xác nhận tất cả các thay đổi

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm tạo phân vùng theo phương pháp round robin
    Round Robin Partitioning: Phân chia dữ liệu tuần tự vào các phân vùng theo thứ tự
    Dòng 1 -> phân vùng 0, dòng 2 -> phân vùng 1, ..., dòng n -> phân vùng (n % numberofpartitions)
    Args:
        ratingstablename: Tên bảng chính chứa dữ liệu
        numberofpartitions: Số phân vùng cần tạo
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    # Tạo tất cả các bảng phân vùng round robin cùng lúc
    # Mỗi bảng có tên rrobin_part0, rrobin_part1, rrobin_part2, ...
    create_tables_sql = '; '.join([
        f"CREATE TABLE IF NOT EXISTS rrobin_part{i} (userid integer, movieid integer, rating float)"
        for i in range(numberofpartitions)
    ])
    cur.execute(create_tables_sql)
    
    # Xóa dữ liệu cũ trong các phân vùng nếu có
    cur.execute('; '.join([f"TRUNCATE TABLE rrobin_part{i}" for i in range(numberofpartitions)]))
    
    # Phân chia dữ liệu vào các phân vùng theo thuật toán round robin
    for i in range(numberofpartitions):
        cur.execute("""
            INSERT INTO rrobin_part{}
            SELECT userid, movieid, rating
            FROM (
                SELECT *, ROW_NUMBER() OVER () - 1 as row_num
                FROM {}
            ) numbered_rows
            WHERE row_num % {} = {}
        """.format(i, ratingstablename, numberofpartitions, i))
        # ROW_NUMBER() đánh số thứ tự các dòng bắt đầu từ 1
        # Trừ 1 để có số thứ tự bắt đầu từ 0
        # Sử dụng modulo để phân chia: dòng có row_num % numberofpartitions = i sẽ vào phân vùng i
    
    cur.close()
    con.commit()  # Xác nhận tất cả các thay đổi

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn dữ liệu mới vào bảng chính và phân vùng round robin tương ứng
    Args:
        ratingstablename: Tên bảng chính
        userid: ID người dùng
        itemid: ID phim (movie)
        rating: Điểm đánh giá
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Chèn dữ liệu vào bảng chính trước
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """.format(ratingstablename), (userid, itemid, rating))
        
        # Đếm tổng số dòng hiện tại trong bảng chính để xác định vị trí round robin
        cur.execute("SELECT COUNT(*) FROM {}".format(ratingstablename))
        total_rows = cur.fetchone()[0]
        
        # Tính chỉ số phân vùng dựa trên thuật toán round robin
        numberofpartitions = count_partitions('rrobin_part', openconnection)
        index = (total_rows - 1) % numberofpartitions  # Trừ 1 vì đã insert vào bảng chính
        
        # Chèn vào phân vùng round robin tương ứng
        cur.execute("""
            INSERT INTO rrobin_part{} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """.format(index), (userid, itemid, rating))
        
        con.commit()  # Xác nhận giao dịch thành công
    except Exception as e:
        con.rollback()  # Hoàn tác nếu có lỗi
        raise e
    finally:
        cur.close()  # Đảm bảo đóng cursor trong mọi trường hợp

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn dữ liệu mới vào bảng chính và phân vùng range tương ứng dựa trên giá trị rating
    Args:
        ratingstablename: Tên bảng chính
        userid: ID người dùng  
        itemid: ID phim (movie)
        rating: Điểm đánh giá (dùng để xác định phân vùng)
        openconnection: Kết nối database
    """
    con = openconnection
    cur = con.cursor()
    
    # Tính toán phân vùng dựa trên giá trị rating
    numberofpartitions = count_partitions('range_part', openconnection)
    delta = 5.0 / numberofpartitions  # Khoảng cách giữa các phân vùng
    index = int(rating / delta)       # Xác định chỉ số phân vùng
    
    # Xử lý trường hợp đặc biệt: nếu rating chia hết cho delta và không phải 0
    # thì thuộc về phân vùng trước đó (để tránh vượt quá số phân vùng)
    if rating % delta == 0 and index != 0:
        index -= 1
    
    # Chèn vào cả bảng chính và phân vùng trong một giao dịch
    cur.execute("""
        BEGIN;
        INSERT INTO {} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        INSERT INTO range_part{} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        COMMIT;
    """.format(ratingstablename, index), 
    (userid, itemid, rating, userid, itemid, rating))
    
    cur.close()
    con.commit()  # Xác nhận giao dịch

def create_db(dbname):
    """
    Hàm tạo cơ sở dữ liệu mới
    Kết nối tới database mặc định 'postgres' để tạo database mới
    Kiểm tra xem database đã tồn tại chưa trước khi tạo
    Args:
        dbname: Tên database cần tạo
    Returns:
        None
    """
    con = getopenconnection(dbname='postgres')  # Kết nối tới database mặc định
    # Thiết lập chế độ autocommit để có thể thực thi câu lệnh CREATE DATABASE
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    # Kiểm tra xem database đã tồn tại chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
    count = cur.fetchone()[0]
    
    if count == 0:
        # Tạo database mới nếu chưa tồn tại
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        # Thông báo nếu database đã tồn tại
        print('A database named {0} already exists'.format(dbname))
    
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Hàm đếm số lượng bảng có tên chứa prefix nhất định
    Sử dụng để đếm số phân vùng đã được tạo
    Args:
        prefix: Tiền tố của tên bảng (vd: 'range_part', 'rrobin_part')
        openconnection: Kết nối database
    Returns:
        Số lượng bảng tìm được
    """
    con = openconnection
    cur = con.cursor()
    
    # Truy vấn từ bảng thống kê của PostgreSQL để tìm các bảng user tạo
    # có tên bắt đầu bằng prefix
    cur.execute("""
        SELECT COUNT(*) 
        FROM pg_stat_user_tables 
        WHERE relname LIKE %s
    """, (prefix + '%',))  # Sử dụng LIKE với ký tự wildcard %
    
    count = cur.fetchone()[0]
    cur.close()
    return count
