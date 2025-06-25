#!/usr/bin/env python3
"""
Module hỗ trợ kiểm thử cho bài tập phân vùng cơ sở dữ liệu phân tán
Chứa các hàm tiện ích để:
1. Quản lý database (tạo, xóa, kết nối)
2. Kiểm tra tính đúng đắn của phân vùng
3. Test các chức năng load dữ liệu và insert
4. Xác minh các tính chất: Completeness, Disjointness, Reconstruction
"""

import traceback  # Để in chi tiết lỗi
import psycopg2   # Thư viện kết nối PostgreSQL

# Các hằng số định nghĩa tên bảng và cột
RANGE_TABLE_PREFIX = 'range_part'     # Tiền tố cho bảng phân vùng range
RROBIN_TABLE_PREFIX = 'rrobin_part'   # Tiền tố cho bảng phân vùng round robin
USER_ID_COLNAME = 'userid'            # Tên cột user ID
MOVIE_ID_COLNAME = 'movieid'          # Tên cột movie ID  
RATING_COLNAME = 'rating'             # Tên cột rating

# ===== PHẦN 1: CÁC HÀM THIẾT LẬP VÀ QUẢN LÝ DATABASE =====

def createdb(dbname):
    """
    Tạo cơ sở dữ liệu mới bằng cách kết nối đến database mặc định của PostgreSQL
    Kiểm tra xem database đã tồn tại chưa trước khi tạo
    Args:
        dbname: Tên database cần tạo
    Returns:
        None
    """
    # Kết nối đến database mặc định 'postgres'
    con = getopenconnection()
    # Thiết lập chế độ autocommit để có thể thực thi CREATE DATABASE
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Kiểm tra xem database đã tồn tại chưa trong catalog của PostgreSQL
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    
    if count == 0:
        # Database chưa tồn tại -> tạo mới
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        # Database đã tồn tại -> thông báo
        print('A database named "{0}" already exists'.format(dbname))

    # Dọn dẹp tài nguyên
    cur.close()
    con.close()

def delete_db(dbname):
    """
    Xóa database đã chỉ định
    Args:
        dbname: Tên database cần xóa
    """
    con = getopenconnection(dbname = 'postgres')  # Kết nối đến database mặc định
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('drop database ' + dbname)  # Thực thi lệnh xóa database
    cur.close()
    con.close()


def deleteAllPublicTables(openconnection):
    """
    Xóa tất cả các bảng trong schema public của database hiện tại
    Được sử dụng để làm sạch database trước khi chạy test
    Args:
        openconnection: Kết nối database đã mở
    """
    cur = openconnection.cursor()
    # Lấy danh sách tất cả bảng trong schema public
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])  # Thu thập tên các bảng
    
    # Xóa từng bảng với CASCADE để xóa cả các ràng buộc liên quan
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    """
    Tạo kết nối đến PostgreSQL database
    Args:
        user: Tên người dùng (mặc định: 'postgres')
        password: Mật khẩu (mặc định: '1234')
        dbname: Tên database (mặc định: 'postgres')
    Returns:
        Đối tượng kết nối psycopg2
    """
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# ===== PHẦN 2: CÁC HÀM HỖ TRỢ KIỂM THỬ PHÂN VÙNG =====

def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tính số dòng dự kiến trong mỗi phân vùng range dựa trên bảng gốc
    Sử dụng để so sánh với kết quả thực tế sau khi phân vùng
    Args:
        ratingstablename: Tên bảng ratings gốc
        numberofpartitions: Số lượng phân vùng
        openconnection: Kết nối database
    Returns:
        List chứa số dòng dự kiến cho từng phân vùng
    """
    cur = openconnection.cursor()
    countList = []  # Danh sách lưu số dòng của từng phân vùng
    interval = 5.0 / numberofpartitions  # Khoảng cách giữa các phân vùng (rating từ 0-5)
    
    # Phân vùng đầu tiên: rating >= 0 AND rating <= interval (bao gồm cả 0)
    cur.execute("select count(*) from {0} where rating >= {1} and rating <= {2}".format(ratingstablename,0, interval))
    countList.append(int(cur.fetchone()[0]))

    # Các phân vùng còn lại: rating > lowerbound AND rating <= upperbound
    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute("select count(*) from {0} where rating > {1} and rating <= {2}".format(ratingstablename,
                                                                                          lowerbound,
                                                                                          lowerbound + interval))
        lowerbound += interval
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList


def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tính số dòng dự kiến trong mỗi phân vùng round robin dựa trên bảng gốc
    Sử dụng ROW_NUMBER() để mô phỏng thuật toán round robin
    Args:
        ratingstablename: Tên bảng ratings gốc
        numberofpartitions: Số lượng phân vùng
        openconnection: Kết nối database
    Returns:
        List chứa số dòng dự kiến cho từng phân vùng
    """
    cur = openconnection.cursor()
    countList = []  # Danh sách lưu số dòng của từng phân vùng
      # Với mỗi phân vùng i, đếm các dòng có (row_number-1) % numberofpartitions = i
    for i in range(0, numberofpartitions):
        cur.execute(
            "select count(*) from (select *, row_number() over () from {0}) as temp where (row_number-1)%{1}= {2}".format(
                ratingstablename, numberofpartitions, i))
        countList.append(int(cur.fetchone()[0]))
    
    cur.close()
    return countList

# ===== PHẦN 3: CÁC HÀM KIỂM TRA TÍNH CHẤT CỦA PHÂN VÙNG =====

def checkpartitioncount(cursor, expectedpartitions, prefix):
    """
    Kiểm tra xem số lượng bảng phân vùng được tạo có đúng như mong đợi không
    Args:
        cursor: Database cursor
        expectedpartitions: Số phân vùng mong đợi
        prefix: Tiền tố tên bảng (range_part hoặc rrobin_part)
    Raises:
        Exception nếu số bảng không khớp
    """
    cursor.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
            prefix))
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:  
        raise Exception(
            'Range partitioning not done properly. Excepted {0} table(s) but found {1} table(s)'.format(
                expectedpartitions, count))


def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    """
    Đếm tổng số dòng trong tất cả các bảng phân vùng bằng UNION ALL
    Sử dụng để kiểm tra tính chất Completeness, Disjointness, Reconstruction
    Args:
        cur: Database cursor
        n: Số lượng phân vùng
        rangepartitiontableprefix: Tiền tố tên bảng phân vùng
        partitionstartindex: Chỉ số bắt đầu của phân vùng (0 hoặc 1)
    Returns:
        Tổng số dòng trong tất cả phân vùng
    """
    selects = []  # Danh sách các câu SELECT cho từng phân vùng
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM {0}{1}'.format(rangepartitiontableprefix, i))
    
    # Sử dụng UNION ALL để ghép tất cả phân vùng và đếm
    cur.execute('SELECT COUNT(*) FROM ({0}) AS T'.format(' UNION ALL '.join(selects)))
    count = int(cur.fetchone()[0])
    return count


def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Kiểm tra các tính chất quan trọng của phân vùng:
    1. Completeness: Không mất dữ liệu (tổng số dòng >= dữ liệu gốc)
    2. Disjointness: Không trùng lặp dữ liệu (tổng số dòng <= dữ liệu gốc)  
    3. Reconstruction: Có thể tái tạo hoàn toàn (tổng số dòng = dữ liệu gốc)
    Args:
        n: Số lượng phân vùng
        openconnection: Kết nối database
        rangepartitiontableprefix: Tiền tố tên bảng
        partitionstartindex: Chỉ số bắt đầu
        ACTUAL_ROWS_IN_INPUT_FILE: Số dòng thực tế trong file gốc
    """
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            # Test 1: Nếu n không hợp lệ, không nên tạo bảng nào
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            # Test 2: Kiểm tra số lượng bảng được tạo đúng như yêu cầu
            checkpartitioncount(cur, n, rangepartitiontableprefix)

            # Test 3: Kiểm tra tính Completeness (đầy đủ)
            # Tổng số dòng sau phân vùng phải >= số dòng gốc
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE: 
                raise Exception(
                    "Completeness property of Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                        ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Test 4: Kiểm tra tính Disjointness (không trùng lặp)
            # Tổng số dòng sau phân vùng phải <= số dòng gốc
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count > ACTUAL_ROWS_IN_INPUT_FILE: 
                raise Exception(
                    "Dijointness property of Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                        ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Test 5: Kiểm tra tính Reconstruction (tái tạo hoàn toàn)
            # Tổng số dòng sau phân vùng phải = số dòng gốc chính xác
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count != ACTUAL_ROWS_IN_INPUT_FILE: 
                raise Exception(
                    "Rescontruction property of Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                        ACTUAL_ROWS_IN_INPUT_FILE, count))


def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    """
    Kiểm tra xem một record cụ thể có được chèn vào đúng bảng phân vùng không
    Args:
        expectedtablename: Tên bảng phân vùng mong đợi
        itemid: Movie ID của record
        openconnection: Kết nối database
        rating: Rating của record  
        userid: User ID của record
    Returns:
        True nếu tìm thấy record trong bảng mong đợi, False nếu không
    """
    with openconnection.cursor() as cur:
        # Tìm kiếm record với các giá trị cụ thể trong bảng phân vùng
        cur.execute(
            'SELECT COUNT(*) FROM {0} WHERE {4} = {1} AND {5} = {2} AND {6} = {3}'.format(expectedtablename, userid,
                                                                                          itemid, rating,
                                                                                          USER_ID_COLNAME,
                                                                                          MOVIE_ID_COLNAME,
                                                                                          RATING_COLNAME))
        count = int(cur.fetchone()[0])
        if count != 1:  # Phải tìm thấy đúng 1 record
            return False
        return True

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    """
    Kiểm tra từng phân vùng range có đúng số lượng dòng như tính toán hay không
    So sánh số dòng thực tế với số dòng dự kiến dựa trên thuật toán range
    Args:
        ratingstablename: Tên bảng gốc
        n: Số lượng phân vùng
        openconnection: Kết nối database
        rangepartitiontableprefix: Tiền tố tên bảng phân vùng
    """
    # Tính số dòng dự kiến cho từng phân vùng
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    
    # Kiểm tra từng phân vùng
    for i in range(0, n):
        cur.execute("select count(*) from {0}{1}".format(rangepartitiontableprefix, i))
        count = int(cur.fetchone()[0])  # Số dòng thực tế
        if count != countList[i]:       # So sánh với số dòng dự kiến
            raise Exception("{0}{1} has {2} of rows while the correct number should be {3}".format(
                rangepartitiontableprefix, i, count, countList[i]
            ))

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    """
    Kiểm tra từng phân vùng round robin có đúng số lượng dòng như tính toán hay không
    So sánh số dòng thực tế với số dòng dự kiến dựa trên thuật toán round robin
    Args:
        ratingstablename: Tên bảng gốc
        n: Số lượng phân vùng
        openconnection: Kết nối database
        roundrobinpartitiontableprefix: Tiền tố tên bảng phân vùng
    """
    # Tính số dòng dự kiến cho từng phân vùng
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    
    # Kiểm tra từng phân vùng
    for i in range(0, n):
        cur.execute("select count(*) from {0}{1}".format(roundrobinpartitiontableprefix, i))
        count = cur.fetchone()[0]       # Số dòng thực tế
        if count != countList[i]:       # So sánh với số dòng dự kiến
            raise Exception("{0}{1} has {2} of rows while the correct number should be {3}".format(
                roundrobinpartitiontableprefix, i, count, countList[i]
            ))

# ===== PHẦN 4: CÁC HÀM TEST CHÍNH CHO TỪNG CHỨC NĂNG =====

def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    """
    Kiểm thử hàm load dữ liệu từ file vào database
    Xác minh xem số lượng dòng được load có đúng như mong đợi không
    Args:
        MyAssignment: Module chứa hàm loadratings cần test
        ratingstablename: Tên bảng để load dữ liệu
        filepath: Đường dẫn file dữ liệu
        openconnection: Kết nối database
        rowsininpfile: Số dòng dự kiến trong file để kiểm tra
    Returns:
        [True, None] nếu thành công, [False, Exception] nếu thất bại
    """
    try:
        # Gọi hàm loadratings từ module cần test
        MyAssignment.loadratings(ratingstablename,filepath,openconnection)
        
        # Test 1: Đếm số dòng được chèn vào database
        with openconnection.cursor() as cur:
            cur.execute('SELECT COUNT(*) from {0}'.format(ratingstablename))
            count = int(cur.fetchone()[0])
            if count != rowsininpfile:
                raise Exception(
                    'Expected {0} rows, but {1} rows in \'{2}\' table'.format(rowsininpfile, count, ratingstablename))
    except Exception as e:
        traceback.print_exc()  # In chi tiết lỗi để debug
        return [False, e]
    return [True, None]


def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Kiểm thử hàm phân vùng theo range (dựa trên phạm vi rating)
    Kiểm tra các tính chất: Completeness, Disjointness, Reconstruction
    Và xác minh mỗi phân vùng có đúng số dòng theo thuật toán range
    Args:
        MyAssignment: Module chứa hàm rangepartition cần test
        ratingstablename: Tên bảng gốc
        n: Số lượng phân vùng cần tạo
        openconnection: Kết nối database
        partitionstartindex: Chỉ số bắt đầu của phân vùng (0 hoặc 1)
        ACTUAL_ROWS_IN_INPUT_FILE: Số dòng thực tế trong dữ liệu gốc
    Returns:
        [True, None] nếu thành công, [False, Exception] nếu thất bại
    """
    try:
        # Gọi hàm rangepartition từ module cần test
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        
        # Kiểm tra các tính chất cơ bản của phân vùng
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        
        # Kiểm tra chi tiết từng phân vùng có đúng số dòng không
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        
        return [True, None]
    except Exception as e:
        traceback.print_exc()  # In chi tiết lỗi để debug
        return [False, e]


def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Kiểm thử hàm phân vùng theo round robin (phân chia tuần tự)
    Kiểm tra các tính chất: Completeness, Disjointness, Reconstruction
    Và xác minh mỗi phân vùng có đúng số dòng theo thuật toán round robin
    Args:
        MyAssignment: Module chứa hàm roundrobinpartition cần test
        ratingstablename: Tên bảng gốc
        numberofpartitions: Số lượng phân vùng cần tạo
        openconnection: Kết nối database
        partitionstartindex: Chỉ số bắt đầu của phân vùng
        ACTUAL_ROWS_IN_INPUT_FILE: Số dòng thực tế trong dữ liệu gốc
    Returns:
        [True, None] nếu thành công, [False, Exception] nếu thất bại
    """
    try:
        # Gọi hàm roundrobinpartition từ module cần test
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        
        # Kiểm tra các tính chất cơ bản của phân vùng
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        
        # Kiểm tra chi tiết từng phân vùng có đúng số dòng không
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
        
    except Exception as e:
        traceback.print_exc()  # In chi tiết lỗi để debug
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Kiểm thử hàm chèn dữ liệu vào phân vùng round robin
    Xác minh xem record được chèn vào đúng phân vùng theo thuật toán round robin không
    Args:
        MyAssignment: Module chứa hàm roundrobininsert cần test
        ratingstablename: Tên bảng chính
        userid: User ID của record cần chèn
        itemid: Movie ID của record cần chèn
        rating: Rating của record cần chèn
        openconnection: Kết nối database
        expectedtableindex: Chỉ số phân vùng mong đợi (vd: '0', '1', '2'...)
    Returns:
        [True, None] nếu thành công, [False, Exception] nếu thất bại
    """
    try:
        # Tạo tên bảng phân vùng mong đợi
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        
        # Gọi hàm roundrobininsert từ module cần test
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        
        # Kiểm tra xem record có được chèn vào đúng phân vùng không
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Round robin insert failed! Couldnt find ({0}, {1}, {2}) tuple in {3} table'.format(userid, itemid, rating,
                                                                                                    expectedtablename))
    except Exception as e:
        traceback.print_exc()  # In chi tiết lỗi để debug
        return [False, e]
    return [True, None]


def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Kiểm thử hàm chèn dữ liệu vào phân vùng range
    Xác minh xem record được chèn vào đúng phân vùng dựa trên giá trị rating không
    Args:
        MyAssignment: Module chứa hàm rangeinsert cần test
        ratingstablename: Tên bảng chính
        userid: User ID của record cần chèn
        itemid: Movie ID của record cần chèn
        rating: Rating của record cần chèn (dùng để xác định phân vùng)
        openconnection: Kết nối database
        expectedtableindex: Chỉ số phân vùng mong đợi dựa trên giá trị rating
    Returns:
        [True, None] nếu thành công, [False, Exception] nếu thất bại
    """
    try:
        # Tạo tên bảng phân vùng mong đợi
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        
        # Gọi hàm rangeinsert từ module cần test
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        
        # Kiểm tra xem record có được chèn vào đúng phân vùng không
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Range insert failed! Couldnt find ({0}, {1}, {2}) tuple in {3} table'.format(userid, itemid, rating,
                                                                                              expectedtablename))
    except Exception as e:
        traceback.print_exc()  # In chi tiết lỗi để debug
        return [False, e]
    return [True, None]