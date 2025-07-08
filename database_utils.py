"""
database_utils.py

데이터베이스 연결 및 기본 유틸리티 함수들
- 데이터베이스 초기화 및 테이블 생성
- 업체코드 자동 생성
- 데이터 파싱 유틸리티
"""

import streamlit as st
import sqlite3
import uuid
import pandas as pd


@st.cache_resource
def init_database():
    """
    SQLite 데이터베이스 연결 생성 및 테이블 초기화
    
    Returns:
        sqlite3.Connection: 데이터베이스 연결 객체
        
    Note:
        - @st.cache_resource로 캐시되어 앱 전체에서 재사용
        - 테이블이 없으면 자동으로 생성
        - 멀티스레드 환경 지원 (check_same_thread=False)
    """
    import os
    
    # 데이터베이스 파일 경로 확인 및 생성
    db_path = 'crm_database.db'
    
    # 파일이 존재하지 않으면 빈 파일 생성
    if not os.path.exists(db_path):
        open(db_path, 'a').close()
    
    # 파일 권한 확인 및 설정
    try:
        if os.path.exists(db_path):
            os.chmod(db_path, 0o666)  # 읽기/쓰기 권한 설정
    except:
        pass  # 권한 설정이 실패해도 계속 진행
    
    # 연결 옵션 개선
    conn = sqlite3.connect(
        db_path, 
        check_same_thread=False,
        timeout=30.0,  # 30초 타임아웃
        isolation_level=None  # autocommit 모드
    )
    
    # WAL 모드 설정 (동시 접근 개선)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL") 
        conn.execute("PRAGMA cache_size=1000")
        conn.execute("PRAGMA temp_store=memory")
    except:
        pass  # PRAGMA 설정이 실패해도 계속 진행
    
    # 기업 테이블 생성
    conn.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            company_code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            revenue_2024 REAL,
            industry TEXT,
            employee_count INTEGER,
            address TEXT,
            products TEXT,
            customer_category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 고객 연락처 테이블 생성
    conn.execute('''
        CREATE TABLE IF NOT EXISTS customer_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_code TEXT,
            customer_name TEXT NOT NULL,
            position TEXT,
            phone TEXT,
            email TEXT,
            acquisition_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_code) REFERENCES companies(company_code)
        )
    ''')
    
    # 상담 이력 테이블 생성
    conn.execute('''
        CREATE TABLE IF NOT EXISTS consultations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_code TEXT,
            customer_name TEXT,
            consultation_date TEXT,
            consultation_content TEXT NOT NULL,
            project_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_code) REFERENCES companies(company_code)
        )
    ''')
    
    # 즉시 커밋
    try:
        conn.commit()
    except Exception as e:
        st.error(f"데이터베이스 초기화 실패: {e}")
        # 연결 재시도
        conn.close()
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        conn.commit()
    
    return conn


def generate_company_code():
    """
    자동 업체코드 생성
    
    Returns:
        str: AUTO + 8자리 랜덤 문자열 (예: AUTO12AB34CD)
        
    Note:
        - 외감기업이 아닌 경우 자동으로 업체코드 생성
        - UUID4의 앞 8자리를 대문자로 변환하여 사용
        - 매번 호출 시 새로운 코드 생성
    """
    return f"AUTO{str(uuid.uuid4())[:8].upper()}"


def parse_revenue(revenue_str):
    """
    매출액 문자열을 숫자로 변환
    
    Args:
        revenue_str (any): 매출액 데이터 (문자열, 숫자, None 등)
        
    Returns:
        float or None: 변환된 매출액 또는 None
        
    Example:
        >>> parse_revenue("1,000,000")
        1000000.0
        >>> parse_revenue("1000000")
        1000000.0
        >>> parse_revenue("")
        None
        >>> parse_revenue("abc")
        None
        
    Note:
        - 쉼표와 공백 자동 제거
        - 변환 실패 시 None 반환
        - pandas의 isna() 함수로 빈 값 처리
    """
    if pd.isna(revenue_str) or revenue_str == "":
        return None
    
    # 쉼표 제거하고 숫자만 추출
    revenue_str = str(revenue_str).replace(",", "").replace(" ", "")
    try:
        return float(revenue_str)
    except (ValueError, TypeError):
        return None


def get_writable_connection():
    """
    쓰기 가능한 새로운 데이터베이스 연결 생성
    편집 작업 시 사용
    
    Returns:
        sqlite3.Connection: 쓰기 가능한 데이터베이스 연결
    """
    import os
    
    db_path = 'crm_database.db'
    
    # 파일 권한 확인
    if os.path.exists(db_path):
        try:
            os.chmod(db_path, 0o666)
        except:
            pass
    
    # 새로운 연결 생성 (캐시되지 않음)
    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
        timeout=30.0,
        isolation_level=None  # autocommit 모드
    )
    
    # WAL 모드 설정
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except:
        pass
    
    return conn


def test_write_permission():
    """
    데이터베이스 쓰기 권한 테스트
    
    Returns:
        bool: 쓰기 가능 여부
    """
    try:
        conn = get_writable_connection()
        
        # 테스트 테이블 생성/삭제
        conn.execute("CREATE TABLE IF NOT EXISTS test_write (id INTEGER)")
        conn.execute("INSERT INTO test_write (id) VALUES (1)")
        conn.execute("DELETE FROM test_write")
        conn.execute("DROP TABLE test_write")
        
        conn.close()
        return True
    except Exception as e:
        st.error(f"쓰기 권한 테스트 실패: {e}")
        return False


def get_table_info(conn):
    """
    데이터베이스 테이블 정보 조회
    
    Args:
        conn (sqlite3.Connection): 데이터베이스 연결
        
    Returns:
        dict: 테이블별 정보 (테이블명, 컬럼 수, 레코드 수)
        
    Example:
        >>> info = get_table_info(conn)
        >>> print(info['companies']['record_count'])
        25
    """
    tables = ['companies', 'customer_contacts', 'consultations']
    table_info = {}
    
    for table in tables:
        try:
            # 레코드 수 조회
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            record_count = count_result[0] if count_result else 0
            
            # 컬럼 정보 조회
            columns_result = conn.execute(f"PRAGMA table_info({table})").fetchall()
            column_count = len(columns_result)
            column_names = [col[1] for col in columns_result]
            
            table_info[table] = {
                'record_count': record_count,
                'column_count': column_count,
                'columns': column_names
            }
        except Exception as e:
            table_info[table] = {
                'record_count': 0,
                'column_count': 0,
                'columns': [],
                'error': str(e)
            }
    
    return table_info


def check_database_health(conn):
    """
    데이터베이스 상태 확인
    
    Args:
        conn (sqlite3.Connection): 데이터베이스 연결
        
    Returns:
        dict: 데이터베이스 상태 정보
        
    Example:
        >>> health = check_database_health(conn)
        >>> print(health['status'])
        'healthy'
    """
    try:
        # 기본 연결 테스트
        conn.execute("SELECT 1").fetchone()
        
        # 테이블 존재 확인
        tables = ['companies', 'customer_contacts', 'consultations']
        missing_tables = []
        
        for table in tables:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table,)
            ).fetchone()
            if not result:
                missing_tables.append(table)
        
        # 외래키 제약 조건 확인
        foreign_key_check = conn.execute("PRAGMA foreign_key_check").fetchall()
        
        # 상태 판단
        if missing_tables:
            status = 'missing_tables'
        elif foreign_key_check:
            status = 'foreign_key_errors'
        else:
            status = 'healthy'
        
        return {
            'status': status,
            'missing_tables': missing_tables,
            'foreign_key_errors': len(foreign_key_check),
            'connection_ok': True
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'connection_ok': False
        }


def test_connection():
    """
    연결 테스트 함수 (개발/디버깅용)
    
    Returns:
        bool: 연결 성공 여부
    """
    try:
        conn = init_database()
        health = check_database_health(conn)
        table_info = get_table_info(conn)
        
        print("=== 데이터베이스 연결 테스트 ===")
        print(f"상태: {health['status']}")
        print(f"연결: {'OK' if health['connection_ok'] else 'FAIL'}")
        print("\n=== 테이블 정보 ===")
        for table, info in table_info.items():
            print(f"{table}: {info['record_count']}개 레코드, {info['column_count']}개 컬럼")
        
        return health['connection_ok']
        
    except Exception as e:
        print(f"연결 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    # 직접 실행 시 테스트
    test_connection()
