"""
database 패키지 초기화

CRM 애플리케이션의 데이터베이스 관련 모듈들
"""

from .connection import (
    init_database,
    generate_company_code,
    parse_revenue,
    get_table_info,
    check_database_health,
    test_connection
)

__all__ = [
    'init_database',
    'generate_company_code', 
    'parse_revenue',
    'get_table_info',
    'check_database_health',
    'test_connection'
]
