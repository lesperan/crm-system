import streamlit as st
import pandas as pd
import sqlite3
import io
import uuid
from datetime import datetime
import re

# 페이지 설정
st.set_page_config(
    page_title="기업 상담 관리 시스템",
    page_icon="🏢",
    layout="wide"
)

# 데이터베이스 연결 및 테이블 생성
@st.cache_resource
def init_database():
    conn = sqlite3.connect('crm_database.db', check_same_thread=False)
    
    # 테이블 생성
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
    
    conn.commit()
    return conn

# 업체코드 자동 생성 함수
def generate_company_code():
    """외감기업이 아닌 경우 자동으로 업체코드 생성"""
    return f"AUTO{str(uuid.uuid4())[:8].upper()}"

# 기업명으로 업체코드 찾기
def find_company_code(conn, company_name):
    """기업명으로 업체코드를 찾고, 없으면 새로 생성"""
    cursor = conn.execute("SELECT company_code FROM companies WHERE company_name = ?", (company_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # 새로운 업체코드 생성
        new_code = generate_company_code()
        return new_code

# 매출액 파싱 함수
def parse_revenue(revenue_str):
    """매출액 문자열을 숫자로 변환"""
    if pd.isna(revenue_str) or revenue_str == "":
        return None
    
    # 쉼표 제거하고 숫자만 추출
    revenue_str = str(revenue_str).replace(",", "").replace(" ", "")
    try:
        return float(revenue_str)
    except:
        return None

# 자동완성용 데이터 가져오기 함수들
@st.cache_data(ttl=300)  # 5분간 캐시
def get_company_names():
    """기업명 목록 가져오기"""
    try:
        conn = sqlite3.connect('crm_database.db', check_same_thread=False)
        cursor = conn.execute("SELECT DISTINCT company_name FROM companies WHERE company_name IS NOT NULL ORDER BY company_name")
        result = [row[0] for row in cursor.fetchall()]
        conn.close()
        return result
    except:
        return []

@st.cache_data(ttl=300)  # 5분간 캐시
def get_customer_names():
    """고객명 목록 가져오기"""
    try:
        conn = sqlite3.connect('crm_database.db', check_same_thread=False)
        cursor = conn.execute("SELECT DISTINCT customer_name FROM customer_contacts WHERE customer_name IS NOT NULL ORDER BY customer_name")
        result = [row[0] for row in cursor.fetchall()]
        conn.close()
        return result
    except:
        return []

@st.cache_data(ttl=300)  # 5분간 캐시
def get_industries():
    """업종 목록 가져오기"""
    try:
        conn = sqlite3.connect('crm_database.db', check_same_thread=False)
        cursor = conn.execute("SELECT DISTINCT industry FROM companies WHERE industry IS NOT NULL ORDER BY industry")
        result = [row[0] for row in cursor.fetchall()]
        conn.close()
        return result
    except:
        return []

@st.cache_data(ttl=300)  # 5분간 캐시
def get_positions():
    """직위 목록 가져오기"""
    try:
        conn = sqlite3.connect('crm_database.db', check_same_thread=False)
        cursor = conn.execute("SELECT DISTINCT position FROM customer_contacts WHERE position IS NOT NULL ORDER BY position")
        result = [row[0] for row in cursor.fetchall()]
        conn.close()
        return result
    except:
        return []

# 데이터 업데이트 함수들
def update_company_data(conn, company_code, updated_data):
    """기업 데이터 업데이트"""
    try:
        conn.execute('''
            UPDATE companies SET 
            company_name = ?, revenue_2024 = ?, industry = ?, 
            employee_count = ?, address = ?, products = ?, 
            customer_category = ?, updated_at = CURRENT_TIMESTAMP
            WHERE company_code = ?
        ''', (
            updated_data.get('기업명'),
            parse_revenue(updated_data.get('매출액_2024')),
            updated_data.get('업종'),
            int(updated_data.get('종업원수')) if updated_data.get('종업원수') else None,
            updated_data.get('주소'),
            updated_data.get('상품'),
            updated_data.get('고객구분'),
            company_code
        ))
        conn.commit()
        return True, "기업 정보가 업데이트되었습니다."
    except Exception as e:
        return False, f"업데이트 실패: {str(e)}"

def insert_new_consultation(conn, consultation_data):
    """새로운 상담 이력 추가"""
    try:
        # 기업명으로 기업코드 찾기 또는 생성
        company_name = consultation_data.get('기업명')
        company_code = find_company_code(conn, company_name)
        
        # 기업이 없으면 기본 정보로 생성
        existing_company = conn.execute("SELECT company_code FROM companies WHERE company_code = ?", (company_code,)).fetchone()
        if not existing_company:
            conn.execute('''
                INSERT INTO companies (company_code, company_name)
                VALUES (?, ?)
            ''', (company_code, company_name))
        
        # 상담 이력 추가
        conn.execute('''
            INSERT INTO consultations 
            (company_code, customer_name, consultation_date, consultation_content, project_name)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            company_code,
            consultation_data.get('고객명'),
            consultation_data.get('상담날짜'),
            consultation_data.get('상담내역'),
            consultation_data.get('프로젝트명')
        ))
        conn.commit()
        return True, "새로운 상담 이력이 추가되었습니다."
    except Exception as e:
        return False, f"추가 실패: {str(e)}"

# 데이터베이스 초기화
conn = init_database()

# 사이드바 메뉴
st.sidebar.title("📋 메뉴")
menu = st.sidebar.selectbox(
    "작업을 선택하세요",
    ["기업 목록 관리", "고객 연락처 관리", "상담 이력 관리", "통합 데이터 조회", "데이터 다운로드"]
)

# 메인 타이틀
st.title("🏢 기업 상담 관리 시스템")
st.markdown("---")

# 1. 기업 목록 관리
if menu == "기업 목록 관리":
    st.header("📋 기업 목록 관리")
    
    tab1, tab2 = st.tabs(["엑셀 업로드", "현재 기업 목록"])
    
    with tab1:
        st.subheader("기업 목록 엑셀 업로드")
        uploaded_file = st.file_uploader(
            "기업 목록 엑셀 파일을 업로드하세요",
            type=['xlsx', 'xls'],
            key="company_upload"
        )
        
        if uploaded_file is not None:
            try:
                df = pd.read_excel(uploaded_file)
                st.success("✅ 파일을 성공적으로 읽었습니다!")
                
                # 데이터 미리보기
                st.subheader("업로드된 데이터 미리보기")
                st.dataframe(df, use_container_width=True)
                
                # 컬럼 매핑
                st.subheader("컬럼 매핑")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**필수 매핑**")
                    company_name_col = st.selectbox("기업명 컬럼", df.columns, key="company_name_mapping")
                
                with col2:
                    st.write("**선택 매핑**")
                    revenue_col = st.selectbox("매출액 컬럼", ["선택안함"] + list(df.columns), key="revenue_mapping")
                    industry_col = st.selectbox("업종 컬럼", ["선택안함"] + list(df.columns), key="industry_mapping")
                    employee_col = st.selectbox("종업원수 컬럼", ["선택안함"] + list(df.columns), key="employee_mapping")
                    address_col = st.selectbox("주소 컬럼", ["선택안함"] + list(df.columns), key="address_mapping")
                    products_col = st.selectbox("상품 컬럼", ["선택안함"] + list(df.columns), key="products_mapping")
                    category_col = st.selectbox("고객구분 컬럼", ["선택안함"] + list(df.columns), key="category_mapping")
                
                # 업체코드 처리
                st.subheader("업체코드 설정")
                code_option = st.radio(
                    "업체코드 처리 방식",
                    ["자동 생성", "파일에서 가져오기"]
                )
                
                if code_option == "파일에서 가져오기":
                    code_col = st.selectbox("업체코드 컬럼", df.columns, key="code_mapping")
                
                # 데이터 저장
                if st.button("데이터베이스에 저장", type="primary"):
                    try:
                        success_count = 0
                        update_count = 0
                        
                        for _, row in df.iterrows():
                            company_name = row[company_name_col]
                            if pd.isna(company_name) or company_name == "":
                                continue
                            
                            # 업체코드 결정
                            if code_option == "자동 생성":
                                company_code = find_company_code(conn, company_name)
                                if company_code.startswith("AUTO"):
                                    # 새로운 기업인 경우
                                    company_code = generate_company_code()
                            else:
                                company_code = row[code_col] if not pd.isna(row[code_col]) else generate_company_code()
                            
                            # 데이터 준비
                            revenue = parse_revenue(row[revenue_col] if revenue_col != "선택안함" else None)
                            industry = row[industry_col] if industry_col != "선택안함" and not pd.isna(row[industry_col]) else None
                            employee_count = int(row[employee_col]) if employee_col != "선택안함" and not pd.isna(row[employee_col]) else None
                            address = row[address_col] if address_col != "선택안함" and not pd.isna(row[address_col]) else None
                            products = row[products_col] if products_col != "선택안함" and not pd.isna(row[products_col]) else None
                            customer_category = row[category_col] if category_col != "선택안함" and not pd.isna(row[category_col]) else None
                            
                            # 기존 데이터 확인
                            existing = conn.execute("SELECT company_code FROM companies WHERE company_code = ?", (company_code,)).fetchone()
                            
                            if existing:
                                # 업데이트
                                conn.execute('''
                                    UPDATE companies SET 
                                    company_name = ?, revenue_2024 = ?, industry = ?, 
                                    employee_count = ?, address = ?, products = ?, 
                                    customer_category = ?, updated_at = CURRENT_TIMESTAMP
                                    WHERE company_code = ?
                                ''', (company_name, revenue, industry, employee_count, address, products, customer_category, company_code))
                                update_count += 1
                            else:
                                # 신규 삽입
                                conn.execute('''
                                    INSERT INTO companies 
                                    (company_code, company_name, revenue_2024, industry, employee_count, address, products, customer_category)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (company_code, company_name, revenue, industry, employee_count, address, products, customer_category))
                                success_count += 1
                        
                        conn.commit()
                        st.success(f"✅ 처리 완료! 신규 저장: {success_count}개, 업데이트: {update_count}개")
                        
                        # 캐시 클리어
                        get_company_names.clear()
                        get_industries.clear()
                        
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {str(e)}")
                        
            except Exception as e:
                st.error(f"파일 읽기 오류: {str(e)}")
    
    with tab2:
        st.subheader("현재 저장된 기업 목록")
        
        # 데이터 조회
        companies_df = pd.read_sql_query("SELECT * FROM companies ORDER BY updated_at DESC", conn)
        
        if not companies_df.empty:
            st.dataframe(companies_df, use_container_width=True)
            
            # 통계 정보
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 기업 수", len(companies_df))
            with col2:
                if 'industry' in companies_df.columns:
                    st.metric("업종 수", companies_df['industry'].nunique())
            with col3:
                if 'revenue_2024' in companies_df.columns:
                    avg_revenue = companies_df['revenue_2024'].mean()
                    st.metric("평균 매출액", f"{avg_revenue:,.0f}" if not pd.isna(avg_revenue) else "N/A")
        else:
            st.info("저장된 기업 목록이 없습니다.")

# 2. 고객 연락처 관리
elif menu == "고객 연락처 관리":
    st.header("👥 고객 연락처 관리")
    
    tab1, tab2 = st.tabs(["엑셀 업로드", "현재 연락처 목록"])
    
    with tab1:
        st.subheader("고객 연락처 엑셀 업로드")
        contact_file = st.file_uploader(
            "고객 연락처 엑셀 파일을 업로드하세요",
            type=['xlsx', 'xls'],
            key="contact_upload"
        )
        
        if contact_file is not None:
            try:
                df = pd.read_excel(contact_file)
                st.success("✅ 파일을 성공적으로 읽었습니다!")
                
                st.subheader("업로드된 데이터 미리보기")
                st.dataframe(df, use_container_width=True)
                
                # 컬럼 매핑
                st.subheader("컬럼 매핑")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**필수 매핑**")
                    company_name_col = st.selectbox("기업명 컬럼", df.columns, key="contact_company_mapping")
                    customer_name_col = st.selectbox("고객명 컬럼", df.columns, key="contact_customer_mapping")
                
                with col2:
                    st.write("**선택 매핑**")
                    position_col = st.selectbox("직위 컬럼", ["선택안함"] + list(df.columns), key="position_mapping")
                    phone_col = st.selectbox("전화 컬럼", ["선택안함"] + list(df.columns), key="phone_mapping")
                    email_col = st.selectbox("이메일 컬럼", ["선택안함"] + list(df.columns), key="email_mapping")
                    path_col = st.selectbox("획득경로 컬럼", ["선택안함"] + list(df.columns), key="path_mapping")
                
                if st.button("연락처 저장", type="primary"):
                    try:
                        success_count = 0
                        for _, row in df.iterrows():
                            company_name = row[company_name_col]
                            customer_name = row[customer_name_col]
                            
                            if pd.isna(company_name) or pd.isna(customer_name) or company_name == "" or customer_name == "":
                                continue
                            
                            # 기업 코드 찾기
                            company_code = find_company_code(conn, company_name)
                            
                            # 기업이 없으면 기본 정보로 생성
                            existing_company = conn.execute("SELECT company_code FROM companies WHERE company_code = ?", (company_code,)).fetchone()
                            if not existing_company:
                                conn.execute('''
                                    INSERT INTO companies (company_code, company_name)
                                    VALUES (?, ?)
                                ''', (company_code, company_name))
                            
                            # 연락처 정보 준비
                            position = row[position_col] if position_col != "선택안함" and not pd.isna(row[position_col]) else None
                            phone = row[phone_col] if phone_col != "선택안함" and not pd.isna(row[phone_col]) else None
                            email = row[email_col] if email_col != "선택안함" and not pd.isna(row[email_col]) else None
                            acquisition_path = row[path_col] if path_col != "선택안함" and not pd.isna(row[path_col]) else None
                            
                            # 연락처 저장
                            conn.execute('''
                                INSERT INTO customer_contacts 
                                (company_code, customer_name, position, phone, email, acquisition_path)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (company_code, customer_name, position, phone, email, acquisition_path))
                            success_count += 1
                        
                        conn.commit()
                        st.success(f"✅ {success_count}개의 연락처를 저장했습니다!")
                        
                        # 캐시 클리어
                        get_customer_names.clear()
                        get_positions.clear()
                        
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {str(e)}")
                        
            except Exception as e:
                st.error(f"파일 읽기 오류: {str(e)}")
    
    with tab2:
        st.subheader("현재 저장된 연락처 목록")
        
        contacts_df = pd.read_sql_query('''
            SELECT cc.*, c.company_name 
            FROM customer_contacts cc
            JOIN companies c ON cc.company_code = c.company_code
            ORDER BY cc.updated_at DESC
        ''', conn)
        
        if not contacts_df.empty:
            st.dataframe(contacts_df, use_container_width=True)
            st.metric("총 연락처 수", len(contacts_df))
        else:
            st.info("저장된 연락처가 없습니다.")

# 3. 상담 이력 관리
elif menu == "상담 이력 관리":
    st.header("📞 상담 이력 관리")
    
    tab1, tab2, tab3 = st.tabs(["엑셀 업로드", "직접 입력", "상담 이력 조회"])
    
    with tab1:
        st.subheader("상담 이력 엑셀 업로드")
        consultation_file = st.file_uploader(
            "상담 이력 엑셀 파일을 업로드하세요",
            type=['xlsx', 'xls'],
            key="consultation_upload"
        )
        
        if consultation_file is not None:
            try:
                df = pd.read_excel(consultation_file)
                st.success("✅ 파일을 성공적으로 읽었습니다!")
                
                st.subheader("업로드된 데이터 미리보기")
                st.dataframe(df, use_container_width=True)
                
                # 컬럼 매핑
                st.subheader("컬럼 매핑")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**필수 매핑**")
                    company_name_col = st.selectbox("기업명 컬럼", df.columns, key="consult_company_mapping")
                    content_col = st.selectbox("상담내역 컬럼", df.columns, key="consult_content_mapping")
                
                with col2:
                    st.write("**선택 매핑**")
                    customer_col = st.selectbox("고객명 컬럼", ["선택안함"] + list(df.columns), key="consult_customer_mapping")
                    date_col = st.selectbox("날짜 컬럼", ["선택안함"] + list(df.columns), key="consult_date_mapping")
                    project_col = st.selectbox("프로젝트 컬럼", ["선택안함"] + list(df.columns), key="consult_project_mapping")
                
                if st.button("상담 이력 저장", type="primary"):
                    try:
                        success_count = 0
                        for _, row in df.iterrows():
                            company_name = row[company_name_col]
                            consultation_content = row[content_col]
                            
                            if pd.isna(company_name) or pd.isna(consultation_content) or company_name == "" or consultation_content == "":
                                continue
                            
                            # 기업 코드 찾기
                            company_code = find_company_code(conn, company_name)
                            
                            # 기업이 없으면 기본 정보로 생성
                            existing_company = conn.execute("SELECT company_code FROM companies WHERE company_code = ?", (company_code,)).fetchone()
                            if not existing_company:
                                conn.execute('''
                                    INSERT INTO companies (company_code, company_name)
                                    VALUES (?, ?)
                                ''', (company_code, company_name))
                            
                            # 상담 정보 준비
                            customer_name = row[customer_col] if customer_col != "선택안함" and not pd.isna(row[customer_col]) else None
                            consultation_date = row[date_col] if date_col != "선택안함" and not pd.isna(row[date_col]) else None
                            project_name = row[project_col] if project_col != "선택안함" and not pd.isna(row[project_col]) else None
                            
                            # 상담 이력 저장
                            conn.execute('''
                                INSERT INTO consultations 
                                (company_code, customer_name, consultation_date, consultation_content, project_name)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (company_code, customer_name, consultation_date, consultation_content, project_name))
                            success_count += 1
                        
                        conn.commit()
                        st.success(f"✅ {success_count}개의 상담 이력을 저장했습니다!")
                        
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {str(e)}")
                        
            except Exception as e:
                st.error(f"파일 읽기 오류: {str(e)}")
    
    with tab2:
        st.subheader("상담 이력 직접 입력")
        
        # 기업명 자동완성
        company_names = get_company_names()
        
        if company_names:
            # 기업명 선택 (자동완성 지원)
            selected_company_name = st.selectbox(
                "기업 선택 (또는 새 기업명 입력)",
                ["새 기업명 입력"] + company_names,
                key="direct_company_select"
            )
            
            if selected_company_name == "새 기업명 입력":
                company_name_input = st.text_input("새 기업명을 입력하세요")
                final_company_name = company_name_input
            else:
                final_company_name = selected_company_name
                st.write(f"선택된 기업: **{selected_company_name}**")
            
            # 고객명 자동완성
            customer_names = get_customer_names()
            selected_customer_name = st.selectbox(
                "고객 선택 (또는 새 고객명 입력)",
                ["새 고객명 입력"] + customer_names,
                key="direct_customer_select"
            )
            
            if selected_customer_name == "새 고객명 입력":
                customer_name_input = st.text_input("새 고객명을 입력하세요")
                final_customer_name = customer_name_input
            else:
                final_customer_name = selected_customer_name
            
            # 입력 폼
            col1, col2 = st.columns(2)
            
            with col1:
                consultation_date = st.date_input("상담 날짜")
                project_name = st.text_input("프로젝트명")
            
            with col2:
                consultation_content = st.text_area("상담 내역 (필수)", height=200)
            
            if st.button("상담 이력 저장", key="direct_save"):
                if consultation_content.strip() and final_company_name.strip():
                    consultation_data = {
                        '기업명': final_company_name,
                        '고객명': final_customer_name if final_customer_name else None,
                        '상담날짜': consultation_date.strftime("%Y.%m.%d"),
                        '상담내역': consultation_content,
                        '프로젝트명': project_name if project_name else None
                    }
                    
                    success, message = insert_new_consultation(conn, consultation_data)
                    if success:
                        st.success(message)
                        # 캐시 클리어
                        get_company_names.clear()
                        get_customer_names.clear()
                    else:
                        st.error(message)
                else:
                    st.error("기업명과 상담 내역은 필수 입력 사항입니다.")
        else:
            st.warning("먼저 기업 목록을 등록해주세요.")
    
    with tab3:
        st.subheader("상담 이력 조회")
        
        consultations_df = pd.read_sql_query('''
            SELECT c.company_name, con.customer_name, con.consultation_date, 
                   con.consultation_content, con.project_name, con.created_at
            FROM consultations con
            JOIN companies c ON con.company_code = c.company_code
            ORDER BY con.consultation_date DESC, con.created_at DESC
        ''', conn)
        
        if not consultations_df.empty:
            st.dataframe(consultations_df, use_container_width=True)
            st.metric("총 상담 건수", len(consultations_df))
        else:
            st.info("저장된 상담 이력이 없습니다.")

# 4. 통합 데이터 조회 (편집 가능한 그리드)
elif menu == "통합 데이터 조회":
    st.header("📊 통합 데이터 조회 및 편집")
    
    # 편집 모드 선택
    edit_mode = st.radio(
        "모드 선택",
        ["조회만", "편집 모드", "새 상담 추가"],
        horizontal=True
    )
    
    if edit_mode == "조회만":
        # 기존 조회 기능
        st.subheader("통합 데이터 조회")
        
        # 통합 데이터 쿼리
        integrated_df = pd.read_sql_query('''
            SELECT 
                c.company_name as 기업명,
                c.revenue_2024 as 매출액_2024,
                c.industry as 업종,
                c.employee_count as 종업원수,
                c.address as 주소,
                c.products as 상품,
                c.customer_category as 고객구분,
                cc.customer_name as 고객명,
                cc.position as 직위,
                cc.phone as 전화,
                cc.email as 이메일,
                cc.acquisition_path as 획득경로,
                con.consultation_date as 상담날짜,
                con.consultation_content as 상담내역,
                con.project_name as 프로젝트명
            FROM companies c
            LEFT JOIN customer_contacts cc ON c.company_code = cc.company_code
            LEFT JOIN consultations con ON c.company_code = con.company_code
            ORDER BY c.company_name, con.consultation_date DESC
        ''', conn)
        
        if not integrated_df.empty:
            st.dataframe(integrated_df, use_container_width=True)
            
            # 요약 통계
            st.subheader("요약 통계")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("총 기업 수", integrated_df['기업명'].nunique())
            with col2:
                st.metric("총 연락처 수", integrated_df['고객명'].count())
            with col3:
                st.metric("총 상담 건수", integrated_df['상담내역'].count())
            with col4:
                if not integrated_df['매출액_2024'].isna().all():
                    avg_revenue = integrated_df['매출액_2024'].mean()
                    st.metric("평균 매출액", f"{avg_revenue:,.0f}" if not pd.isna(avg_revenue) else "N/A")
        else:
            st.info("통합할 데이터가 없습니다.")
    
    elif edit_mode == "편집 모드":
        st.subheader("📝 기업 정보 편집")
        st.info("💡 **기업 정보만 편집 가능합니다.** 연락처와 상담 이력은 각각의 메뉴에서 관리하세요.")
        
        # 기업 데이터만 조회 (편집용)
        companies_df = pd.read_sql_query('''
            SELECT 
                company_code as 업체코드,
                company_name as 기업명,
                revenue_2024 as 매출액_2024,
                industry as 업종,
                employee_count as 종업원수,
                address as 주소,
                products as 상품,
                customer_category as 고객구분
            FROM companies 
            ORDER BY company_name
        ''', conn)
        
        if not companies_df.empty:
            # 자동완성 데이터 준비
            company_names = get_company_names()
            industries = get_industries()
            
            # 컬럼 설정 (편집 가능한 컬럼 지정)
            column_config = {
                "업체코드": st.column_config.TextColumn(
                    "업체코드",
                    disabled=True,  # 업체코드는 편집 불가
                    help="자동 생성된 업체코드 (편집 불가)"
                ),
                "기업명": st.column_config.TextColumn(
                    "기업명",
                    required=True,
                    help="기업명을 입력하세요"
                ),
                "매출액_2024": st.column_config.NumberColumn(
                    "매출액 (2024)",
                    format="%.0f",
                    help="2024년 매출액 (단위: 원)"
                ),
                "업종": st.column_config.SelectboxColumn(
                    "업종",
                    options=[""] + industries,
                    help="업종을 선택하거나 새로 입력하세요"
                ),
                "종업원수": st.column_config.NumberColumn(
                    "종업원수",
                    format="%.0f",
                    min_value=0,
                    help="전체 종업원 수"
                ),
                "주소": st.column_config.TextColumn(
                    "주소",
                    help="기업 주소"
                ),
                "상품": st.column_config.TextColumn(
                    "상품/서비스",
                    help="주요 상품이나 서비스"
                ),
                "고객구분": st.column_config.SelectboxColumn(
                    "고객구분",
                    options=["", "신규", "기존", "잠재", "VIP"],
                    help="고객 구분"
                )
            }
            
            # 편집 가능한 데이터 에디터
            edited_df = st.data_editor(
                companies_df,
                column_config=column_config,
                use_container_width=True,
                num_rows="dynamic",  # 행 추가/삭제 가능
                key="companies_editor"
            )
            
            # 변경사항 저장
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("💾 변경사항 저장", type="primary"):
                    try:
                        changes_count = 0
                        errors = []
                        
                        # 원본과 편집된 데이터 비교
                        for idx, (original_row, edited_row) in enumerate(zip(companies_df.itertuples(), edited_df.itertuples())):
                            # 변경사항이 있는지 확인
                            if not original_row[1:] == edited_row[1:]:  # 인덱스 제외하고 비교
                                company_code = edited_row.업체코드
                                
                                # 필수 필드 검증
                                if not edited_row.기업명 or edited_row.기업명.strip() == "":
                                    errors.append(f"행 {idx+1}: 기업명은 필수입니다.")
                                    continue
                                
                                # 업데이트 실행
                                updated_data = {
                                    '기업명': edited_row.기업명,
                                    '매출액_2024': edited_row.매출액_2024,
                                    '업종': edited_row.업종,
                                    '종업원수': edited_row.종업원수,
                                    '주소': edited_row.주소,
                                    '상품': edited_row.상품,
                                    '고객구분': edited_row.고객구분
                                }
                                
                                success, message = update_company_data(conn, company_code, updated_data)
                                if success:
                                    changes_count += 1
                                else:
                                    errors.append(f"행 {idx+1}: {message}")
                        
                        # 새로 추가된 행 처리
                        if len(edited_df) > len(companies_df):
                            for idx in range(len(companies_df), len(edited_df)):
                                new_row = edited_df.iloc[idx]
                                
                                if new_row['기업명'] and new_row['기업명'].strip():
                                    try:
                                        # 새 업체코드 생성
                                        new_company_code = generate_company_code()
                                        
                                        # 새 기업 추가
                                        conn.execute('''
                                            INSERT INTO companies 
                                            (company_code, company_name, revenue_2024, industry, employee_count, address, products, customer_category)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                        ''', (
                                            new_company_code,
                                            new_row['기업명'],
                                            parse_revenue(new_row['매출액_2024']),
                                            new_row['업종'] if new_row['업종'] else None,
                                            int(new_row['종업원수']) if new_row['종업원수'] else None,
                                            new_row['주소'] if new_row['주소'] else None,
                                            new_row['상품'] if new_row['상품'] else None,
                                            new_row['고객구분'] if new_row['고객구분'] else None
                                        ))
                                        changes_count += 1
                                    except Exception as e:
                                        errors.append(f"새 행 {idx+1}: 추가 실패 - {str(e)}")
                        
                        conn.commit()
                        
                        # 결과 표시
                        if changes_count > 0:
                            st.success(f"✅ {changes_count}개의 변경사항이 저장되었습니다!")
                            # 캐시 클리어
                            get_company_names.clear()
                            get_industries.clear()
                            st.rerun()
                        
                        if errors:
                            st.error("❌ 다음 오류가 발생했습니다:")
                            for error in errors:
                                st.write(f"- {error}")
                        
                        if changes_count == 0 and not errors:
                            st.info("변경사항이 없습니다.")
                            
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {str(e)}")
            
            with col2:
                if st.button("🔄 새로고침"):
                    st.rerun()
            
            with col3:
                st.write("**사용법:** 셀을 클릭하여 직접 편집하거나, 맨 아래 행에서 새 기업을 추가할 수 있습니다.")
        
        else:
            st.info("편집할 기업 데이터가 없습니다. 먼저 기업 목록을 추가해주세요.")
    
    elif edit_mode == "새 상담 추가":
        st.subheader("📞 새 상담 이력 추가")
        st.info("💡 **빠른 상담 이력 추가:** 기존 고객사와 담당자 정보를 활용하여 신속하게 상담 이력을 추가할 수 있습니다.")
        
        # 자동완성 데이터 준비
        company_names = get_company_names()
        customer_names = get_customer_names()
        
        if company_names:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**기업 정보**")
                
                # 기업명 자동완성
                company_option = st.selectbox(
                    "기업명",
                    ["새 기업명 입력"] + company_names,
                    key="quick_company_select"
                )
                
                if company_option == "새 기업명 입력":
                    company_name_input = st.text_input("새 기업명", key="quick_new_company")
                    final_company_name = company_name_input
                else:
                    final_company_name = company_option
                
                # 고객명 자동완성
                customer_option = st.selectbox(
                    "고객명",
                    ["새 고객명 입력"] + customer_names,
                    key="quick_customer_select"
                )
                
                if customer_option == "새 고객명 입력":
                    customer_name_input = st.text_input("새 고객명", key="quick_new_customer")
                    final_customer_name = customer_name_input
                else:
                    final_customer_name = customer_option
                
                # 상담 날짜
                consultation_date = st.date_input("상담 날짜", key="quick_date")
                
                # 프로젝트명
                project_name = st.text_input("프로젝트명 (선택)", key="quick_project")
            
            with col2:
                st.write("**상담 내용**")
                
                # 상담 내역 (큰 텍스트 영역)
                consultation_content = st.text_area(
                    "상담 내역 (필수)",
                    height=300,
                    placeholder="상담 내용을 자세히 입력하세요...",
                    key="quick_content"
                )
            
            # 저장 버튼
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("💾 상담 이력 저장", type="primary"):
                    if consultation_content.strip() and final_company_name.strip():
                        consultation_data = {
                            '기업명': final_company_name,
                            '고객명': final_customer_name if final_customer_name else None,
                            '상담날짜': consultation_date.strftime("%Y.%m.%d"),
                            '상담내역': consultation_content,
                            '프로젝트명': project_name if project_name else None
                        }
                        
                        success, message = insert_new_consultation(conn, consultation_data)
                        if success:
                            st.success(message)
                            # 입력 필드 초기화를 위한 rerun
                            st.rerun()
                        else:
                            st.error(message)
                    else:
                        st.error("기업명과 상담 내역은 필수 입력 사항입니다.")
            
            with col2:
                if st.button("🔄 입력 초기화"):
                    st.rerun()
            
            with col3:
                st.write("**💡 팁:** 기존에 등록된 기업명이나 고객명을 선택하면 자동으로 연결됩니다.")
        
        else:
            st.warning("⚠️ 등록된 기업이 없습니다. 먼저 '기업 목록 관리'에서 기업을 등록해주세요.")
    
    # 최근 상담 이력 표시 (모든 모드에서 공통)
    if edit_mode in ["편집 모드", "새 상담 추가"]:
        st.markdown("---")
        st.subheader("📋 최근 상담 이력 (최근 10건)")
        
        recent_consultations = pd.read_sql_query('''
            SELECT 
                c.company_name as 기업명,
                con.customer_name as 고객명,
                con.consultation_date as 상담날짜,
                con.consultation_content as 상담내역,
                con.project_name as 프로젝트명,
                con.created_at as 등록일시
            FROM consultations con
            JOIN companies c ON con.company_code = c.company_code
            ORDER BY con.created_at DESC
            LIMIT 10
        ''', conn)
        
        if not recent_consultations.empty:
            st.dataframe(recent_consultations, use_container_width=True)
        else:
            st.info("최근 상담 이력이 없습니다.")
