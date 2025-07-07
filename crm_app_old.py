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
        
        # 기업 선택
        companies_list = pd.read_sql_query("SELECT company_code, company_name FROM companies ORDER BY company_name", conn)
        
        if not companies_list.empty:
            company_options = {f"{row['company_name']} ({row['company_code']})": row['company_code'] 
                             for _, row in companies_list.iterrows()}
            
            selected_company = st.selectbox("기업 선택", list(company_options.keys()))
            selected_company_code = company_options[selected_company]
            
            # 입력 폼
            col1, col2 = st.columns(2)
            
            with col1:
                customer_name = st.text_input("고객명")
                consultation_date = st.date_input("상담 날짜")
                project_name = st.text_input("프로젝트명")
            
            with col2:
                consultation_content = st.text_area("상담 내역 (필수)", height=200)
            
            if st.button("상담 이력 저장"):
                if consultation_content.strip():
                    try:
                        conn.execute('''
                            INSERT INTO consultations 
                            (company_code, customer_name, consultation_date, consultation_content, project_name)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (selected_company_code, customer_name or None, 
                              consultation_date.strftime("%Y.%m.%d"), 
                              consultation_content, project_name or None))
                        
                        conn.commit()
                        st.success("✅ 상담 이력이 저장되었습니다!")
                        
                    except Exception as e:
                        st.error(f"저장 중 오류 발생: {str(e)}")
                else:
                    st.error("상담 내역은 필수 입력 사항입니다.")
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

# 4. 통합 데이터 조회
elif menu == "통합 데이터 조회":
    st.header("📊 통합 데이터 조회")
    
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
        st.subheader("통합 데이터")
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

# 5. 데이터 다운로드
elif menu == "데이터 다운로드":
    st.header("💾 데이터 다운로드")
    
    # 다운로드 옵션
    download_option = st.selectbox(
        "다운로드할 데이터 선택",
        ["통합 데이터", "기업 목록", "고객 연락처", "상담 이력"]
    )
    
    # 엑셀 파일 생성 함수
    def create_excel_file(dataframes_dict):
        """여러 시트를 가진 엑셀 파일 생성"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet_name, df in dataframes_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 워크시트 포맷팅
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                
                # 헤더 포맷
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # 헤더 적용
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # 열 너비 자동 조정
                for i, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).str.len().max(),
                        len(str(col))
                    )
                    worksheet.set_column(i, i, min(max_length + 2, 50))
        
        return output.getvalue()
    
    if download_option == "통합 데이터":
        st.subheader("📊 통합 데이터 다운로드")
        
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
            st.dataframe(integrated_df.head(), use_container_width=True)
            st.info(f"총 {len(integrated_df)}개의 레코드가 있습니다.")
            
            # 다운로드 버튼
            excel_data = create_excel_file({"통합데이터": integrated_df})
            
            st.download_button(
                label="📥 통합 데이터 엑셀 다운로드",
                data=excel_data,
                file_name=f"통합데이터_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("다운로드할 데이터가 없습니다.")
    
    elif download_option == "기업 목록":
        st.subheader("🏢 기업 목록 다운로드")
        
        companies_df = pd.read_sql_query('''
            SELECT 
                company_name as 기업명,
                company_code as 업체코드,
                revenue_2024 as 매출액_2024,
                industry as 업종,
                employee_count as 종업원수,
                address as 주소,
                products as 상품,
                customer_category as 고객구분,
                created_at as 등록일,
                updated_at as 수정일
            FROM companies 
            ORDER BY company_name
        ''', conn)
        
        if not companies_df.empty:
            st.dataframe(companies_df.head(), use_container_width=True)
            st.info(f"총 {len(companies_df)}개의 기업이 있습니다.")
            
            excel_data = create_excel_file({"기업목록": companies_df})
            
            st.download_button(
                label="📥 기업 목록 엑셀 다운로드",
                data=excel_data,
                file_name=f"기업목록_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("다운로드할 기업 목록이 없습니다.")
    
    elif download_option == "고객 연락처":
        st.subheader("👥 고객 연락처 다운로드")
        
        contacts_df = pd.read_sql_query('''
            SELECT 
                c.company_name as 기업명,
                c.company_code as 업체코드,
                cc.customer_name as 고객명,
                cc.position as 직위,
                cc.phone as 전화,
                cc.email as 이메일,
                cc.acquisition_path as 획득경로,
                cc.created_at as 등록일,
                cc.updated_at as 수정일
            FROM customer_contacts cc
            JOIN companies c ON cc.company_code = c.company_code
            ORDER BY c.company_name, cc.customer_name
        ''', conn)
        
        if not contacts_df.empty:
            st.dataframe(contacts_df.head(), use_container_width=True)
            st.info(f"총 {len(contacts_df)}개의 연락처가 있습니다.")
            
            excel_data = create_excel_file({"고객연락처": contacts_df})
            
            st.download_button(
                label="📥 고객 연락처 엑셀 다운로드",
                data=excel_data,
                file_name=f"고객연락처_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("다운로드할 연락처가 없습니다.")
    
    elif download_option == "상담 이력":
        st.subheader("📞 상담 이력 다운로드")
        
        consultations_df = pd.read_sql_query('''
            SELECT 
                c.company_name as 기업명,
                c.company_code as 업체코드,
                con.customer_name as 고객명,
                con.consultation_date as 상담날짜,
                con.consultation_content as 상담내역,
                con.project_name as 프로젝트명,
                con.created_at as 등록일,
                con.updated_at as 수정일
            FROM consultations con
            JOIN companies c ON con.company_code = c.company_code
            ORDER BY con.consultation_date DESC, c.company_name
        ''', conn)
        
        if not consultations_df.empty:
            st.dataframe(consultations_df.head(), use_container_width=True)
            st.info(f"총 {len(consultations_df)}개의 상담 이력이 있습니다.")
            
            excel_data = create_excel_file({"상담이력": consultations_df})
            
            st.download_button(
                label="📥 상담 이력 엑셀 다운로드",
                data=excel_data,
                file_name=f"상담이력_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("다운로드할 상담 이력이 없습니다.")
    
    # 전체 데이터 백업
    st.markdown("---")
    st.subheader("💾 전체 데이터 백업")
    st.write("모든 데이터를 하나의 엑셀 파일로 다운로드합니다.")
    
    if st.button("전체 데이터 백업 다운로드"):
        # 모든 테이블 데이터 가져오기
        companies_df = pd.read_sql_query("SELECT * FROM companies", conn)
        contacts_df = pd.read_sql_query("SELECT * FROM customer_contacts", conn)
        consultations_df = pd.read_sql_query("SELECT * FROM consultations", conn)
        
        # 통합 데이터도 포함
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
        
        # 다중 시트 엑셀 파일 생성
        backup_data = {
            "통합데이터": integrated_df,
            "기업목록": companies_df,
            "고객연락처": contacts_df,
            "상담이력": consultations_df
        }
        
        excel_backup = create_excel_file(backup_data)
        
        st.download_button(
            label="📥 전체 데이터 백업 다운로드",
            data=excel_backup,
            file_name=f"CRM_전체백업_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# 사이드바에 시스템 정보 표시
st.sidebar.markdown("---")
st.sidebar.subheader("📈 시스템 현황")

try:
    # 현재 데이터 통계
    companies_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    contacts_count = conn.execute("SELECT COUNT(*) FROM customer_contacts").fetchone()[0]
    consultations_count = conn.execute("SELECT COUNT(*) FROM consultations").fetchone()[0]
    
    st.sidebar.metric("등록된 기업 수", companies_count)
    st.sidebar.metric("등록된 연락처 수", contacts_count)
    st.sidebar.metric("등록된 상담 건수", consultations_count)
    
    # 데이터베이스 파일 정보
    db_size = os.path.getsize('crm_database.db') if os.path.exists('crm_database.db') else 0
    st.sidebar.metric("DB 파일 크기", f"{db_size / 1024:.1f} KB")
    
except Exception as e:
    st.sidebar.error("시스템 정보를 불러올 수 없습니다.")

st.sidebar.markdown("---")
st.sidebar.info("""
**사용법:**
1. 기업 목록을 먼저 업로드하세요
2. 고객 연락처와 상담 이력을 추가하세요
3. 통합 데이터에서 전체 현황을 확인하세요
4. 필요한 데이터를 엑셀로 다운로드하세요
""")

# 메인 실행부
if __name__ == "__main__":
    # 애플리케이션 시작 시 초기화
    st.markdown("""
    <style>
    .main-header {
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-message {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .warning-message {
        background: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 환영 메시지 (처음 접속 시에만 표시)
    if 'first_visit' not in st.session_state:
        st.session_state.first_visit = True
        st.balloons()
        st.success("""
        🎉 **기업 상담 관리 시스템에 오신 것을 환영합니다!**
        
        이 시스템으로 다음 작업을 수행할 수 있습니다:
        - 📋 기업 목록 관리 (엑셀 업로드/수정)
        - 👥 고객 연락처 관리
        - 📞 상담 이력 기록 및 관리
        - 📊 통합 데이터 조회 및 분석
        - 💾 데이터 백업 및 엑셀 다운로드
        
        **시작하려면 왼쪽 메뉴에서 원하는 기능을 선택하세요!**
        """)

# 추가 유틸리티 함수들

def validate_excel_columns(df, required_columns, optional_columns=None):
    """
    엑셀 파일의 컬럼 유효성 검사
    """
    missing_required = []
    for col in required_columns:
        if col not in df.columns:
            missing_required.append(col)
    
    if missing_required:
        return False, f"필수 컬럼이 없습니다: {', '.join(missing_required)}"
    
    return True, "유효한 파일입니다."

def clean_phone_number(phone):
    """
    전화번호 형식 정리
    """
    if pd.isna(phone) or phone == "":
        return None
    
    # 숫자만 추출
    phone = re.sub(r'[^\d]', '', str(phone))
    
    # 형식 적용
    if len(phone) == 11 and phone.startswith('010'):
        return f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
    elif len(phone) == 10:
        return f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
    else:
        return phone

def validate_email(email):
    """
    이메일 형식 유효성 검사
    """
    if pd.isna(email) or email == "":
        return True  # 빈 값은 허용
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, str(email)) is not None

def backup_database():
    """
    데이터베이스 백업 함수
    """
    import shutil
    from datetime import datetime
    
    try:
        backup_filename = f"crm_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2('crm_database.db', backup_filename)
        return True, backup_filename
    except Exception as e:
        return False, str(e)

def restore_database(backup_file):
    """
    데이터베이스 복원 함수
    """
    import shutil
    
    try:
        shutil.copy2(backup_file, 'crm_database.db')
        return True, "데이터베이스가 성공적으로 복원되었습니다."
    except Exception as e:
        return False, f"복원 중 오류 발생: {str(e)}"

# 데이터 검증 함수들
def validate_company_data(row):
    """
    기업 데이터 유효성 검사
    """
    errors = []
    
    # 기업명 필수 체크
    if pd.isna(row.get('기업명')) or row.get('기업명') == "":
        errors.append("기업명은 필수입니다.")
    
    # 매출액 숫자 체크
    if row.get('매출액') is not None:
        try:
            float(str(row.get('매출액')).replace(',', ''))
        except:
            errors.append("매출액은 숫자여야 합니다.")
    
    # 종업원수 숫자 체크
    if row.get('종업원수') is not None:
        try:
            int(row.get('종업원수'))
        except:
            errors.append("종업원수는 정수여야 합니다.")
    
    return len(errors) == 0, errors

def validate_contact_data(row):
    """
    연락처 데이터 유효성 검사
    """
    errors = []
    
    # 고객명 필수 체크
    if pd.isna(row.get('고객명')) or row.get('고객명') == "":
        errors.append("고객명은 필수입니다.")
    
    # 이메일 형식 체크
    if row.get('이메일') and not validate_email(row.get('이메일')):
        errors.append("올바른 이메일 형식이 아닙니다.")
    
    return len(errors) == 0, errors

def validate_consultation_data(row):
    """
    상담 데이터 유효성 검사
    """
    errors = []
    
    # 상담내역 필수 체크
    if pd.isna(row.get('상담내역')) or row.get('상담내역') == "":
        errors.append("상담내역은 필수입니다.")
    
    # 날짜 형식 체크
    if row.get('날짜'):
        date_str = str(row.get('날짜'))
        if not re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_str):
            errors.append("날짜는 YYYY.MM.DD 형식이어야 합니다.")
    
    return len(errors) == 0, errors

# 보고서 생성 함수
def generate_summary_report(conn):
    """
    요약 보고서 생성
    """
    try:
        # 기본 통계
        companies_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        contacts_count = conn.execute("SELECT COUNT(*) FROM customer_contacts").fetchone()[0]
        consultations_count = conn.execute("SELECT COUNT(*) FROM consultations").fetchone()[0]
        
        # 업종별 통계
        industry_stats = pd.read_sql_query('''
            SELECT industry as 업종, COUNT(*) as 기업수
            FROM companies 
            WHERE industry IS NOT NULL AND industry != ''
            GROUP BY industry
            ORDER BY COUNT(*) DESC
        ''', conn)
        
        # 월별 상담 건수
        monthly_consultations = pd.read_sql_query('''
            SELECT 
                substr(consultation_date, 1, 7) as 월,
                COUNT(*) as 상담건수
            FROM consultations 
            WHERE consultation_date IS NOT NULL
            GROUP BY substr(consultation_date, 1, 7)
            ORDER BY 월 DESC
            LIMIT 12
        ''', conn)
        
        # 상위 매출 기업
        top_revenue_companies = pd.read_sql_query('''
            SELECT company_name as 기업명, revenue_2024 as 매출액
            FROM companies 
            WHERE revenue_2024 IS NOT NULL
            ORDER BY revenue_2024 DESC
            LIMIT 10
        ''', conn)
        
        return {
            'basic_stats': {
                'companies': companies_count,
                'contacts': contacts_count,
                'consultations': consultations_count
            },
            'industry_stats': industry_stats,
            'monthly_consultations': monthly_consultations,
            'top_revenue_companies': top_revenue_companies
        }
        
    except Exception as e:
        st.error(f"보고서 생성 중 오류 발생: {str(e)}")
        return None

# 데이터 내보내기 형식들
def export_to_csv(df, filename):
    """
    CSV 형식으로 내보내기
    """
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    return csv_data.encode('utf-8-sig')

def export_to_json(df, filename):
    """
    JSON 형식으로 내보내기
    """
    json_data = df.to_json(orient='records', force_ascii=False, indent=2)
    return json_data.encode('utf-8')

# 시스템 모니터링 함수들
def get_system_status(conn):
    """
    시스템 상태 확인
    """
    try:
        # 데이터베이스 연결 테스트
        conn.execute("SELECT 1").fetchone()
        db_status = "정상"
        
        # 테이블 존재 확인
        tables = ['companies', 'customer_contacts', 'consultations']
        table_status = {}
        
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_status[table] = f"정상 ({count}개 레코드)"
            except:
                table_status[table] = "오류"
        
        # 파일 크기 확인
        import os
        db_size = os.path.getsize('crm_database.db') if os.path.exists('crm_database.db') else 0
        
        return {
            'database': db_status,
            'tables': table_status,
            'file_size': f"{db_size / 1024:.1f} KB"
        }
        
    except Exception as e:
        return {
            'database': f"오류: {str(e)}",
            'tables': {},
            'file_size': "알 수 없음"
        }

# 로그 함수
def log_activity(activity_type, description, user="system"):
    """
    활동 로그 기록 (향후 확장용)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {user}: {activity_type} - {description}"
    
    # 파일에 로그 저장 (선택사항)
    with open('crm_activity.log', 'a', encoding='utf-8') as log_file:
        log_file.write(log_entry + "\n")

# 설정 관리
def load_settings():
    """
    설정 파일 로드
    """
    import json
    try:
        with open('crm_settings.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        # 기본 설정
        default_settings = {
            'date_format': 'YYYY.MM.DD',
            'currency_format': '원',
            'backup_interval': 'daily',
            'max_file_size': 10  # MB
        }
        save_settings(default_settings)
        return default_settings

def save_settings(settings):
    """
    설정 파일 저장
    """
    import json
    with open('crm_settings.json', 'w', encoding='utf-8') as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)

# 애플리케이션 종료 시 정리 작업
import atexit

def cleanup_on_exit():
    """
    애플리케이션 종료 시 정리 작업
    """
    try:
        if 'conn' in globals():
            conn.close()
        log_activity("SYSTEM", "애플리케이션 정상 종료")
    except:
        pass

# 종료 시 정리 함수 등록
atexit.register(cleanup_on_exit)

# 성능 모니터링 (개발용)
def performance_monitor():
    """
    성능 모니터링 함수
    """
    import psutil
    import time
    
    cpu_percent = psutil.cpu_percent()
    memory_info = psutil.virtual_memory()
    
    return {
        'cpu_usage': f"{cpu_percent}%",
        'memory_usage': f"{memory_info.percent}%",
        'available_memory': f"{memory_info.available / (1024**3):.1f} GB"
    }
