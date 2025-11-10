import streamlit as st
import duckdb
import pandas as pd

# 1. DB 연결 설정
DB_FILE = 'madang.db'
try:
    # madang.db 파일에 읽기 전용으로 연결
    conn = duckdb.connect(database=DB_FILE, read_only=True)
except Exception as e:
    st.error(f"❌ 데이터베이스 연결 오류: {DB_FILE} 파일을 찾을 수 없습니다. (오류: {e})")
    st.stop()

# Streamlit 페이지 설정
st.title("📚 DuckDB 마당 도서 데이터 뷰어")
st.subheader(f"데이터베이스 파일: **{DB_FILE}**")

# --- 2. 테이블 목록 및 기본 정보 표시 ---
st.header("🔍 데이터베이스 정보")
tables = conn.execute("SHOW TABLES;").fetchall()
table_names = [t[0] for t in tables]

if table_names:
    st.write(f"총 **{len(table_names)}개**의 테이블이 로드되었습니다: {', '.join(table_names)}")

    # 테이블 선택 드롭다운
    selected_table = st.selectbox("테이블을 선택하여 상위 데이터를 확인하세요:", table_names)

    if selected_table:
        # 선택된 테이블의 상위 5개 데이터 조회
        query = f"SELECT * FROM {selected_table} LIMIT 5"
        df_head = conn.execute(query).fetchdf()

        st.dataframe(df_head)
        st.caption(f"테이블 `{selected_table}`의 상위 5개 레코드")

# --- 3. 사용자 정의 SQL 쿼리 실행 ---
st.header("💻 사용자 정의 SQL 쿼리")
st.warning("경고: 읽기 전용 모드이므로 데이터는 변경되지 않습니다. SELECT 쿼리만 사용하세요.")

# 기본 쿼리 예시 (Book과 Orders 테이블을 조인하여 가격이 10000원 이상인 항목 조회)
default_query = """
SELECT 
    B.b_name AS "도서명", 
    B.b_publisher AS "출판사", 
    O.o_price AS "주문가격"
FROM Orders O
JOIN Book B ON O.bookid = B.b_id  -- ✅ 'O.b_id'를 'O.bookid'로 수정
WHERE O.o_price >= 10000
ORDER BY "주문가격" DESC;
"""

user_query = st.text_area("SQL 쿼리를 입력하세요:", value=default_query, height=200)

if st.button("쿼리 실행"):
    try:
        # 사용자 입력 쿼리 실행
        query_result = conn.execute(user_query).fetchdf()

        st.success("쿼리 실행 완료!")
        st.dataframe(query_result)

        # 간단한 시각화 (Pandas DataFrame을 지원)
        if not query_result.empty:
            if '주문가격' in query_result.columns:  # 예시 쿼리의 결과 컬럼인 경우 차트 표시
                st.subheader("📊 시각화 (주문가격 기준)")
                st.bar_chart(query_result, x="도서명", y="주문가격")

    except Exception as e:
        st.error(f"쿼리 실행 중 오류가 발생했습니다: {e}")