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

# 🌟🌟🌟 수정된 기본 쿼리 (bookid 및 custid 반영) 🌟🌟🌟
default_query = """
-- Q: 가장 많은 책을 주문한 고객의 이름과 주문 총액을 조회
SELECT
    C.c_name AS "고객 이름",
    SUM(O.o_price) AS "총 주문 금액",
    COUNT(O.custid) AS "총 주문 횟수"
FROM Orders O
JOIN Customer C ON O.custid = C.c_id  -- Orders.custid와 Customer.c_id 연결
GROUP BY 1
ORDER BY "총 주문 금액" DESC;
"""

user_query = st.text_area("SQL 쿼리를 입력하세요:", value=default_query, height=200)

if st.button("쿼리 실행"):
    try:
        # 사용자 입력 쿼리 실행
        query_result = conn.execute(user_query).fetchdf()

        st.success("쿼리 실행 완료!")
        st.dataframe(query_result)

        # 간단한 시각화 (총 주문 금액 컬럼이 있다면 차트 표시)
        if not query_result.empty and "총 주문 금액" in query_result.columns:
            st.subheader("📊 시각화 (총 주문 금액 기준)")
            st.bar_chart(query_result, x="고객 이름", y="총 주문 금액")

    except Exception as e:
        st.error(f"쿼리 실행 중 오류가 발생했습니다: {e}")