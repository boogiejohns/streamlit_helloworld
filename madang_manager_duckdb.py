import streamlit as st
import duckdb
import pandas as pd
import time
from pathlib import Path

DB_PATH = "madang.duckdb"

# 1) DuckDB 커넥션 (앱 시작 시 한 번만 생성)
@st.cache_resource
def get_conn():
    conn = duckdb.connect(DB_PATH)

    # 테이블이 없으면 CSV로부터 생성 (앱 실행 시 항상 확인)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Book AS
        SELECT * FROM read_csv_auto('Book.csv');
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Customer AS
        SELECT * FROM read_csv_auto('Customer.csv');
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Orders AS
        SELECT * FROM read_csv_auto('Orders.csv');
    """)
    return conn

def query_df(sql: str, params=None) -> pd.DataFrame:
    """SELECT 계열용: DataFrame으로 결과 반환"""
    conn = get_conn()
    if params is None:
        return conn.execute(sql).df()
    else:
        return conn.execute(sql, params).df()

def execute(sql: str, params=None):
    """INSERT/UPDATE/DELETE 계열용"""
    conn = get_conn()
    if params is None:
        conn.execute(sql)
    else:
        conn.execute(sql, params)
    conn.commit()

# 2) 화면 구성
st.title("마당서점 고객 관리 (DuckDB 버전)")

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# ───────────────────────
# 공통: 도서 목록 불러오기
# ───────────────────────
books_df = query_df("SELECT bookid, bookname FROM Book ORDER BY bookid;")
book_options = ["선택 안 함"] + [
    f"{row.bookid},{row.bookname}" for row in books_df.itertuples(index=False)
]

# ───────────────────────
# 탭 1: 고객 조회
# ───────────────────────
name = tab1.text_input("고객명")

custid = None
result_df = pd.DataFrame()

if name:
    sql = """
        SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice
        FROM Customer c
        JOIN Orders o ON c.custid = o.custid
        JOIN Book b   ON o.bookid = b.bookid
        WHERE c.name = ?
        ORDER BY o.orderdate;
    """
    result_df = query_df(sql, [name])

    if result_df.empty:
        tab1.write("해당 이름의 거래 내역이 없습니다.")
    else:
        tab1.write(result_df)
        custid = int(result_df["custid"].iloc[0])  # 첫 행 기준으로 고객번호 사용

# ───────────────────────
# 탭 2: 거래 입력
# ───────────────────────
if custid is not None:
    tab2.write(f"고객번호: {custid}")
    tab2.write(f"고객명: {name}")

    select_book = tab2.selectbox("구매 서적:", book_options)

    if select_book != "선택 안 함":
        bookid = int(select_book.split(",")[0])

        # 오늘 날짜
        dt = time.strftime("%Y-%m-%d", time.localtime())

        # 다음 orderid
        next_id_df = query_df("SELECT COALESCE(MAX(orderid), 0) + 1 AS next_id FROM Orders;")
        orderid = int(next_id_df["next_id"].iloc[0])

        price_str = tab2.text_input("금액", value="0")

        if tab2.button("거래 입력"):
            try:
                price = int(price_str)
                execute(
                    "INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) "
                    "VALUES (?, ?, ?, ?, ?);",
                    [orderid, custid, bookid, price, dt],
                )
                tab2.success("거래가 입력되었습니다.")
            except ValueError:
                tab2.error("금액은 숫자로 입력해야 합니다.")
else:
    tab2.write("먼저 [고객조회] 탭에서 고객명을 입력해 주세요.")
