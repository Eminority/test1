import os
import cx_Oracle
import pandas as pd
from tkinter import Tk, ttk
from datetime import datetime
import pytz  # 한국 시간대를 사용하기 위해 추가

# Oracle DB 연결 설정 함수
def get_db_connection():
    """
    Oracle DB 연결 설정
    """
    try:
        dsn_tns = cx_Oracle.makedsn("localhost", 1521, service_name="xe")
        conn = cx_Oracle.connect(user="jsb", password="jsb0328", dsn=dsn_tns)
        return conn
    except cx_Oracle.DatabaseError as e:
        print(f"Oracle DB 연결 오류: {e}")
        raise

# 데이터 조회 함수
def fetch_top_data(conn):
    query = """
    SELECT MARKET, CANDLE_DATE_TIME_KST, OPINION, CONFIDENCE
    FROM (
        SELECT 
            MARKET, 
            CANDLE_DATE_TIME_KST, 
            OPINION, 
            CONFIDENCE, 
            ROW_NUMBER() OVER (PARTITION BY MARKET ORDER BY CANDLE_DATE_TIME_KST DESC) AS RN
        FROM   upbit_minute_data_2
    )
    WHERE RN = 1
    """
    df = pd.read_sql(query, conn)

    # CONFIDENCE 값을 정수형으로 변환
    df["CONFIDENCE"] = df["CONFIDENCE"].astype(int)
    return df

# GUI 생성 함수
def create_gui(conn):
    root = Tk()
    root.title("Market Data Viewer")

    # Create a treeview widget
    tree = ttk.Treeview(root, columns=("MARKET", "CANDLE_DATE_TIME_KST", "OPINION", "CONFIDENCE"), show="headings")

    # Define column headings
    tree.heading("MARKET", text="Market")
    tree.heading("CANDLE_DATE_TIME_KST", text="Date Time (KST)")
    tree.heading("OPINION", text="Opinion")
    tree.heading("CONFIDENCE", text="Confidence")

    # Adjust column widths
    tree.column("MARKET", width=100)
    tree.column("CANDLE_DATE_TIME_KST", width=200)
    tree.column("OPINION", width=100)
    tree.column("CONFIDENCE", width=100)

    # Pack the treeview
    tree.pack(fill="both", expand=True)

    # 데이터 업데이트 함수
    def update_data():
        try:
            # Fetch the latest data
            data = fetch_top_data(conn)

            # Clear existing treeview data
            tree.delete(*tree.get_children())

            # 한국 현재 시간 구하기
            korea_tz = pytz.timezone("Asia/Seoul")
            current_time = datetime.now(korea_tz).strftime("%Y-%m-%d %H:%M:%S")

            # Insert updated data into the treeview
            for index, row in data.iterrows():
                tree.insert("", "end", values=(row["MARKET"], current_time, row["OPINION"], row["CONFIDENCE"]))

        except Exception as e:
            print(f"데이터 업데이트 중 오류 발생: {e}")

        # Schedule the next update after 2 seconds
        root.after(1000, update_data)

    # Start the update loop
    update_data()

    # Start the GUI event loop
    root.mainloop()

# 메인 실행 함수
def main():
    try:
        # Connect to the database
        conn = get_db_connection()

        # Create GUI
        create_gui(conn)

    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()