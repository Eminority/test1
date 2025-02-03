import cx_Oracle
import pandas as pd

def get_db_connection():
    """
    Oracle DB 연결 설정 및 반환
    """
    dsn_tns = cx_Oracle.makedsn("192.168.60.19", 1521, service_name="xe")
    conn = cx_Oracle.connect(user="bdv", password="bdv0328", dsn=dsn_tns)
    return conn

def fetch_data(query, conn):
    """
    DB에서 데이터를 조회하여 Pandas DataFrame으로 반환
    """
    df = pd.read_sql(query, conn)
    return df

def update_data(update_query, data, conn):
    """
    데이터 업데이트 실행
    """
    cursor = conn.cursor()
    for _, row in data.iterrows():
        cursor.execute(update_query, {
            "ema15": row['EMA_15'],
            "ema360": row['EMA_360'],
            "ball_high": row['BALL_HIGH'],
            "ball_low": row['BALL_LOW'],
            "rsi360": row.get('RSI_360', None),  # RSI_360이 없으면 None
            "sto_k": row.get('STO_K', None),    # STO_K가 없으면 None
            "sto_d": row.get('STO_D', None),    # STO_D가 없으면 None
            "candle_date_time_utc": row['CANDLE_DATE_TIME_UTC'],
            "market": row['MARKET']
        })
    conn.commit()
    cursor.close()

