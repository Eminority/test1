import os
import time
import requests
import pandas as pd
import cx_Oracle
from datetime import datetime, timedelta

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

# 테이블이 없으면 자동으로 생성하는 함수
def create_table_if_not_exists(conn):
    """
    테이블이 없으면 자동으로 생성하는 함수
    """
    cursor = conn.cursor()

    # upbit_minute_data 테이블 생성
    create_upbit_minute_data_query = """
    CREATE TABLE upbit_minute_data (
        market VARCHAR2(50) NOT NULL, 
        candle_date_time_utc TIMESTAMP NOT NULL,
        candle_date_time_kst TIMESTAMP NOT NULL,
        opening_price NUMBER(20, 8),
        high_price NUMBER(20, 8),
        low_price NUMBER(20, 8),
        trade_price NUMBER(20, 8),
        volume NUMBER,
        candle_acc_trade_price NUMBER(20, 8),
        candle_acc_trade_volume NUMBER(20, 8),
        PRIMARY KEY (market, candle_date_time_utc)
    )
    """
    try:
        cursor.execute("SELECT 1 FROM upbit_minute_data WHERE ROWNUM = 1")  # 테이블 존재 확인
    except cx_Oracle.DatabaseError:
        print("[INFO] upbit_minute_data 테이블이 존재하지 않아 생성합니다.")
        cursor.execute(create_upbit_minute_data_query)

    # upbit_minute_data_archive 테이블 생성
    create_upbit_minute_data_archive_query = """
    CREATE TABLE upbit_minute_data_archive (
        market VARCHAR2(50) NOT NULL, 
        candle_date_time_utc TIMESTAMP NOT NULL,
        candle_date_time_kst TIMESTAMP NOT NULL,
        opening_price NUMBER(20, 8),
        high_price NUMBER(20, 8),
        low_price NUMBER(20, 8),
        trade_price NUMBER(20, 8),
        volume NUMBER,
        candle_acc_trade_price NUMBER(20, 8),
        candle_acc_trade_volume NUMBER(20, 8),
        PRIMARY KEY (market, candle_date_time_utc)
    )
    """
    try:
        cursor.execute("SELECT 1 FROM upbit_minute_data_archive WHERE ROWNUM = 1")  # 테이블 존재 확인
    except cx_Oracle.DatabaseError:
        print("[INFO] upbit_minute_data_archive 테이블이 존재하지 않아 생성합니다.")
        cursor.execute(create_upbit_minute_data_archive_query)
    finally:
        cursor.close()

# 10일치 데이터를 초기화 시 수집
def fetch_10days_data(market):
    data = pd.DataFrame()
    end_time = datetime.now()
    target_time = end_time - timedelta(days=20)

    while True:
        try:
            url = "https://api.upbit.com/v1/candles/minutes/1"
            params = {
                'market': market,
                'count': 200,
                'to': end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            response = requests.get(url, params=params)

            if response.status_code == 200:
                batch = pd.DataFrame(response.json())
                if batch.empty:
                    break

                data = pd.concat([data, batch], ignore_index=True).drop_duplicates(subset=['candle_date_time_utc'])
                end_time = datetime.strptime(batch['candle_date_time_utc'].min(), "%Y-%m-%dT%H:%M:%S")

                if end_time <= target_time:
                    break
            elif response.status_code == 429:
                print("[WARN] Too many requests. Waiting for 1 minute...")
                time.sleep(60)
            else:
                print(f"[ERROR] API 요청 실패: {response.status_code} - {response.text}")
                break

            time.sleep(0.5)
        except Exception as e:
            print(f"[ERROR] 오류 발생: {e}")
            time.sleep(60)

    if not data.empty:
        data = data[[
            'candle_date_time_utc', 'candle_date_time_kst',
            'opening_price', 'high_price', 'low_price', 'trade_price',
            'candle_acc_trade_price', 'candle_acc_trade_volume'
        ]]

    return data

# 실시간 데이터를 가져오는 함수
def fetch_latest_data(market):
    try:
        url = "https://api.upbit.com/v1/candles/minutes/1"
        params = {
            'market': market,
            'count': 1
        }
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = pd.DataFrame(response.json())
            if not data.empty:
                data = data[[
                    'candle_date_time_utc', 'candle_date_time_kst',
                    'opening_price', 'high_price', 'low_price', 'trade_price',
                    'candle_acc_trade_price', 'candle_acc_trade_volume'
                ]]
            return data
        elif response.status_code == 429:
            print("[WARN] Too many requests. Waiting for 1 minute...")
            time.sleep(60)
        else:
            print(f"[ERROR] API 요청 실패: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] 오류 발생: {e}")
        time.sleep(60)

    return pd.DataFrame()

# Oracle DB에 데이터 업데이트
def update_db_with_data(conn, data, market):
    cursor = conn.cursor()
    query = """
    MERGE INTO upbit_minute_data d
    USING (SELECT :market AS market, :timestamp AS timestamp FROM dual) s
    ON (d.market = s.market AND d.candle_date_time_utc = s.timestamp)
    WHEN MATCHED THEN
        UPDATE SET opening_price = :opening_price,
                   high_price = :high_price,
                   low_price = :low_price,
                   trade_price = :trade_price,
                   volume = :volume,
                   candle_acc_trade_price = :candle_acc_trade_price,
                   candle_acc_trade_volume = :candle_acc_trade_volume
    WHEN NOT MATCHED THEN
        INSERT (market, candle_date_time_utc, candle_date_time_kst, opening_price, high_price, low_price, trade_price, volume, candle_acc_trade_price, candle_acc_trade_volume)
        VALUES (:market, :timestamp, :kst_time, :opening_price, :high_price, :low_price, :trade_price, :volume, :candle_acc_trade_price, :candle_acc_trade_volume)
    """
    for _, row in data.iterrows():
        try:
            utc_time = datetime.strptime(row['candle_date_time_utc'], "%Y-%m-%dT%H:%M:%S")
            kst_time = datetime.strptime(row['candle_date_time_kst'], "%Y-%m-%dT%H:%M:%S")
            cursor.execute(query, {
                'market': market,
                'timestamp': utc_time,
                'kst_time': kst_time,
                'opening_price': row['opening_price'],
                'high_price': row['high_price'],
                'low_price': row['low_price'],
                'trade_price': row['trade_price'],
                'volume': row['candle_acc_trade_volume'],
                'candle_acc_trade_price': row['candle_acc_trade_price'],
                'candle_acc_trade_volume': row['candle_acc_trade_volume'],
            })
        except cx_Oracle.IntegrityError as e:
            print(f"[ERROR] 데이터 삽입 중 무결성 제약 조건 위배: {e}")
    conn.commit()
    cursor.close()

# 10일 이상된 데이터를 archive 테이블로 이동
def archive_old_data(conn):
    cursor = conn.cursor()
    cutoff_time = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d %H:%M:%S")

    archive_query = """
    INSERT INTO upbit_minute_data_archive (market, candle_date_time_utc, candle_date_time_kst, opening_price, high_price, low_price, trade_price, volume, candle_acc_trade_price, candle_acc_trade_volume)
    SELECT market, candle_date_time_utc, candle_date_time_kst, opening_price, high_price, low_price, trade_price, volume, candle_acc_trade_price, candle_acc_trade_volume
    FROM upbit_minute_data
    WHERE candle_date_time_utc < TO_TIMESTAMP(:cutoff_time, 'YYYY-MM-DD HH24:MI:SS')
    """
    cursor.execute(archive_query, {'cutoff_time': cutoff_time})

    delete_query = """
    DELETE FROM upbit_minute_data
    WHERE candle_date_time_utc < TO_TIMESTAMP(:cutoff_time, 'YYYY-MM-DD HH24:MI:SS')
    """
    cursor.execute(delete_query, {'cutoff_time': cutoff_time})

    conn.commit()
    cursor.close()

# 초기 데이터 수집
def initialize_db(conn, markets):
    if os.path.exists("db_initialized.flag"):
        print("[INFO] DB 초기화 작업은 이미 완료되었습니다. 건너뜁니다.")
        return

    print("[INFO] 초기 데이터 수집 시작...")
    create_table_if_not_exists(conn)
    for market in markets:
        data = fetch_10days_data(market)
        if not data.empty:
            update_db_with_data(conn, data, market)
    open("db_initialized.flag", "w").close()
    print("[INFO] 초기 데이터 저장 완료.")

# 실시간 데이터 업데이트
def update_db_periodically(conn, markets):
    while True:
        print(f"[{datetime.now()}] 데이터 업데이트 시작...")
        for market in markets:
            data = fetch_latest_data(market)
            if not data.empty:
                update_db_with_data(conn, data, market)
        archive_old_data(conn)
        print(f"[{datetime.now()}] 데이터 업데이트 완료. 다음 업데이트까지 대기 중...")
        time.sleep(2)

# 메인 함수
if __name__=="__main__":
    markets = ["KRW-BTC", "KRW-XRP", "KRW-ETH", "KRW-ETC"]
    conn = get_db_connection()

    try:
        initialize_db(conn, markets)
        update_db_periodically(conn, markets)
    except KeyboardInterrupt:
        print("프로그램 종료.")
    except Exception as e:
        print(f"[ERROR] 프로그램 실행 중 오류 발생: {e}")
    finally:
        conn.close()