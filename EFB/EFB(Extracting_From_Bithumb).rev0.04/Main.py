import os
import time
import requests
import pandas as pd
import cx_Oracle
from datetime import datetime, timedelta

# --------------------------------------------------
#  Oracle DB 연결 설정
# --------------------------------------------------
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

# --------------------------------------------------
#  테이블이 없으면 자동으로 생성
# --------------------------------------------------
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


# --------------------------------------------------
#  20일치(실제로는 20일치) 데이터를 초기 수집
# --------------------------------------------------
def fetch_20days_data(market):
    data = pd.DataFrame()
    # 본 코드에서는 end_time을 한국 시간(now)으로 사용하지만,
    # UTC로 쓰고 싶으면 datetime.utcnow()로 바꿀 수 있음
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

                # 중복 제거
                data = pd.concat([data, batch], ignore_index=True)
                data.drop_duplicates(subset=['candle_date_time_utc'], inplace=True)

                # batch 중 가장 과거 시각
                min_str = batch['candle_date_time_utc'].min()
                min_time = datetime.strptime(min_str, "%Y-%m-%dT%H:%M:%S")

                # end_time을 더 과거로 조정하며 반복
                end_time = min_time

                # 20일 전까지 과거로 내려갔으면 stop
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

# --------------------------------------------------
#  DB 내 '가장 최근 시각' 이후 분봉만 가져오는 함수 (누락 데이터 수집)
# --------------------------------------------------
def fetch_incremental_data(conn, market):
    """
    - DB에서 해당 market의 가장 최근 candle_date_time_utc를 찾음
    - DB가 비어있다면(fetch_20days_data로) 초기 20일치 수집
    - DB가 비어있지 않다면, 그 '마지막 시각' 이후 데이터를 모두 가져옴
    """
    # 1) DB에서 가장 최근 시각 확인
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(candle_date_time_utc)
        FROM upbit_minute_data
        WHERE market = :market
    """, {'market': market})
    row = cursor.fetchone()
    cursor.close()

    last_time = row[0]  # None or datetime 객체

    # 2) DB에 해당 market 데이터가 없으면 -> 초기 20일치
    if last_time is None:
        print(f"[INFO] {market} 데이터가 DB에 없어, 20일치 데이터를 수집합니다.")
        return fetch_20days_data(market)

    # 3) DB에 데이터가 있으면 -> last_time 이후부터 현재까지 수집
    data = pd.DataFrame()
    end_time = datetime.now()
    target_time = last_time  # last_time 이후분이 필요

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

                # batch 내 가장 과거 시각(문자열)
                min_str = batch['candle_date_time_utc'].min()
                min_time = datetime.strptime(min_str, "%Y-%m-%dT%H:%M:%S")

                # last_time 이후만 필터링 (★ 오탈자 수정)
                batch = batch[
                    batch['candle_date_time_utc'] > last_time.strftime("%Y-%m-%dT%H:%M:%S")
                ]
                data = pd.concat([data, batch], ignore_index=True)

                # end_time을 더 과거로 이동
                end_time = min_time

                # target_time(=last_time) 이전이면 stop
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
        data.drop_duplicates(subset=['candle_date_time_utc'], inplace=True)
        data = data[[
            'candle_date_time_utc', 'candle_date_time_kst',
            'opening_price', 'high_price', 'low_price', 'trade_price',
            'candle_acc_trade_price', 'candle_acc_trade_volume'
        ]]

    return data


# --------------------------------------------------
#  DB에 데이터 (MERGE) 업데이트
# --------------------------------------------------
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

# --------------------------------------------------
#  20일 이상 지난 데이터는 Archive로 이동
# --------------------------------------------------
def archive_old_data(conn):
    cursor = conn.cursor()
    cutoff_time = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d %H:%M:%S")

    archive_query = """
    INSERT INTO upbit_minute_data_archive (market, candle_date_time_utc, candle_date_time_kst,
        opening_price, high_price, low_price, trade_price, volume,
        candle_acc_trade_price, candle_acc_trade_volume)
    SELECT market, candle_date_time_utc, candle_date_time_kst,
           opening_price, high_price, low_price, trade_price, volume,
           candle_acc_trade_price, candle_acc_trade_volume
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

# --------------------------------------------------
#  초기 데이터 수집 (최초 1회)
# --------------------------------------------------
def initialize_db(conn, markets):
    """
    db_initialized.flag 파일이 없으면(최초 실행이라면) 20일치 데이터를 수집/저장
    """
    if os.path.exists("db_initialized.flag"):
        print("[INFO] DB 초기화 작업은 이미 완료되었습니다. 건너뜁니다.")
        return

    print("[INFO] 초기 데이터 수집 시작...")
    create_table_if_not_exists(conn)  # 테이블이 없으면 생성

    for market in markets:
        data = fetch_20days_data(market)
        if not data.empty:
            update_db_with_data(conn, data, market)

    open("db_initialized.flag", "w").close()
    print("[INFO] 초기 데이터 저장 완료.")

# --------------------------------------------------
#  프로그램 동작 중 (또는 재실행 시) 누락된 데이터만 채우기
# --------------------------------------------------
def update_db_periodically(conn, markets):
    while True:
        print(f"[{datetime.now()}] 데이터 업데이트 시작...")
        for market in markets:
            # '가장 최근 시각' 이후의 모든 분봉을 수집
            data = fetch_incremental_data(conn, market)
            if not data.empty:
                update_db_with_data(conn, data, market)

        # 20일 이상 지난 데이터는 archive로 이동
        archive_old_data(conn)

        print(f"[{datetime.now()}] 데이터 업데이트 완료. 다음 업데이트까지 대기 중...")
        time.sleep(2)

# --------------------------------------------------
#  메인 함수
# --------------------------------------------------
if __name__=="__main__":
    markets = ["KRW-BTC", "KRW-XRP", "KRW-ETH", "KRW-ETC"]
    conn = get_db_connection()

    try:
        # 1) 최초 실행 시, DB가 없다면 생성 & 20일치 데이터 수집
        initialize_db(conn, markets)
        # 2) 그 후에는 주기적으로 DB에 누락분만 채우는 루프
        update_db_periodically(conn, markets)
    except KeyboardInterrupt:
        print("프로그램 종료.")
    except Exception as e:
        print(f"[ERROR] 프로그램 실행 중 오류 발생: {e}")
    finally:
        conn.close()
