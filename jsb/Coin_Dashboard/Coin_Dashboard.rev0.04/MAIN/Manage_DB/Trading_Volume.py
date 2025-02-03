import cx_Oracle
from Manage_DB.DB_Managing import get_db_connection

def manage_trading_volume(conn):
    """
    TRADING_VOLUME 테이블을 삭제하고 재생성한 후 데이터를 삽입하는 함수.
    """
    delete_table_query = """
    DROP TABLE TRADING_VOLUME"""
    create_table_query = """
    CREATE TABLE TRADING_VOLUME (
        KOREAN_NAME VARCHAR2(100),
        H1_TTV NUMBER
    )"""
    insert_query = """
    INSERT INTO TRADING_VOLUME (KOREAN_NAME, H1_TTV)
    SELECT
        KOREAN_NAME,
        SUM(CANDLE_ACC_TRADE_PRICE) AS H1_TTV
    FROM (
        SELECT
            KOREAN_NAME,
            CANDLE_ACC_TRADE_PRICE,
            ROW_NUMBER() OVER (
                PARTITION BY KOREAN_NAME
                ORDER BY CANDLE_DATE_TIME_UTC DESC
            ) AS RN
        FROM K_REAL_TIME
    )
    WHERE RN <= 20
    GROUP BY KOREAN_NAME
    """
    try:
        cursor = conn.cursor()
        # 테이블 삭제
        try:
            cursor.execute(delete_table_query)
            print("TRADING_VOLUME 테이블이 성공적으로 삭제되었습니다.")
        except cx_Oracle.DatabaseError as e:
            print(f"Error deleting table (ignored if not exists): {e}")

        # 테이블 생성
        cursor.execute(create_table_query)
        conn.commit()
        print("TRADING_VOLUME 테이블이 성공적으로 생성되었습니다.")

        # 데이터 삽입
        cursor.execute(insert_query)
        conn.commit()
        print("데이터가 성공적으로 삽입되었습니다.")
    except cx_Oracle.DatabaseError as e:
        print(f"Error managing TRADING_VOLUME: {e}")
    finally:
        cursor.close()