import time
from Indication_Updator import process_indicators_update
from Manage_DB.Add_missing_columns import add_missing_columns
from Manage_DB.Clone_Table import clone_table
from Manage_DB.DB_Managing import get_db_connection
from Manage_DB.Sync_Table import sync_table_data
from Manage_DB.Trading_Volume import manage_trading_volume

# 테이블명 변수 선언
SOURCE_TABLE = 'upbit_minute_data'  
TARGET_TABLE = 'upbit_minute_data_2'

if __name__ == "__main__":
    conn = None
    try:
        # DB 연결 (SQLAlchemy 기반 엔진 또는 커넥션 객체를 반환한다고 가정)
        conn = get_db_connection()
        
        while True:
            
            # 거래량 업데이트 함수 호출
            manage_trading_volume(conn)
            
            # 대상 테이블 복제 (존재하지 않을 때만)
            clone_table(conn, SOURCE_TABLE, TARGET_TABLE)

            # source_table에서 target_table로 데이터 동기화 (MERGE 사용)
            sync_table_data(conn, SOURCE_TABLE, TARGET_TABLE)



            # 지표 계산 및 업데이트
            process_indicators_update(conn, TARGET_TABLE)


            
            # 30초 대기 후 반복
            time.sleep(1)

    except Exception as e:
        print(f"오류 발생: {e}")

    finally:
        if conn:
            conn.close()
