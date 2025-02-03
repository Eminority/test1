import time
import keyboard
from Indication_Updator import *
from Calculate_Indicator.Indactors_Pacakge import *
from Manage_DB.Add_missing_columns import *
from Manage_DB.Clone_Table import *
from Manage_DB.DB_Managing import *
from Manage_DB.Sync_Table import *

if __name__ == "__main__":
    conn = None
    try:
        # DB 연결
        conn = get_db_connection()

        while True:
            
            # '입력한' 테이블 생성
            clone_table(conn, 'K_REAL_TIME', 'K_REAL_TIME_2')
            # 'K_REAL_TIME_2' 테이블을 3분마다 업데이트
            sync_table_data(conn, 'K_REAL_TIME', 'K_REAL_TIME_2')
            # 지표 계산 및 업데이트
            process_indicators_update(conn)

            time.sleep(120)  # 2분 대기

    except Exception as e:
        print(f"오류 발생: {e}")

    finally:
        if conn:
            conn.close()
