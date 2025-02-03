import cx_Oracle
import pandas as pd

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


def backtest_coin(df, n_minutes):
    """
    단일 시점(n분 후) 백테스트 수행
    """
    df[f'NEXT_TRADE_PRICE_{n_minutes}'] = df['TRADE_PRICE'].shift(-n_minutes)
    
    def determine_win(row):
        if pd.isna(row[f'NEXT_TRADE_PRICE_{n_minutes}']):
            return 0  # 데이터가 부족한 경우 실패 처리
        
        if row['OPINION'] == 'Long' and row[f'NEXT_TRADE_PRICE_{n_minutes}'] > row['TRADE_PRICE']:
            return 1  # Long 예측 성공
        if row['OPINION'] == 'Short' and row[f'NEXT_TRADE_PRICE_{n_minutes}'] < row['TRADE_PRICE']:
            return 1  # Short 예측 성공
        
        return 0  # 실패
    
    df['WIN'] = df.apply(determine_win, axis=1)
    
    bins = [0, 20, 40, 60, 80, 100]
    labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    df['confidence_bin'] = pd.cut(df['CONFIDENCE'], bins=bins, labels=labels, right=False)
    
    grouped = df.groupby('confidence_bin').agg(
        total_count=('WIN', 'count'),
        success_count=('WIN', 'sum')
    ).reset_index()
    
    grouped['win_rate'] = (grouped['success_count'] / grouped['total_count']) * 100
    grouped['n_minutes'] = n_minutes
    
    return grouped


def backtest_confidence_with_multiple_coins(n_range):
    """
    여러 코인과 다양한 n에 대한 백테스트 수행
    """
    conn = get_db_connection()
    query = """
        SELECT 
            MARKET,
            CANDLE_DATE_TIME_KST,
            TRADE_PRICE,
            OPINION,
            CONFIDENCE
        FROM UPBIT_MINUTE_DATA_2
        ORDER BY MARKET, CANDLE_DATE_TIME_KST
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    results = []
    for n in n_range:
        print(f"\n=== {n}분 후 백테스트 시작 ===")
        for market, group_df in df.groupby('MARKET'):
            print(f" → {market} 코인 처리 중...")
            result = backtest_coin(group_df, n)
            result['MARKET'] = market
            results.append(result)
    
    final_result = pd.concat(results)
    print("\n=== 전체 코인 및 시간별 백테스트 결과 ===")
    print(final_result)
    return final_result


def create_backtesting_table():
    """
    BACK_TESTING 테이블을 삭제하고 새로 생성
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    drop_table_query = """
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE BACK_TESTING PURGE';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN -- ORA-00942: 테이블이 존재하지 않습니다.
                RAISE;
            END IF;
    END;
    """
    
    create_table_query = """
    BEGIN
        EXECUTE IMMEDIATE '
            CREATE TABLE BACK_TESTING (
                MARKET VARCHAR2(50) NOT NULL,
                CONFIDENCE_BIN VARCHAR2(10) NOT NULL,
                TOTAL_COUNT NUMBER NOT NULL,
                SUCCESS_COUNT NUMBER NOT NULL,
                WIN_RATE NUMBER NOT NULL,
                N_MINUTES NUMBER NOT NULL,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ';
    END;
    """
    
    try:
        # 기존 테이블 삭제
        cursor.execute(drop_table_query)
        conn.commit()
        print("✅ 기존 BACK_TESTING 테이블이 삭제되었습니다.")
        
        # 새 테이블 생성
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ BACK_TESTING 테이블이 성공적으로 생성되었습니다.")
    except cx_Oracle.DatabaseError as e:
        print(f"❌ 테이블 생성 오류: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()



def save_backtest_results_to_db(df):
    """
    백테스트 결과를 BACK_TESTING 테이블에 저장
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO BACK_TESTING (
            MARKET,
            CONFIDENCE_BIN,
            TOTAL_COUNT,
            SUCCESS_COUNT,
            WIN_RATE,
            N_MINUTES
        ) VALUES (
            :market,
            :confidence_bin,
            :total_count,
            :success_count,
            :win_rate,
            :n_minutes
        )
    """
    
    try:
        # NaN 값을 0 또는 적절한 기본값으로 변환
        df = df.fillna({
            'total_count': 0,
            'success_count': 0,
            'win_rate': 0.0,
            'n_minutes': 0
        })
        
        # 데이터 타입 변환
        df['total_count'] = df['total_count'].astype(int)
        df['success_count'] = df['success_count'].astype(int)
        df['win_rate'] = df['win_rate'].astype(float)
        df['n_minutes'] = df['n_minutes'].astype(int)
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, {
                'market': row['MARKET'],
                'confidence_bin': row['confidence_bin'],
                'total_count': row['total_count'],
                'success_count': row['success_count'],
                'win_rate': row['win_rate'],
                'n_minutes': row['n_minutes']
            })
        conn.commit()
        print("✅ 백테스트 결과가 성공적으로 저장되었습니다.")
    except cx_Oracle.DatabaseError as e:
        print(f"❌ 데이터 저장 실패: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()



if __name__ == "__main__":
    # n 값 구간 설정
    start_n = 1  # 시작 n값
    end_n = 30   # 끝 n값
    
    # 구간을 기반으로 n_range 자동 생성
    n_range = list(range(start_n, end_n + 1))  # 시작부터 끝까지 1씩 증가
    
    print(f"📊 설정된 n_range: {n_range}")
    
    # 기존 테이블 삭제 및 재생성
    create_backtesting_table()
    
    # 백테스트 실행 및 결과 저장
    result_df = backtest_confidence_with_multiple_coins(n_range=n_range)
    save_backtest_results_to_db(result_df)