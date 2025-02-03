import cx_Oracle
import pandas as pd
import numpy as np


# Oracle DB 연결
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


# 단일 코인 백테스트
def backtest_coin(df, n_minutes):
    """
    단일 시점(n분 후) 백테스트 수행 및 다양한 성능 지표 계산
    """
    df[f'NEXT_TRADE_PRICE_{n_minutes}'] = df['TRADE_PRICE'].shift(-n_minutes)
    
    def determine_win(row):
        if pd.isna(row[f'NEXT_TRADE_PRICE_{n_minutes}']):
            return 0  # 데이터 부족
        if row['OPINION'] == 'Long' and row[f'NEXT_TRADE_PRICE_{n_minutes}'] > row['TRADE_PRICE']:
            return 1  # Long 예측 성공
        if row['OPINION'] == 'Short' and row[f'NEXT_TRADE_PRICE_{n_minutes}'] < row['TRADE_PRICE']:
            return 1  # Short 예측 성공
        return 0  # 실패
    
    df['WIN'] = df.apply(determine_win, axis=1)
    
    # ✅ 수익률(RETURN) 계산 개선
    df['RETURN'] = np.where(
        df['OPINION'] == 'Long',
        (df[f'NEXT_TRADE_PRICE_{n_minutes}'] - df['TRADE_PRICE']) / df['TRADE_PRICE'],
        np.where(
            df['OPINION'] == 'Short',
            (df['TRADE_PRICE'] - df[f'NEXT_TRADE_PRICE_{n_minutes}']) / df['TRADE_PRICE'],
            0  # OPINION이 Long/Short가 아닌 경우
        )
    )
    
    # ✅ NaN 및 비정상 값 처리
    df['RETURN'] = df['RETURN'].fillna(0)
    
    # ✅ 누적 수익률 (Cumulative Return)
    cumulative_return = (1 + df['RETURN']).prod() - 1 if len(df['RETURN']) > 0 else 0
    
    # ✅ 평균 거래 수익률 (Average Trade Return)
    avg_trade_return = df['RETURN'].mean() if len(df['RETURN']) > 0 else 0
    
    # ✅ 최대 낙폭 (Maximum Drawdown)
    cumulative_returns = (1 + df['RETURN']).cumprod()
    peak = cumulative_returns.cummax()
    drawdown = (cumulative_returns - peak) / peak
    max_drawdown = drawdown.min() if not drawdown.empty else 0
    
    # ✅ 연환산 수익률 (Annualized Return)
    num_days = len(df) / (24 * 60 / n_minutes)  # n_minutes 단위로 일수 환산
    annualized_return = (1 + cumulative_return) ** (365 / max(1, num_days)) - 1 if num_days > 0 else 0
    
    bins = [0, 20, 40, 60, 80, 100]
    labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    df['confidence_bin'] = pd.cut(df['CONFIDENCE'], bins=bins, labels=labels, right=False)
    
    grouped = df.groupby('confidence_bin').agg(
        total_count=('WIN', 'count'),
        success_count=('WIN', 'sum')
    ).reset_index()
    
    grouped['win_rate'] = (grouped['success_count'] / grouped['total_count']) * 100
    grouped['n_minutes'] = n_minutes
    grouped['cumulative_return'] = cumulative_return
    grouped['avg_trade_return'] = avg_trade_return
    grouped['max_drawdown'] = max_drawdown
    grouped['annualized_return'] = annualized_return
    
    return grouped


# 여러 코인과 다양한 n에 대한 백테스트
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


# BACK_TESTING 테이블 생성
def create_backtesting_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    drop_table_query = """
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE BACK_TESTING PURGE';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
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
                CUMULATIVE_RETURN FLOAT,
                AVG_TRADE_RETURN FLOAT,
                MAX_DRAWDOWN FLOAT,
                ANNUALIZED_RETURN FLOAT,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ';
    END;
    """
    
    try:
        cursor.execute(drop_table_query)
        conn.commit()
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ BACK_TESTING 테이블이 성공적으로 생성되었습니다.")
    except cx_Oracle.DatabaseError as e:
        print(f"❌ 테이블 생성 오류: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# 백테스트 결과 저장
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
            N_MINUTES,
            CUMULATIVE_RETURN,
            AVG_TRADE_RETURN,
            MAX_DRAWDOWN,
            ANNUALIZED_RETURN
        ) VALUES (
            :market,
            :confidence_bin,
            :total_count,
            :success_count,
            :win_rate,
            :n_minutes,
            :cumulative_return,
            :avg_trade_return,
            :max_drawdown,
            :annualized_return
        )
    """
    
    try:
        # NaN 값 처리 및 데이터 타입 변환
        df = df.fillna({
            'total_count': 0,
            'success_count': 0,
            'win_rate': 0.0,
            'n_minutes': 0,
            'cumulative_return': 0.0,
            'avg_trade_return': 0.0,
            'max_drawdown': 0.0,
            'annualized_return': 0.0
        })

        # 데이터 타입 명확히 변환
        df['total_count'] = df['total_count'].astype(int)
        df['success_count'] = df['success_count'].astype(int)
        df['win_rate'] = df['win_rate'].astype(float)
        df['n_minutes'] = df['n_minutes'].astype(int)
        df['cumulative_return'] = df['cumulative_return'].astype(float)
        df['avg_trade_return'] = df['avg_trade_return'].astype(float)
        df['max_drawdown'] = df['max_drawdown'].astype(float)
        df['annualized_return'] = df['annualized_return'].astype(float)
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, {
                'market': row['MARKET'],
                'confidence_bin': row['confidence_bin'],
                'total_count': int(row['total_count']),
                'success_count': int(row['success_count']),
                'win_rate': float(row['win_rate']),
                'n_minutes': int(row['n_minutes']),
                'cumulative_return': float(row['cumulative_return']),
                'avg_trade_return': float(row['avg_trade_return']),
                'max_drawdown': float(row['max_drawdown']),
                'annualized_return': float(row['annualized_return'])
            })
        
        conn.commit()
        print("✅ 백테스트 결과가 성공적으로 저장되었습니다.")
    except cx_Oracle.DatabaseError as e:
        print(f"❌ 데이터 저장 실패: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# 메인 실행
if __name__ == "__main__":
    start_n = 1
    end_n = 30
    n_range = list(range(start_n, end_n + 1))
    
    create_backtesting_table()
    result_df = backtest_confidence_with_multiple_coins(n_range=n_range)
    save_backtest_results_to_db(result_df)
