from sqlalchemy import create_engine, Table, Column, String, Integer, Float, Date, MetaData, text
import pandas as pd
from datetime import datetime, timedelta


# ✅ DB 연결 (SQLAlchemy 사용)
def get_db_connection():
    try:
        engine = create_engine('oracle+cx_oracle://jsb:jsb0328@localhost:1521/xe')
        conn = engine.connect()
        print("✅ Oracle DB 연결 성공 (SQLAlchemy)!")
        return conn, engine
    except Exception as e:
        print(f"❌ Oracle DB 연결 오류: {e}")
        raise


# ✅ BACK_TESTING_2 테이블 생성
def create_backtesting_table(engine):
    metadata = MetaData()
    back_testing = Table(
        'BACK_TESTING_2', metadata,
        Column('MARKET', String(50), nullable=False),
        Column('CONFIDENCE_BIN', String(10), nullable=False),
        Column('OPINION', String(10), nullable=False),
        Column('TIME_WINDOW_MIN', Integer, nullable=False),
        Column('PERCENT_THRESHOLD', Float, nullable=False),
        Column('SUCCESS_RATE', Float, nullable=False),
        Column('TOTAL_CASES', Integer, nullable=False),
        Column('SUCCESS_CASES', Integer, nullable=False),
        Column('ANALYSIS_DATE', Date, nullable=False)
    )
    metadata.create_all(engine)
    print("✅ BACK_TESTING_2 테이블이 생성되었거나 이미 존재합니다.")


# ✅ 날짜 형식 확인 함수
def validate_date_column(df, column_name):
    df.columns = [col.upper() for col in df.columns]  # 컬럼 이름을 모두 대문자로 변환
    if column_name.upper() in df.columns:
        try:
            df[column_name.upper()] = pd.to_datetime(df[column_name.upper()])
            print(f"✅ '{column_name.upper()}' 컬럼 날짜 형식 변환 성공")
        except Exception as e:
            print(f"❌ '{column_name.upper()}' 날짜 형식 변환 오류: {e}")
            raise
    else:
        raise KeyError(f"❌ '{column_name.upper()}' 컬럼이 존재하지 않습니다.")


# ✅ CONFIDENCE 구간별 분류 함수
def categorize_confidence(df):
    bins = [0, 30, 50, 60, 100]
    labels = ["0-30", "31-50", "51-60", "61-100"]
    df['CONFIDENCE_BIN'] = pd.cut(df['CONFIDENCE'], bins=bins, labels=labels, right=True, include_lowest=True)
    print("✅ CONFIDENCE 값을 구간별로 분류 완료")
    return df


# ✅ 백테스트 함수 (다중 n, m 지원)
# ✅ 백테스트 함수 (최적화 버전)
def backtest_upbit_data(conn, engine, n_minutes_list, m_percent_list):
    try:
        query = """
            SELECT MARKET, CANDLE_DATE_TIME_KST, CONFIDENCE, OPINION, 
                   CANDLE_ACC_TRADE_PRICE, CANDLE_ACC_TRADE_VOLUME
            FROM UPBIT_MINUTE_DATA_2
        """
        df = pd.read_sql(query, con=engine)

        # ✅ 컬럼 이름을 대문자로 변환 후 날짜 형식 검증
        validate_date_column(df, 'CANDLE_DATE_TIME_KST')

        # ✅ CONFIDENCE 구간별 분류
        df = categorize_confidence(df)

        df.sort_values(by='CANDLE_DATE_TIME_KST', inplace=True)

        # ✅ 가격 데이터 한 번에 조회
        start_time = df['CANDLE_DATE_TIME_KST'].min()
        end_time = df['CANDLE_DATE_TIME_KST'].max() + timedelta(minutes=max(n_minutes_list))

        price_query = f"""
            SELECT MARKET, CANDLE_DATE_TIME_KST, CANDLE_ACC_TRADE_PRICE
            FROM UPBIT_MINUTE_DATA_2
            WHERE CANDLE_DATE_TIME_KST BETWEEN TO_TIMESTAMP('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
            AND TO_TIMESTAMP('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
        """
        price_data = pd.read_sql(price_query, con=engine)
        price_data.columns = [col.upper() for col in price_data.columns]
        price_data['CANDLE_DATE_TIME_KST'] = pd.to_datetime(price_data['CANDLE_DATE_TIME_KST'])

        # ✅ 결과 저장용 리스트
        results = []
        analysis_date = datetime.now().date()

        # ✅ 최적화된 반복 구조
        for n_minutes in n_minutes_list:
            for m_percent in m_percent_list:
                print(f"\n🚀 [시간 창: {n_minutes}분 | 변동률: {m_percent}%] 백테스트 시작 🚀")
                
                df['END_TIME'] = df['CANDLE_DATE_TIME_KST'] + timedelta(minutes=n_minutes)
                merged = pd.merge(
                    df, 
                    price_data, 
                    left_on=['MARKET', 'END_TIME'], 
                    right_on=['MARKET', 'CANDLE_DATE_TIME_KST'], 
                    suffixes=('', '_END'), 
                    how='left'
                )

                merged['PRICE_CHANGE'] = (merged['CANDLE_ACC_TRADE_PRICE_END'] - merged['CANDLE_ACC_TRADE_PRICE']) / merged['CANDLE_ACC_TRADE_PRICE'] * 100
                
                # ✅ 성공 여부 판별
                merged['SUCCESS'] = (
                    (merged['OPINION'].str.upper() == 'LONG') & (merged['PRICE_CHANGE'] >= m_percent)
                ) | (
                    (merged['OPINION'].str.upper() == 'SHORT') & (merged['PRICE_CHANGE'] <= -m_percent)
                )
                
                summary = merged.groupby(['MARKET', 'CONFIDENCE_BIN', 'OPINION']).agg(
                    TOTAL_CASES=('SUCCESS', 'size'),
                    SUCCESS_CASES=('SUCCESS', 'sum')
                ).reset_index()

                summary['SUCCESS_RATE'] = (summary['SUCCESS_CASES'] / summary['TOTAL_CASES'] * 100).fillna(0)
                summary['TIME_WINDOW_MIN'] = n_minutes
                summary['PERCENT_THRESHOLD'] = m_percent
                summary['ANALYSIS_DATE'] = analysis_date

                results.append(summary)

        # ✅ 결과 병합 및 일괄 저장
        final_results = pd.concat(results, ignore_index=True)
        
        with engine.begin() as transaction_conn:
            for _, row in final_results.iterrows():
                transaction_conn.execute(text("""
                    INSERT INTO BACK_TESTING_2 (
                        MARKET, CONFIDENCE_BIN, OPINION, TIME_WINDOW_MIN, PERCENT_THRESHOLD,
                        SUCCESS_RATE, TOTAL_CASES, SUCCESS_CASES, ANALYSIS_DATE
                    ) VALUES (
                        :market, :confidence_bin, :opinion, :time_window, :percent_threshold,
                        :success_rate, :total_cases, :success_cases, :analysis_date
                    )
                """), {
                    'market': row['MARKET'],
                    'confidence_bin': row['CONFIDENCE_BIN'],
                    'opinion': row['OPINION'].upper(),
                    'time_window': row['TIME_WINDOW_MIN'],
                    'percent_threshold': row['PERCENT_THRESHOLD'],
                    'success_rate': row['SUCCESS_RATE'],
                    'total_cases': row['TOTAL_CASES'],
                    'success_cases': row['SUCCESS_CASES'],
                    'analysis_date': row['ANALYSIS_DATE']
                })

        print("✅ 모든 백테스트 완료 및 결과 저장 성공.")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        conn.close()
        engine.dispose()


# ✅ 메인 실행부
if __name__ == "__main__":
    try:
        connection, engine = get_db_connection()

        # ✅ 테이블 확인 및 생성
        create_backtesting_table(engine)

        n_minutes_list = list(map(int, input("분 단위 시간 창(n분)을 입력하세요 (예: 5,15,30): ").split(',')))
        m_percent_list = list(map(float, input("변동률 임계값(m%)을 입력하세요 (예: 1.5,2.0,2.5): ").split(',')))

        backtest_upbit_data(connection, engine, n_minutes_list, m_percent_list)

    except Exception as e:
        print(f"❌ 메인 프로세스 오류: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
        if 'engine' in locals() and engine:
            engine.dispose()
