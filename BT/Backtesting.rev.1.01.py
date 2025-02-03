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


# ✅ BACK_TESTING 테이블 생성
def create_backtesting_table(engine):
    metadata = MetaData()
    back_testing = Table(
        'BACK_TESTING', metadata,
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
    print("✅ BACK_TESTING 테이블이 생성되었거나 이미 존재합니다.")


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

        # ✅ 현재 날짜
        analysis_date = datetime.now().date()

        for n_minutes in n_minutes_list:
            for m_percent in m_percent_list:
                print(f"\n🚀 [시간 창: {n_minutes}분 | 변동률: {m_percent}%] 백테스트 시작 🚀")

                # ✅ 시간 범위 데이터를 한 번에 가져오기
                start_time = df['CANDLE_DATE_TIME_KST'].min()
                end_time = df['CANDLE_DATE_TIME_KST'].max() + timedelta(minutes=n_minutes)

                price_query = f"""
                    SELECT MARKET, CANDLE_DATE_TIME_KST, CANDLE_ACC_TRADE_PRICE
                    FROM UPBIT_MINUTE_DATA_2
                    WHERE CANDLE_DATE_TIME_KST BETWEEN TO_TIMESTAMP('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    AND TO_TIMESTAMP('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                """
                price_data = pd.read_sql(price_query, con=engine)
                price_data.columns = [col.upper() for col in price_data.columns]
                price_data['CANDLE_DATE_TIME_KST'] = pd.to_datetime(price_data['CANDLE_DATE_TIME_KST'])

                for market in df['MARKET'].unique():
                    market_df = df[df['MARKET'] == market]
                    for confidence_bin in df['CONFIDENCE_BIN'].unique():
                        bin_df = market_df[market_df['CONFIDENCE_BIN'] == confidence_bin]
                        for opinion in df['OPINION'].unique():
                            opinion_df = bin_df[bin_df['OPINION'] == opinion]

                            success_cases = 0
                            total_cases = 0

                            for _, row in opinion_df.iterrows():
                                start_time = row['CANDLE_DATE_TIME_KST']
                                end_time = start_time + timedelta(minutes=n_minutes)

                                filtered_prices = price_data[
                                    (price_data['MARKET'] == market) &
                                    (price_data['CANDLE_DATE_TIME_KST'] >= start_time) &
                                    (price_data['CANDLE_DATE_TIME_KST'] <= end_time)
                                ]

                                if not filtered_prices.empty:
                                    start_price = row['CANDLE_ACC_TRADE_PRICE']
                                    end_price = filtered_prices.iloc[-1]['CANDLE_ACC_TRADE_PRICE']
                                    price_change = (end_price - start_price) / start_price * 100

                                    if (opinion.upper() == 'LONG' and price_change >= m_percent) or \
                                       (opinion.upper() == 'SHORT' and price_change <= -m_percent):
                                        success_cases += 1

                                    total_cases += 1

                            success_rate = (success_cases / total_cases * 100) if total_cases > 0 else 0

                            with engine.begin() as transaction_conn:
                                transaction_conn.execute(text("""
                                    INSERT INTO BACK_TESTING (
                                        MARKET, CONFIDENCE_BIN, OPINION, TIME_WINDOW_MIN, PERCENT_THRESHOLD,
                                        SUCCESS_RATE, TOTAL_CASES, SUCCESS_CASES, ANALYSIS_DATE
                                    ) VALUES (
                                        :market, :confidence_bin, :opinion, :time_window, :percent_threshold,
                                        :success_rate, :total_cases, :success_cases, :analysis_date
                                    )
                                """), {
                                    'market': market,
                                    'confidence_bin': confidence_bin,
                                    'opinion': opinion.upper(),
                                    'time_window': n_minutes,
                                    'percent_threshold': m_percent,
                                    'success_rate': success_rate,
                                    'total_cases': total_cases,
                                    'success_cases': success_cases,
                                    'analysis_date': analysis_date
                                })

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
