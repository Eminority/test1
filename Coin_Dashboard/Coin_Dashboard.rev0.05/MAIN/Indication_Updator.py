import pandas as pd
import pytz
import cx_Oracle
from Calculate_Indicator.Indactors_Pacakge import cal_ema, cal_ball, cal_rsi, cal_sto, generate_trade_signal
from Manage_DB.DB_Managing import get_db_connection, fetch_data
from Manage_DB.Add_missing_columns import add_missing_columns
import datetime

def process_indicators_update(conn, table_name):
    try:
        # 필요한 컬럼과 데이터 타입 정의
        required_columns = {
            'EMA_15': 'NUMBER',
            'EMA_360': 'NUMBER',
            'BALL_HIGH': 'NUMBER',
            'BALL_LOW': 'NUMBER',
            'RSI_360': 'NUMBER',
            'STO_K': 'NUMBER',
            'STO_D': 'NUMBER',
            'OPINION': 'VARCHAR2(26)',
            'CONFIDENCE': 'NUMBER'
        }

        # 누락된 컬럼 추가
        add_missing_columns(conn, table_name, required_columns)

        # 데이터 조회 (필요한 열만 명시적으로 지정)
        query = f"""
            SELECT MARKET, CANDLE_DATE_TIME_UTC, HIGH_PRICE, LOW_PRICE, TRADE_PRICE, 
                   CANDLE_ACC_TRADE_VOLUME, CANDLE_ACC_TRADE_PRICE
            FROM {table_name}
            ORDER BY CANDLE_DATE_TIME_UTC
        """
        df = fetch_data(query, conn)

        if df.empty:
            print("테이블에 데이터가 없어 지표를 계산할 수 없습니다.")
            return

        # 결측값 처리
        if df.isnull().any().any():
            print("결측값이 존재합니다. 결측값 처리 중...")
            df.dropna(subset=['TRADE_PRICE', 'CANDLE_ACC_TRADE_VOLUME'], inplace=True)

        # VWAP 계산
        df['VWAP'] = df['CANDLE_ACC_TRADE_PRICE'] / df['CANDLE_ACC_TRADE_VOLUME']

        # 종목별로 데이터 처리
        grouped = df.groupby('MARKET')
        result_list = []

        for name, group in grouped:
            print(f"{name}에 대한 지표 계산 중...")

            # 지표 계산
            group['EMA_15'] = cal_ema(group['VWAP'], 112)
            group['EMA_360'] = cal_ema(group['VWAP'], 224)

            if len(group['VWAP']) >= 224:
                group['BALL_HIGH'], group['BALL_LOW'] = cal_ball(group['VWAP'], window=224, num_std_dev=2)
            else:
                group['BALL_HIGH'], group['BALL_LOW'] = pd.NA, pd.NA

            group['RSI_360'] = cal_rsi(group['VWAP'], period=14)

            sto_df = cal_sto(group[['HIGH_PRICE', 'LOW_PRICE', 'TRADE_PRICE']], period=224, smooth_k=14, smooth_d=3)
            group['STO_K'] = sto_df['STO_K']
            group['STO_D'] = sto_df['STO_D']

            group = generate_trade_signal(group)
            group['OPINION'] = group['Signal']
            group['CONFIDENCE'] = group['Confidence']

            # NaN 값 처리
            group.fillna({
                'EMA_15': group['VWAP'].iloc[0] if len(group) > 0 else 0,
                'EMA_360': group['VWAP'].iloc[0] if len(group) > 0 else 0,
                'BALL_HIGH': group['VWAP'].max() if len(group) > 0 else float('nan'),
                'BALL_LOW': group['VWAP'].min() if len(group) > 0 else float('nan'),
                'RSI_360': 50,
                'STO_K': 50,
                'STO_D': 50
            }, inplace=True)

            result_list.append(group)

        # 종목별 계산 결과 병합
        df = pd.concat(result_list, ignore_index=True)

        # 누락된 컬럼 체크
        required_update_cols = ['EMA_15', 'EMA_360', 'BALL_HIGH', 'BALL_LOW', 
                                'RSI_360', 'STO_K', 'STO_D', 'OPINION', 
                                'CANDLE_DATE_TIME_UTC', 'MARKET', 'CONFIDENCE']
        missing_columns = set(required_update_cols) - set(df.columns)
        if missing_columns:
            raise ValueError(f"DataFrame에 필요한 컬럼이 없습니다: {missing_columns}")

        # 업데이트 쿼리
        update_query = f"""
            UPDATE {table_name}
            SET EMA_15 = :ema15, EMA_360 = :ema360, BALL_HIGH = :ball_high, 
                BALL_LOW = :ball_low, RSI_360 = :rsi360, STO_K = :sto_k, STO_D = :sto_d,
                OPINION = :opinion, CONFIDENCE = :confidence
            WHERE CANDLE_DATE_TIME_UTC = :candle_date_time_utc
              AND MARKET = :market
        """

        # 일괄 업데이트 함수
        def execute_batch_update(query, df, conn):
            cursor = conn.cursor()
            data = [
                {
                    'ema15': row['EMA_15'],
                    'ema360': row['EMA_360'],
                    'ball_high': row['BALL_HIGH'],
                    'ball_low': row['BALL_LOW'],
                    'rsi360': row['RSI_360'],
                    'sto_k': row['STO_K'],
                    'sto_d': row['STO_D'],
                    'opinion': row['OPINION'],
                    'confidence': row['CONFIDENCE'],
                    'candle_date_time_utc': row['CANDLE_DATE_TIME_UTC'],
                    'market': row['MARKET']
                }
                for _, row in df.iterrows()
            ]
            cursor.executemany(query, data)
            conn.commit()
            cursor.close()

        # 업데이트 실행
        execute_batch_update(update_query, df, conn)


        latest_data = df.loc[df['CANDLE_DATE_TIME_UTC'] == df['CANDLE_DATE_TIME_UTC'].max()]


        for _, row in latest_data.iterrows():

        # 현재 시간 출력
        # 한국 표준시(KST) 설정
            kst = pytz.timezone('Asia/Seoul')
        current_time = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")
        print(f"지표 업데이트 성공. 업데이트 시간: {current_time}")
        print(f"MARKET: {row['MARKET']}, CANDLE_DATE_TIME_KST: {row['CANDLE_DATE_TIME_UTC']}, OPINION: {row['OPINION']}, CONFIDENCE: {row['CONFIDENCE']}")

    except Exception as e:
        print(f"오류 발생: {e}")
    # finally에서 conn.close() 호출하지 않음
