import pandas as pd
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

        # 데이터 조회
        query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY KOREAN_NAME, CANDLE_DATE_TIME_UTC desc
        """
        df = fetch_data(query, conn)

        if df.empty:
            print("테이블에 데이터가 없어 지표를 계산할 수 없습니다.")
            return

        # 거래량 0 이상인 데이터만 사용 (거래량 0인 경우 VWAP 계산 불가)
        df = df[df['CANDLE_ACC_TRADE_VOLUME'] > 0].copy()
        if df.empty:
            print("거래량이 0을 초과하는 데이터가 없어 지표 계산 불가.")
            return

        # VWAP 계산
        df['VWAP'] = df['CANDLE_ACC_TRADE_PRICE'] / df['CANDLE_ACC_TRADE_VOLUME']

        # EMA 계산 (VWAP 기준)
        df['EMA_15'] = cal_ema(df['VWAP'], 5)
        df['EMA_360'] = cal_ema(df['VWAP'], 20)

        # 볼린저 밴드 계산 (VWAP 기준, 99기간)
        if len(df) >= 20:
            df['BALL_HIGH'], df['BALL_LOW'] = cal_ball(df['VWAP'], window=20, num_std_dev=2)
        else:
            df['BALL_HIGH'] = float('nan')
            df['BALL_LOW'] = float('nan')

        # RSI 계산 (VWAP 기준, 5기간)
        df['RSI_360'] = cal_rsi(df['VWAP'], period=5)

        # 스토캐스틱 계산 (TRADE_PRICE 기준, 99기간)
        sto_df = cal_sto(df[['HIGH_PRICE', 'LOW_PRICE', 'TRADE_PRICE']], period=20, smooth_k=3, smooth_d=3)
        df['STO_K'] = sto_df['STO_K']
        df['STO_D'] = sto_df['STO_D']

        # 신호, 신호 강세 계산
        df = generate_trade_signal(df)
        df['OPINION'] = df['Signal']
        df['CONFIDENCE'] = df['Confidence']

        # NaN 값 처리
        # - EMA_15, EMA_360이 NaN이면 VWAP의 첫 값으로 대체
        # - BALL_HIGH, BALL_LOW가 NaN이면 VWAP의 max/min으로 대체 (데이터가 충분할 경우 자연스레 채워질 것)
        # - RSI_360, STO_K, STO_D가 NaN이면 초기값 50
        df.fillna({
            'EMA_15': df['VWAP'].iloc[0] if len(df) > 0 else 0,
            'EMA_360': df['VWAP'].iloc[0] if len(df) > 0 else 0,
            'BALL_HIGH': df['VWAP'].max() if len(df) > 0 else float('nan'),
            'BALL_LOW': df['VWAP'].min() if len(df) > 0 else float('nan'),
            'RSI_360': 50,
            'STO_K': 50,
            'STO_D': 50
        }, inplace=True)

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

        # 현재 시간 출력
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"지표 업데이트 성공. 업데이트 시간: {current_time}")

    except Exception as e:
        print(f"오류 발생: {e}")
    # finally에서 conn.close() 호출하지 않음
