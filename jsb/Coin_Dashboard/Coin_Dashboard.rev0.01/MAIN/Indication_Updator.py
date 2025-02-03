import pandas as pd
import cx_Oracle
from Calculate_Indicator.BALL_Calculator import *
from Calculate_Indicator.RSI_Calculator import *
from Calculate_Indicator.EMA_Calculator import *
from Calculate_Indicator.SIGNAL_Generator import *
from Calculate_Indicator.STO_Calculator import *

from Manage_DB.DB_Managing import *
from Manage_DB.Add_missing_columns import *

import datetime  # datetime 모듈 임포트

def process_indicators_update(conn):
    conn = None
    try:
        # DB 연결
        conn = get_db_connection()

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
        add_missing_columns(conn, 'K_REAL_TIME_2', required_columns)

        # 데이터 조회
        query = """
            SELECT *
            FROM K_REAL_TIME_2
            ORDER BY KOREAN_NAME, CANDLE_DATE_TIME_UTC
        """
        df = fetch_data(query, conn)
        
        # VWAP 계산
        df['VWAP'] = df['CANDLE_ACC_TRADE_PRICE'] / df['CANDLE_ACC_TRADE_VOLUME']
        
        # EMA 계산 (VWAP 기준)
        df['EMA_15'] = cal_ema(df['VWAP'], 5)
        df['EMA_360'] = cal_ema(df['VWAP'], 120)

        # 볼린저 밴드 계산 (VWAP 기준, 360분 기준)
        df['BALL_HIGH'], df['BALL_LOW'] = cal_ball(df['VWAP'], window=120, num_std_dev=2)

        # RSI 계산 (VWAP 기준, 360분)
        df['RSI_360'] = cal_rsi(df['VWAP'], period=5)

        # 스토캐스틱 계산 (TRADE_PRICE 기준)
        sto_df = cal_sto(df[['HIGH_PRICE', 'LOW_PRICE', 'TRADE_PRICE']], period=120, smooth_k=3, smooth_d=3)
        df['STO_K'] = sto_df['STO_K']
        df['STO_D'] = sto_df['STO_D']
        
        # 신호, 신호 강세 계산
        df = generate_trade_signal(df)
        df['OPINION'] = df['Signal']
        df['CONFIDENCE'] = df['Confidence']
        
        # NaN 값 처리
        df.fillna({
            'EMA_15': df['VWAP'].iloc[0],
            'EMA_360': df['VWAP'].iloc[0],
            'BALL_HIGH': df['VWAP'].max(),
            'BALL_LOW': df['VWAP'].min(),
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

        # 데이터 업데이트 쿼리
        update_query = """
            UPDATE K_REAL_TIME_2
            SET EMA_15 = :ema15, EMA_360 = :ema360, BALL_HIGH = :ball_high, 
                BALL_LOW = :ball_low, RSI_360 = :rsi360, STO_K = :sto_k, STO_D = :sto_d,
                OPINION = :opinion, CONFIDENCE = :confidence
            WHERE CANDLE_DATE_TIME_UTC = :candle_date_time_utc
              AND MARKET = :market
        """

        # 업데이트 실행 함수
        def execute_update(query, df, conn):
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(query, {
                    'ema15': row['EMA_15'],
                    'ema360': row['EMA_360'],
                    'ball_high': row['BALL_HIGH'],
                    'ball_low': row['BALL_LOW'],
                    'rsi360': row['RSI_360'],
                    'sto_k': row['STO_K'],
                    'sto_d': row['STO_D'],
                    'opinion': row['OPINION'],
                    'candle_date_time_utc': row['CANDLE_DATE_TIME_UTC'],
                    'market': row['MARKET'],
                    'confidence' : row['CONFIDENCE']
                })
            conn.commit()

        # 업데이트 실행
        execute_update(update_query, df, conn)

        # 현재 시간 가져오기
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"EMA, 볼린저 밴드, RSI, 스토캐스틱 및 매매 신호(OPINION)가 성공적으로 업데이트되었습니다. 업데이트 시간: {current_time}")

    except Exception as e:
        print(f"오류 발생: {e}")

    finally:
        if conn:
            conn.close()
