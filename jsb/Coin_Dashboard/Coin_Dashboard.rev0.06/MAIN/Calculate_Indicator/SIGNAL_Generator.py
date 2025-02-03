import pandas as pd
import numpy as np

def generate_trade_signal(df):
    """
    각 지표를 '연속형'으로 계산하면서도,
    전통적으로 임계 구간에서 의미가 큰 지표들은
    추가 가중치(Adaptive Weight)를 줘서
    최종 Score와 Confidence를 보다 유의미하게 산출합니다.
    
    [가중치 수정 버전]
      - EMA:   0.3
      - Boll:  0.2
      - RSI:   0.3
      - Stoch: 0.2
    """
    # --------------------------------------------------
    # 1) 필수 컬럼 존재 여부 확인
    # --------------------------------------------------
    required_columns = {
        'EMA_15', 'EMA_360',
        'VWAP', 'BALL_HIGH', 'BALL_LOW',
        'RSI_360',
        'STO_K', 'STO_D'
    }
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"입력 데이터에 다음 필수 컬럼이 누락되었습니다: {missing}")
    
    # --------------------------------------------------
    # 2) 가중치 (수정)
    # --------------------------------------------------
    base_weights = {
        'EMA': 0.3,         # 기존 0.4 -> 0.3
        'Bollinger': 0.2,   # 기존 0.3 -> 0.2
        'RSI': 0.3,         # 기존 0.2 -> 0.3
        'Stochastic': 0.2   # 기존 0.1 -> 0.2
    }
    
    # --------------------------------------------------
    # 3) "연속형" 지표 계산 (이전과 동일)
    # --------------------------------------------------
    
    # (A) EMA 연속 신호
    df['ema_ratio'] = (df['EMA_15'] - df['EMA_360']) / df['EMA_360']
    df['ema_signal'] = df['ema_ratio'].clip(-1, 1)
    
    # (B) Bollinger 신호
    df['BALL_MID'] = (df['BALL_HIGH'] + df['BALL_LOW']) / 2.0
    df['bollinger_ratio'] = (df['VWAP'] - df['BALL_MID']) / (df['BALL_HIGH'] - df['BALL_MID'])
    df['bollinger_signal'] = df['bollinger_ratio'].clip(-1, 1)
    
    # (C) RSI 신호
    df['rsi_signal'] = (50 - df['RSI_360']) / 50
    df['rsi_signal'] = df['rsi_signal'].clip(-1, 1)
    
    # (D) Stochastic
    df['stoch_diff'] = df['STO_K'] - df['STO_D']
    df['stochastic_signal'] = (df['stoch_diff'] / 100).clip(-1, 1)
    
    # --------------------------------------------------
    # 4) 이벤트 구간(임계 구간)에서의 가중치 강화
    # --------------------------------------------------
    
    # (A) RSI: 과매수/과매도 상태에서 가중치 2배
    def rsi_adaptive_factor(rsi_value):
        if rsi_value < 30 or rsi_value > 70:
            return 2.0
        else:
            return 1.0
    
    # (B) Bollinger: 밴드 상/하단 벗어나면 가중치 2배
    def bollinger_adaptive_factor(row):
        if row['VWAP'] > row['BALL_HIGH'] or row['VWAP'] < row['BALL_LOW']:
            return 2.0
        else:
            return 1.0
    
    # (C) EMA 이벤트 (골든/데드 크로스 보너스)
    df['ema_prev_diff'] = (df['EMA_15'].shift(1) - df['EMA_360'].shift(1))
    df['ema_curr_diff'] = (df['EMA_15'] - df['EMA_360'])
    
    def ema_event_bonus(row):
        if (row['ema_prev_diff'] <= 0) and (row['ema_curr_diff'] > 0):
            # 골든크로스
            return +0.3
        elif (row['ema_prev_diff'] >= 0) and (row['ema_curr_diff'] < 0):
            # 데드크로스
            return -0.3
        else:
            return 0.0
    
    # 행별 적용
    df['rsi_factor'] = df['RSI_360'].apply(rsi_adaptive_factor)
    df['boll_factor'] = df.apply(bollinger_adaptive_factor, axis=1)
    df['ema_event_score'] = df.apply(ema_event_bonus, axis=1)
    
    # --------------------------------------------------
    # 5) 서브 점수 계산
    # --------------------------------------------------
    df['ema_score'] = (df['ema_signal'] 
                       * base_weights['EMA']
                       + df['ema_event_score'])
    
    df['bollinger_score'] = (df['bollinger_signal']
                             * base_weights['Bollinger']
                             * df['boll_factor'])
    
    df['rsi_score'] = (df['rsi_signal']
                       * base_weights['RSI']
                       * df['rsi_factor'])
    
    df['stochastic_score'] = (df['stochastic_signal']
                              * base_weights['Stochastic'])
    
    # --------------------------------------------------
    # 6) 최종 Score
    # --------------------------------------------------
    df['Score'] = (df['ema_score']
                   + df['bollinger_score']
                   + df['rsi_score']
                   + df['stochastic_score'])
    
    # --------------------------------------------------
    # 7) Signal 결정
    #    (조금 더 부드럽게: 임계값 ±0.3)
    # --------------------------------------------------
    conditions = [
        df['Score'] >  0.3,
        df['Score'] < -0.3
    ]
    choices = ['Long', 'Short']
    df['Signal'] = np.select(conditions, choices, default='Hold')
    
    # --------------------------------------------------
    # 8) Confidence
    # --------------------------------------------------
    df['Confidence'] = df['Score'].abs() * 100
    df.loc[df['Signal'] == 'Hold', 'Confidence'] = 0
    
    # --------------------------------------------------
    # 9) 필요시 정리
    # --------------------------------------------------
    drop_cols = [
        'ema_ratio', 'ema_signal',
        'bollinger_ratio', 'bollinger_signal',
        'rsi_signal',
        'stoch_diff', 'stochastic_signal',
        'ema_prev_diff', 'ema_curr_diff',
        'ema_event_score',
        'rsi_factor', 'boll_factor',
        'ema_score', 'bollinger_score', 'rsi_score', 'stochastic_score',
        'BALL_MID'
    ]
    df.drop(columns=drop_cols, inplace=True)
    
    return df
