import pandas as pd
import numpy as np

def generate_trade_signal(df):
    """
    각 지표를 기반으로 Short, Long, Hold 신호와 그 강도를 생성합니다.
    
    Parameters:
    df (pd.DataFrame): 기술 지표가 포함된 DataFrame. 필수 컬럼은 'EMA_15', 'EMA_360',
                       'VWAP', 'BALL_HIGH', 'BALL_LOW', 'RSI_360', 'STO_K', 'STO_D'입니다.
    
    Returns:
    pd.DataFrame: 원본 DataFrame에 'Signal'과 'Confidence' 컬럼이 추가된 DataFrame.
                  'Signal'은 'Long', 'Short', 'Hold' 중 하나이며,
                  'Confidence'는 신호의 강도를 퍼센트(%)로 나타냅니다.
    
    Raises:
    ValueError: 필수 컬럼이 데이터프레임에 존재하지 않을 경우.
    """
    # 필수 컬럼 존재 여부 확인
    required_columns = {'EMA_15', 'EMA_360', 'VWAP', 'BALL_HIGH', 'BALL_LOW', 'RSI_360', 'STO_K', 'STO_D'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"입력 데이터에 다음 필수 컬럼이 누락되었습니다: {missing}")
    
    # 가중치 설정
    weights = {
        'EMA': 0.4,
        'Bollinger': 0.3,
        'RSI': 0.2,
        'Stochastic': 0.1
    }
    
    # EMA 신호 계산
    ema_long_signal = np.where(df['EMA_15'] > df['EMA_360'], 1, 
                               np.where(df['EMA_15'] < df['EMA_360'], -1, 0))
    ema_score = ema_long_signal * weights['EMA']
    
    # 볼린저 밴드 신호 계산
    bollinger_signal = np.where(df['VWAP'] > df['BALL_HIGH'], -1, 
                                np.where(df['VWAP'] < df['BALL_LOW'], 1, 0))
    bollinger_score = bollinger_signal * weights['Bollinger']
    
    # RSI 신호 계산
    rsi_signal = np.where(df['RSI_360'] > 70, -1, 
                          np.where(df['RSI_360'] < 30, 1, 0))
    rsi_score = rsi_signal * weights['RSI']
    
    # 스토캐스틱 신호 계산
    stochastic_signal = np.where(df['STO_K'] > df['STO_D'], 1, 
                                 np.where(df['STO_K'] < df['STO_D'], -1, 0))
    stochastic_score = stochastic_signal * weights['Stochastic']
    
    # 전체 점수 계산
    df['Score'] = ema_score + bollinger_score + rsi_score + stochastic_score
    
    # 신호 결정
    conditions = [
        df['Score'] > 0.49,
        df['Score'] < -0.49
    ]
    choices = ['Long', 'Short']
    df['Signal'] = np.select(conditions, choices, default='Hold')
    
    # 신호 강도 계산 (절대값을 퍼센트로 변환)
    df['Confidence'] = df['Score'].abs() * 100  # 신호 강도는 0에서 100%
    
    # 신호가 'Hold'인 경우 강도를 0으로 설정 (선택 사항)
    df.loc[df['Signal'] == 'Hold', 'Confidence'] = 0
    
    # 불필요한 'Score' 컬럼 제거 (원하는 경우)
    df.drop(columns=['Score'], inplace=True)
    
    return df
