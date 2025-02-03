import pandas as pd
import numpy as np

def cal_rsi(data, period=120):
    """
    RSI(Relative Strength Index)를 계산합니다.

    Parameters:
    data (pd.Series): 가격 데이터 시리즈.
    period (int, optional): RSI를 계산할 기간. 기본값은 360입니다.

    Returns:
    pd.Series: 계산된 RSI 값 시리즈. 데이터가 부족한 경우 가능한 범위 내에서 계산됩니다.
    
    Notes:
    - RSI는 0에서 100 사이의 값을 가지며, 일반적으로 70 이상은 과매수, 30 이하는 과매도로 간주됩니다.
    - 데이터가 부족한 경우, 초기 RSI 값은 덜 정확할 수 있습니다.
    """
    # 가격 변화량 계산
    delta = data.diff()
    
    # 상승분과 하락분 분리
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # 평균 상승과 평균 하락 계산 (단순 이동 평균)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    # RS(Relative Strength) 계산
    rs = avg_gain / avg_loss

    # RSI 계산
    rsi = 100 - (100 / (1 + rs))

    # 평균 손실이 0인 경우 RSI를 100으로 설정 (완전 과매수)
    rsi = np.where(avg_loss == 0, 100, rsi)
    # 평균 상승이 0인 경우 RSI를 0으로 설정 (완전 과매도)
    rsi = np.where(avg_gain == 0, 0, rsi)

    # RSI를 pandas Series로 변환
    rsi = pd.Series(rsi, index=data.index)

    return rsi