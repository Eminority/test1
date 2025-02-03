import pandas as pd

def cal_ball(data, window, num_std_dev):
    """
    볼린저 밴드를 계산합니다.

    Parameters:
    data (pd.Series): 입력 데이터 시리즈.
    window (int): 이동 평균을 계산할 윈도우 크기.
    num_std_dev (float): 표준 편차의 배수.

    Returns:
    tuple: 상단 밴드(pd.Series), 하단 밴드(pd.Series).
           데이터 길이가 window보다 짧으면 빈 시리즈를 반환합니다.
    """
    if len(data) < window:
        # 데이터 부족 시 빈 시리즈 반환
        return pd.Series(dtype=float), pd.Series(dtype=float)

    # 이동 평균과 표준 편차 계산 (표본 표준 편차 사용)
    rolling_mean = data.rolling(window=window, min_periods=window).mean()
    rolling_std = data.rolling(window=window, min_periods=window).std(ddof=1)  # ddof=1: 표본 기준 표준편차

    # 상단 밴드와 하단 밴드 계산
    upper_band = rolling_mean + (rolling_std * num_std_dev)
    lower_band = rolling_mean - (rolling_std * num_std_dev)

    return upper_band, lower_band
