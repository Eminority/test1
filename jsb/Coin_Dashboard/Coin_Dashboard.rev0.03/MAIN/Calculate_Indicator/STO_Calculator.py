import pandas as pd
import numpy as np

def cal_sto(data, period=14, smooth_k=3, smooth_d=3):
    """
    스토캐스틱 오실레이터(Stochastic Oscillator)를 계산합니다.

    Parameters:
    data (pd.DataFrame): 가격 데이터가 포함된 DataFrame. 필수 컬럼은 'LOW_PRICE', 'HIGH_PRICE', 'TRADE_PRICE'입니다.
    period (int, optional): 스토캐스틱 계산 기간. 기본값은 14입니다.
    smooth_k (int, optional): %K의 스무딩을 위한 이동평균 기간. 기본값은 3입니다.
    smooth_d (int, optional): %D의 스무딩을 위한 이동평균 기간. 기본값은 3입니다.

    Returns:
    pd.DataFrame: 'STO_K'와 'STO_D' 컬럼이 포함된 DataFrame.

    Raises:
    ValueError: 필수 컬럼이 데이터프레임에 존재하지 않을 경우.
    """
    # 필수 컬럼 존재 여부 확인
    required_columns = {'LOW_PRICE', 'HIGH_PRICE', 'TRADE_PRICE'}
    if not required_columns.issubset(data.columns):
        missing = required_columns - set(data.columns)
        raise ValueError(f"입력 데이터에 다음 필수 컬럼이 누락되었습니다: {missing}")

    # 최저가와 최고가 계산
    low_min = data['LOW_PRICE'].rolling(window=period, min_periods=period).min()
    high_max = data['HIGH_PRICE'].rolling(window=period, min_periods=period).max()
    
    # %K 계산 (0으로 나누기 방지)
    denominator = high_max - low_min
    denominator = denominator.replace(0, np.nan)  # 0을 NaN으로 대체하여 나누기 방지
    raw_k = 100 * ((data['TRADE_PRICE'] - low_min) / denominator)
    
    # %K 스무딩
    sto_k = raw_k.rolling(window=smooth_k, min_periods=1).mean()
    
    # %D 계산 (%K의 이동평균)
    sto_d = sto_k.rolling(window=smooth_d, min_periods=1).mean()
    
    # 결과를 새로운 DataFrame으로 반환
    stochastic_df = pd.DataFrame({
        'STO_K': sto_k,
        'STO_D': sto_d
    }, index=data.index)
    
    return stochastic_df
