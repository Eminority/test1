import pandas as pd

def cal_ema(data, period):

    # EMA 계산
    ema = data.ewm(span=period, adjust=False).mean()
    return ema
