import pandas as pd
import numpy as np
from utils import logger

def calc_rsi(candles, period=14, return_series=False):
    if len(candles) < period:
        logger.warning("Insufficient data for RSI")
        return None
    closes = pd.Series([c['close'] for c in candles])
    delta = closes.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    if return_series:
        return rsi
    return rsi.iloc[-1]

def calc_stoch_rsi(candles, period=14, k=3, d=3, return_series=False):
    rsi = calc_rsi(candles, period, return_series=True)
    if rsi is None:
        return None
    stoch = (rsi - rsi.rolling(period).min()) / (rsi.rolling(period).max() - rsi.rolling(period).min())
    k_line = stoch.rolling(k).mean() * 100
    d_line = k_line.rolling(d).mean()
    if return_series:
        return k_line
    return k_line.iloc[-1], d_line.iloc[-1]

def calc_sma(candles, period=9):
    if len(candles) < period:
        return None
    closes = pd.Series([c['close'] for c in candles])
    return closes.rolling(window=period).mean().iloc[-1]

def calculate_adx(candles, period=14):
    if len(candles) < period + 1:
        logger.debug(f"Not enough candles ({len(candles)}) for ADX calculation")
        return None

    df = pd.DataFrame(candles)
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')

    # True Range (TR)
    df['prev_close'] = df['close'].shift()
    df['tr'] = df[['high', 'prev_close']].max(axis=1) - df[['low', 'prev_close']].min(axis=1)

    # Directional Movement (+DM, -DM)
    df['up_move'] = df['high'] - df['high'].shift()
    df['down_move'] = df['low'].shift() - df['low']

    df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
    df['minus_dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)

    # Wilder’s smoothing
    atr = df['tr'].ewm(alpha=1/period, min_periods=period).mean()
    plus_di = 100 * df['plus_dm'].ewm(alpha=1/period, min_periods=period).mean() / atr
    minus_di = 100 * df['minus_dm'].ewm(alpha=1/period, min_periods=period).mean() / atr

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.ewm(alpha=1/period, min_periods=period).mean()

    return adx.iloc[-1] if not np.isnan(adx.iloc[-1]) else None

def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        logger.debug(f"Not enough candles ({len(candles)}) for ATR calculation")
        return None

    df = pd.DataFrame(candles)
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')

    df['prev_close'] = df['close'].shift()
    df['tr'] = df[['high', 'prev_close']].max(axis=1) - df[['low', 'prev_close']].min(axis=1)

    atr = df['tr'].rolling(window=period).mean()
    return atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else None