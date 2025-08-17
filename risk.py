from indicators import calc_atr
from global_data import current_balance
from exchange_handler import get_position_info  # For open PnL

def get_position_size(method='percent', value=1, symbol=None, price=0):
    balance = current_balance
    if symbol:
        pos = get_position_info(symbol)
        balance += pos.get('pnl', 0)  # Include open PnL
    if method == 'percent':
        return balance * (value / 100)
    elif method == 'flat':
        return value / price if price else 0
    return 0

def set_sl_tp(entry, side, method='atr', mult=2, candles=None):
    if method == 'atr' and candles:
        atr = calc_atr(candles)
        if atr:
            if side == 'long':
                sl = entry - mult * atr
                tp = entry + mult * 1.5 * atr
            else:
                sl = entry + mult * atr
                tp = entry - mult * 1.5 * atr
            return sl, tp
    # % fallback
    if side == 'long':
        return entry * 0.975, entry * 1.05
    return entry * 1.025, entry * 0.95

def trailing_sl(current_price, entry, side, distance_pct=1):
    if side == 'long' and current_price > entry * (1 + distance_pct / 100):
        return current_price * (1 - distance_pct / 100)
    if side == 'short' and current_price < entry * (1 - distance_pct / 100):
        return current_price * (1 + distance_pct / 100)
    return None