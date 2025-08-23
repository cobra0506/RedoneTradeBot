import time

from utils import logger
from exchange_handler import place_smart_order, close_position, get_market_price
import global_data
from global_data import mode, config, positions
from risk import trailing_sl


def apply_simulation_adjustments(price, side):
    # Simulate latency and slippage
    time.sleep(config['defaults']['latency_ms'] / 1000)
    slippage = config['defaults']['slippage_pct']
    adjusted_price = price * (1 + slippage if side in ['Buy', 'long'] else 1 - slippage)
    return adjusted_price


def update_pnl(symbol, current_price):
    pos = positions.get(symbol)
    if pos:
        if pos['side'] == 'long':
            pos['pnl'] = (current_price - pos['entry']) * pos['size']
        else:
            pos['pnl'] = (pos['entry'] - current_price) * pos['size']


def check_sl_tp(symbol, current_price):
    pos = positions.get(symbol)
    if not pos:
        return False

    if (
        (pos['side'] == 'long' and (current_price <= pos['sl'] or current_price >= pos['tp'])) or
        (pos['side'] == 'short' and (current_price >= pos['sl'] or current_price <= pos['tp']))
    ):
        close_func = close_long if pos['side'] == 'long' else close_short
        close_func(symbol)
        return True

    # Trailing SL
    new_sl = trailing_sl(current_price, pos['entry'], pos['side'])
    if new_sl:
        pos['sl'] = new_sl
    return False


def open_long(symbol, amount, sl=None, tp=None):
    if len(positions) >= config['defaults']['max_positions']:
        logger.info(f"Ignoring open long for {symbol} - max positions reached")
        return False

    price = get_market_price(symbol)
    if not price:
        return False

    if mode == 'backtest':
        exec_price = apply_simulation_adjustments(price, 'Buy')
        fee = amount * exec_price * config['defaults']['fee_rate'] * 2  # entry + est exit
        global_data.current_balance -= fee
        positions[symbol] = {'side': 'long', 'entry': exec_price, 'size': amount, 'sl': sl, 'tp': tp, 'pnl': 0}
        logger.info(f"Simulated open long for {symbol} at {exec_price}")
        return True
    else:
        return place_smart_order(symbol, 'Buy', amount, sl=sl, tp=tp)


def open_short(symbol, amount, sl=None, tp=None):
    if len(positions) >= config['defaults']['max_positions']:
        logger.info(f"Ignoring open short for {symbol} - max positions reached")
        return False

    price = get_market_price(symbol)
    if not price:
        return False

    if mode == 'backtest':
        exec_price = apply_simulation_adjustments(price, 'Sell')
        fee = amount * exec_price * config['defaults']['fee_rate'] * 2
        global_data.current_balance -= fee
        positions[symbol] = {'side': 'short', 'entry': exec_price, 'size': amount, 'sl': sl, 'tp': tp, 'pnl': 0}
        logger.info(f"Simulated open short for {symbol} at {exec_price}")
        return True
    else:
        return place_smart_order(symbol, 'Sell', amount, sl=sl, tp=tp)


def close_long(symbol):
    pos = positions.get(symbol)
    if not pos or pos['side'] != 'long':
        return True

    price = get_market_price(symbol)
    if not price:
        return False

    if mode == 'backtest':
        exec_price = apply_simulation_adjustments(price, 'Sell')
        pnl = (exec_price - pos['entry']) * pos['size']
        fee = pos['size'] * exec_price * config['defaults']['fee_rate'] * 2
        global_data.current_balance += pnl - fee
        del positions[symbol]
        logger.info(f"Simulated close long for {symbol} at {exec_price}, PnL: {pnl}")
        return True
    else:
        return close_position(symbol, pos)


def close_short(symbol):
    pos = positions.get(symbol)
    if not pos or pos['side'] != 'short':
        return True

    price = get_market_price(symbol)
    if not price:
        return False

    if mode == 'backtest':
        exec_price = apply_simulation_adjustments(price, 'Buy')
        pnl = (pos['entry'] - exec_price) * pos['size']
        fee = pos['size'] * exec_price * config['defaults']['fee_rate'] * 2
        global_data.current_balance += pnl - fee
        del positions[symbol]
        logger.info(f"Simulated close short for {symbol} at {exec_price}, PnL: {pnl}")
        return True
    else:
        return close_position(symbol, pos)


def place_market(symbol, side, qty):
    if mode == 'backtest':
        logger.info(f"Simulated market {side} for {symbol} qty {qty}")
        return True
    else:
        return place_smart_order(symbol, side, qty * get_market_price(symbol))


def place_limit(symbol, side, qty, limit_price):
    if mode == 'backtest':
        logger.info(f"Simulated limit {side} for {symbol} qty {qty} at {limit_price}")
        return True
    else:
        logger.info(f"Placing limit {side} for {symbol} at {limit_price} (implement API if needed)")
        return True
