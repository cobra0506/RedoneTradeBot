import concurrent.futures
import time
from copy import deepcopy
from utils import logger
import global_data
from strategies.srsi_strategy import SRSIStrategy
from strategies.grid_strategy import GridStrategy
from indicators import calc_atr, calc_adx, calc_sma
from orders import open_long, open_short, close_long, close_short
from global_data import symbols, candle_data, symbol_locks, run_strategy, mode, selected_strategy, positions, config
from backtester import backtest  # For mode check

def get_data_snapshot():
    snapshot = {}
    for symbol in symbols:
        with symbol_locks[symbol]:
            snapshot[symbol] = deepcopy(candle_data.get(symbol, {}))
    return snapshot

def select_top_symbols(num_symbols):
    snapshot = get_data_snapshot()
    scores = []
    for symbol in symbols:
        candles = snapshot.get(symbol, {}).get('15', [])  # Use 15m for volatility
        if len(candles) < 21:
            continue
        atr = calc_atr(candles)
        adx = calc_adx(candles)
        sma = calc_sma(candles, 21)
        current_price = candles[-1]['close']
        trend = 'up' if current_price > sma else 'down' if current_price < sma else 'flat'
        if trend == 'flat':
            continue  # Skip flat
        score = atr * adx if adx > 0 else 0
        scores.append((symbol, score, trend))
    # Sort by score descending, take top num_symbols with up/down trend
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)[:num_symbols]
    return [s[0] for s in sorted_scores]

def run_strategy_for_symbol(symbol, snapshot, strategy):
    market_data = {'symbol': symbol, 'candles_by_tf': snapshot.get(symbol, {})}
    signal = strategy.analyze(market_data, mode)
    if signal == 'HOLD' or len(positions) >= config['defaults']['max_positions']:
        return
    if signal['signal'] == 'OPEN_LONG':
        open_long(symbol, signal['amount'], signal['sl'], signal['tp'])
    elif signal['signal'] == 'OPEN_SHORT':
        open_short(symbol, signal['amount'], signal['sl'], signal['tp'])
    elif signal['signal'] == 'CLOSE_LONG':
        close_long(symbol)
    elif signal['signal'] == 'CLOSE_SHORT':
        close_short(symbol)

def run_strategy_loop(batch_size=50, max_workers=20):
    logger.info("Strategy loop started")
    global_data.strategy_running = True
    strategy_classes = {'srsi': SRSIStrategy, 'grid': GridStrategy}  # Factory
    while True:
        if not run_strategy:
            time.sleep(1)
            continue
        if mode == 'backtest':
            backtest(selected_strategy)
            global_data.run_strategy = False  # One-run for backtest
            continue
        try:
            start_time = time.time()
            snapshot = get_data_snapshot()
            num_symbols = config['strategies'].get(selected_strategy, {}).get('num_symbols', 5)
            selected = select_top_symbols(num_symbols)  # Dynamic reselect
            strategy = strategy_classes[selected_strategy]()  # Instantiate
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for i in range(0, len(selected), batch_size):
                    batch = selected[i:i + batch_size]
                    futures = [executor.submit(run_strategy_for_symbol, sym, snapshot, strategy) for sym in batch]
                    concurrent.futures.wait(futures)
            elapsed = time.time() - start_time
            logger.info(f"Strategy iteration completed in {elapsed:.2f}s")
            time.sleep(60 - (time.time() % 60) + 1)  # Sync to minute + buffer
        except Exception as e:
            logger.error(f"Strategy loop error: {e}")
            time.sleep(1)
    global_data.strategy_running = False
    logger.info("Strategy loop stopped")