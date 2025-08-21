import concurrent.futures
import time
from copy import deepcopy
from utils import logger, select_top_symbols, get_data_snapshot  # Moved select_top_symbols here
from indicators import *  # If needed
from orders import open_long, open_short, close_long, close_short
import global_data
from global_data import symbols, candle_data, symbol_locks, run_strategy, mode, selected_strategy, positions, config
from backtester import backtest  # For mode check
from strategies.srsi_strategy import SRSIStrategy
from strategies.grid_strategy import GridStrategy

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