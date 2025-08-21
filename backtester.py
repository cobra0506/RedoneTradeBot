import optuna
import pandas as pd
import time
from copy import deepcopy
from utils import logger, select_top_symbols, get_data_snapshot  # Moved select_top_symbols here
from data_handler import get_historical_data
from orders import open_long, open_short, close_long, close_short  # Mode='backtest' sim
from global_data import config, symbols, time_frames, positions, current_balance

def simulate_time_step(strategy, all_candles, ts, selected_symbols):
    # Update 'current' candles for this timestamp across all symbols (forward-fill if missing)
    market_data = {}
    for symbol in selected_symbols:
        candles_by_tf = {}
        for tf in time_frames:
            tf_data = all_candles.get(symbol, {}).get(tf, [])
            # Find candles up to ts (simulate live feed)
            current_tf = [c for c in tf_data if c['timestamp'] <= ts][-config['defaults']['candle_limit']:]
            if not current_tf:
                continue  # Skip if no data
            candles_by_tf[tf] = current_tf
        market_data[symbol] = {'symbol': symbol, 'candles_by_tf': candles_by_tf}
    
    # Run strategy for each symbol, execute simulated orders
    for symbol in selected_symbols:
        if symbol not in market_data:
            continue
        signal = strategy.analyze(market_data[symbol], 'backtest')
        if signal == 'HOLD' or len(positions) >= config['defaults']['max_positions']:
            continue
        if signal['signal'] == 'OPEN_LONG':
            open_long(symbol, signal['amount'], signal['sl'], signal['tp'])
        elif signal['signal'] == 'OPEN_SHORT':
            open_short(symbol, signal['amount'], signal['sl'], signal['tp'])
        elif signal['signal'] == 'CLOSE_LONG':
            close_long(symbol)
        elif signal['signal'] == 'CLOSE_SHORT':
            close_short(symbol)
    
    # Update PnL for open positions (sim price movement)
    peak_balance = initial_balance = current_balance  # Track for drawdown
    for symbol in list(positions.keys()):
        if symbol not in market_data or '1' in market_data[symbol]['candles_by_tf']:
            continue
        current_price = market_data[symbol]['candles_by_tf']['1'][-1]['close']
        pos = positions[symbol]
        if pos['side'] == 'long':
            pos['pnl'] = (current_price - pos['entry']) * pos['size']
        else:
            pos['pnl'] = (pos['entry'] - current_price) * pos['size']
        # Check SL/TP hits
        if (pos['side'] == 'long' and (current_price <= pos['sl'] or current_price >= pos['tp'])) or \
           (pos['side'] == 'short' and (current_price >= pos['sl'] or current_price <= pos['tp'])):
            close_func = close_long if pos['side'] == 'long' else close_short
            close_func(symbol)
    
    # Update global metrics (example; expand as needed)
    current_total = current_balance + sum(p['pnl'] for p in positions.values() if 'pnl' in p)
    peak_balance = max(peak_balance, current_total)
    drawdown = (peak_balance - current_total) / peak_balance if peak_balance > 0 else 0

def backtest(strategy_name, range_days=30, optimize=False):
    logger.info(f"Starting backtest for {strategy_name} over {range_days} days, optimize={optimize}")
    # Fetch historical for test symbols (subset for speed)
    test_symbols = symbols[:50]  # Adjust as needed
    all_candles = {}
    end_time = int(time.time() * 1000)
    start_time = end_time - (range_days * 24 * 60 * 60 * 1000)
    for symbol in test_symbols:
        all_candles[symbol] = {}
        for tf in time_frames:
            all_candles[symbol][tf] = get_historical_data(symbol, tf, start_time=start_time, end_time=end_time)
    
    # Find unique timestamps aligned to 1m
    all_ts = sorted(set(ts for sym_data in all_candles.values() for tf_data in sym_data.values() for ts in [c['timestamp'] for c in tf_data]))
    min_interval = 60 * 1000  # 1m
    timestamps = [ts for ts in range(all_ts[0], all_ts[-1] + min_interval, min_interval) if ts in all_ts]  # Only existing ts
    
    # Reset state
    initial_balance = current_balance
    positions.clear()
    metrics = {'pnl': 0, 'wins': 0, 'trades': 0, 'max_drawdown': 0, 'peak_balance': initial_balance}
    
    # Simulate time-steps
    for ts in timestamps:
        selected = select_top_symbols(config['strategies'].get(strategy_name, {}).get('num_symbols', 5))
        simulate_time_step(strategy_name, all_candles, ts, selected)
        # Update aggregate metrics
        current_pnl = sum(p['pnl'] for p in positions.values() if 'pnl' in p)
        current_total = current_balance + current_pnl
        metrics['pnl'] = current_total - initial_balance
        metrics['peak_balance'] = max(metrics['peak_balance'], current_total)
        drawdown = (metrics['peak_balance'] - current_total) / metrics['peak_balance'] if metrics['peak_balance'] > 0 else 0
        metrics['max_drawdown'] = max(metrics['max_drawdown'], drawdown)
        # Win/trades (simplified; count positive PnL on close in close funcs and update metrics)

    if optimize:
        def objective(trial):
            # Suggest params based on strategy (example for srsi)
            if strategy_name == 'srsi':
                buy_thresh = trial.suggest_int('buy_threshold', 15, 25, step=5)
                sell_thresh = trial.suggest_int('sell_threshold', 75, 85, step=5)
                config['strategies']['srsi']['buy_threshold'] = buy_thresh
                config['strategies']['srsi']['sell_threshold'] = sell_thresh
            # Run backtest with these params (recursive call or mini-sim)
            temp_metrics = backtest(strategy_name, range_days, optimize=False)  # Mini-run
            pnl = temp_metrics['pnl']
            win_rate = temp_metrics['wins'] / temp_metrics['trades'] if temp_metrics['trades'] > 0 else 0
            max_drawdown = temp_metrics['max_drawdown'] or 1e-6  # Avoid division by zero
            score = (pnl * win_rate) / max_drawdown
            logger.info(f"Trial score: {score} (pnl={pnl}, win_rate={win_rate}, drawdown={max_drawdown})")
            return score
        
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=100)  # Efficient, <1hr for small n
        # Re-test top to avoid overfitting (run full backtest on best)
        best_params = study.best_params
        logger.info(f"Best params: {best_params}")
        final_metrics = backtest(strategy_name, range_days, optimize=False)  # Re-test with best
        # Rank and output top 10 (from study trials)
        top_trials = sorted(study.trials, key=lambda t: t.value, reverse=True)[:10]
        df = pd.DataFrame([{'trial': t.number, 'score': t.value, 'params': t.params, 'pnl': t.user_attrs.get('pnl', 0)} for t in top_trials])  # Add attrs if set in objective
        df.to_csv('logs/backtest_results.csv', index=False)
        logger.info("Optimization complete. Results in logs/backtest_results.csv")
    
    logger.info(f"Backtest complete. PnL: {metrics['pnl']}, Win Rate: {metrics['wins']/metrics['trades'] if metrics['trades'] else 0}, Max Drawdown: {metrics['max_drawdown']}")
    return metrics