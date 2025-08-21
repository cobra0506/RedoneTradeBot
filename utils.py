import os
import json
import threading
import logging
import logging.handlers
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import csv
from collections import Counter
import global_data
from global_data import POSITION_FILE  # Assume 'positions.json'
from copy import deepcopy
from indicators import calc_atr, calc_adx, calc_sma

# Ensure logs directory
os.makedirs('logs', exist_ok=True)

def setup_logging():
    logger = logging.getLogger('TradingBot')
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Console handler (WARNING+)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)  # Fixed: addHandler

    # bot.log (INFO+ , rotate 5MB, 3 backups)
    bot_log_handler = logging.handlers.RotatingFileHandler(
        os.path.join('logs', 'bot.log'),
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    bot_log_handler.setLevel(logging.INFO)
    bot_log_handler.setFormatter(formatter)
    logger.addHandler(bot_log_handler)  # Fixed

    # error.log (WARNING+, rotate 2MB, 5 backups)
    error_log_handler = logging.handlers.RotatingFileHandler(
        os.path.join('logs', 'error.log'),
        maxBytes=2*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_log_handler.setLevel(logging.WARNING)
    error_log_handler.setFormatter(formatter)
    logger.addHandler(error_log_handler)  # Fixed

    # debug.log (DEBUG, rotate 1MB, 2 backups)
    debug_log_handler = logging.handlers.RotatingFileHandler(
        os.path.join('logs', 'debug.log'),
        maxBytes=1*1024*1024,
        backupCount=2,
        encoding='utf-8'
    )
    debug_log_handler.setLevel(logging.DEBUG)
    debug_log_handler.setFormatter(formatter)
    logger.addHandler(debug_log_handler)  # Fixed

    logger.info("Logging setup complete")
    return logger

# Initialize logger
logger = setup_logging()

# CSV initialization
signals_filepath = os.path.join('logs', 'signals_log.csv')
with open(signals_filepath, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'symbol', 'signal', 'price'])

opened_positions_filepath = os.path.join('logs', 'opened_positions.csv')
with open(opened_positions_filepath, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'symbol', 'action', 'price', 'amount_usd'])

def convert_timestamp_to_readable(timestamp_milliseconds):
    timestamp_seconds = timestamp_milliseconds / 1000
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def write_candle_data_to_csv(candle_data, output_dir="logs/candle_data"):
    os.makedirs(output_dir, exist_ok=True)
    for symbol, intervals in candle_data.items():
        for interval, candles in intervals.items():
            filepath = os.path.join(output_dir, f"{symbol}-{interval}.csv")
            with global_data.symbol_locks[symbol]:
                unique_candles = {c['timestamp']: c for c in candles}
                sorted_candles = sorted(unique_candles.values(), key=lambda c: c['timestamp'])[-50:]  # Latest 50, overwrite old
            with open(filepath, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['symbol', 'interval', 'timestamp_utc', 'open', 'high', 'low', 'close', 'volume'])
                for candle in sorted_candles:
                    writer.writerow([
                        symbol,
                        interval,
                        convert_timestamp_to_readable(candle['timestamp']),
                        candle['open'],
                        candle['high'],
                        candle['low'],
                        candle['close'],
                        candle.get('volume', 0)
                    ])

def add_candle_uniquely(candle_deque, new_candle, interval_minutes):
    interval_ms = interval_minutes * 60 * 1000
    for i, existing in enumerate(candle_deque):
        if existing['timestamp'] == new_candle['timestamp']:
            existing.update(new_candle)
            return
        if new_candle['timestamp'] < existing['timestamp']:
            candle_deque.insert(i, new_candle)
            return
    candle_deque.append(new_candle)
    # Enforce max 50 (discard oldest if exceeded, though deque does this)
    if len(candle_deque) > 50:
        candle_deque.popleft()

def validate_candle(candle):
    try:
        o, h, l, c, v = candle['open'], candle['high'], candle['low'], candle['close'], candle.get('volume', 0)
        is_valid = l <= o <= h and l <= c <= h and v >= 0 and all(isinstance(x, (int, float)) for x in [o, h, l, c, v])
        if not is_valid:
            logger.warning(f"Invalid candle data: {candle}")
        return is_valid
    except (KeyError, TypeError) as e:
        logger.error(f"Invalid candle format: {e}, candle: {candle}")
        return False

def log_signal(symbol, signal_type, price):
    filepath = os.path.join('logs', 'signals_log.csv')
    try:
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp, symbol, signal_type, price])
            logger.info(f"Logged signal: {symbol}, {signal_type}, {price}")
    except Exception as e:
        logger.error(f"Failed to log signal: {e}")

def log_opened_position(symbol, action, price, amount_usd):
    filepath = os.path.join('logs', 'opened_positions.csv')
    try:
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp, symbol, action, price, amount_usd])
            logger.info(f"Logged position: {symbol}, {action}, {price}, {amount_usd}")
    except Exception as e:
        logger.error(f"Failed to log position: {e}")

# GUI popup helper (simple; call from main to update error_label)
def show_error_gui(message):
    logger.error(message)
    # In main.py, update self.error_label.text = message (already in the GUI code)

# Moved from strategy_runner.py to break circular import
def select_top_symbols(num_symbols):
    snapshot = get_data_snapshot()
    scores = []
    for symbol in global_data.symbols:
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

def get_data_snapshot():
    snapshot = {}
    for symbol in global_data.symbols:
        with global_data.symbol_locks[symbol]:
            snapshot[symbol] = deepcopy(global_data.candle_data.get(symbol, {}))
    return snapshot