import threading
from collections import deque
import json

POSITION_FILE = 'positions.json'  # JSON file for persisting positions

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

demo = True  # Toggle demo/live (overridden by mode)
time_frames = config['defaults']['time_frames']
symbols = ['BTCUSDT']  # Filled dynamically
candle_data = {}  # symbol -> tf -> deque(maxlen=candle_limit)
symbol_locks = {}  # symbol -> threading.Lock
ws_connected = False
run_strategy = False
strategy_running = False
current_balance = config['defaults']['start_balance']
mode = 'paper'  # backtest/paper/live
selected_strategy = 'srsi'  # Default
symbol_health = {}  # symbol: error_count
candle_limit = config['defaults']['candle_limit']
positions = {}  # symbol: {'side': 'long/short', 'entry': price, 'size': amount, 'sl': sl, 'tp': tp}
balance_offset = 0  # For paper mode adjustment