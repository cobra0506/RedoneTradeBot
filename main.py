from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.dropdown import DropDown
from kivy.clock import Clock
import threading
import time
import os
import atexit
from collections import deque
from data_handler import get_symbols, BybitWebSocketManager, fetch_historical_data
from strategy_runner import run_strategy_loop
from backtester import backtest
from utils import logger, setup_logging, write_candle_data_to_csv
import global_data
from global_data import config

# Initialize logging
logger = setup_logging()

# Startup: Load symbols, init data
global_data.symbols = get_symbols()
logger.info(f"Total symbols fetched: {len(global_data.symbols)}")
global_data.candle_data = {symbol: {tf: deque(maxlen=global_data.candle_limit) for tf in global_data.time_frames} for symbol in global_data.symbols}
global_data.symbol_health = {symbol: 0 for symbol in global_data.symbols}
global_data.symbol_locks = {symbol: threading.Lock() for symbol in global_data.symbols}

class ControlPanel(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(orientation='vertical', padding=10, spacing=10, **kwargs)
        
        # Mode dropdown
        mode_dropdown = DropDown()
        for m in ['backtest', 'paper', 'live']:
            btn = Button(text=m, size_hint_y=None, height=44)
            btn.bind(on_release=lambda btn: self.set_mode(btn.text))
            mode_dropdown.add_widget(btn)
        mode_btn = Button(text=global_data.mode, size_hint=(0.3, None), height=40)
        mode_btn.bind(on_release=mode_dropdown.open)
        self.add_widget(mode_btn)
        
        # Strategy dropdown
        strategy_dropdown = DropDown()
        for s in ['srsi', 'grid']:
            btn = Button(text=s, size_hint_y=None, height=44)
            btn.bind(on_release=lambda btn: self.set_strategy(btn.text))
            strategy_dropdown.add_widget(btn)
        strategy_btn = Button(text=global_data.selected_strategy, size_hint=(0.3, None), height=40)
        strategy_btn.bind(on_release=strategy_dropdown.open)
        self.add_widget(strategy_btn)
        
        # Toggle button
        self.toggle_button = Button(text="Start" if global_data.run_strategy else "Stop", size_hint=(None, None), size=(120, 40))
        self.toggle_button.bind(on_press=self.toggle_strategy)
        self.add_widget(self.toggle_button)
        
        # Balance label and input
        self.balance_label = Label(text=f"Balance: ${global_data.current_balance:.2f}", halign="right")
        self.add_widget(self.balance_label)
        self.balance_input = TextInput(text=str(global_data.current_balance), multiline=False, size_hint_y=None, height=40)
        self.add_widget(self.balance_input)
        apply_btn = Button(text="Apply Balance", size_hint=(None, None), size=(120, 40))
        apply_btn.bind(on_press=self.apply_balance)
        self.add_widget(apply_btn)
        
        # Backtest options (hidden until backtest mode)
        self.backtest_panel = BoxLayout(orientation='vertical', size_hint_y=None, height=0)
        self.range_input = TextInput(text="30", hint_text="Days for backtest", multiline=False, size_hint_y=None, height=40)
        self.optimize_check = Button(text="Optimize? No", size_hint_y=None, height=40)
        self.optimize_check.bind(on_press=self.toggle_optimize)
        self.backtest_panel.add_widget(self.range_input)
        self.backtest_panel.add_widget(self.optimize_check)
        self.add_widget(self.backtest_panel)
        
        # Log viewer (simple label, updates with last 5 lines)
        self.log_label = Label(text="Logs:", valign='top', halign='left', text_size=(self.width, None))
        self.add_widget(self.log_label)
        
        # Position list (simple text)
        self.position_label = Label(text="Positions: None", valign='top', halign='left')
        self.add_widget(self.position_label)
        
        # Error popup (simple label that turns red on error)
        self.error_label = Label(text="", color=(1,0,0,1))
        self.add_widget(self.error_label)
        
        # Periodic UI update
        Clock.schedule_interval(self.update_ui, 1)

    def set_mode(self, m):
        global_data.mode = m
        self.backtest_panel.height = 100 if m == 'backtest' else 0  # Show/hide
        logger.info(f"Mode set to {m}")

    def set_strategy(self, s):
        global_data.selected_strategy = s
        logger.info(f"Strategy set to {s}")

    def toggle_strategy(self, instance):
        global_data.run_strategy = not global_data.run_strategy
        self.toggle_button.text = "Stop" if global_data.run_strategy else "Start"
        if global_data.mode == 'backtest' and global_data.run_strategy:
            days = int(self.range_input.text or 30)
            optimize = "Yes" in self.optimize_check.text
            threading.Thread(target=backtest, args=(global_data.selected_strategy, days, optimize), daemon=True).start()

    def apply_balance(self, instance):
        try:
            value = float(self.balance_input.text)
            if value >= 0:
                global_data.current_balance = value
                logger.info(f"Balance updated to {value}")
            else:
                self.error_label.text = "Value must be non-negative"
        except ValueError:
            self.error_label.text = "Invalid number entered"

    def toggle_optimize(self, instance):
        current = "Yes" if "No" in instance.text else "No"
        instance.text = f"Optimize? {current}"

    def update_ui(self, dt):
        self.balance_label.text = f"Balance: ${global_data.current_balance:.2f}"
        # Update logs (last 5 lines from bot.log)
        try:
            with open('logs/bot.log', 'r') as f:
                logs = f.readlines()[-5:]
                self.log_label.text = "Logs:\n" + ''.join(logs)
        except:
            self.log_label.text = "Logs: N/A"
        # Update positions
        pos_text = "Positions:\n" + '\n'.join([f"{s}: {p['side']} @ {p['entry']}" for s, p in global_data.positions.items()])
        self.position_label.text = pos_text or "Positions: None"
        # Check for errors (poll error.log last line)
        try:
            with open('logs/error.log', 'r') as f:
                last_error = f.readlines()[-1].strip() if os.path.exists('logs/error.log') else ""
                self.error_label.text = last_error if last_error else ""
        except:
            self.error_label.text = ""

class TradingBotApp(App):
    def build(self):
        return ControlPanel()

# WebSocket manager instance
ws_manager = BybitWebSocketManager()

def periodic_csv_dump(interval=60):
    while True:
        time.sleep(interval)
        write_candle_data_to_csv(global_data.candle_data)

def monitor_connection():
    while True:
        if time.time() - ws_manager.last_message_time > 60:
            logger.warning("WS disconnect detected. Restarting...")
            ws_manager.hard_recovery()
        time.sleep(5)

if __name__ == '__main__':
    # Startup sequence
    atexit.register(ws_manager.stop)
    
    # Start WebSocket and wait for connect
    threading.Thread(target=ws_manager.start, args=(global_data.symbols, global_data.time_frames), daemon=True).start()
    time.sleep(2)  # Wait 2s or until pong (add check if needed)
    
    # Fetch historical after WS
    threading.Thread(target=fetch_historical_data, daemon=True).start()
    
    # Start periodic tasks
    threading.Thread(target=periodic_csv_dump, daemon=True).start()
    threading.Thread(target=monitor_connection, daemon=True).start()
    threading.Thread(target=run_strategy_loop, daemon=True).start()
    
    TradingBotApp().run()