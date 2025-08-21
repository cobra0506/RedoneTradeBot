from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.dropdown import DropDown
from kivy.clock import Clock
from kivy.uix.gridlayout import GridLayout
from kivy.uix.scrollview import ScrollView
import threading
import time
import os
import atexit
import asyncio
from collections import deque
from data_handler import get_symbols, BybitWebSocketManager, fetch_historical_data
from strategy_runner import run_strategy_loop
from backtester import backtest
from utils import logger, setup_logging, write_candle_data_to_csv
import global_data
from global_data import config
import os
os.environ["KIVY_NO_CONSOLELOG"] = "1"  # Hide console logs
os.environ["KIVY_LOG_MODE"] = "PYTHON"  # Or "NONE" to fully suppress

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
        
        # Main grid for better alignment (2 columns)
        main_grid = GridLayout(cols=2, spacing=10, size_hint_y=None, height=200)
        
        # Mode label and dropdown
        main_grid.add_widget(Label(text="Mode:"))
        mode_dropdown = DropDown()
        for m in ['backtest', 'paper', 'live']:
            btn = Button(text=m, size_hint_y=None, height=44)
            btn.bind(on_release=lambda btn: self.set_mode(btn.text, mode_btn))
            mode_dropdown.add_widget(btn)
        mode_btn = Button(text=global_data.mode, size_hint=(1, None), height=40)
        mode_btn.bind(on_release=mode_dropdown.open)
        main_grid.add_widget(mode_btn)
        
        # Strategy label and dropdown
        main_grid.add_widget(Label(text="Strategy:"))
        strategy_dropdown = DropDown()
        for s in ['srsi', 'grid']:
            btn = Button(text=s, size_hint_y=None, height=44)
            btn.bind(on_release=lambda btn: self.set_strategy(btn.text, strategy_btn))
            strategy_dropdown.add_widget(btn)
        strategy_btn = Button(text=global_data.selected_strategy, size_hint=(1, None), height=40)
        strategy_btn.bind(on_release=strategy_dropdown.open)
        main_grid.add_widget(strategy_btn)
        
        # Toggle button
        main_grid.add_widget(Label(text="Control:"))
        self.toggle_button = Button(text="Start" if global_data.run_strategy else "Stop", size_hint=(1, None), height=40)
        self.toggle_button.bind(on_press=self.toggle_strategy)
        main_grid.add_widget(self.toggle_button)
        
        # Balance label and input
        main_grid.add_widget(Label(text="Balance:"))
        self.balance_label = Label(text=f"${global_data.current_balance:.2f}", halign="left")
        main_grid.add_widget(self.balance_label)
        main_grid.add_widget(Label(text="Set Balance:"))
        self.balance_input = TextInput(text=str(global_data.current_balance), multiline=False, size_hint=(1, None), height=40)
        main_grid.add_widget(self.balance_input)
        apply_btn = Button(text="Apply", size_hint=(1, None), height=40)
        apply_btn.bind(on_press=self.apply_balance)
        main_grid.add_widget(apply_btn)
        
        self.add_widget(main_grid)
        self.add_widget(Label(size_hint_y=0.2))  # Spacer
        # Backtest options (hidden until backtest mode)
        self.backtest_panel = GridLayout(cols=2, spacing=10, size_hint_y=None, height=0)
        self.backtest_panel.add_widget(Label(text="Days:"))
        self.range_input = TextInput(text="30", multiline=False, size_hint=(1, None), height=40)
        self.backtest_panel.add_widget(self.range_input)
        self.backtest_panel.add_widget(Label(text="Optimize:"))
        self.optimize_check = Button(text="No", size_hint=(1, None), height=40)
        self.optimize_check.bind(on_press=self.toggle_optimize)
        self.backtest_panel.add_widget(self.optimize_check)
        self.add_widget(self.backtest_panel)
        
        # Log viewer (with ScrollView for scaling)
        self.add_widget(Label(size_hint_y=0.3))  # Spacer
        log_scroll = ScrollView(size_hint=(1, 0.4))
        self.log_label = Label(text="Logs:", valign='top', halign='left', size_hint_y=None)
        self.log_label.bind(texture_size=self.log_label.setter('size'))
        log_scroll.add_widget(self.log_label)
        self.add_widget(log_scroll)
        
        # Position list (with ScrollView)
        pos_scroll = ScrollView(size_hint=(1, 0.3))
        self.position_label = Label(text="Positions: None", valign='top', halign='left', size_hint_y=None)
        self.position_label.bind(texture_size=self.position_label.setter('size'))
        pos_scroll.add_widget(self.position_label)
        self.add_widget(pos_scroll)
        
        # Error label
        self.error_label = Label(text="", color=(1,0,0,1), size_hint_y=0.1)
        self.add_widget(self.error_label)
        
        # Periodic UI update
        Clock.schedule_interval(self.update_ui, 1)

    def set_mode(self, m, btn):
        global_data.mode = m
        btn.text = m
        self.backtest_panel.height = 100 if m == 'backtest' else 0
        logger.info(f"Mode set to {m}")

    def set_strategy(self, s, btn):
        global_data.selected_strategy = s
        btn.text = s
        logger.info(f"Strategy set to {s}")

    def toggle_strategy(self, instance):
        global_data.run_strategy = not global_data.run_strategy
        instance.text = "Stop" if global_data.run_strategy else "Start"
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
        instance.text = current

    def update_ui(self, dt):
        self.balance_label.text = f"${global_data.current_balance:.2f}"
        # Update logs (last 5 lines from bot.log)
        try:
            with open('logs/bot.log', 'r') as f:
                logs = f.readlines()[-5:]
                self.log_label.text = "Logs:\n" + ''.join(logs)
        except:
            self.log_label.text = "Logs: N/A"
        # Update positions
        pos_text = "Positions:\n" + '\n'.join([f"{s}: {p['side']} @ {p['entry']}" for s, p in global_data.positions.items()])
        self.position_label.text = pos_text or "None"
        # Check for errors (poll last error.log line)
        try:
            with open('logs/error.log', 'r') as f:
                last_error = f.readlines()[-1].strip() if os.path.getsize('logs/error.log') > 0 else ""
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

async def monitor_connection(ws_manager):
    while True:
        if time.time() - ws_manager.last_message_time > 120:  # Increased to 120s
            logger.warning("WS disconnect detected. Restarting...")
            await ws_manager.hard_recovery()
        await asyncio.sleep(5)

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
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, lambda: loop.run_until_complete(monitor_connection(ws_manager)))  # Run async in thread
    threading.Thread(target=run_strategy_loop, daemon=True).start()
    
    TradingBotApp().run()