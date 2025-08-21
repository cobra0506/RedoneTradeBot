import websocket
import json

def on_message(ws, message):
    print("Received:", message)

def on_open(ws):
    print("Connected")
    ws.send(json.dumps({"op": "subscribe", "args": ["kline.1.BTCUSDT"]}))

ws = websocket.WebSocketApp("wss://stream.bybit.com/v5/public/linear", on_open=on_open, on_message=on_message)
ws.run_forever()