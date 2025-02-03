import asyncio
import json
import websockets
import datetime

class TradingClient:
    def __init__(self, uri="ws://localhost:3030/ws"):
        self.uri = uri
        self.market_data = {}
        self.news_events = []

    async def connect(self):
        """Connect to the WebSocket server and handle messages."""
        async with websockets.connect(self.uri) as websocket:
            print(f"Connected to {self.uri}")
            try:
                while True:
                    message = await websocket.recv()
                    await self.handle_message(json.loads(message))
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")

    async def handle_message(self, message):
        """Handle different types of messages."""
        msg_type = message.get('type')
        data = message.get('data')

        if msg_type == 'market_data':
            await self.handle_market_data(data)
        elif msg_type == 'news_event':
            await self.handle_news_event(data)

    async def handle_market_data(self, data):
        """Process market data updates."""
        symbol = data.get('symbol')
        price = data.get('price')
        timestamp = data.get('timestamp')
        
        self.market_data[symbol] = {
            'price': price,
            'timestamp': timestamp,
            'updated_at': datetime.datetime.now().isoformat()
        }
        
        print(f"Market Data: {symbol} @ ${price:.2f} ({timestamp})")

    async def handle_news_event(self, data):
        """Process news event updates."""
        event = {
            'player_id': data.get('player_id'),
            'event_type': data.get('event_type'),
            'description': data.get('description'),
            'timestamp': data.get('timestamp'),
            'received_at': datetime.datetime.now().isoformat()
        }
        
        self.news_events.append(event)
        print(f"News Event: {event['event_type']} - {event['description']}")

    def get_latest_price(self, symbol):
        """Get the latest price for a symbol."""
        if symbol in self.market_data:
            return self.market_data[symbol]['price']
        return None

    def get_recent_news(self, limit=10):
        """Get recent news events."""
        return self.news_events[-limit:]

async def main():
    client = TradingClient()
    try:
        await client.connect()
    except KeyboardInterrupt:
        print("\nDisconnecting...")

if __name__ == "__main__":
    asyncio.run(main())