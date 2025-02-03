import websockets
import asyncio
import json
import requests
import google.generativeai as genai
import os
import re

# Configure Gemini API
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-pro')

# Store player events and market data
player_data = {}
# Map player names to symbols
player_symbol_map = {}

async def market_data_listener():
    uri = "ws://localhost:3030/ws/market"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("Connected to market WebSocket")
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        if data["type"] == "market_data":
                            player = data["data"]["player"]
                            symbol = data["data"]["symbol"]
                            # Add to player-symbol map if not already present
                            if player not in player_symbol_map:
                                player_symbol_map[player] = symbol
                            if player not in player_data:
                                player_data[player] = {"market": [], "events": []}
                            player_data[player]["market"].append(data["data"])
                            # print(f"Market update for {player}: {data['data']}")
                            await analyze_and_trade(player)
                    except websockets.exceptions.ConnectionClosed:
                        print("Market WebSocket connection closed, reconnecting...")
                        break
                    except Exception as e:
                        print(f"Error in market WebSocket: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to market WebSocket: {e}")
        await asyncio.sleep(5)  # Wait before reconnecting

async def event_data_listener():
    uri = "ws://localhost:3030/ws/events"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("Connected to events WebSocket")
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        if data["type"] == "player_event":
                            player = data["data"]["player"]
                            if player not in player_data:
                                player_data[player] = {"market": [], "events": []}
                            player_data[player]["events"].append(data["data"])
                            # print(f"Event update for {player}: {data['data']}")
                            await analyze_and_trade(player)
                    except websockets.exceptions.ConnectionClosed:
                        print("Events WebSocket connection closed, reconnecting...")
                        break
                    except Exception as e:
                        print(f"Error in events WebSocket: {e}")
                        break
        except Exception as e:
            print(f"Failed to connect to events WebSocket: {e}")
        await asyncio.sleep(5)  # Wait before reconnecting

async def analyze_and_trade(player):
    # Check if there are at least 3 events and market data exists
    if len(player_data[player]["events"]) >= 3 and len(player_data[player]["market"]) > 0:
        events = player_data[player]["events"][-3:]  # Get last 3 events
        market_data = player_data[player]["market"][-1]  # Get latest market data

        # Prepare prompt for Gemini
        prompt = f"""
        Analyze these events for {player} and suggest a trading action:
        Events:
        {json.dumps(events, indent=2)}
        Latest Market Data:
        {json.dumps(market_data, indent=2)}
        Should we buy, sell, or hold? Provide reasoning and suggest:
        1. Order type (market or limit)
        2. Quantity
        3. If limit order, suggest price
        """

        # Get Gemini's advice
        response = model.generate_content(prompt)
        print(f"\n=== Gemini's Analysis for {player} ===")
        print(f"📊 Analysis:\n{response.text}")
        print("=" * 40)
        # Sleep for 5 seconds after Gemini request
        await asyncio.sleep(5)

        # Parse Gemini's response and place an order
        if "buy" in response.text.lower() or "sell" in response.text.lower():
            order_type = "limit" if "limit" in response.text.lower() else "market"
            side = "buy" if "buy" in response.text.lower() else "sell"
            
            # Extract quantity and price from Gemini's response
            quantity = 100  # Default quantity
            price = None  # Default price for market orders
            
            # Try to parse quantity and price from response
            try:
                # Look for quantity in response (e.g., "buy 200 shares")
                quantity_match = re.search(r'(\d+)\s*shares', response.text.lower())
                if quantity_match:
                    quantity = int(quantity_match.group(1))
                
                # Look for price in response (e.g., "at $150.50")
                price_match = re.search(r'at\s*\$?(\d+\.?\d*)', response.text.lower())
                if price_match:
                    price = float(price_match.group(1))
                elif order_type == "limit":
                    print("⚠️ No price found in Gemini's response for limit order, using latest market price")
                    price = market_data["price"]  # Use latest market price as fallback
            except (ValueError, AttributeError):
                print("⚠️ Could not parse quantity or price from Gemini's response, using defaults")
            
            place_order(player, side, quantity, order_type, price)
            # Sleep for 5 seconds after placing an order
            await asyncio.sleep(5)

def place_order(player, side, quantity, order_type="market", price=None):
    order = {
        "symbol": player_symbol_map.get(player, player),  # Use mapped symbol or fallback to player name
        "order_type": order_type,
        "side": side,
        "quantity": quantity,
        "price": price,  # Always include price, even if None
        "username": "agent1"
    }
    print(f"📤 Sending order: {order}")  # Debug: Print the order payload
    response = requests.post(
        "http://localhost:3030/order",
        headers={"Content-Type": "application/json"},
        data=json.dumps(order)
    )
    if response.status_code == 201:
        if order_type == "market":
            print(f"✅ Market order placed for {player}: {side} {quantity} shares")
        else:
            print(f"✅ Limit order placed for {player}: {side} {quantity} shares at ${price:.2f}")
    else:
        print(f"❌ Failed to place order: {response.text}, {response.status_code}")

async def main():
    await asyncio.gather(
        market_data_listener(),
        event_data_listener()
    )

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main()) 