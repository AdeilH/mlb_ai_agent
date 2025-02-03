import websockets
import asyncio
import json

async def market_data_listener():
    uri = "ws://localhost:3030/ws/market"
    
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        try:
            while True:
                message = await websocket.recv()
                if message.__contains__("Jonah"):
                    print(message)
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")

async def event_data_listener():
    uri = "ws://localhost:3030/ws/events"
    
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        try:
            while True:
                message = await websocket.recv()
                if message.__contains__("Jonah"):
                    print(message)
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(market_data_listener()) 
    # asyncio.get_event_loop().run_until_complete(event_data_listener())