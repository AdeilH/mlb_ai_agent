import google.generativeai as genai
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import json
from collections import defaultdict

# Set up your API key
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key=GOOGLE_API_KEY)

def generate_news_events(player_data, trading_hours=7):
    """Generate broader news events that could affect player value"""
    model = genai.GenerativeModel('gemini-pro')
    
    time.sleep(5)  # Rate limiting
    
    prompt = f"""
    Generate {trading_hours} realistic news events that could affect this baseball player's market value.
    Include team news, league updates, and general baseball market conditions.
    For each event, provide:
    1. Time (between 9:30 AM and 4:00 PM)
    2. News headline
    3. Detailed description
    4. Category (team, league, market, or personal)
    5. Sentiment (positive, negative, or neutral)
    
    Player: {player_data['name']}
    Position: {player_data['position']}
    
    Provide response in valid JSON array format exactly like this example:
    [
        {{
            "time": "10:30 AM",
            "headline": "Team Announces Lineup Changes",
            "description": "Manager confirms starting rotation adjustment",
            "category": "team",
            "sentiment": "neutral"
        }}
    ]
    
    Ensure all times are in HH:MM AM/PM format and sentiment is one of: positive, negative, neutral
    """
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Clean up common JSON formatting issues
        response_text = response_text.replace('\n', '')
        response_text = response_text.replace('```json', '').replace('```', '')
        # Remove 'JSON' prefix if present
        response_text = response_text.replace('JSON', '', 1)
        
        try:
            events = json.loads(response_text)
        except json.JSONDecodeError as je:
            print(f"JSON parsing error for {player_data['name']}: {je}")
            print(f"Raw response: {response_text}")
            return []
        
        # Validate event format
        valid_events = []
        for event in events:
            if all(k in event for k in ('time', 'headline', 'description', 'category', 'sentiment')):
                try:
                    # Ensure time format is correct
                    datetime.strptime(event['time'], '%I:%M %p')
                    # Ensure sentiment is valid
                    if event['sentiment'].lower() in ('positive', 'negative', 'neutral'):
                        valid_events.append(event)
                except ValueError as e:
                    print(f"Validation error for event: {event}")
                    continue
        
        return valid_events
    except Exception as e:
        print(f"Error generating news events for {player_data['name']}: {e}")
        return []

def generate_intraday_events(player_data, trading_hours=7):
    """Generate intraday events and price impacts for a player"""
    model = genai.GenerativeModel('gemini-pro')
    
    time.sleep(5)  # Rate limiting
    
    prompt = f"""
    Generate {trading_hours} realistic intraday events for this baseball player that could affect their stock price.
    For each event, provide:
    1. Time (between 9:30 AM and 4:00 PM)
    2. Event description
    3. Price impact (-3% to +3%)
    
    Player: {player_data['name']}
    Position: {player_data['position']}
    Current Price: ${player_data['current_price']}
    
    Provide response in valid JSON array format exactly like this example:
    [
        {{"time": "10:30 AM", "event": "Player announced as starting pitcher", "impact": 1.2}},
        {{"time": "11:45 AM", "event": "Team announces roster move", "impact": -0.8}}
    ]
    
    Ensure all times are in HH:MM AM/PM format and impacts are numbers between -3.0 and 3.0.
    """
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Clean up common JSON formatting issues
        response_text = response_text.replace('\n', '')
        response_text = response_text.replace('```json', '').replace('```', '')
        
        try:
            events = json.loads(response_text)
        except json.JSONDecodeError as je:
            print(f"JSON parsing error for {player_data['name']}: {je}")
            print(f"Raw response: {response_text}")
            return []
        
        # Validate event format
        valid_events = []
        for event in events:
            if all(k in event for k in ('time', 'event', 'impact')):
                try:
                    # Ensure time format is correct
                    datetime.strptime(event['time'], '%I:%M %p')
                    # Ensure impact is a number within range
                    impact = float(event['impact'])
                    if -3.0 <= impact <= 3.0:
                        valid_events.append(event)
                except (ValueError, TypeError) as e:
                    print(f"Validation error for event: {event}")
                    continue
        
        return valid_events
    except Exception as e:
        print(f"Error generating events for {player_data['name']}: {e}")
        return []

def calculate_intraday_prices(base_price, events):
    """Calculate intraday prices based on events"""
    prices = []
    current_price = base_price
    
    # Sort events by time
    events.sort(key=lambda x: datetime.strptime(x['time'], '%I:%M %p'))
    
    for event in events:
        impact = event['impact']
        new_price = current_price * (1 + impact/100)
        prices.append({
            'time': event['time'],
            'price': round(new_price, 2),
            'event': event['event'],
            'impact': f"{impact:+.2f}%"
        })
        current_price = new_price
    
    return prices

def generate_intraday_data():
    # Load base market data
    df = pd.read_csv('player_market_data.csv')
    
    # Only use first 20 players
    df = df.head(20)
    print(f"\nGenerating data for {len(df)} players:")
    for _, player in df.iterrows():
        print(f"- {player['name']} ({player['position']})")
    
    intraday_data = []
    news_events_data = defaultdict(list)
    
    # Process players in batches
    batch_size = 5
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} of {(len(df) + batch_size - 1) // batch_size}...")
        
        for _, player in batch.iterrows():
            events = generate_intraday_events(player)
            news = generate_news_events(player)
            
            if events:
                prices = calculate_intraday_prices(player['current_price'], events)
                
                for price_point in prices:
                    intraday_data.append({
                        'player_id': player['player_id'],
                        'name': player['name'],
                        'position': player['position'],
                        'time': price_point['time'],
                        'price': price_point['price'],
                        'event': price_point['event'],
                        'impact': price_point['impact']
                    })
            
            if news:
                news_events_data[player['player_id']].extend(news)
    
    # Create DataFrame and save to CSV
    intraday_df = pd.DataFrame(intraday_data)
    intraday_df.to_csv('player_intraday_data.csv', index=False)
    print("\nIntraday data generated and saved to player_intraday_data.csv")
    
    # Save news events to JSON
    news_events = {
        'generated_at': datetime.now().isoformat(),
        'events': dict(news_events_data)
    }
    with open('player_news_events.json', 'w') as f:
        json.dump(news_events, f, indent=2)
    print("\nNews events saved to player_news_events.json")
    
    # Display sample of the data
    print("\nSample of intraday data:")
    print(intraday_df.head(10))
    
    # Display sample of news events
    print("\nSample of news events:")
    first_player = list(news_events_data.keys())[0]
    print(json.dumps(news_events_data[first_player][:2], indent=2))

if __name__ == "__main__":
    generate_intraday_data() 