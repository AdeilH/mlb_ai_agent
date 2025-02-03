import google.generativeai as genai
import json
import pandas as pd
import os
import time
from typing import List

# Set up your API key
# Or use environment variable (more secure):
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# Configure the library
genai.configure(api_key=GOOGLE_API_KEY)

def calculate_base_price(player_stats, position):
    """Calculate a base price based on recent stats"""
    if position == "P":
        # Pitcher valuation
        recent_stats = player_stats.get("2023", {})
        if not recent_stats:
            return 10.0  # Base price for players with no recent stats
            
        era = float(recent_stats.get("earnedRunAverage", "5.00"))
        whip = float(recent_stats.get("walksAndHitsPerInning", "1.50"))
        strikeouts = int(recent_stats.get("strikeouts", 0))
        
        # Basic formula for pitcher value
        base_price = (
            100 - (era * 5) +  # Lower ERA = higher value
            (strikeouts * 0.5) +  # More strikeouts = higher value
            (50 - (whip * 20))  # Lower WHIP = higher value
        )
    else:
        # Batter valuation
        recent_stats = player_stats.get("2023", {})
        if not recent_stats:
            return 10.0
            
        avg = float(recent_stats.get("battingAverage", ".200").replace(".", "0."))
        hr = int(recent_stats.get("homeRuns", 0))
        rbi = int(recent_stats.get("runsBattedIn", 0))
        
        # Basic formula for batter value
        base_price = (
            (avg * 300) +  # Higher average = higher value
            (hr * 5) +     # More home runs = higher value
            (rbi * 1)      # More RBIs = higher value
        )
    
    return max(1.0, base_price)

def generate_daily_price_movements(players_batch: List[dict]) -> List[float]:
    """Generate price movement using Gemini API"""
    model = genai.GenerativeModel('gemini-pro')
    
    # Add delay before API request
    time.sleep(5)
    
    # Create a batch prompt for multiple players
    prompt = f"""
    Given these players' recent performances, generate a daily stock price movement percentage 
    between -5% and +5% for each player based on their performance trend.
    Return only a comma-separated list of percentages in the same order as the players.
    
    Players:
    {[f"{i+1}. {p['fullName']} ({p['position']}): {p['stats'].get('2023', 'No recent stats')}" 
       for i, p in enumerate(players_batch)]}
    """
    
    try:
        response = model.generate_content(prompt)
        # Parse comma-separated percentages
        movements = [float(x.strip().replace('%', '')) for x in response.text.strip().split(',')]
        # Ensure we have the right number of movements and they're within bounds
        if len(movements) != len(players_batch):
            print(f"Warning: Expected {len(players_batch)} movements, got {len(movements)}")
            movements = [0.0] * len(players_batch)
        return [max(-5.0, min(5.0, m)) for m in movements]
    except Exception as e:
        print(f"Error generating price movements for batch: {e}")
        return [0.0] * len(players_batch)

def update_player_prices(players_data):
    """Update prices for all players"""
    market_data = []
    
    # Process players in batches of 10
    batch_size = 10
    for i in range(0, len(players_data), batch_size):
        batch = players_data[i:i+batch_size]
        movements = generate_daily_price_movements(batch)
        
        for player, movement in zip(batch, movements):
            base_price = calculate_base_price(player['stats'], player['position'])
            market_data.append({
                'player_id': player['id'],
                'name': player['fullName'],
                'position': player['position'],
                'base_price': round(base_price, 2),
                'daily_movement': f"{movement:+.2f}%",
                'current_price': round(base_price * (1 + movement/100), 2)
            })
        
        print(f"Processed batch of {len(batch)} players...")
    
    return market_data

# Load player data
with open('players_data_yearly.json', 'r') as f:
    players_data = json.load(f)

# Generate market data
market_data = update_player_prices(players_data)

# Convert to DataFrame for easy viewing/analysis
df = pd.DataFrame(market_data)
print(df.head())

# Optionally save to CSV
df.to_csv('player_market_data.csv', index=False)