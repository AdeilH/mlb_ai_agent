import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_symbol(name, position):
    """Generate a stock symbol for a player"""
    parts = name.split()
    if len(parts) >= 2:
        symbol = (parts[0][0] + parts[-1][:3]).upper()
    else:
        symbol = name[:4].upper()
    symbol += position
    return symbol

def generate_random_data(df, num_points=1000000):
    """Generate randomized intraday data with ±5% movements"""
    # Get unique players
    players = df[['player_id', 'name', 'position']].drop_duplicates()
    
    # Calculate number of points per player
    points_per_player = num_points // len(players)
    
    # Create new dataframe for randomized data
    randomized_data = []
    
    for _, player in players.iterrows():
        player_id = player['player_id']
        symbol = generate_symbol(player['name'], player['position'])
        
        # Get base price from original data
        base_price = df[df['player_id'] == player_id]['price'].mean()
        min_price = base_price * 0.1  # Minimum price is 10% of base price
        
        # Generate random timestamps throughout the day
        start_time = datetime.strptime("09:30 AM", "%I:%M %p")
        end_time = datetime.strptime("04:00 PM", "%I:%M %p")
        time_delta = (end_time - start_time) / points_per_player
        
        current_price = base_price
        for i in range(points_per_player):
            # Adjust price change probability based on current price
            if current_price < base_price * 0.2:  # If price is below 20% of base
                price_change = random.uniform(-2, 8)  # More likely to go up
            elif current_price > base_price * 2:  # If price is above 200% of base
                price_change = random.uniform(-8, 2)  # More likely to go down
            else:
                price_change = random.uniform(-5, 5)
            
            # Calculate new price and ensure it doesn't go below minimum
            new_price = max(min_price, current_price * (1 + price_change/100))
            
            # Generate random event
            event = random.choice([
                "Trade activity",
                "Market movement",
                "Player performance update",
                "Team news",
                "League announcement"
            ])
            
            # Calculate timestamp
            timestamp = start_time + i * time_delta
            
            randomized_data.append({
                'player_id': player_id,
                'name': player['name'],
                'position': player['position'],
                'symbol': symbol,
                'time': timestamp.strftime('%I:%M %p'),
                'price': round(new_price, 2),
                'event': event,
                'impact': f"{price_change:+.2f}%"
            })
            
            current_price = new_price
    
    # Create new dataframe and sort by time for each player
    new_df = pd.DataFrame(randomized_data)
    new_df = new_df.sort_values(['player_id', 'time'])
    
    return new_df

def main():
    # Read original data
    df = pd.read_csv('player_intraday_data.csv')
    
    # Generate randomized data
    randomized_df = generate_random_data(df)
    
    # Save to new CSV
    randomized_df.to_csv('player_intraday_data_randomized.csv', index=False)
    
    # Print sample of data
    print("\nSample of randomized data:")
    print(randomized_df.head(10))
    
    # Print symbols for each player
    print("\nPlayer Symbols:")
    symbols = randomized_df[['name', 'position', 'symbol']].drop_duplicates()
    for _, row in symbols.iterrows():
        print(f"{row['name']} ({row['position']}): {row['symbol']}")

if __name__ == "__main__":
    main() 