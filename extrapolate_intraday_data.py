import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_symbol(name, position):
    """Generate a stock symbol for a player"""
    # Take first letter of first name and up to 3 letters of last name
    parts = name.split()
    if len(parts) >= 2:
        symbol = (parts[0][0] + parts[-1][:3]).upper()
    else:
        symbol = name[:4].upper()
    
    # Add position identifier without dot (e.g., JDOEP for a pitcher)
    symbol += position
    
    return symbol

def extrapolate_data(df):
    """Extrapolate intraday data to 100 points per player"""
    # Convert time strings to datetime for interpolation
    df['datetime'] = pd.to_datetime(df['time'].apply(lambda x: f"2024-01-01 {x}"))
    
    # Create new dataframe for extrapolated data
    extrapolated_data = []
    
    # Get unique players
    players = df[['player_id', 'name', 'position']].drop_duplicates()
    
    for _, player in players.iterrows():
        player_id = player['player_id']
        player_data = df[df['player_id'] == player_id].copy()
        
        if len(player_data) == 0:
            continue
            
        # Generate symbol for player
        symbol = generate_symbol(player['name'], player['position'])
        
        # Sort by time
        player_data = player_data.sort_values('datetime')
        
        # Create 100 evenly spaced timestamps between first and last event
        start_time = player_data['datetime'].min()
        end_time = player_data['datetime'].max()
        
        new_times = pd.date_range(start=start_time, end=end_time, periods=100)
        
        # Interpolate prices
        price_interpolator = np.interp(
            new_times.astype(np.int64), 
            player_data['datetime'].astype(np.int64),
            player_data['price']
        )
        
        # Generate events and impacts for interpolated points
        for i, (timestamp, price) in enumerate(zip(new_times, price_interpolator)):
            # Find nearest real event
            nearest_idx = abs(player_data['datetime'] - timestamp).argmin()
            nearest_event = player_data.iloc[nearest_idx]
            
            # Calculate price change
            prev_price = price_interpolator[i-1] if i > 0 else price
            price_change = ((price - prev_price) / prev_price) * 100 if i > 0 else 0
            
            extrapolated_data.append({
                'player_id': player_id,
                'name': player['name'],
                'position': player['position'],
                'symbol': symbol,
                'time': timestamp.strftime('%I:%M %p'),
                'price': round(price, 2),
                'event': nearest_event['event'],
                'impact': f"{price_change:+.2f}%"
            })
    
    # Create new dataframe and sort by time for each player
    new_df = pd.DataFrame(extrapolated_data)
    new_df = new_df.sort_values(['player_id', 'time'])
    
    return new_df

def main():
    # Read original data
    df = pd.read_csv('player_intraday_data.csv')
    
    # Extrapolate data
    extrapolated_df = extrapolate_data(df)
    
    # Save to new CSV
    extrapolated_df.to_csv('player_intraday_data_extrapolated.csv', index=False)
    
    # Print sample of data
    print("\nSample of extrapolated data:")
    print(extrapolated_df.head(10))
    
    # Print symbols for each player
    print("\nPlayer Symbols:")
    symbols = extrapolated_df[['name', 'position', 'symbol']].drop_duplicates()
    for _, row in symbols.iterrows():
        print(f"{row['name']} ({row['position']}): {row['symbol']}")

if __name__ == "__main__":
    main() 