import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np

def generate_player_specific_news(player_name, position, news_type='performance'):
    """Generate position-specific news events for a player with different types"""
    news_events = {
        'performance': {
            'P': [
                f"{player_name} throws a no-hitter in simulated game",
                f"{player_name} develops new pitch in bullpen session",
                f"{player_name} records 10 strikeouts in live batting practice",
                f"{player_name} struggles with control in bullpen",
                f"{player_name} shows increased velocity in workout"
            ],
            'infield': [
                f"{player_name} hits grand slam in spring training game",
                f"{player_name} makes spectacular diving stop at {position}",
                f"{player_name} goes 4-for-4 with 2 home runs",
                f"{player_name} commits throwing error at {position}",
                f"{player_name} turns double play with perfect timing"
            ],
            'outfield': [
                f"{player_name} robs home run with leaping catch at wall",
                f"{player_name} hits walk-off home run in extra innings",
                f"{player_name} throws out runner at home plate",
                f"{player_name} misjudges fly ball leading to extra bases",
                f"{player_name} shows off arm strength in outfield practice"
            ]
        },
        'injury': {
            'P': [
                f"{player_name} placed on 15-day injured list with forearm tightness",
                f"{player_name} experiences shoulder discomfort during warmup",
                f"{player_name} undergoes precautionary MRI on elbow",
                f"{player_name} returns from injury rehabilitation assignment",
                f"{player_name} cleared to resume throwing program"
            ],
            'infield': [
                f"{player_name} day-to-day with hamstring tightness",
                f"{player_name} exits game with wrist soreness",
                f"{player_name} undergoes evaluation for knee injury",
                f"{player_name} cleared to return after finger injury",
                f"{player_name} begins rehab assignment at Triple-A"
            ],
            'outfield': [
                f"{player_name} leaves game with oblique strain",
                f"{player_name} dealing with ankle sprain after wall collision",
                f"{player_name} undergoes concussion protocol",
                f"{player_name} returns to lineup after quad injury",
                f"{player_name} starts light running after leg injury"
            ]
        },
        'team': [
            f"Team considering contract extension for {player_name}",
            f"{player_name} subject of trade rumors as deadline approaches",
            f"{player_name} named team captain for upcoming season",
            f"Manager praises {player_name}'s leadership in clubhouse",
            f"{player_name} wins team's community service award"
        ],
        'market': [
            f"Analysts upgrade {player_name}'s season projections",
            f"Multiple teams showing interest in {player_name}",
            f"Market value of {player_name} rises after recent performance",
            f"Insider reports growing confidence in {player_name}",
            f"Fantasy baseball managers targeting {player_name} in trades"
        ]
    }
    
    # Determine position category
    if position == "P":
        pos_category = "P"
    elif position in ["1B", "2B", "3B", "SS"]:
        pos_category = "infield"
    else:
        pos_category = "outfield"
    
    # Select news type with weighted probabilities
    news_type = random.choices(
        ['performance', 'injury', 'team', 'market'],
        weights=[0.5, 0.2, 0.15, 0.15]
    )[0]
    
    if news_type in ['team', 'market']:
        return random.choice(news_events[news_type])
    else:
        return random.choice(news_events[news_type][pos_category])

def generate_million_news_events(df, num_events=1000000):
    """Generate a million news events from the randomized data"""
    # Get unique players
    players = df[['player_id', 'name', 'position']].drop_duplicates()
    
    # Create new dataframe for news events
    news_events = []
    
    # Calculate events per player (ensuring total will be close to num_events)
    events_per_player = num_events // len(players)
    
    print(f"Generating approximately {events_per_player} events per player...")
    
    for _, player in players.iterrows():
        # Generate random timestamps throughout the day
        start_time = datetime.strptime("09:30 AM", "%I:%M %p")
        end_time = datetime.strptime("04:00 PM", "%I:%M %p")
        
        # Generate random timestamps
        timestamps = sorted([
            start_time + timedelta(
                seconds=random.randint(0, int((end_time - start_time).total_seconds()))
            )
            for _ in range(events_per_player)
        ])
        
        for timestamp in timestamps:
            # Generate news event
            event = generate_player_specific_news(player['name'], player['position'])
            
            news_events.append({
                'player_id': player['player_id'],
                'name': player['name'],
                'position': player['position'],
                'time': timestamp.strftime('%I:%M %p'),
                'event': event
            })
    
    # Create new dataframe and sort by time
    news_df = pd.DataFrame(news_events)
    news_df = news_df.sort_values(['time', 'player_id'])
    
    return news_df

def main():
    # Read randomized data
    df = pd.read_csv('player_intraday_data_randomized.csv')
    
    # Generate million news events
    print("Generating news events...")
    news_df = generate_million_news_events(df)
    
    # Save to new CSV
    news_df.to_csv('player_news_events_million.csv', index=False)
    print(f"\nGenerated {len(news_df)} news events")
    
    # Print sample of news events
    print("\nSample of news events:")
    pd.set_option('display.max_colwidth', None)
    print(news_df[['player_id', 'name', 'position', 'time', 'event']].head(10))
    
    # Print some statistics
    print("\nEvents per player:")
    events_per_player = news_df.groupby('name').size()
    print(f"Mean: {events_per_player.mean():.0f}")
    print(f"Min: {events_per_player.min()}")
    print(f"Max: {events_per_player.max()}")

if __name__ == "__main__":
    main() 