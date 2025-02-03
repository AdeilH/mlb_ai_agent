import requests
import json
import csv

i = 0
j = 0
# Step 1: Fetch all active players
def fetch_active_players():
    global i 
    url = "https://statsapi.mlb.com/api/v1/teams?season=2023&sportId=1"  # 2023 season, MLB (sportId=1)
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch teams: {response.status_code}")
        return []
    
    teams = response.json().get("teams", [])
    active_players = []
    
    # Fetch roster for each team
    for team in teams:
        team_id = team.get("id")
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/Active?season=2023"
        roster_response = requests.get(roster_url)
        
        if roster_response.status_code == 200:
            roster = roster_response.json().get("roster", [])
            active_players.extend(roster)
    print(i)
    i += 1
    return active_players

# Step 2: Fetch player stats for active players
def fetch_player_stats(player_id):
    global j
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}?hydrate=stats(group=[hitting,pitching,fielding],type=[yearByYear])"
    response = requests.get(url)
    print(j)
    j += 1
    if response.status_code == 200:
        return response.json().get("people", [])[0]
    else:
        print(f"Failed to fetch stats for player {player_id}: {response.status_code}")
        return None

# Step 3: Extract relevant stats for fantasy game
def extract_relevant_stats(player_data):
    relevant_stats = {
        "id": player_data.get("id"),
        "fullName": player_data.get("fullName"),
        "position": player_data.get("primaryPosition", {}).get("abbreviation"),
        "team": player_data.get("currentTeam", {}).get("name"),
        "stats": {}
    }
    
    # Extract hitting stats
    for stat_group in player_data.get("stats", []):
        if stat_group.get("group", {}).get("displayName") == "hitting":
            for split in stat_group.get("splits", []):
                if split.get("season") == "2023":  # Only current season stats
                    relevant_stats["stats"]["hitting"] = {
                        "homeRuns": split.get("stat", {}).get("homeRuns", 0),
                        "runsBattedIn": split.get("stat", {}).get("rbi", 0),
                        "battingAverage": split.get("stat", {}).get("avg", ".000"),
                        "onBasePercentage": split.get("stat", {}).get("obp", ".000"),
                        "sluggingPercentage": split.get("stat", {}).get("slg", ".000")
                    }
        
        # Extract pitching stats
        elif stat_group.get("group", {}).get("displayName") == "pitching":
            for split in stat_group.get("splits", []):
                if split.get("season") == "2023":  # Only current season stats
                    relevant_stats["stats"]["pitching"] = {
                        "wins": split.get("stat", {}).get("wins", 0),
                        "strikeouts": split.get("stat", {}).get("strikeOuts", 0),
                        "earnedRunAverage": split.get("stat", {}).get("era", 0.00),
                        "walksAndHitsPerInning": split.get("stat", {}).get("whip", 0.00)
                    }
    
    return relevant_stats

# Step 4: Save data to a JSON file
def save_to_json(data, filename="players_data.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {filename}")

# Step 5: Save data to a CSV file
def save_to_csv(data, filename="players_data.csv"):
    # Define CSV headers
    headers = [
        "id", "fullName", "position", "team",
        "homeRuns", "runsBattedIn", "battingAverage", "onBasePercentage", "sluggingPercentage",
        "wins", "strikeouts", "earnedRunAverage", "walksAndHitsPerInning"
    ]
    
    # Prepare rows for CSV
    rows = []
    for player in data:
        row = [
            player["id"],
            player["fullName"],
            player["position"],
            player["team"],
            player["stats"].get("hitting", {}).get("homeRuns", 0),
            player["stats"].get("hitting", {}).get("runsBattedIn", 0),
            player["stats"].get("hitting", {}).get("battingAverage", ".000"),
            player["stats"].get("hitting", {}).get("onBasePercentage", ".000"),
            player["stats"].get("hitting", {}).get("sluggingPercentage", ".000"),
            player["stats"].get("pitching", {}).get("wins", 0),
            player["stats"].get("pitching", {}).get("strikeouts", 0),
            player["stats"].get("pitching", {}).get("earnedRunAverage", 0.00),
            player["stats"].get("pitching", {}).get("walksAndHitsPerInning", 0.00)
        ]
        rows.append(row)
    
    # Write to CSV
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Data saved to {filename}")

# Step 6: Main function to fetch and process data
def main():
    active_players = fetch_active_players()
    fantasy_data = []
    
    for player in active_players:
        player_id = player.get("person", {}).get("id")
        player_data = fetch_player_stats(player_id)
        
        if player_data:
            relevant_stats = extract_relevant_stats(player_data)
            fantasy_data.append(relevant_stats)
    
    # Save data to JSON and CSV files
    save_to_json(fantasy_data)
    save_to_csv(fantasy_data)

# Run the script
if __name__ == "__main__":
    main()