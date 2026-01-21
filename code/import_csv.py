import pandas as pd
from .db import engine

def import_csv_files():
    try:
        match_data_file = 'csv-files/match_data.csv'
        player_possession_file = 'csv-files/player_possession.csv'
        player_shooting_file = 'csv-files/player_shooting.csv'
        player_stats_file = 'csv-files/player_stats.csv'

        match_data = pd.read_csv(match_data_file)
        player_possession = pd.read_csv(player_possession_file)
        player_shooting = pd.read_csv(player_shooting_file)
        player_stats = pd.read_csv(player_stats_file)

        match_data.to_sql('match_data', con=engine, if_exists='replace', index=False)
        player_possession.to_sql('player_possession', con=engine, if_exists='replace', index=False)
        player_shooting.to_sql('player_shooting', con=engine, if_exists='replace', index=False)
        player_stats.to_sql('player_stats', con=engine, if_exists='replace', index=False)

        return "All CSV files have been successfully imported into MySQL!"
    except Exception as e:
        return f"Error: {e}"
    
