from sqlalchemy import text
from .db import engine  
def get_total_matches():
    try:
        with engine.connect() as conn:
            query = text("SELECT COUNT(*) AS total_matches FROM match_data")
            result = conn.execute(query)
            total = result.scalar()
        return total
        
    except Exception as e:
        return f"Error: {e}"
    
def get_unique_referees():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT referee FROM match_data WHERE referee IS NOT NULL")
            result = conn.execute(query)
            referees = [row[0] for row in result.fetchall()]
        return referees
        
    except Exception as e:
        return f"Error: {e}"
    
def get_most_common_match_hour():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT hour AS match_hour, COUNT(*) AS match_count
                FROM match_data
                GROUP BY hour
                ORDER BY match_count DESC
                LIMIT 1
            """)
            result = conn.execute(query).fetchone()
            if result:
                return {"match_hour": result[0], "total_matches": result[1]}
            else:
                return {"match_hour": None, "total_matches": "No match time data available."}
        
    except Exception as e:
        return f"Error: {e}"    

def get_match_by_number(match_number:int):
    try:
        with engine.connect() as conn:
            query = text("SELECT * FROM match_data ORDER BY match_no ASC LIMIT 1 OFFSET :offset_val")
            result = conn.execute(query, {"offset_val": match_number - 1}).fetchone()
            if result:
                return dict(result._mapping)
            else:
                return {"message": f"No match found for match number {match_number}"}        
    except Exception as e:
        return f"Error: {e}"

def get_possession_by_match_number(match_number:int):
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT first_team, second_team, 
                       `1_poss` AS home_possession, 
                       `2_poss` AS away_possession
                FROM match_data
                ORDER BY match_no ASC
                LIMIT 1 OFFSET :offset_val
            """)
            result = conn.execute(query, {"offset_val": match_number - 1}).fetchone()
            if result:
                row = result._mapping
                return {
                    "match_number": match_number,
                    "home_team": row.get("first_team"),
                    "away_team": row.get("second_team"),
                    "home_possession": row.get("home_possession"),
                    "away_possession": row.get("away_possession")
                }
            else:
                return {"message": f"No match found for match number {match_number}"}        
    except Exception as e:
        return f"Error: {e}"    

def get_goal_prevention_by_match_number(match_number:int):
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT first_team AS home_team, 
                         second_team AS away_team,
                        `1_goal_prevented` AS home_goal_prevented, 
                         `2_goal_prevented` AS away_goal_prevented 
                FROM match_data ORDER BY match_no ASC
                LIMIT 1 OFFSET :offset_val
            """)
            result = conn.execute(query, {"offset_val": match_number - 1}).fetchone()
            if result:
                return dict(result._mapping)
            else:
                return {"message": f"No match found for match number {match_number}"}        
    except Exception as e:
        return f"Error: {e}"    
    
def get_match_with_max_shots_team1():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT *
                FROM match_data
                WHERE `1_ontarget` = (SELECT MAX(`1_ontarget`) FROM match_data)
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available."}        
    except Exception as e:
        return f"Error: {e}"    
    
def get_match_with_max_shots_team2(): 
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    second_team AS team2,
                    first_team AS team1,
                    `1_ontarget` AS team1_shots_on_target,
                    `2_ontarget` AS team2_shots_on_target,
                    `2_goals` AS team2_goals,
                    ROUND((`2_goals` / `2_ontarget`) * 100, 2) AS conversion_rate_percentage
                FROM match_data
                WHERE `2_ontarget` = (SELECT MAX(`2_ontarget`) FROM match_data)
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available."}        
    except Exception as e:
        return f"Error: {e}"    
    
def get_match_with_max_attendance():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    match_no,
                    first_team,
                    second_team,
                    venue,
                    attendance
                FROM match_data
                WHERE attendance = (SELECT MAX(attendance) FROM match_data);
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available."}        
    except Exception as e:
        return f"Error: {e}"    
    
def get_possession_and_conversion_al_janoub():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    first_team,
                    second_team,
                    `1_poss` AS first_team_possession,
                    `2_poss` AS second_team_possession,
                    `1_goals` AS first_team_goals,
                    `2_goals` AS second_team_goals,
                    ROUND((`1_goals` / `1_ontarget`) * 100, 2) AS first_team_conversion_percentage,
                    ROUND((`2_goals` / `2_ontarget`) * 100, 2) AS second_team_conversion_percentage
                FROM match_data
                WHERE venue = 'Al Janoub Stadium';
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available for Al Janoub Stadium."}        
    except Exception as e:
        return f"Error: {e}"    
    
def get_goals_inside_outside_by_venue():
    try:
        with engine.connect() as conn:
            query = text("""
                    SELECT 
                    venue,
                    SUM(`1_goal_inside_penalty_area` + `2_goal_inside_penalty_area`) 
                        AS total_goals_inside_penalty,
                    SUM(`1_goal_outside_penalty_area` + `2_goal_outside_penalty_area`) 
                        AS total_goals_outside_penalty
                FROM match_data
                GROUP BY venue
                ORDER BY venue;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available."}       
    except Exception as e:
        return f"Error: {e}"
    
def get_conversion_rate_inside_outside_by_venue ():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    venue,
                    ROUND(
                        (SUM(`1_goal_inside_penalty_area` + `2_goal_inside_penalty_area`) /
                        NULLIF(SUM(`1_attempts_inside_penalty_area` + `2_attempts_inside_penalty_area`), 0)) * 100,
                        2
                    ) AS inside_penalty_conversion_rate,
                    ROUND(
                        (SUM(`1_goal_outside_penalty_area` + `2_goal_outside_penalty_area`) /
                        NULLIF(SUM(`1_attempts_outside_penalty_area` + `2_attempts_outside_penalty_area`), 0)) * 100,
                        2
                    ) AS outside_penalty_conversion_rate
                FROM match_data
                GROUP BY venue
                ORDER BY venue;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No data available."}       
    except Exception as e:
        return f"Error: {e}"    
    
def get_match_data_two_teams(first_team: str, second_team: str):
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT *
                FROM match_data
                WHERE 
                    (first_team = :firstteam AND second_team = :secondteam)
                    OR
                    (first_team = :secondteam AND second_team = :firstteam)
            """)
            result = conn.execute(
                query,
                {
                    "firstteam": first_team,
                    "secondteam": second_team
                }
            ).fetchall()

            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No matches found between {first_team} and {second_team}."}

    except Exception as e:
        return f"Error: {e}"

def get_match_argentina_vs_france():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT *
                FROM match_data
                WHERE 
                    (first_team = 'Argentina' AND second_team = 'France')
                    OR
                    (first_team = 'France' AND second_team = 'Argentina');
            """)
            result = conn.execute(query).fetchall()

            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "No matches found between Argentina and France."}

    except Exception as e:
        return f"Error: {e}"        
    
def get_top_goals():
    try:
        with engine.connect() as conn:
            query = text("SELECT player, team, goals FROM player_stats ORDER BY goals DESC LIMIT 5")
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def get_top_assists():
    try:
        with engine.connect() as conn:
            query = text("SELECT player, team, assists FROM player_stats ORDER BY assists DESC LIMIT 5")
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def get_top_yellow_cards():
    try:
        with engine.connect() as conn:
            query = text("SELECT player, team, cards_yellow FROM player_stats ORDER BY cards_yellow DESC LIMIT 5")
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def get_top_red_cards():
    try:
        with engine.connect() as conn:
            query = text("SELECT player, team, cards_red FROM player_stats ORDER BY cards_red DESC LIMIT 5")
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def get_top_dribble_success():
    try:
        with engine.connect() as conn:
            query = text("SELECT player, team, goals_per90 FROM player_stats ORDER BY goals_per90 DESC LIMIT 5")
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"
    
def get_goal_scoring_efficiency_per_game():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    goals,
                    games,
                    CASE
                        WHEN games = 0 THEN 0
                        ELSE ROUND(goals / games, 2)
                    END AS goal_efficiency_per_game
                FROM player_stats
                ORDER BY goals DESC;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    
    
def get_portuguese_offensive_limited_starts():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT player, team, games_starts, goals, goals_per90, assists, assists_per90 FROM player_stats WHERE team = 'Portugal' AND games_starts < 2 AND goals > 0 ORDER BY goals DESC;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"
    
def get_club_goals_comparison():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    club,
                    SUM(goals) AS total_goals,
                    MAX(goals) AS max_goals_by_player
                FROM player_stats
                GROUP BY club
                ORDER BY total_goals DESC
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    

def get_goals_under_25():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    COUNT(*) AS total_players_under_25,
                    SUM(goals) AS total_goals_under_25,
                    ROUND(SUM(goals) / COUNT(*), 2) AS goals_per_player
                FROM player_stats
                WHERE age < 25
            """)
            result = conn.execute(query).fetchone()
            if result:
                return dict(result._mapping)
            else:
                return {"message": "No players under 25 found."}
    except Exception as e:
        return f"Error: {e}"

def get_goals_25_and_over():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    COUNT(*) AS total_players_25_and_over,
                    SUM(goals) AS total_goals_25_and_over,
                    ROUND(SUM(goals) / COUNT(*), 2) AS goals_per_player
                FROM player_stats
                WHERE age >= 25
            """)
            result = conn.execute(query).fetchone()
            if result:
                return dict(result._mapping)
            else:
                return {"message": "No players aged 25 and over found."}
    except Exception as e:
        return f"Error: {e}"   
    
def top_clubs_under_25():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    club,
                    COUNT(*) AS players_under_25
                FROM player_stats
                WHERE age < 25
                GROUP BY club
                ORDER BY players_under_25 DESC
                LIMIT 5
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def top_players_goals_per_90():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    club,
                    goals,
                    ROUND(minutes_90s, 2) AS games_90s,
                    ROUND(goals_per90, 2) AS goals_per_90
                FROM player_stats
                ORDER BY goals_per90 DESC
                LIMIT 10
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def get_players_highest_shot_accuracy():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    club,
                    goals,
                    games,
                    ROUND(goals / NULLIF(games, 0), 2) AS goals_per_game,
                    ROUND(goals_per90, 2) AS goals_per_90
                FROM player_stats
                WHERE games > 0
                ORDER BY goals_per_90 DESC
                LIMIT 10;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    

def get_players_highest_shot_accuracy_proxy():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    club,
                    goals,
                    games,
                    ROUND(goals / NULLIF(games, 0), 2) AS goals_per_game,
                    ROUND(goals_per90, 2) AS goals_per_90
                FROM player_stats
                WHERE games > 0 AND goals > 2
                ORDER BY goals_per_90 DESC
                LIMIT 10;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"

def top_clubs_young_high_accuracy_players():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    club,
                    COUNT(*) AS high_accuracy_young_players
                FROM player_stats
                WHERE age < 28
                AND goals > 0
                AND (goals_per90 * 100) > 20  -- proxy for "shot accuracy over 20%"
                GROUP BY club
                ORDER BY high_accuracy_young_players DESC;

            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    

def top_teams_young_high_accuracy_players():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    team,
                    COUNT(*) AS high_accuracy_young_players
                FROM player_stats
                WHERE age < 28
                AND goals > 0
                AND (goals_per90 * 100) > 20  -- proxy for "shot accuracy over 20%"
                GROUP BY team
                ORDER BY high_accuracy_young_players DESC;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    
    
def player_performance_analysis():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    club,
                    games,
                    goals,
                    ROUND(goals_per90, 2) AS goals_per_90,
                    ROUND(assists_per90, 2) AS assists_per_90,
                    ROUND(goals_per90 * 100, 2) AS shooting_accuracy_proxy
                FROM player_stats
                WHERE games > 2
                ORDER BY goals DESC, player ASC;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No data found for player."}
    except Exception as e:
        return f"Error: {e}"
    
def get_players_att_pen_touches():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    player,
                    team,
                    touches_att_pen_area
                FROM player_possession
                WHERE touches_att_pen_area > 0
                ORDER BY touches_att_pen_area DESC,  -- Most touches first
                player ASC;
            """)
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return f"Error: {e}"    
    
def get_att_pen_players_by_club():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    ps.club,
                    COUNT(pp.player) AS num_players
                FROM player_possession pp
                JOIN player_stats ps ON pp.player = ps.player
                WHERE pp.touches_att_pen_area > 0
                GROUP BY ps.club
                ORDER BY num_players DESC;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No attacking penalty area touch data found for club."}
    except Exception as e:
        return f"Error: {e}"

def get_avg_touches_by_area():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT team, ROUND(AVG(touches_def_pen_area), 2) AS avg_touches_def_pen_area, ROUND(AVG(touches_def_3rd), 2) AS avg_touches_def_3rd, ROUND(AVG(touches_mid_3rd), 2) AS avg_touches_mid_3rd, ROUND(AVG(touches_att_3rd), 2) AS avg_touches_att_3rd, ROUND(AVG(touches_att_pen_area), 2) AS avg_touches_att_pen_area FROM player_possession GROUP BY team ORDER BY team;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No touch data found for areas."}
    except Exception as e:
        return f"Error: {e}"    
    
def get_avg_touches_by_area_players_starting_with_a():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    ROUND(AVG(touches_def_pen_area), 2) AS avg_def_pen_area, 
                    ROUND(AVG(touches_def_3rd), 2) AS avg_def_third, 
                    ROUND(AVG(touches_mid_3rd), 2) AS avg_mid_third, 
                    ROUND(AVG(touches_att_3rd), 2) AS avg_att_third, 
                    ROUND(AVG(touches_att_pen_area), 2) AS avg_att_pen_area 
                FROM player_possession 
                WHERE player LIKE 'A%';
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No touch data found for players starting with A."}
    except Exception as e:
        return f"Error: {e}" 

def get_goal_distribution_by_position():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    position,
                    SUM(goals) AS total_goals
                FROM player_stats
                GROUP BY position
                ORDER BY total_goals DESC;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No goal data found by position."}
    except Exception as e:
        return f"Error: {e}"       

def get_top_scoring_defenders():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    goals
                FROM player_stats
                WHERE position = 'DF'
                ORDER BY goals DESC
                LIMIT 5;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No data found for defenders."}
    except Exception as e:
        return f"Error: {e}"    

def get_top_scoring_midfielders():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    goals
                FROM player_stats
                WHERE position = 'MF'
                ORDER BY goals DESC
                LIMIT 5;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No data found for midfielders."}
    except Exception as e:
        return f"Error: {e}"    

def get_top_scoring_forward():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    player,
                    team,
                    goals
                FROM player_stats
                WHERE position = 'FW'
                ORDER BY goals DESC
                LIMIT 5;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No data found for forwards."}
    except Exception as e:
        return f"Error: {e}"   

def get_best_shots_on_target_per_progressive_pass():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT *
                FROM (
                    SELECT
                        pp.player,
                        pp.team,
                        pp.progressive_passes_received,
                        ps.shots_on_target,
                        ROUND(
                            ps.shots_on_target / NULLIF(pp.progressive_passes_received, 0),
                            4
                        ) AS shots_on_target_per_progressive_pass
                    FROM player_possession pp
                    JOIN player_shooting ps
                        ON pp.player = ps.player
                    WHERE pp.progressive_passes_received > 0
                    ORDER BY pp.progressive_passes_received DESC
                    LIMIT 200
                ) ranked_players
                ORDER BY shots_on_target_per_progressive_pass DESC
                LIMIT 1;
            """)
            result = conn.execute(query).fetchall()
            if result:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": f"No data found for shots on target per progressive pass."}
    except Exception as e:
        return f"Error: {e}"    