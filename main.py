from fastapi import FastAPI
from code.import_csv import import_csv_files
from code.queries import get_goals_inside_outside_by_venue, get_match_with_max_attendance, get_total_matches,get_unique_referees,get_most_common_match_hour,get_match_by_number,get_possession_by_match_number,get_goal_prevention_by_match_number,get_match_with_max_shots_team1,get_match_with_max_shots_team2,get_match_with_max_attendance,get_possession_and_conversion_al_janoub,get_goals_inside_outside_by_venue,get_conversion_rate_inside_outside_by_venue,get_match_data_two_teams, get_top_goals, get_top_assists, get_top_yellow_cards, get_top_red_cards, get_top_dribble_success,get_goal_scoring_efficiency_per_game,get_portuguese_offensive_limited_starts,get_club_goals_comparison,get_goals_under_25,get_goals_25_and_over,top_clubs_under_25,top_players_goals_per_90
from typing import Optional
app = FastAPI(title="Football Analysis API", version="1.0.0")

@app.get("/")
def home():
    return {"message": "Welcome! Go to /import to load CSVs into MySQL."}

@app.get("/import")
def import_data():
    result = import_csv_files()
    return {"message": result}

@app.get("/total-matches")
def total_matches():
    result = get_total_matches()
    return {"total_matches": result}
@app.get("/unique-referees")
def unique_referees(): 
    result = get_unique_referees()
    return {"unique_referees": result} 

@app.get("/most-common-match-hour")
def most_common_match_hour():
    result = get_most_common_match_hour()
    return {"most_common_match_hour": result}

@app.get("/match/{match_number}/possession")
def match_possession(match_number: int):
    result = get_possession_by_match_number(match_number)
    return {"possession_data": result}

@app.get("/match/{match_number}/goal_prevented")     
def match_goal_prevented(match_number: int):
    result = get_goal_prevention_by_match_number(match_number)
    return {"goal_prevention_data": result}

@app.get("/match/max-shots-team1")
def match_max_shots_team1():
    result = get_match_with_max_shots_team1()
    return {"max_shots_team": result}

@app.get("/match/max-shots-team2")
def match_max_shots_team2():
    result = get_match_with_max_shots_team2()
    return {"max_shots_team": result}

@app.get("/match/max-attendance")
def match_max_attendance():
    result = get_match_with_max_attendance()
    return {"max_attendance_match": result} 

@app.get("/matches/al-janoub/possession_conversion")
def matches_al_janoub_possession_conversion():
    result = get_possession_and_conversion_al_janoub()
    return {"matches": result}

@app.get("/venues/goals-inside-outside")
def goals_inside_outside_by_venue():
    result = get_goals_inside_outside_by_venue()
    return {"goals_by_venue": result}

@app.get("/venues/conversion-rate-inside-outside")
def conversion_rate_inside_outside_by_venue():
    result = get_conversion_rate_inside_outside_by_venue()
    return {"conversion_rate_by_venue": result}

@app.get("/matches/two-teams")
def match_data_two_teams(
    first_team: Optional[str] = None,
    second_team: Optional[str] = None
):
    if not first_team or not second_team:
        return {"error": "first_team and second_team are required"}

    result = get_match_data_two_teams(first_team, second_team)
    return {
        "first_team": first_team,
        "second_team": second_team,
        "match_data": result
    }

@app.get("/matches/argentina-vs-france")
def match_argentina_vs_france():
    from code.queries import get_match_argentina_vs_france
    result = get_match_argentina_vs_france()
    return {
        "match_data": result
    }

@app.get("/top-performers")
def top_performers():
        
    return {
        "top_goals": get_top_goals(),
        "top_assists": get_top_assists(),
        "top_yellow_cards": get_top_yellow_cards(),
        "top_red_cards": get_top_red_cards(),
        "top_dribble_success": get_top_dribble_success()
    }
@app.get("/goal-scoring-efficiency")
def goal_scoring_efficiency():
    result = get_goal_scoring_efficiency_per_game()
    return {"goal_scoring_efficiency": result}

@app.get("/portuguese-limited-starts")
def portuguese_limited_starts():
    result = get_portuguese_offensive_limited_starts()
    return {"portuguese_offensive_limited_starts": result}

@app.get("/club-goals-comparison")
def club_goals_comparison():
    result = get_club_goals_comparison()
    return {"club_goals_comparison": result}

@app.get("/goals-under-25")
def goals_under_25():
    result = get_goals_under_25()
    return {"goals_under_25": result}

@app.get("/goals-25-and-over")
def goals_25_and_over():
    result = get_goals_25_and_over()
    return {"goals_25_and_over": result}

@app.get("/top-clubs-under-25")  #Module3-Task 7
def api_top_clubs_under_25():
    result = top_clubs_under_25()
    return {"top_clubs_under_25": result}

@app.get("/top-10-players-goals-per-90")  #Module 4 - Task 1
def api_top_players_goals_per_90():
    result = top_players_goals_per_90()
    return {"top_players_goals_per_90": result}


@app.get("/match/{match_number}")
def match_details(match_number: int): 
    result = get_match_by_number(match_number)
    return {"match_data": result} 


