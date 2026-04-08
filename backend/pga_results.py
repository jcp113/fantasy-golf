"""
Fetch PGA tournament results from ESPN's public API.
"""
import json
import urllib.request
import re
from database import get_db
from sql_compat import upsert_golfer_result


def search_espn_tournament(tournament_name, year=2026):
    """Search ESPN for a PGA tournament and return leaderboard data."""
    url = 'https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching ESPN scoreboard: {e}")
        return None

    events = data.get('events', [])
    for event in events:
        if tournament_name.lower() in event.get('name', '').lower():
            return event.get('id')

    return None


def fetch_espn_leaderboard(event_id):
    """Fetch the leaderboard for a specific ESPN event ID."""
    url = f'https://site.api.espn.com/apis/site/v2/sports/golf/pga/leaderboard?event={event_id}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching leaderboard: {e}")
        return []

    results = []
    events = data.get('events', [])
    if not events:
        return []

    competitions = events[0].get('competitions', [])
    if not competitions:
        return []

    competitors = competitions[0].get('competitors', [])
    for comp in competitors:
        athlete = comp.get('athlete', {})
        name = athlete.get('displayName', '')
        status = comp.get('status', {}).get('type', {}).get('name', '')

        position_raw = comp.get('status', {}).get('position', {}).get('displayName', '')
        position = None
        missed_cut = False
        withdrawn = False
        is_winner = False

        if status == 'cut':
            missed_cut = True
        elif status == 'wd':
            withdrawn = True
        else:
            pos_str = re.sub(r'[T]', '', position_raw)
            try:
                position = int(pos_str)
                if position == 1:
                    is_winner = True
            except (ValueError, TypeError):
                pass

        score_to_par = comp.get('score', {}).get('value', None)
        if score_to_par is not None:
            try:
                score_to_par = int(score_to_par)
            except (ValueError, TypeError):
                score_to_par = None

        results.append({
            'name': name,
            'position': position,
            'score_to_par': score_to_par,
            'is_winner': is_winner,
            'missed_cut': missed_cut,
            'withdrawn': withdrawn,
        })

    return results


def save_tournament_results(tournament_id, results):
    """Save fetched PGA results to the database."""
    db = get_db()
    for r in results:
        db.execute(upsert_golfer_result(), (
            tournament_id, r['name'], r['position'], r['score_to_par'],
            1 if r['is_winner'] else 0,
            1 if r['missed_cut'] else 0,
            1 if r['withdrawn'] else 0,
        ))
    db.execute('UPDATE tournaments SET completed = 1 WHERE id = ?', (tournament_id,))
    db.commit()
    db.close()
    print(f"Saved {len(results)} golfer results for tournament {tournament_id}.")


def fetch_and_save_results(tournament_id):
    """Full pipeline: look up a tournament and save its results."""
    db = get_db()
    tourney = db.execute('SELECT * FROM tournaments WHERE id = ?', (tournament_id,)).fetchone()
    db.close()

    if not tourney:
        print(f"Tournament {tournament_id} not found.")
        return False

    event_id = search_espn_tournament(tourney['name'])
    if not event_id:
        print(f"Could not find '{tourney['name']}' on ESPN.")
        return False

    results = fetch_espn_leaderboard(event_id)
    if not results:
        print(f"No leaderboard data for '{tourney['name']}'.")
        return False

    save_tournament_results(tournament_id, results)
    return True
