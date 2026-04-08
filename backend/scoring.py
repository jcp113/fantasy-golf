"""
Scoring engine: calculate weekly scores from picks + PGA results.

Rules:
- Pick 4 golfers per week
- Score = average finishing position of your 4 golfers (lowest wins)
- Bonus subtracted if you picked the tournament winner
- Missed cut / WD golfers get a penalty position
- Season standings = average of all weekly scores
"""
from database import get_db
from sql_compat import upsert_weekly_score


def get_setting(db, key, default=None):
    row = db.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    return row['value'] if row else default


def calculate_weekly_scores(tournament_id):
    """Calculate scores for all players who submitted picks for a tournament."""
    db = get_db()

    winner_bonus = float(get_setting(db, 'winner_bonus', '2.0'))
    missed_cut_penalty = int(get_setting(db, 'missed_cut_penalty', '80'))

    results = db.execute(
        'SELECT * FROM golfer_results WHERE tournament_id = ?', (tournament_id,)
    ).fetchall()

    if not results:
        print(f"No golfer results for tournament {tournament_id}. Fetch results first.")
        db.close()
        return

    result_map = {}
    for r in results:
        result_map[r['golfer_name'].lower()] = r

    picks = db.execute(
        'SELECT * FROM picks WHERE tournament_id = ?', (tournament_id,)
    ).fetchall()

    for pick in picks:
        golfer_names = [pick['pick1'], pick['pick2'], pick['pick3'], pick['pick4']]
        positions = []
        has_winner = False

        for gname in golfer_names:
            if not gname:
                positions.append(missed_cut_penalty)
                continue

            r = result_map.get(gname.lower())
            if not r:
                gname_lower = gname.lower()
                for key, val in result_map.items():
                    if gname_lower in key or key in gname_lower:
                        r = val
                        break

            if r:
                if r['missed_cut'] or r['withdrawn']:
                    positions.append(missed_cut_penalty)
                elif r['finish_position']:
                    positions.append(r['finish_position'])
                    if r['is_winner']:
                        has_winner = True
                else:
                    positions.append(missed_cut_penalty)
            else:
                positions.append(missed_cut_penalty)

        raw_score = sum(positions) / len(positions) if positions else 0
        bonus = winner_bonus if has_winner else 0
        final_score = raw_score - bonus

        db.execute(upsert_weekly_score(), (
            pick['player_id'], tournament_id, raw_score, bonus, final_score
        ))

    db.commit()
    _determine_weekly_winners(db, tournament_id)
    db.close()
    print(f"Calculated scores for {len(picks)} players in tournament {tournament_id}.")


def _determine_weekly_winners(db, tournament_id):
    """Find the winner in each division for a tournament."""
    tourney = db.execute('SELECT * FROM tournaments WHERE id = ?', (tournament_id,)).fetchone()
    payout = float(get_setting(db, 'major_payout' if tourney['is_major'] else 'weekly_payout', '100'))

    divisions = db.execute('SELECT * FROM divisions').fetchall()

    for div in divisions:
        best = db.execute('''
            SELECT ws.*, p.name as player_name
            FROM weekly_scores ws
            JOIN players p ON ws.player_id = p.id
            WHERE ws.tournament_id = ? AND p.division_id = ?
            ORDER BY ws.final_score ASC
            LIMIT 1
        ''', (tournament_id, div['id'])).fetchone()

        if best:
            tied = db.execute('''
                SELECT ws.*, p.name as player_name
                FROM weekly_scores ws
                JOIN players p ON ws.player_id = p.id
                WHERE ws.tournament_id = ? AND p.division_id = ? AND ws.final_score = ?
                ORDER BY p.name
            ''', (tournament_id, div['id'], best['final_score'])).fetchall()

            split_payout = payout / len(tied)

            db.execute('''DELETE FROM weekly_winners
                          WHERE tournament_id = ? AND division_id = ?''',
                       (tournament_id, div['id']))

            for t in tied:
                db.execute('''
                    INSERT INTO weekly_winners (tournament_id, division_id, player_id, score, winnings)
                    VALUES (?, ?, ?, ?, ?)
                ''', (tournament_id, div['id'], t['player_id'], t['final_score'], split_payout))

    db.commit()
