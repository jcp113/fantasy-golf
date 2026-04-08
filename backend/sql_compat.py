"""
SQL compatibility helpers for SQLite vs Postgres.
"""
from database import USE_POSTGRES


def upsert_picks():
    if USE_POSTGRES:
        return '''
            INSERT INTO picks (player_id, tournament_id, pick1, pick2, pick3, pick4, alternate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id, tournament_id)
            DO UPDATE SET pick1=EXCLUDED.pick1, pick2=EXCLUDED.pick2,
                          pick3=EXCLUDED.pick3, pick4=EXCLUDED.pick4,
                          alternate=EXCLUDED.alternate, submitted_at=NOW()
        '''
    return '''
        INSERT OR REPLACE INTO picks
        (player_id, tournament_id, pick1, pick2, pick3, pick4, alternate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''


def upsert_golfer_result():
    if USE_POSTGRES:
        return '''
            INSERT INTO golfer_results
            (tournament_id, golfer_name, finish_position, score_to_par, is_winner, missed_cut, withdrawn)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tournament_id, golfer_name)
            DO UPDATE SET finish_position=EXCLUDED.finish_position, score_to_par=EXCLUDED.score_to_par,
                          is_winner=EXCLUDED.is_winner, missed_cut=EXCLUDED.missed_cut, withdrawn=EXCLUDED.withdrawn
        '''
    return '''
        INSERT OR REPLACE INTO golfer_results
        (tournament_id, golfer_name, finish_position, score_to_par, is_winner, missed_cut, withdrawn)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''


def upsert_weekly_score():
    if USE_POSTGRES:
        return '''
            INSERT INTO weekly_scores (player_id, tournament_id, raw_score, winner_bonus, final_score)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id, tournament_id)
            DO UPDATE SET raw_score=EXCLUDED.raw_score, winner_bonus=EXCLUDED.winner_bonus,
                          final_score=EXCLUDED.final_score
        '''
    return '''
        INSERT OR REPLACE INTO weekly_scores
        (player_id, tournament_id, raw_score, winner_bonus, final_score)
        VALUES (?, ?, ?, ?, ?)
    '''


def insert_ignore(table, columns, conflict_col=None):
    """Generate an INSERT ... ON CONFLICT DO NOTHING statement."""
    placeholders = ', '.join(['%s' if USE_POSTGRES else '?'] * len(columns))
    cols = ', '.join(columns)
    if USE_POSTGRES:
        conflict = f'({conflict_col})' if conflict_col else f'({columns[0]})'
        return f'INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT {conflict} DO NOTHING'
    return f'INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})'
