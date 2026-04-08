"""
One-time import: seed the database from the existing Google Sheet data.
Run this once to migrate all existing data into SQLite or Postgres.
"""
import csv
import io
import urllib.request
import urllib.parse
from database import get_db, init_db
from sql_compat import insert_ignore, upsert_weekly_score

SHEET_ID = '1fdFvJYYBXeYNxQeo2X63Sy0HcuOgOL5jqsXQMRB4nBU'


def fetch_sheet_csv(sheet_name):
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(sheet_name)}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req)
    text = resp.read().decode('utf-8')
    return list(csv.reader(io.StringIO(text)))


def seed_conferences_and_divisions(db):
    conferences = [
        ('Monmouth County Conference', ['Beacon Hill', 'Manasquan River']),
        ('Ocean County Conference', ['Pine Barrens', 'Metedeconk'])
    ]
    for conf_name, divs in conferences:
        db.execute(
            insert_ignore('conferences', ['name'], 'name'),
            (conf_name,)
        )
        conf_id = db.execute('SELECT id FROM conferences WHERE name = ?', (conf_name,)).fetchone()['id']
        for div_name in divs:
            db.execute(
                insert_ignore('divisions', ['name', 'conference_id'], 'name'),
                (div_name, conf_id)
            )
    db.commit()
    print("Seeded conferences and divisions.")


def seed_tournaments(db):
    rows = fetch_sheet_csv('Tourney List')
    major_weeks = [10, 15, 20, 24]

    for row in rows[1:]:
        if not row or not row[0].strip():
            continue
        week_str = row[0].strip()
        week_num = int(week_str.replace('Week ', ''))
        name = row[1].strip() if len(row) > 1 else ''
        location = row[2].strip() if len(row) > 2 else ''
        start_date = row[3].strip() if len(row) > 3 else ''
        is_major = 1 if week_num in major_weeks else 0

        db.execute(
            insert_ignore('tournaments', ['week_number', 'name', 'location', 'start_date', 'is_major'], 'week_number'),
            (week_num, name, location, start_date, is_major)
        )
    db.commit()
    print("Seeded tournaments.")


def seed_players_from_leaderboard(db):
    rows = fetch_sheet_csv('Leaderboard')

    division_cols = [
        ('Beacon Hill', 0, 1, 2),
        ('Manasquan River', 3, 4, 5),
        ('Pine Barrens', 7, 8, 9),
        ('Metedeconk', 10, 11, 12),
    ]

    for div_name, name_col, score_col, payout_col in division_cols:
        div_id = db.execute('SELECT id FROM divisions WHERE name = ?', (div_name,)).fetchone()['id']

        for row in rows[2:]:
            if not row or len(row) <= max(name_col, score_col, payout_col):
                continue
            player_name = row[name_col].strip()
            if not player_name or player_name.startswith('*'):
                continue

            try:
                float(row[score_col].strip())
            except (ValueError, IndexError):
                continue

            # Check if player already exists
            existing = db.execute(
                'SELECT id FROM players WHERE name = ? AND division_id = ?',
                (player_name, div_id)
            ).fetchone()
            if not existing:
                db.execute(
                    insert_ignore('players', ['name', 'division_id'], 'id'),
                    (player_name, div_id)
                )
        db.commit()

    count = db.execute('SELECT COUNT(*) as c FROM players').fetchone()['c']
    print(f"Seeded {count} players.")


def seed_weekly_winners(db):
    rows = fetch_sheet_csv('Weekly Winners')

    current_week = None

    for row in rows[1:]:
        if not row:
            continue

        week_cell = row[0].strip() if row[0] else ''
        division = row[1].strip() if len(row) > 1 else ''
        winner = row[2].strip() if len(row) > 2 else ''
        score = row[3].strip() if len(row) > 3 else ''
        winnings = row[4].strip() if len(row) > 4 else ''

        if week_cell.startswith('Week'):
            week_num = int(week_cell.replace('Week ', ''))
            current_week = week_num

        if current_week and division and winner:
            tourney = db.execute('SELECT id FROM tournaments WHERE week_number = ?',
                                 (current_week,)).fetchone()
            if not tourney:
                continue

            div = db.execute('SELECT id FROM divisions WHERE name = ?', (division,)).fetchone()
            if not div:
                continue

            player = db.execute('SELECT id FROM players WHERE name = ? AND division_id = ?',
                                (winner, div['id'])).fetchone()
            if not player:
                continue

            try:
                winnings_val = float(winnings.replace('$', '').replace(',', '')) if winnings else 0
            except ValueError:
                winnings_val = 0

            try:
                score_val = float(score) if score else None
            except ValueError:
                score_val = None

            db.execute('''
                INSERT INTO weekly_winners (tournament_id, division_id, player_id, score, winnings)
                VALUES (?, ?, ?, ?, ?)
            ''', (tourney['id'], div['id'], player['id'], score_val, winnings_val))

            db.execute('UPDATE tournaments SET completed = 1 WHERE id = ?', (tourney['id'],))

    db.commit()
    count = db.execute('SELECT COUNT(*) as c FROM weekly_winners').fetchone()['c']
    print(f"Seeded {count} weekly winner records.")


def seed_player_emails(db):
    """Set player emails from the division sheets (known mapping)."""
    EMAIL_MAP = {
        'Beacon Hill': {
            'Justin Verhasselt': 'Jverhasselt@gmail.com',
            'Scott Kilpatrick': 'Scott.m.Kilpatrick24@gmail.com',
            'John Vaccaro': 'johnrvaccaro@gmail.com',
            'Richie Bohny': 'richard.bohny@gmail.com',
            'Kyle Lebrecht': 'Lebrecht.k@gmail.com',
            'John Hoy': 'johnhoy89@gmail.com',
            'Tim Faranda': 'timfaranda7@gmail.com',
            'Pete Quinn': 'quinnpeter8@gmail.com',
            'Casey Verst': 'cverst@gmail.com',
            'Mike Pirozzi': 'Mapirozzi9@gmail.com',
            'John McGraner': 'johnmcgraner@gmail.com',
            'Joe Morelli': 'jpmorelli11@gmail.com',
            'Sean Williamson': 'seanwilliamson926@gmail.com',
            'Andrew Nathanson': 'andrewocm@gmail.com',
            'Jared Nathanson': 'jw.nathanson@gmail.com',
            'Chris Preuster': 'Preuster1@yahoo.com',
        },
        'Manasquan River': {
            'Mike Guarnieri': 'guarnieri67@gmail.com',
            'Greg Campanile': 'gregorycampanile@gmail.com',
            'Kevin Cooney': 'cooneynj@gmail.com',
            'Brian Dudzinski': 'Bdudzinski30@gmail.com',
            'Mike Wask': 'michael.waskiewicz89@gmail.com',
            'Mike Brunner': 'michaeljbrunner@gmail.com',
            'James Curcio': 'jamescurcio13@gmail.com',
            'Spencer Van Wagoner': 'scvanwagoner@gmail.com',
            'Charlie Tacopino': 'tacopinocharles5@gmail.com',
            'Kevin Dudzinski': 'kevindudzinski@gmail.com',
            'Marc Johnson': 'marcej51881012@gmail.com',
            'Pete O\'Hagan': 'peteohagan77@gmail.com',
            'Jimmy Boytano': 'jboytano@gmail.com',
            'Pat McGuinness': 'pat.mcguinness91@gmail.com',
            'Kevin Bohny': 'kevinbohny@gmail.com',
            'Greg Marcy': 'gregory.t.marcy@gmail.com',
        },
        'Metedeconk': {
            'Brian Desena': 'brian.desena@gmail.com',
            'Danny Orr': 'daniel.orr@placeexchange.com',
            'Colin Marshall': 'colin.marshall7@gmail.com',
            'Matt Sharkey': 'mpsharkey@gmail.com',
            'Patrick Moriarty': 'moriartypatrick9@gmail.com',
            'Matt Ubriaco': 'matthew.ubriaco@gmail.com',
            'Michael Curtis': 'michael.eugene.curtis@gmail.com',
            'Al Desanctis': 'alex.desanctis3@gmail.com',
            'Mike Arnott': 'mkarnott@gmail.com',
            'Matt Therrien': 'therrien.m.s@gmail.com',
            'Connor Ostrowski': 'connorostrowski1@gmail.com',
            'Chris Lancos': 'chris.lancos@gmail.com',
            'Cody Smith': 'codycameronsmith@gmail.com',
            'Neal Cimino': 'njc1483@gmail.com',
            'Kevin Brewster': 'kcb280@gmail.com',
            'Mike Boytano': 'maboytano@gmail.com',
        },
        'Pine Barrens': {
            'Vin Citro': 'vincitro@gmail.com',
            'Tommy Brunner': 'tkbrunner@gmail.com',
            'Chris Falvo': 'cfalvo01@gmail.com',
            'Kris Basmagy': 'bazmaygee@gmail.com',
            'Kiley Kmiec': 'kileykmiec@gmail.com',
            'Tim Rhatigan': 'rhatigan_tim@yahoo.com',
            'Brian Malloy': 'bmalloy214@gmail.com',
            'Steve Troup': 'stevenrtroup@gmail.com',
            'Ian Rappaport': 'ian@bigoutdoor.com',
            'Steve Angelovich': 'sjangelovich@gmail.com',
            'CJ Arnedo': 'carnedo@gmail.com',
            'Adam Popkin': 'apopkin@gmail.com',
            'Ryan Harkins': 'ryanharkins323@gmail.com',
            'Phillipe Hebert': 'cookedphil@gmail.com',
            'Ken Rothman': 'kenneth.rothman92@gmail.com',
            'Chris Jordan': 'christophermichaeljordan@gmail.com',
        },
    }

    # Also track alternate emails seen in pick data
    ALT_EMAILS = {
        'marcej101219@gmail.com': 'marcej51881012@gmail.com',  # Marc Johnson alt
    }

    count = 0
    for div_name, players in EMAIL_MAP.items():
        div = db.execute('SELECT id FROM divisions WHERE name = ?', (div_name,)).fetchone()
        if not div:
            continue
        for player_name, email in players.items():
            db.execute('UPDATE players SET email = ? WHERE name = ? AND division_id = ?',
                       (email.lower(), player_name, div['id']))
            count += 1
    db.commit()
    print(f"Set emails for {count} players.")
    return ALT_EMAILS


def seed_picks(db, alt_emails=None):
    """Import all historical pick data from the Pick Data sheet."""
    rows = fetch_sheet_csv('Pick Data')
    if not rows:
        print("No pick data found.")
        return

    alt_emails = alt_emails or {}
    from sql_compat import upsert_picks

    count = 0
    skipped = 0

    for row in rows[1:]:
        if not row or len(row) < 9:
            continue

        email = (row[1] or '').strip().lower()
        week_str = (row[2] or '').strip()
        pick1 = (row[3] or '').strip()
        pick2 = (row[4] or '').strip()
        pick3 = (row[5] or '').strip()
        pick4 = (row[6] or '').strip()
        alternate = (row[7] or '').strip()
        division = (row[8] or '').strip()

        if not week_str or 'test' in week_str.lower() or not pick1:
            continue

        try:
            week_num = int(week_str.replace('Week ', '').strip())
        except ValueError:
            continue

        tourney = db.execute('SELECT id FROM tournaments WHERE week_number = ?', (week_num,)).fetchone()
        if not tourney:
            skipped += 1
            continue

        # Resolve alternate emails
        if email in alt_emails:
            email = alt_emails[email]

        # Find player by email
        player = db.execute('SELECT id FROM players WHERE email = ?', (email,)).fetchone()
        if not player:
            skipped += 1
            continue

        db.execute(upsert_picks(), (
            player['id'], tourney['id'], pick1, pick2, pick3, pick4, alternate
        ))
        count += 1

    db.commit()
    print(f"Seeded {count} pick records ({skipped} skipped).")


def seed_avg_scores(db):
    """Backfill season average scores from the leaderboard."""
    rows = fetch_sheet_csv('Leaderboard')

    tournaments = db.execute(
        'SELECT id FROM tournaments WHERE completed = 1 ORDER BY week_number'
    ).fetchall()
    if not tournaments:
        print("No completed tournaments, skipping score seeding.")
        return

    division_cols = [
        ('Beacon Hill', 0, 1, 2),
        ('Manasquan River', 3, 4, 5),
        ('Pine Barrens', 7, 8, 9),
        ('Metedeconk', 10, 11, 12),
    ]

    count = 0
    for div_name, name_col, score_col, payout_col in division_cols:
        div = db.execute('SELECT id FROM divisions WHERE name = ?', (div_name,)).fetchone()
        if not div:
            continue

        for row in rows[2:]:
            if not row or len(row) <= max(name_col, score_col, payout_col):
                continue
            player_name = row[name_col].strip()
            if not player_name or player_name.startswith('*'):
                continue

            try:
                avg_score = float(row[score_col].strip())
            except (ValueError, IndexError):
                continue

            player = db.execute(
                'SELECT id FROM players WHERE name = ? AND division_id = ?',
                (player_name, div['id'])
            ).fetchone()
            if not player:
                continue

            for t in tournaments:
                db.execute(upsert_weekly_score(), (
                    player['id'], t['id'], avg_score, 0, avg_score
                ))
            count += 1

    db.commit()
    print(f"Seeded average scores for {count} players across {len(tournaments)} tournaments.")


def main():
    init_db()
    db = get_db()
    try:
        seed_conferences_and_divisions(db)
        seed_tournaments(db)
        seed_players_from_leaderboard(db)
        seed_weekly_winners(db)
        alt_emails = seed_player_emails(db)
        seed_picks(db, alt_emails)
        seed_avg_scores(db)
        print("\nDone! Database seeded from Google Sheet.")
    finally:
        db.close()


if __name__ == '__main__':
    main()
