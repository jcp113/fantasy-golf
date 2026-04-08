"""
Fantasy Golf League - Flask API (production-ready)
"""
import os
import decimal
import functools
from flask import Flask, jsonify, request, send_from_directory
from database import get_db, init_db, USE_POSTGRES
from sql_compat import upsert_picks, upsert_golfer_result, upsert_weekly_score

class CustomJSONProvider(Flask.json_provider_class):
    """Handle Decimal types from Postgres ROUND()."""
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

app = Flask(__name__, static_folder='../frontend', static_url_path='')
app.json_provider_class = CustomJSONProvider
app.json = CustomJSONProvider(app)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

ADMIN_KEY = os.environ.get('ADMIN_KEY', 'admin')


# ── Helpers ─────────────────────────────────────────────────────────────────

def require_admin(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-Admin-Key', '')
        if key != ADMIN_KEY:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ── Serve frontend ──────────────────────────────────────────────────────────

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# ── API: Auth ───────────────────────────────────────────────────────────────

@app.route('/api/auth/verify', methods=['POST'])
def api_verify_pin():
    """Verify a player's PIN. Returns player info if valid."""
    data = request.get_json()
    if not data or 'player_id' not in data or 'pin' not in data:
        return jsonify({'error': 'player_id and pin required'}), 400

    db = get_db()
    player = db.execute(
        'SELECT id, name, pin FROM players WHERE id = ?', (data['player_id'],)
    ).fetchone()
    db.close()

    if not player:
        return jsonify({'error': 'Player not found'}), 404

    if not player['pin']:
        return jsonify({'error': 'PIN not set. Contact your commissioner.'}), 403

    if player['pin'] != str(data['pin']).strip():
        return jsonify({'error': 'Incorrect PIN'}), 403

    return jsonify({'success': True, 'player_id': player['id'], 'name': player['name']})


@app.route('/api/auth/set-pin', methods=['POST'])
def api_set_pin():
    """Set PIN for a player (first time setup or admin reset)."""
    data = request.get_json()
    if not data or 'player_id' not in data or 'pin' not in data:
        return jsonify({'error': 'player_id and pin required'}), 400

    new_pin = str(data['pin']).strip()
    if len(new_pin) < 4 or not new_pin.isdigit():
        return jsonify({'error': 'PIN must be at least 4 digits'}), 400

    db = get_db()
    player = db.execute(
        'SELECT id, pin FROM players WHERE id = ?', (data['player_id'],)
    ).fetchone()

    if not player:
        db.close()
        return jsonify({'error': 'Player not found'}), 404

    # Allow setting if no PIN exists yet, or if admin key is provided
    admin_key = request.headers.get('X-Admin-Key', '')
    if player['pin'] and admin_key != ADMIN_KEY:
        db.close()
        return jsonify({'error': 'PIN already set. Contact commissioner to reset.'}), 403

    db.execute('UPDATE players SET pin = ? WHERE id = ?', (new_pin, data['player_id']))
    db.commit()
    db.close()
    return jsonify({'success': True, 'message': 'PIN set successfully'})


# ── API: Standings ──────────────────────────────────────────────────────────

@app.route('/api/standings')
def api_standings():
    db = get_db()
    if USE_POSTGRES:
        standings = db.execute('''
            SELECT
                p.id, p.name,
                d.name as division, d.id as division_id,
                c.name as conference,
                ROUND(AVG(ws.final_score)::numeric, 1) as avg_score,
                COUNT(ws.id) as weeks_played,
                COALESCE(MAX(earnings.total), 0) as total_winnings
            FROM players p
            JOIN divisions d ON p.division_id = d.id
            JOIN conferences c ON d.conference_id = c.id
            LEFT JOIN weekly_scores ws ON ws.player_id = p.id
            LEFT JOIN (
                SELECT player_id, SUM(winnings) as total
                FROM weekly_winners
                GROUP BY player_id
            ) earnings ON earnings.player_id = p.id
            WHERE p.active = 1
            GROUP BY p.id, p.name, d.name, d.id, c.name
            ORDER BY c.name, d.name, avg_score ASC NULLS LAST
        ''').fetchall()
    else:
        standings = db.execute('''
            SELECT
                p.id, p.name,
                d.name as division, d.id as division_id,
                c.name as conference,
                ROUND(AVG(ws.final_score), 1) as avg_score,
                COUNT(ws.id) as weeks_played,
                COALESCE(earnings.total, 0) as total_winnings
            FROM players p
            JOIN divisions d ON p.division_id = d.id
            JOIN conferences c ON d.conference_id = c.id
            LEFT JOIN weekly_scores ws ON ws.player_id = p.id
            LEFT JOIN (
                SELECT player_id, SUM(winnings) as total
                FROM weekly_winners
                GROUP BY player_id
            ) earnings ON earnings.player_id = p.id
            WHERE p.active = 1
            GROUP BY p.id
            ORDER BY c.name, d.name, avg_score ASC
        ''').fetchall()
    db.close()

    result = {}
    for row in standings:
        conf = row['conference']
        div = row['division']
        if conf not in result:
            result[conf] = {}
        if div not in result[conf]:
            result[conf][div] = []
        result[conf][div].append({
            'id': row['id'],
            'name': row['name'],
            'avg_score': float(row['avg_score']) if row['avg_score'] is not None else None,
            'weeks_played': row['weeks_played'],
            'total_winnings': float(row['total_winnings']),
        })

    return jsonify(result)


# ── API: Tournaments ────────────────────────────────────────────────────────

@app.route('/api/tournaments')
def api_tournaments():
    db = get_db()
    rows = db.execute('SELECT * FROM tournaments ORDER BY week_number').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/tournaments/current')
def api_current_tournament():
    db = get_db()
    row = db.execute('''
        SELECT * FROM tournaments
        WHERE completed = 0
        ORDER BY week_number ASC
        LIMIT 1
    ''').fetchone()
    if not row:
        row = db.execute('SELECT * FROM tournaments ORDER BY week_number DESC LIMIT 1').fetchone()
    db.close()
    return jsonify(dict(row) if row else {})


# ── API: Weekly Results ─────────────────────────────────────────────────────

@app.route('/api/weekly/<int:week>')
def api_weekly(week):
    db = get_db()
    tourney = db.execute(
        'SELECT * FROM tournaments WHERE week_number = ?', (week,)
    ).fetchone()

    if not tourney:
        db.close()
        return jsonify({'error': 'Week not found'}), 404

    winners = db.execute('''
        SELECT ww.score, ww.winnings,
               p.name as player_name,
               d.name as division
        FROM weekly_winners ww
        JOIN players p ON ww.player_id = p.id
        JOIN divisions d ON ww.division_id = d.id
        WHERE ww.tournament_id = ?
        ORDER BY d.name
    ''', (tourney['id'],)).fetchall()

    db.close()
    return jsonify({
        'tournament': dict(tourney),
        'winners': [dict(w) for w in winners],
    })


@app.route('/api/weekly')
def api_weekly_all():
    db = get_db()
    weeks = db.execute('''
        SELECT DISTINCT t.week_number, t.name, t.is_major, t.start_date
        FROM weekly_winners ww
        JOIN tournaments t ON ww.tournament_id = t.id
        ORDER BY t.week_number
    ''').fetchall()
    db.close()
    return jsonify([dict(w) for w in weeks])


# ── API: Picks ──────────────────────────────────────────────────────────────

@app.route('/api/picks', methods=['POST'])
def api_submit_picks():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    required = ['player_id', 'tournament_id', 'pick1', 'pick2', 'pick3', 'pick4', 'pin']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing field: {field}'}), 400

    db = get_db()

    # Authenticate player
    player = db.execute('SELECT * FROM players WHERE id = ?', (data['player_id'],)).fetchone()
    if not player:
        db.close()
        return jsonify({'error': 'Player not found'}), 404

    if not player['pin'] or player['pin'] != str(data['pin']).strip():
        db.close()
        return jsonify({'error': 'Incorrect PIN'}), 403

    # Validate tournament
    tourney = db.execute('SELECT * FROM tournaments WHERE id = ?', (data['tournament_id'],)).fetchone()
    if not tourney:
        db.close()
        return jsonify({'error': 'Tournament not found'}), 404

    # Major golfer repeat check
    if tourney['is_major']:
        prev_major_picks = db.execute('''
            SELECT pick1, pick2, pick3, pick4 FROM picks pk
            JOIN tournaments t ON pk.tournament_id = t.id
            WHERE pk.player_id = ? AND t.is_major = 1 AND t.id != ?
        ''', (data['player_id'], data['tournament_id'])).fetchall()

        used_golfers = set()
        for pp in prev_major_picks:
            used_golfers.update([pp['pick1'].lower(), pp['pick2'].lower(),
                                 pp['pick3'].lower(), pp['pick4'].lower()])

        new_picks = [data['pick1'].lower(), data['pick2'].lower(),
                     data['pick3'].lower(), data['pick4'].lower()]
        repeats = [p for p in new_picks if p in used_golfers]
        if repeats:
            db.close()
            return jsonify({
                'error': f'Cannot repeat golfers in majors. Already used: {", ".join(repeats)}'
            }), 400

    try:
        db.execute(upsert_picks(), (
            data['player_id'], data['tournament_id'],
            data['pick1'], data['pick2'], data['pick3'], data['pick4'],
            data.get('alternate', '')
        ))
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({'error': str(e)}), 500

    db.close()
    return jsonify({'success': True, 'message': 'Picks submitted successfully'})


@app.route('/api/picks/used-major-golfers/<int:player_id>')
def api_used_major_golfers(player_id):
    db = get_db()
    rows = db.execute('''
        SELECT pk.pick1, pk.pick2, pk.pick3, pk.pick4, t.name as tournament_name, t.week_number
        FROM picks pk
        JOIN tournaments t ON pk.tournament_id = t.id
        WHERE pk.player_id = ? AND t.is_major = 1
        ORDER BY t.week_number
    ''', (player_id,)).fetchall()
    db.close()

    used = []
    for row in rows:
        tourney_label = f"Week {row['week_number']}: {row['tournament_name']}"
        for golfer in [row['pick1'], row['pick2'], row['pick3'], row['pick4']]:
            if golfer:
                used.append({'golfer': golfer, 'tournament': tourney_label})

    return jsonify(used)


@app.route('/api/picks/<int:tournament_id>/<int:player_id>')
def api_get_picks(tournament_id, player_id):
    db = get_db()
    pick = db.execute(
        'SELECT * FROM picks WHERE tournament_id = ? AND player_id = ?',
        (tournament_id, player_id)
    ).fetchone()
    db.close()
    return jsonify(dict(pick) if pick else {})


# ── API: Players ────────────────────────────────────────────────────────────

@app.route('/api/players')
def api_players():
    db = get_db()
    rows = db.execute('''
        SELECT p.id, p.name, d.name as division, c.name as conference,
               CASE WHEN p.pin IS NOT NULL THEN 1 ELSE 0 END as has_pin
        FROM players p
        JOIN divisions d ON p.division_id = d.id
        JOIN conferences c ON d.conference_id = c.id
        WHERE p.active = 1
        ORDER BY p.name
    ''').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/divisions')
def api_divisions():
    db = get_db()
    rows = db.execute('''
        SELECT d.id, d.name, c.name as conference
        FROM divisions d
        JOIN conferences c ON d.conference_id = c.id
        ORDER BY c.name, d.name
    ''').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


# ── API: Golfer names ──────────────────────────────────────────────────────

@app.route('/api/golfers')
def api_golfers():
    db = get_db()
    rows = db.execute('''
        SELECT DISTINCT golfer_name FROM golfer_results
        ORDER BY golfer_name
    ''').fetchall()
    db.close()

    if not rows:
        return jsonify([])

    return jsonify([r['golfer_name'] for r in rows])


# ── API: Admin ──────────────────────────────────────────────────────────────

@app.route('/api/admin/calculate-scores/<int:tournament_id>', methods=['POST'])
@require_admin
def api_calculate_scores(tournament_id):
    from scoring import calculate_weekly_scores
    try:
        calculate_weekly_scores(tournament_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/fetch-results/<int:tournament_id>', methods=['POST'])
@require_admin
def api_fetch_results(tournament_id):
    from pga_results import fetch_and_save_results
    success = fetch_and_save_results(tournament_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'error': 'Could not fetch results'}), 500


@app.route('/api/admin/reset-pin/<int:player_id>', methods=['POST'])
@require_admin
def api_reset_pin(player_id):
    db = get_db()
    db.execute('UPDATE players SET pin = NULL WHERE id = ?', (player_id,))
    db.commit()
    db.close()
    return jsonify({'success': True})


# ── Init & Run ──────────────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    app.run(debug=debug, host='0.0.0.0', port=port)
