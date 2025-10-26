import json
import os

# Use /data directory if it exists (Fly.io), otherwise current directory
DATA_DIR = '/data' if os.path.exists('/data') else '.'


def get_file_path(filename):
    """Get the full path for a data file"""
    return os.path.join(DATA_DIR, filename)


def ensure_json_files():
    """Create JSON files if they don't exist - call this on bot startup"""
    json_files = {
        'watch_data.json': {},
        'scheduled_votes.json': {},
        'completed_watches.json': {},
        'mod_logs.json': [],
        'status_submissions.json': {'pending': [], 'approved': []},
        'ping_history.json': []
    }

    for filename, default_content in json_files.items():
        filepath = get_file_path(filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                json.dump(default_content, f, indent=4)
            print(f'✅ Created {filepath}')


# ==================== WATCHES ====================
def load_watches():
    try:
        filepath = get_file_path('watch_data.json')
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("Warning: watch_data.json is corrupted. Starting fresh.")
        return {}


def save_watches(data):
    filepath = get_file_path('watch_data.json')
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


def load_scheduled_votes():
    try:
        filepath = get_file_path('scheduled_votes.json')
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("Warning: scheduled_votes.json is corrupted. Starting fresh.")
        return {}


def save_scheduled_votes(data):
    filepath = get_file_path('scheduled_votes.json')
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


def load_completed_watches():
    try:
        filepath = get_file_path('completed_watches.json')
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("Warning: completed_watches.json is corrupted. Starting fresh.")
        return {}


def save_completed_watches(data):
    filepath = get_file_path('completed_watches.json')
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


# ==================== MOD LOGS ====================
def load_mod_logs():
    try:
        filepath = get_file_path('mod_logs.json')
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return []
            return json.loads(content)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        print("Warning: mod_logs.json is corrupted. Starting fresh.")
        return []


def save_mod_logs(data):
    filepath = get_file_path('mod_logs.json')
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ==================== STATUS SUBMISSIONS ====================
def load_status_submissions():
    try:
        filepath = get_file_path('status_submissions.json')
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return {'pending': [], 'approved': []}
            return json.loads(content)
    except FileNotFoundError:
        return {'pending': [], 'approved': []}
    except json.JSONDecodeError:
        print("Warning: status_submissions.json is corrupted. Starting fresh.")
        return {'pending': [], 'approved': []}


def save_status_submissions(data):
    filepath = get_file_path('status_submissions.json')
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


# ==================== PING HISTORY ====================
def load_ping_history():
    try:
        filepath = get_file_path('ping_history.json')
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return []
            return json.loads(content)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        print("Warning: ping_history.json is corrupted. Starting fresh.")
        return []


def save_ping_history(data):
    filepath = get_file_path('ping_history.json')
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)