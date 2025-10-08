import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime

from flask import Flask, render_template, request, jsonify

try:
    from tkinter import Tk, filedialog

    TKINTER_AVAILABLE = True
except Exception as exception:
    print('Tkinter not available:', exception)
    TKINTER_AVAILABLE = False
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit

LOGBOOK_FILENAME = "X-Plane Pilot.txt"
DEFAULT_LOG_FILE = f"logs/{LOGBOOK_FILENAME}"
DEFAULT_LOGBOOK_NAME = "Default Logbook"

# --- In-memory cache and watcher state ---
cached_flights = None
cached_filename = DEFAULT_LOGBOOK_NAME
watched_folder = None
observer = None
watcher_initialized = False
watcher_init_lock = threading.Lock()


def _persist_watched_folder(path):
    try:
        os.makedirs("uploads", exist_ok=True)
        with open(os.path.join("uploads", "watched_folder.txt"), "w") as f:
            f.write(path or "")
    except Exception as e:
        print('Failed to persist watched folder:', e)
        pass


def _load_persisted_watched_folder():
    path = os.path.join("uploads", "watched_folder.txt")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return f.read().strip() or None
        except Exception as e:
            print('Failed to load persisted watched folder:', e)
            return None
    return None


def parse_logbook(file_path):
    flights = []
    if not os.path.exists(file_path):
        return flights

    with open(file_path) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 11 or parts[0] != "2":
                continue

            try:
                date = datetime.strptime(parts[1], "%y%m%d").strftime("%Y-%m-%d")
                dep = parts[2]
                arr = parts[3]
                landings = int(parts[4])
                hours = float(parts[5])
                tail = parts[-2]
                aircraft = parts[-1]
                flights.append({
                    "date": date,
                    "dep": dep,
                    "arr": arr,
                    "landings": landings,
                    "hours": hours,
                    "tail": tail,
                    "aircraft": aircraft
                })
            except Exception as e:
                print('Exception occurred:', e)
                continue
    return flights


def get_current_flights():
    global cached_flights
    if cached_flights is not None:
        return cached_flights
    return parse_logbook(DEFAULT_LOG_FILE)


class LogbookHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global cached_flights, cached_filename
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == LOGBOOK_FILENAME:
            cached_flights = parse_logbook(event.src_path)
            cached_filename = os.path.basename(event.src_path)


def start_watcher(folder_path):
    global observer
    if observer is not None:
        try:
            observer.stop()
            observer.join(timeout=2)
        except Exception as e:
            print('Failed to stop existing observer:', e)
            pass
        observer = None

    handler = LogbookHandler()
    observer = Observer()
    observer.schedule(handler, folder_path, recursive=False)
    thread = threading.Thread(target=observer.start, daemon=True)
    thread.start()


@app.route("/pick_folder", methods=["POST"])
def pick_folder():
    if not TKINTER_AVAILABLE:
        return jsonify({"error": "Folder picker not available on this system."}), 501

    # Use a native folder picker dialog; this must run on the host where Flask runs
    try:
        root = Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        selected = filedialog.askdirectory(title="Select folder containing X-Plane Pilot.txt")
        root.destroy()
        if not selected:
            return jsonify({"error": "No folder selected"}), 400
        return jsonify({"folder_path": selected})
    except Exception as e:
        return jsonify({"error": f"Failed to open folder picker: {e}"}), 500


def summarise_flights(flights):
    total_hours = sum(f["hours"] for f in flights)
    flights_by_aircraft = defaultdict(float)
    count_by_aircraft = defaultdict(int)
    flights_by_route = defaultdict(int)
    flights_by_date = defaultdict(float)

    for f in flights:
        flights_by_aircraft[f["aircraft"]] += f["hours"]
        count_by_aircraft[f["aircraft"]] += 1
        flights_by_route[f"{f['dep']} → {f['arr']}"] += 1
        flights_by_date[f["date"]] += f["hours"]

    return {
        "total_hours": total_hours,
        "flights_by_aircraft": dict(flights_by_aircraft),
        "count_by_aircraft": dict(count_by_aircraft),
        "flights_by_route": dict(flights_by_route),
        "flights_by_date": dict(flights_by_date),
    }


def init_watcher_if_configured():
    global watched_folder, cached_flights, cached_filename
    persisted = _load_persisted_watched_folder()
    if persisted and os.path.isdir(persisted):
        logbook = os.path.join(persisted, LOGBOOK_FILENAME)
        if os.path.exists(logbook):
            watched_folder = persisted
            cached_flights = parse_logbook(logbook)
            cached_filename = os.path.basename(logbook)
            start_watcher(watched_folder)


@app.before_request
def ensure_watcher_initialized():
    global watcher_initialized
    if watcher_initialized:
        return
    with watcher_init_lock:
        if watcher_initialized:
            return
        init_watcher_if_configured()
        watcher_initialized = True


@app.route("/", methods=['GET', 'POST'])
def dashboard():
    global cached_flights, cached_filename
    filename = cached_filename or DEFAULT_LOGBOOK_NAME

    if request.method == "POST":
        uploaded_file = request.files.get("logfile")
        if uploaded_file and uploaded_file.filename.endswith(".txt"):
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
            uploaded_file.save(temp_path)
            filename = uploaded_file.filename
            cached_flights = parse_logbook(temp_path)
            cached_filename = filename

    flights = get_current_flights()
    data = summarise_flights(flights)

    return render_template("dashboard.html", data=data, flights=flights, filename=filename,
                           watched_folder=watched_folder)


@app.route("/set_folder", methods=["POST"])
def set_folder():
    global watched_folder, cached_flights, cached_filename
    folder_path = request.form.get("folder_path", "").strip()
    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({"error": "Invalid folder path"}), 400

    logbook_path = os.path.join(folder_path, LOGBOOK_FILENAME)
    if not os.path.exists(logbook_path):
        return jsonify({"error": f"{LOGBOOK_FILENAME} not found in folder"}), 400

    watched_folder = folder_path
    _persist_watched_folder(watched_folder)
    cached_flights = parse_logbook(logbook_path)
    cached_filename = os.path.basename(logbook_path)
    start_watcher(watched_folder)

    return jsonify({"message": f"Watching {watched_folder}"})


@app.route("/data", methods=["GET"])
def get_data():
    flights = get_current_flights()
    summary = summarise_flights(flights)
    return jsonify({
        "summary": summary,
        "flights": flights,
        "filename": cached_filename or DEFAULT_LOGBOOK_NAME,
        "watched_folder": watched_folder,
    })


if __name__ == "__main__":
    app.run(debug=True)
