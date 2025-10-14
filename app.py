import os
import re
import tempfile
import threading
from collections import defaultdict
from csv import reader
from datetime import datetime

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

try:
    from tkinter import Tk, filedialog

    TKINTER_AVAILABLE = True
except Exception as exception:
    print('Tkinter not available:', exception)
    TKINTER_AVAILABLE = False
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit

LOGBOOK_FILENAME = "X-Plane Pilot.txt"
DEFAULT_LOG_FILE = f"logs/{LOGBOOK_FILENAME}"
DEFAULT_LOGBOOK_NAME = "Default Logbook"

LANDING_RATE_FILENAME = "LandingRate.log"

# --- In-memory cache and watcher state ---
cached_flights = None
cached_filename = DEFAULT_LOGBOOK_NAME
watched_folder = None
observer = None
watcher_initialized = False
watcher_init_lock = threading.Lock()

# Landing rate in-memory state and watcher
cached_landings = []
landing_rate_path = None  # explicit file path or folder containing LandingRate.log
landing_watched_folder = None  # folder being watched (file's parent if file set)
landing_observer = None

# Derived maps
landing_links = {}  # key: landing index -> { flight, linkConfidence }
flight_link_index_by_key = {}  # key: (date, norm_ac, flight_idx_in_group) -> landing indices
flight_to_landing_indices = {}  # key: global flight idx -> [landing indices]

# Manual overrides: landing index -> flight index
manual_overrides = {}

# Heuristic config (in minutes). Can override via env LANDING_CLUSTER_MINUTES
try:
    CLUSTER_MINUTES = int(os.getenv('LANDING_CLUSTER_MINUTES', '10'))
except Exception as config_exception:
    print('Failed to parse LANDING_CLUSTER_MINUTES env var:', config_exception)
    CLUSTER_MINUTES = 10


def _persist_config():
    try:
        import json
        os.makedirs("uploads", exist_ok=True)
        with open(os.path.join("uploads", "config.json"), "w") as f:
            json.dump({"cluster_minutes": CLUSTER_MINUTES}, f)
    except Exception as e:
        print('Failed to persist config:', e)


def _load_config():
    global CLUSTER_MINUTES
    path = os.path.join("uploads", "config.json")
    if os.path.exists(path):
        try:
            import json
            with open(path) as f:
                data = json.load(f) or {}
                cm = int(data.get("cluster_minutes", CLUSTER_MINUTES))
                if 1 <= cm <= 60:
                    CLUSTER_MINUTES = cm
        except Exception as e:
            print('Failed to load config:', e)


def _persist_watched_folder(path):
    try:
        os.makedirs("uploads", exist_ok=True)
        with open(os.path.join("uploads", "watched_folder.txt"), "w") as f:
            f.write(path or "")
    except Exception as e:
        print('Failed to persist watched folder:', e)


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


def _persist_landing_rate_path(path):
    try:
        os.makedirs("uploads", exist_ok=True)
        with open(os.path.join("uploads", "landing_rate_path.txt"), "w") as f:
            f.write(path or "")
    except Exception as e:
        print('Failed to persist landing rate path:', e)


def _load_persisted_landing_rate_path():
    path = os.path.join("uploads", "landing_rate_path.txt")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return f.read().strip() or None
        except Exception as e:
            print('Failed to load persisted landing rate path:', e)
            return None
    return None


def _persist_manual_links(mapping: dict):
    try:
        import json
        os.makedirs("uploads", exist_ok=True)
        with open(os.path.join("uploads", "manual_links.json"), "w") as f:
            json.dump(mapping or {}, f)
    except Exception as e:
        print('Failed to persist manual links:', e)


def _load_persisted_manual_links():
    path = os.path.join("uploads", "manual_links.json")
    if os.path.exists(path):
        try:
            import json
            with open(path) as f:
                data = json.load(f) or {}
                # ensure keys are ints
                return {int(k): int(v) for k, v in data.items() if isinstance(k, (str, int))}
        except Exception as e:
            print('Failed to load persisted manual links:', e)
            return {}
    return {}


def normalize_aircraft(name: str) -> str:
    if not name:
        return ""
    token = re.sub(r"[^A-Za-z0-9]", "", str(name)).upper()
    if token.startswith("C172") or "CESSNA172" in token or "CESSNA172SP" in token:
        return "C172"
    if "B738" in token or "B737800" in token or token.startswith("ZB738"):
        return "B738"
    return token


def parse_landing_rates(file_path):
    rows = []
    if not file_path or not os.path.exists(file_path):
        return rows
    try:
        with open(file_path, newline='') as csvfile:
            for parts in reader(csvfile):
                if not parts or len(parts) < 9:
                    continue
                try:
                    # Columns: time, Aircraft, VS, G, noserate, float, quality, Q, Qrad_abs
                    time_str = parts[0].strip()
                    # Parse to ISO and date
                    dt = None
                    try:
                        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        print('Failed to parse time with first format:', e)
                        # Fallback: try common variants
                        try:
                            dt = datetime.fromisoformat(time_str.replace("/", "-").replace("T", " "))
                        except Exception as e2:
                            print('Failed to parse time with fallback format:', e2)
                            dt = None
                    iso_time = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else time_str
                    date_only = dt.strftime("%Y-%m-%d") if dt else time_str.split(" ")[0]

                    ac = parts[1].strip()
                    vs = float(parts[2]) if parts[2] else None
                    g = float(parts[3]) if parts[3] else None
                    nose_rate = float(parts[4]) if parts[4] else None
                    flt = float(parts[5]) if parts[5] else None
                    quality = parts[6].strip()
                    q = float(parts[7]) if parts[7] else None
                    qrad_abs = float(parts[8]) if parts[8] else None

                    norm_ac = normalize_aircraft(ac)
                    rows.append({
                        "time": iso_time,
                        "date": date_only,
                        "aircraft": ac,
                        "norm_ac": norm_ac,
                        "VS": vs,
                        "G": g,
                        "nose_rate": nose_rate,
                        "float": flt,
                        "quality": quality,
                        "Q": q,
                        "Qrad_abs": qrad_abs,
                    })
                except Exception as e:
                    print('Failed to parse landing rate line:', e)
                    # Skip malformed line
                    continue
    except Exception as e:
        print('Failed to parse landing rates:', e)
    return rows


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
                norm_ac = normalize_aircraft(aircraft)
                flights.append({
                    "date": date,
                    "dep": dep,
                    "arr": arr,
                    "landings": landings,
                    "hours": hours,
                    "tail": tail,
                    "aircraft": aircraft,
                    "norm_ac": norm_ac,
                })
            except Exception as e:
                print('Exception occurred:', e)
                continue
    return flights


def get_current_flights():
    global cached_flights, watched_folder
    # Prefer the actively watched folder if set
    if watched_folder:
        watched_path = os.path.join(watched_folder, LOGBOOK_FILENAME)
        if os.path.exists(watched_path):
            return parse_logbook(watched_path)
    if cached_flights is not None:
        return cached_flights
    return parse_logbook(DEFAULT_LOG_FILE)


def get_current_landings():
    global landing_rate_path, landing_watched_folder
    # If explicit file path set, prefer it
    if landing_rate_path and os.path.isfile(landing_rate_path):
        return parse_landing_rates(landing_rate_path)
    # If a folder is set for landing rates, read from there
    if landing_watched_folder:
        path = os.path.join(landing_watched_folder, LANDING_RATE_FILENAME)
        if os.path.exists(path):
            return parse_landing_rates(path)
    # Try the logbook watched folder as a fallback
    if watched_folder:
        path = os.path.join(watched_folder, LANDING_RATE_FILENAME)
        if os.path.exists(path):
            return parse_landing_rates(path)
    # Default to project logs folder if exists
    default_path = os.path.join("logs", LANDING_RATE_FILENAME)
    if os.path.exists(default_path):
        return parse_landing_rates(default_path)
    return []


class LogbookHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global cached_flights, cached_filename
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == LOGBOOK_FILENAME:
            cached_flights = parse_logbook(event.src_path)
            cached_filename = os.path.basename(event.src_path)
            broadcast_update()

    def on_created(self, event):
        # Some editors write by creating a new file and replacing the old
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == LOGBOOK_FILENAME:
            self.on_modified(event)

    def on_moved(self, event):
        # X-Plane or OS may move/replace the file atomically
        try:
            dest_path = getattr(event, 'dest_path', None)
        except Exception as e:
            print('Error getting dest_path from event:', e)
            dest_path = None
        path = dest_path or event.src_path
        if os.path.basename(path) == LOGBOOK_FILENAME:
            class E:  # minimal shim with required attrs
                is_directory = False
                src_path = path

            self.on_modified(E)


def start_watcher(folder_path):
    global observer
    if observer is not None:
        try:
            observer.stop()
            observer.join(timeout=2)
        except Exception as e:
            print('Failed to stop existing observer:', e)
        observer = None

    handler = LogbookHandler()
    observer = Observer()
    observer.schedule(handler, folder_path, recursive=False)
    thread = threading.Thread(target=observer.start, daemon=True)
    thread.start()


class LandingRateHandler(FileSystemEventHandler):
    def __init__(self, target_filename=LANDING_RATE_FILENAME):
        super().__init__()
        self.target = target_filename

    @staticmethod
    def _maybe_refresh(path):
        global cached_landings
        try:
            cached_landings = get_current_landings()
            recompute_links()
            broadcast_landing_update()
        except Exception as e:
            print('Failed to refresh landing rates:', e)

    def on_modified(self, event):
        if event.is_directory:
            return
        base = os.path.basename(event.src_path)
        if base == self.target or landing_rate_path and os.path.abspath(event.src_path) == os.path.abspath(
                landing_rate_path):
            self._maybe_refresh(event.src_path)

    def on_created(self, event):
        if event.is_directory:
            return
        self.on_modified(event)

    def on_moved(self, event):
        try:
            dest_path = getattr(event, 'dest_path', None)
        except Exception as e:
            print('Error getting dest_path from event:', e)
            dest_path = None
        path = dest_path or event.src_path
        base = os.path.basename(path)
        if base == self.target or (landing_rate_path and os.path.abspath(path) == os.path.abspath(landing_rate_path)):
            self._maybe_refresh(path)


def start_landing_rate_watcher(folder_path):
    global landing_observer
    if landing_observer is not None:
        try:
            landing_observer.stop()
            landing_observer.join(timeout=2)
        except Exception as e:
            print('Failed to stop existing landing observer:', e)
        landing_observer = None
    handler = LandingRateHandler()
    landing_observer = Observer()
    landing_observer.schedule(handler, folder_path, recursive=False)
    thread = threading.Thread(target=landing_observer.start, daemon=True)
    thread.start()


def broadcast_update():
    try:
        flights = get_current_flights()
        # Prepare landing mapping for dashboard consumers
        landing_idx_for_flight = []
        landing_avail = False
        try:
            recompute_links()
            landing_avail = bool(
                (landing_rate_path or landing_watched_folder) or (cached_landings and len(cached_landings) > 0))
            for i, _ in enumerate(flights):
                indices = flight_to_landing_indices.get(i) if 'flight_to_landing_indices' in globals() else None
                landing_idx_for_flight.append(indices[0] if indices else None)
        except Exception as e:
            print('Failed to compute landing indices for broadcast:', e)
            landing_idx_for_flight = [None] * len(flights)
            landing_avail = False
        payload = {
            "summary": summarise_flights(flights),
            "flights": flights,
            "filename": cached_filename or DEFAULT_LOGBOOK_NAME,
            "watched_folder": watched_folder,
            "landing_index_for_flight": landing_idx_for_flight,
            "landing_available": landing_avail,
        }
        socketio.emit("log_update", payload)
    except Exception as e:
        print('Failed to broadcast update:', e)


def summarise_landings(landings):
    # compute simple summary for charts
    from collections import defaultdict
    vs_values = [l.get("VS") for l in landings if isinstance(l.get("VS"), (int, float))]
    mean_vs = sum(vs_values) / len(vs_values) if vs_values else 0.0
    avg_vs_by_ac = defaultdict(list)
    for l in landings:
        if isinstance(l.get("VS"), (int, float)):
            avg_vs_by_ac[l.get("norm_ac")].append(l.get("VS"))
    avg_vs_per_aircraft = {k: (sum(v) / len(v) if v else 0.0) for k, v in avg_vs_by_ac.items()}
    return {
        "count": len(landings),
        "mean_vs": mean_vs,
        "avg_vs_per_aircraft": avg_vs_per_aircraft,
    }


def recompute_links():
    global landing_links, flight_link_index_by_key, flight_to_landing_indices, manual_overrides
    flights = get_current_flights()
    landings = cached_landings or []
    # Build groups
    from collections import defaultdict
    flights_by_group = defaultdict(list)
    for idx, f in enumerate(flights):
        if int(f.get("landings", 0)) >= 1:
            key = (f.get("date"), f.get("norm_ac"))
            flights_by_group[key].append({"idx": idx, "flight": f})
    landings_by_group = defaultdict(list)
    for i, l in enumerate(landings):
        key = (l.get("date"), l.get("norm_ac"))
        landings_by_group[key].append({"idx": i, "landing": l})
    # Sort landings by time within group
    for key in landings_by_group.keys():
        landings_by_group[key].sort(key=lambda x: x["landing"].get("time"))

    # Reset outputs
    landing_links = {}
    flight_link_index_by_key = {}
    flight_to_landing_indices = {}

    for key, landing_list in landings_by_group.items():
        flights_list = flights_by_group.get(key, [])
        lf = len(flights_list)
        ll = len(landing_list)
        if lf == 0:
            for item in landing_list:
                landing_links[item["idx"]] = {"flight": None, "linkConfidence": "unmatched"}
            continue

        # Apply manual overrides first for this group (remove from pools)
        overridden_landing_indices = set()
        overridden_flight_indices = set()
        for item in landing_list:
            li = item["idx"]
            if li in manual_overrides:
                fidx = manual_overrides.get(li)
                if isinstance(fidx, int) and 0 <= fidx < len(flights):
                    fref_all = flights[fidx]
                    # Accept override even if cross-group; but tag as manual
                    landing_links[li] = {"flight": fref_all, "flightIndex": fidx, "linkConfidence": "manual"}
                    flight_to_landing_indices.setdefault(fidx, []).append(li)
                    overridden_landing_indices.add(li)
                    overridden_flight_indices.add(fidx)

        # Build working pools excluding overrides
        working_landings = [it for it in landing_list if it["idx"] not in overridden_landing_indices]
        working_flights = [it for it in flights_list if it["idx"] not in overridden_flight_indices]

        lf_work = len(working_flights)
        ll_work = len(working_landings)

        if lf_work == 0 and ll_work > 0:
            for item in working_landings:
                landing_links[item["idx"]] = {"flight": None, "linkConfidence": "unmatched"}
            continue

        # Case: one-to-one
        if lf_work == 1 and ll_work == 1:
            fref = working_flights[0]
            item = working_landings[0]
            landing_links[item["idx"]] = {"flight": fref["flight"], "flightIndex": fref["idx"],
                                          "linkConfidence": "unique-date-aircraft"}
            flight_key = (fref["flight"]["date"], fref["flight"]["norm_ac"], 0)
            flight_link_index_by_key.setdefault(flight_key, []).append(item["idx"])
            flight_to_landing_indices.setdefault(fref["idx"], []).append(item["idx"])
            continue

        # Heuristic 1: sequence-assumed when counts equal
        if lf_work == ll_work and lf_work > 0:
            for i, item in enumerate(working_landings):
                fref = working_flights[i]
                landing_links[item["idx"]] = {"flight": fref["flight"], "flightIndex": fref["idx"],
                                              "linkConfidence": "sequence-assumed"}
                flight_key = (fref["flight"]["date"], fref["flight"]["norm_ac"], i)
                flight_link_index_by_key.setdefault(flight_key, []).append(item["idx"])
                flight_to_landing_indices.setdefault(fref["idx"], []).append(item["idx"])
            continue

        # Heuristic 2: cluster landings by short time window, then assign clusters
        def _parse_dt_safe(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            except Exception as parse_exception:
                print('Failed to parse datetime in cluster heuristic:', parse_exception)
                try:
                    return datetime.fromisoformat(str(s).replace("/", "-").replace("T", " "))
                except Exception as normalisation_exception:
                    print('Failed to parse datetime in cluster heuristic fallback:', normalisation_exception)
                    return None
        clusters = []  # list[list[item]] where item ∈ working_landings
        if ll_work > 0:
            sorted_work = sorted(working_landings, key=lambda x: x["landing"].get("time") or "")
            current = []
            last_dt = None
            for it in sorted_work:
                tstr = it["landing"].get("time")
                dt = _parse_dt_safe(tstr)
                if not current:
                    current = [it]
                    last_dt = dt
                else:
                    delta_min = None
                    if last_dt and dt:
                        try:
                            delta_min = abs((dt - last_dt).total_seconds()) / 60.0
                        except Exception as e:
                            print('Failed to compute delta minutes in clustering:', e)
                            delta_min = None
                    if delta_min is not None and delta_min <= CLUSTER_MINUTES:
                        current.append(it)
                        last_dt = dt or last_dt
                    else:
                        clusters.append(current)
                        current = [it]
                        last_dt = dt
            if current:
                clusters.append(current)

        if clusters:
            total_clusters = len(clusters)
            total_declared = sum(int(max(0, f["flight"].get("landings", 0))) for f in working_flights)
            # Case A: clusters count matches flights count → sequence by cluster
            if total_clusters == lf_work and lf_work > 0:
                for i, cluster in enumerate(clusters):
                    fref = working_flights[i]
                    for it in cluster:
                        landing_links[it["idx"]] = {"flight": fref["flight"], "flightIndex": fref["idx"],
                                                      "linkConfidence": "cluster-sequence"}
                        flight_to_landing_indices.setdefault(fref["idx"], []).append(it["idx"])
                continue
            # Case B: clusters count matches total declared → distribute by declared counts
            if total_declared == total_clusters and total_declared > 0:
                ci = 0
                for fref in working_flights:
                    k = int(max(0, fref["flight"].get("landings", 0)))
                    for _ in range(k):
                        if ci >= total_clusters:
                            break
                        for it in clusters[ci]:
                            landing_links[it["idx"]] = {"flight": fref["flight"], "flightIndex": fref["idx"],
                                                          "linkConfidence": "cluster-assigned"}
                            flight_to_landing_indices.setdefault(fref["idx"], []).append(it["idx"])
                        ci += 1
                # Mark any leftover in working_landings that didn't get assigned as ambiguous
                assigned = {it["idx"] for k in range(min(ci, total_clusters)) for it in clusters[k]}
                for it in working_landings:
                    if it["idx"] not in assigned and it["idx"] not in landing_links:
                        landing_links[it["idx"]] = {"flight": None, "linkConfidence": "ambiguous"}
                continue

        # Heuristic 3: distribute by declared flight landing counts if totals match (raw landings)
        total_declared = sum(int(max(0, f["flight"].get("landings", 0))) for f in working_flights)
        if ll_work > 0 and total_declared == ll_work and total_declared > 0:
            cursor = 0
            for fi, fref in enumerate(working_flights):
                k = int(max(0, fref["flight"].get("landings", 0)))
                for j in range(k):
                    if cursor >= ll_work:
                        break
                    item = working_landings[cursor]
                    landing_links[item["idx"]] = {"flight": fref["flight"], "flightIndex": fref["idx"],
                                                  "linkConfidence": "count-assigned"}
                    flight_to_landing_indices.setdefault(fref["idx"], []).append(item["idx"])
                    cursor += 1
            # Any leftovers should be marked ambiguous (shouldn't happen if sums matched)
            for r in range(cursor, ll_work):
                item = working_landings[r]
                landing_links[item["idx"]] = {"flight": None, "linkConfidence": "ambiguous"}
            continue

        # Fallback: ambiguous for remaining
        for item in working_landings:
            landing_links[item["idx"]] = {"flight": None, "linkConfidence": "ambiguous"}


def broadcast_landing_update():
    try:
        payload = {
            "landings": cached_landings,
            "links": landing_links,
            "flight_to_landing_indices": flight_to_landing_indices,
            "summary": summarise_landings(cached_landings),
            "source": {
                "file": landing_rate_path,
                "folder": landing_watched_folder,
            }
        }
        socketio.emit("landing_rate_update", payload)
    except Exception as e:
        print('Failed to broadcast landing update:', e)


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
    # Landing rates: try persisted, else try watched_folder, else logs/LandingRate.log
    try:
        global cached_landings, landing_rate_path, landing_watched_folder
        persisted_lr = _load_persisted_landing_rate_path()
        if persisted_lr:
            if os.path.isdir(persisted_lr):
                landing_watched_folder = persisted_lr
                path = os.path.join(landing_watched_folder, LANDING_RATE_FILENAME)
                cached_landings = parse_landing_rates(path) if os.path.exists(path) else []
                start_landing_rate_watcher(landing_watched_folder)
            elif os.path.isfile(persisted_lr):
                landing_rate_path = persisted_lr
                landing_watched_folder = os.path.dirname(landing_rate_path)
                cached_landings = parse_landing_rates(landing_rate_path)
                start_landing_rate_watcher(landing_watched_folder)
        elif watched_folder:
            # auto: if LandingRate.log exists alongside logbook, use it
            candidate = os.path.join(watched_folder, LANDING_RATE_FILENAME)
            if os.path.exists(candidate):
                landing_watched_folder = watched_folder
                cached_landings = parse_landing_rates(candidate)
                start_landing_rate_watcher(landing_watched_folder)
        else:
            default_candidate = os.path.join("logs", LANDING_RATE_FILENAME)
            if os.path.exists(default_candidate):
                landing_watched_folder = "logs"
                cached_landings = parse_landing_rates(default_candidate)
                start_landing_rate_watcher(landing_watched_folder)
        # Load config and manual overrides
        try:
            _load_config()
            global manual_overrides
            manual_overrides = _load_persisted_manual_links()
        except Exception as e:
            print('Failed to load manual overrides:', e)
        recompute_links()
    except Exception as e:
        print('Failed to initialize landing rate watcher:', e)


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
            broadcast_update()

    flights = get_current_flights()
    # Ensure links are up to date and build a per-flight mapping to first landing index
    try:
        global cached_landings
        if cached_landings is None or not isinstance(cached_landings, list):
            cached_landings = get_current_landings()
        recompute_links()
        landing_index_for_flight = []
        for i, _ in enumerate(flights):
            indices = flight_to_landing_indices.get(i) if 'flight_to_landing_indices' in globals() else None
            landing_index_for_flight.append(indices[0] if indices else None)
    except Exception as e:
        print('Failed to compute landing indices for flights:', e)
        landing_index_for_flight = [None] * len(flights)
    data = summarise_flights(flights)

    landing_available = bool(
        (landing_rate_path or landing_watched_folder) or (cached_landings and len(cached_landings) > 0))
    return render_template("dashboard.html", data=data, flights=flights, filename=filename,
                           watched_folder=watched_folder,
                           landing_index_for_flight=landing_index_for_flight,
                           landing_available=landing_available)


@app.route("/landing-rates", methods=['GET'])
def landing_rates_page():
    # Initial render, page will fetch live data via socket/API
    global landing_rate_path, landing_watched_folder
    landings = cached_landings or get_current_landings()
    try:
        recompute_links()
    except Exception as e:
        print('Failed to recompute links for landing rates page:', e)
    return render_template("landing_rates.html",
                           summary=summarise_landings(landings),
                           landings=landings,
                           links=landing_links,
                           source_file=landing_rate_path,
                           source_folder=landing_watched_folder)


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
    print(f"Watching folder set to: {watched_folder}")
    broadcast_update()
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


# --- Landing rate endpoints ---

@app.route("/landing-rates/data", methods=["GET"])
def get_landing_data():
    landings = cached_landings or get_current_landings()
    return jsonify({
        "landings": landings,
        "links": landing_links,
        "flight_to_landing_indices": flight_to_landing_indices,
        "summary": summarise_landings(landings),
        "source": {
            "file": landing_rate_path,
            "folder": landing_watched_folder,
        },
        "cluster_minutes": CLUSTER_MINUTES,
    })


# --- Manual linking endpoints ---

@app.route("/links/resolve", methods=["POST"])
def resolve_link():
    try:
        li = int(request.form.get("landing_index", ""))
        fi = int(request.form.get("flight_index", ""))
    except Exception as e:
        print('Error parsing indices for resolve_link:', e)
        return jsonify({"error": "Invalid indices"}), 400
    global manual_overrides
    manual_overrides[li] = fi
    _persist_manual_links(manual_overrides)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Link set", "landing_index": li, "flight_index": fi})


@app.route("/links/candidates", methods=["GET"])
def candidates_for_landing():
    try:
        landing_index = int(request.args.get("landing_index", ""))
    except Exception as e:
        print('Error parsing landing_index for candidates:', e)
        return jsonify({"error": "Invalid landing_index"}), 400
    flights = get_current_flights()
    landings = cached_landings or []
    if landing_index < 0 or landing_index >= len(landings):
        return jsonify({"error": "landing_index out of range"}), 400
    landing = landings[landing_index]
    # Basic candidate heuristic: same (date, norm_ac)
    same_group = []
    for idx, flight in enumerate(flights):
        if flight.get("date") == landing.get("date") and flight.get("norm_ac") == landing.get("norm_ac"):
            same_group.append({"flight_index": idx, "flight": flight})
    return jsonify({"candidates": same_group})


@app.route("/links/list", methods=["GET"])
def list_overrides():
    flights = get_current_flights()
    landings = cached_landings or []
    items = []
    for li, fi in (manual_overrides or {}).items():
        landing = landings[li] if 0 <= li < len(landings) else None
        flight = flights[fi] if 0 <= fi < len(flights) else None
        items.append({"landing_index": li, "flight_index": fi, "landing": landing, "flight": flight})
    return jsonify({"overrides": items})


@app.route("/links/clear", methods=["POST"])
def clear_override():
    global manual_overrides
    li_raw = request.form.get("landing_index", "").strip()
    if li_raw:
        try:
            li = int(li_raw)
        except Exception as e:
            print('Error parsing landing_index for clear_override:', e)
            return jsonify({"error": "Invalid landing_index"}), 400
        if li in manual_overrides:
            manual_overrides.pop(li, None)
    else:
        # Clear all
        manual_overrides = {}
    _persist_manual_links(manual_overrides)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Cleared"})


@app.route("/config/cluster", methods=["POST"])
def set_cluster_minutes():
    global CLUSTER_MINUTES
    try:
        minutes = int(request.form.get("minutes", ""))
    except Exception as e:
        print('Error parsing minutes for set_cluster_minutes:', e)
        return jsonify({"error": "Invalid minutes"}), 400
    if minutes < 1 or minutes > 60:
        return jsonify({"error": "Minutes out of range (1-60)"}), 400
    CLUSTER_MINUTES = minutes
    _persist_config()
    # Recompute with new setting
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Updated", "cluster_minutes": CLUSTER_MINUTES})


@app.route("/pick_landing_rate_folder", methods=["POST"])
def pick_landing_rate_folder():
    if not TKINTER_AVAILABLE:
        return jsonify({"error": "Folder picker not available on this system."}), 501
    try:
        root = Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        selected = filedialog.askdirectory(title="Select folder containing LandingRate.log")
        root.destroy()
        if not selected:
            return jsonify({"error": "No folder selected"}), 400
        return jsonify({"folder_path": selected})
    except Exception as e:
        return jsonify({"error": f"Failed to open folder picker: {e}"}), 500


@app.route("/set_landing_rate_folder", methods=["POST"])
def set_landing_rate_folder():
    global landing_watched_folder, landing_rate_path, cached_landings
    folder_path = request.form.get("folder_path", "").strip()
    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({"error": "Invalid folder path"}), 400
    log_path = os.path.join(folder_path, LANDING_RATE_FILENAME)
    if not os.path.exists(log_path):
        return jsonify({"error": f"{LANDING_RATE_FILENAME} not found in folder"}), 400
    landing_watched_folder = folder_path
    landing_rate_path = None
    _persist_landing_rate_path(landing_watched_folder)
    cached_landings = parse_landing_rates(log_path)
    start_landing_rate_watcher(landing_watched_folder)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": f"Watching {landing_watched_folder}"})


@app.route("/pick_landing_rate_file", methods=["POST"])
def pick_landing_rate_file():
    if not TKINTER_AVAILABLE:
        return jsonify({"error": "File picker not available on this system."}), 501
    try:
        root = Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        selected = filedialog.askopenfilename(title="Select LandingRate.log",
                                              filetypes=[("Log/CSV", "*.log *.csv"), ("All", "*.*")])
        root.destroy()
        if not selected:
            return jsonify({"error": "No file selected"}), 400
        return jsonify({"file_path": selected})
    except Exception as e:
        return jsonify({"error": f"Failed to open file picker: {e}"}), 500


@app.route("/set_landing_rate_file", methods=["POST"])
def set_landing_rate_file():
    global landing_rate_path, landing_watched_folder, cached_landings
    file_path = request.form.get("file_path", "").strip()
    if not file_path or not os.path.isfile(file_path):
        return jsonify({"error": "Invalid file path"}), 400
    landing_rate_path = file_path
    landing_watched_folder = os.path.dirname(file_path)
    _persist_landing_rate_path(landing_rate_path)
    cached_landings = parse_landing_rates(landing_rate_path)
    start_landing_rate_watcher(landing_watched_folder)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": f"Watching {landing_rate_path}"})


@app.route("/upload_landing_rate", methods=["POST"])
def upload_landing_rate():
    global cached_landings
    uploaded_file = request.files.get("landingrate")
    if not uploaded_file:
        return jsonify({"error": "No file uploaded"}), 400
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
    uploaded_file.save(temp_path)
    cached_landings = parse_landing_rates(temp_path)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Landing rate file processed"})


if __name__ == "__main__":
    socketio.run(app, debug=True)
