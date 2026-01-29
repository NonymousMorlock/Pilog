import copy
import csv
import json
import logging
import os
import re
import tempfile
import threading
import time
from collections import OrderedDict, defaultdict
from csv import reader
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from tkinter import Tk, filedialog

    TKINTER_AVAILABLE = True
except Exception as exception:
    TKINTER_AVAILABLE = False

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit

LOGBOOK_FILENAME = "X-Plane Pilot.txt"
DEFAULT_LOG_FILE = f"logs/{LOGBOOK_FILENAME}"
DEFAULT_LOGBOOK_NAME = "Default Logbook"
LANDING_RATE_FILENAME = "LandingRate.log"

MAP_TILE_URL_DEFAULT = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
MAP_TILE_ATTRIBUTION_DEFAULT = "(c) OpenStreetMap contributors"
MAP_MAX_AIRPORTS_DEFAULT = 200
MAP_MAX_ROUTES_DEFAULT = 200
MAP_CACHE_MAX_ENTRIES = 32
MAP_CACHE_TTL_SECONDS = 15 * 60

AIRPORT_BUNDLE_PATH = Path(__file__).resolve().parent / "static" / "data" / "airports_small.csv"
AIRPORT_OVERRIDE_PATH = Path("uploads") / "airports.csv"
MAP_INDEX_PATH = Path("uploads") / "cache" / "map_index.json"

_AIRPORT_DB_CACHE: Dict[str, Any] = {"data": {}, "bundle_mtime": None, "override_mtime": None}
_MAP_DATA_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_MAP_CACHE_LOCK = threading.Lock()


class AppState:
    """Thread-safe application state manager for Pilog."""
    
    def __init__(self) -> None:
        """Initialize app state with locks for thread safety."""
        self._lock = threading.RLock()
        
        # Flight/logbook state
        self.cached_flights: Optional[List[Dict[str, Any]]] = None
        self.cached_filename: str = DEFAULT_LOGBOOK_NAME
        self.watched_folder: Optional[str] = None
        self.observer: Optional[Observer] = None
        self.watcher_initialized: bool = False
        
        # Landing rate state
        self.cached_landings: List[Dict[str, Any]] = []
        self.landing_rate_path: Optional[str] = None
        self.landing_watched_folder: Optional[str] = None
        self.landing_observer: Optional[Observer] = None
        
        # Link mappings
        self.landing_links: Dict[int, Dict[str, Any]] = {}
        self.flight_to_landing_indices: Dict[int, List[int]] = {}
        
        # Manual overrides
        self.manual_overrides: Dict[int, int] = {}
        
        # Configuration
        self.cluster_minutes: int = self._load_cluster_minutes()
    
    def _load_cluster_minutes(self) -> int:
        """Load cluster minutes from environment or config."""
        try:
            return int(os.getenv('LANDING_CLUSTER_MINUTES', '10'))
        except ValueError:
            logger.warning('Invalid LANDING_CLUSTER_MINUTES, using default 10')
            return 10
    
    def with_lock(self, func, *args, **kwargs) -> Any:
        """Execute function with lock."""
        with self._lock:
            return func(*args, **kwargs)
    
    def get_cluster_minutes(self) -> int:
        """Thread-safe getter for cluster minutes."""
        with self._lock:
            return self.cluster_minutes
    
    def set_cluster_minutes(self, minutes: int) -> None:
        """Thread-safe setter for cluster minutes."""
        if not 1 <= minutes <= 60:
            raise ValueError("Cluster minutes must be between 1 and 60")
        with self._lock:
            self.cluster_minutes = minutes


# Global app state instance
app_state = AppState()
watcher_init_lock = threading.Lock()



def _persist_config() -> None:
    """Persist cluster configuration to uploads/config.json."""
    try:
        os.makedirs("uploads", exist_ok=True)
        config_path = os.path.join("uploads", "config.json")
        with open(config_path, "w") as f:
            json.dump({"cluster_minutes": app_state.cluster_minutes}, f)
        logger.debug(f"Config persisted to {config_path}")
    except OSError as e:
        logger.error(f"Failed to persist config: {e}")


def _load_config() -> None:
    """Load cluster configuration from uploads/config.json."""
    config_path = os.path.join("uploads", "config.json")
    if not os.path.exists(config_path):
        return
    
    try:
        with open(config_path) as f:
            data = json.load(f) or {}
            cm = int(data.get("cluster_minutes", app_state.cluster_minutes))
            if 1 <= cm <= 60:
                app_state.cluster_minutes = cm
                logger.info(f"Loaded cluster minutes: {cm}")
    except (IOError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load config: {e}")


def _persist_watched_folder(path: Optional[str]) -> None:
    """Persist watched folder path to uploads/watched_folder.txt."""
    try:
        os.makedirs("uploads", exist_ok=True)
        config_path = os.path.join("uploads", "watched_folder.txt")
        with open(config_path, "w") as f:
            f.write(path or "")
        logger.debug(f"Watched folder persisted: {path}")
    except OSError as e:
        logger.error(f"Failed to persist watched folder: {e}")


def _load_persisted_watched_folder() -> Optional[str]:
    """Load watched folder path from uploads/watched_folder.txt."""
    config_path = os.path.join("uploads", "watched_folder.txt")
    if not os.path.exists(config_path):
        return None
    
    try:
        with open(config_path) as f:
            path = f.read().strip() or None
            if path and os.path.isdir(path):
                logger.debug(f"Loaded persisted watched folder: {path}")
                return path
            return None
    except IOError as e:
        logger.error(f"Failed to load persisted watched folder: {e}")
        return None


def _persist_landing_rate_path(path: Optional[str]) -> None:
    """Persist landing rate path to uploads/landing_rate_path.txt."""
    try:
        os.makedirs("uploads", exist_ok=True)
        config_path = os.path.join("uploads", "landing_rate_path.txt")
        with open(config_path, "w") as f:
            f.write(path or "")
        logger.debug(f"Landing rate path persisted: {path}")
    except OSError as e:
        logger.error(f"Failed to persist landing rate path: {e}")


def _load_persisted_landing_rate_path() -> Optional[str]:
    """Load landing rate path from uploads/landing_rate_path.txt."""
    config_path = os.path.join("uploads", "landing_rate_path.txt")
    if not os.path.exists(config_path):
        return None
    
    try:
        with open(config_path) as f:
            path = f.read().strip() or None
            if path and (os.path.isdir(path) or os.path.isfile(path)):
                logger.debug(f"Loaded persisted landing rate path: {path}")
                return path
            return None
    except IOError as e:
        logger.error(f"Failed to load persisted landing rate path: {e}")
        return None


def _persist_manual_links(mapping: Dict[int, int]) -> None:
    """Persist manual link overrides to uploads/manual_links.json."""
    try:
        os.makedirs("uploads", exist_ok=True)
        config_path = os.path.join("uploads", "manual_links.json")
        with open(config_path, "w") as f:
            json.dump(mapping or {}, f)
        logger.debug(f"Manual links persisted: {len(mapping)} overrides")
    except OSError as e:
        logger.error(f"Failed to persist manual links: {e}")


def _load_persisted_manual_links() -> Dict[int, int]:
    """Load manual link overrides from uploads/manual_links.json."""
    config_path = os.path.join("uploads", "manual_links.json")
    if not os.path.exists(config_path):
        return {}
    
    try:
        with open(config_path) as f:
            data = json.load(f) or {}
            # Ensure keys and values are ints
            result = {int(k): int(v) for k, v in data.items() if isinstance(k, (str, int))}
            logger.debug(f"Loaded manual links: {len(result)} overrides")
            return result
    except (IOError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load persisted manual links: {e}")
        return {}


def normalize_aircraft(name: str) -> str:
    """Normalize aircraft name to standard code.
    
    Args:
        name: Aircraft name/type string
        
    Returns:
        Normalized aircraft code (e.g., 'C172', 'B738')
    """
    if not name:
        return ""
    token = re.sub(r"[^A-Za-z0-9]", "", str(name)).upper()
    if token.startswith("C172") or "CESSNA172" in token or "CESSNA172SP" in token:
        return "C172"
    if "B738" in token or "B737800" in token or token.startswith("ZB738"):
        return "B738"
    return token


def parse_landing_rates(file_path: Optional[str]) -> List[Dict[str, Any]]:
    """Parse landing rate CSV file.
    
    Args:
        file_path: Path to landing rate log file
        
    Returns:
        List of landing records with normalized fields
    """
    rows: List[Dict[str, Any]] = []
    if not file_path or not os.path.exists(file_path):
        return rows
    
    try:
        with open(file_path, newline='') as csvfile:
            for line_num, parts in enumerate(reader(csvfile), start=1):
                if not parts or len(parts) < 9:
                    continue
                try:
                    # Columns: time, Aircraft, VS, G, noserate, float, quality, Q, Qrad_abs
                    time_str = parts[0].strip()
                    dt: Optional[datetime] = None
                    
                    try:
                        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            dt = datetime.fromisoformat(time_str.replace("/", "-").replace("T", " "))
                        except ValueError:
                            logger.debug(f"Could not parse time at line {line_num}: {time_str}")
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
                except (ValueError, IndexError) as e:
                    logger.debug(f"Failed to parse landing rate line {line_num}: {e}")
                    continue
    except IOError as e:
        logger.error(f"Failed to read landing rates file {file_path}: {e}")
    
    logger.info(f"Parsed {len(rows)} landing records from {file_path}")
    return rows


def parse_logbook(file_path: str) -> List[Dict[str, Any]]:
    """Parse X-Plane logbook file.
    
    Args:
        file_path: Path to X-Plane Pilot.txt logbook file
        
    Returns:
        List of flight records
    """
    flights: List[Dict[str, Any]] = []
    if not os.path.exists(file_path):
        logger.warning(f"Logbook file not found: {file_path}")
        return flights

    try:
        with open(file_path) as f:
            for line_num, line in enumerate(f, start=1):
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
                except (ValueError, IndexError) as e:
                    logger.debug(f"Failed to parse logbook line {line_num}: {e}")
                    continue
    except IOError as e:
        logger.error(f"Failed to read logbook file {file_path}: {e}")
    
    logger.info(f"Parsed {len(flights)} flights from {file_path}")
    return flights


def get_current_flights() -> List[Dict[str, Any]]:
    """Get current list of flights, preferring watched folder if set.
    
    Returns:
        List of flight dictionaries
    """
    # Prefer the actively watched folder if set
    if app_state.watched_folder:
        watched_path = os.path.join(app_state.watched_folder, LOGBOOK_FILENAME)
        if os.path.exists(watched_path):
            return parse_logbook(watched_path)
    if app_state.cached_flights is not None:
        return app_state.cached_flights
    return parse_logbook(DEFAULT_LOG_FILE)


def get_current_landings() -> List[Dict[str, Any]]:
    """Get current list of landing records.
    
    Priority:
        1. Explicit landing_rate_path file
        2. landing_watched_folder
        3. watched_folder (fallback)
        4. logs/LandingRate.log (default)
    
    Returns:
        List of landing records
    """
    # If explicit file path set, prefer it
    if app_state.landing_rate_path and os.path.isfile(app_state.landing_rate_path):
        return parse_landing_rates(app_state.landing_rate_path)
    
    # If a folder is set for landing rates, read from there
    if app_state.landing_watched_folder:
        path = os.path.join(app_state.landing_watched_folder, LANDING_RATE_FILENAME)
        if os.path.exists(path):
            return parse_landing_rates(path)
    
    # Try the logbook watched folder as a fallback
    if app_state.watched_folder:
        path = os.path.join(app_state.watched_folder, LANDING_RATE_FILENAME)
        if os.path.exists(path):
            return parse_landing_rates(path)
    
    # Default to project logs folder if exists
    default_path = os.path.join("logs", LANDING_RATE_FILENAME)
    if os.path.exists(default_path):
        return parse_landing_rates(default_path)
    
    return []


class LogbookHandler(FileSystemEventHandler):
    """Watch for changes to X-Plane logbook file."""
    
    def on_modified(self, event) -> None:  # type: ignore
        """Handle file modification events."""
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == LOGBOOK_FILENAME:
            logger.info(f"Logbook modified: {event.src_path}")
            app_state.cached_flights = parse_logbook(event.src_path)
            app_state.cached_filename = os.path.basename(event.src_path)
            broadcast_update()

    def on_created(self, event) -> None:  # type: ignore
        """Handle file creation events."""
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == LOGBOOK_FILENAME:
            logger.info(f"Logbook created: {event.src_path}")
            self.on_modified(event)

    def on_moved(self, event) -> None:  # type: ignore
        """Handle file move/replace events."""
        try:
            dest_path: Optional[str] = getattr(event, 'dest_path', None)
        except AttributeError:
            dest_path = None
        path = dest_path or event.src_path
        if os.path.basename(path) == LOGBOOK_FILENAME:
            logger.info(f"Logbook moved: {path}")
            # Create minimal event shim for on_modified
            class _FakeEvent:
                is_directory = False
                src_path = path
            self.on_modified(_FakeEvent())


def start_watcher(folder_path: str) -> None:
    """Start file system watcher for logbook folder.
    
    Args:
        folder_path: Path to folder containing X-Plane Pilot.txt
    """
    try:
        if app_state.observer is not None:
            try:
                app_state.observer.stop()
                app_state.observer.join(timeout=2)
            except Exception as e:
                logger.warning(f"Failed to stop existing observer: {e}")
            app_state.observer = None

        handler = LogbookHandler()
        app_state.observer = Observer()
        app_state.observer.schedule(handler, folder_path, recursive=False)
        thread = threading.Thread(target=app_state.observer.start, daemon=True)
        thread.start()
        logger.info(f"Started logbook watcher on {folder_path}")
    except Exception as e:
        logger.error(f"Failed to start watcher: {e}")


class LandingRateHandler(FileSystemEventHandler):
    """Watch for changes to landing rate file."""
    
    def __init__(self, target_filename: str = LANDING_RATE_FILENAME) -> None:
        """Initialize handler.
        
        Args:
            target_filename: Name of landing rate file to watch
        """
        super().__init__()
        self.target = target_filename

    @staticmethod
    def _maybe_refresh() -> None:
        """Refresh landing data and recompute links."""
        try:
            app_state.cached_landings = get_current_landings()
            recompute_links()
            broadcast_landing_update()
            logger.debug("Landing data refreshed after file system event")
        except Exception as e:
            logger.error(f"Failed to refresh landing rates: {e}")

    def on_modified(self, event) -> None:  # type: ignore
        """Handle file modification events."""
        if event.is_directory:
            return
        base = os.path.basename(event.src_path)
        if base == self.target and app_state.landing_rate_path:
            if os.path.abspath(event.src_path) == os.path.abspath(app_state.landing_rate_path):
                logger.debug(f"Landing rate file modified: {event.src_path}")
                self._maybe_refresh()

    def on_created(self, event) -> None:  # type: ignore
        """Handle file creation events."""
        if event.is_directory:
            return
        self.on_modified(event)

    def on_moved(self, event) -> None:  # type: ignore
        """Handle file move/replace events."""
        try:
            dest_path: Optional[str] = getattr(event, 'dest_path', None)
        except AttributeError:
            dest_path = None
        path = dest_path or event.src_path
        base = os.path.basename(path)
        if base == self.target or (app_state.landing_rate_path and 
                                   os.path.abspath(path) == os.path.abspath(app_state.landing_rate_path)):
            logger.debug(f"Landing rate file moved: {path}")
            self._maybe_refresh()


def start_landing_rate_watcher(folder_path: str) -> None:
    """Start file system watcher for landing rate folder.
    
    Args:
        folder_path: Path to folder containing landing rate file
    """
    try:
        if app_state.landing_observer is not None:
            try:
                app_state.landing_observer.stop()
                app_state.landing_observer.join(timeout=2)
            except Exception as e:
                logger.warning(f"Failed to stop existing landing observer: {e}")
            app_state.landing_observer = None
        
        handler = LandingRateHandler()
        app_state.landing_observer = Observer()
        app_state.landing_observer.schedule(handler, folder_path, recursive=False)
        thread = threading.Thread(target=app_state.landing_observer.start, daemon=True)
        thread.start()
        logger.info(f"Started landing rate watcher on {folder_path}")
    except Exception as e:
        logger.error(f"Failed to start landing rate watcher: {e}")


def broadcast_update() -> None:
    """Broadcast flight data update to all connected clients."""
    try:
        flights = get_current_flights()
        landing_idx_for_flight: List[Optional[int]] = []
        landing_avail = False
        
        try:
            recompute_links()
            landing_avail = bool(
                (app_state.landing_rate_path or app_state.landing_watched_folder) or 
                (app_state.cached_landings and len(app_state.cached_landings) > 0))
            for i in range(len(flights)):
                indices = app_state.flight_to_landing_indices.get(i)
                landing_idx_for_flight.append(indices[0] if indices else None)
        except Exception as e:
            logger.error(f"Failed to compute landing indices for broadcast: {e}")
            landing_idx_for_flight = [None] * len(flights)
            landing_avail = False
        
        payload = {
            "summary": summarise_flights(flights),
            "flights": flights,
            "filename": app_state.cached_filename or DEFAULT_LOGBOOK_NAME,
            "watched_folder": app_state.watched_folder,
            "landing_index_for_flight": landing_idx_for_flight,
            "landing_available": landing_avail,
        }
        socketio.emit("log_update", payload)
        logger.debug(f"Broadcasted update: {len(flights)} flights")
    except Exception as e:
        logger.error(f"Failed to broadcast update: {e}")


def summarise_landings(landings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute summary statistics for landing records.
    
    Args:
        landings: List of landing records
        
    Returns:
        Dictionary with count, mean_vs, and avg_vs_per_aircraft
    """
    vs_values = [l.get("VS") for l in landings if isinstance(l.get("VS"), (int, float))]
    mean_vs = sum(vs_values) / len(vs_values) if vs_values else 0.0
    
    avg_vs_by_ac: Dict[str, List[float]] = defaultdict(list)
    for l in landings:
        if isinstance(l.get("VS"), (int, float)):
            avg_vs_by_ac[l.get("norm_ac", "")].append(l.get("VS"))
    
    avg_vs_per_aircraft = {k: (sum(v) / len(v) if v else 0.0) for k, v in avg_vs_by_ac.items()}
    
    return {
        "count": len(landings),
        "mean_vs": mean_vs,
        "avg_vs_per_aircraft": avg_vs_per_aircraft,
    }


def recompute_links() -> None:
    """Recompute landing-to-flight links using multiple heuristics.
    
    This function attempts to match landing records to flights using the following strategy:
    
    1. **Group by date & aircraft**: Both flights and landings are grouped by (date, norm_ac)
    2. **Apply manual overrides**: Any manually configured links take precedence
    3. **Heuristic matching** (in priority order):
       - One-to-one: If 1 flight and 1 landing in group -> automatic match
       - Sequence: If flight count equals landing count -> assume 1-to-1 sequence
       - Clustering: Group landings by time (within CLUSTER_MINUTES window)
         * If clusters == flights: 1 cluster per flight
         * If clusters == total declared landings: Distribute by declared count
       - Count-based: Distribute landings by each flight's declared landing count
       - Fallback: Mark as ambiguous
    
    Results are stored in app_state:
    - landing_links: Dict[landing_idx] -> {flight, flightIndex, linkConfidence}
    - flight_to_landing_indices: Dict[flight_idx] -> [landing_indices]
    
    Confidence levels:
    - 'manual': User-configured override
    - 'unique-date-aircraft': Only one flight/landing combo
    - 'sequence-assumed': Counts matched and assumed 1-to-1
    - 'cluster-sequence': One cluster per flight
    - 'cluster-assigned': Distributed by declared counts
    - 'count-assigned': Distributed by flight landing counts
    - 'unmatched': No flights in group for landing
    - 'ambiguous': Multiple possible matches
    """
    flights = get_current_flights()
    landings = app_state.cached_landings or []
    manual_overrides = app_state.manual_overrides
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
    landing_links: Dict[int, Dict[str, Any]] = {}
    flight_link_index_by_key: Dict[Tuple[Any, Any, Any], List[int]] = {}
    flight_to_landing_indices: Dict[int, List[int]] = {}

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
            landing_links[item["idx"]] = {
                "flight": fref["flight"],
                "flightIndex": fref["idx"],
                "linkConfidence": "unique-date-aircraft",
            }
            flight_key = (fref["flight"]["date"], fref["flight"]["norm_ac"], 0)
            flight_link_index_by_key.setdefault(flight_key, []).append(item["idx"])
            flight_to_landing_indices.setdefault(fref["idx"], []).append(item["idx"])
            continue

        # Heuristic 1: sequence-assumed when counts equal
        if lf_work == ll_work and lf_work > 0:
            for i, item in enumerate(working_landings):
                fref = working_flights[i]
                landing_links[item["idx"]] = {
                    "flight": fref["flight"],
                    "flightIndex": fref["idx"],
                    "linkConfidence": "sequence-assumed",
                }
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
        cluster_minutes = app_state.get_cluster_minutes()
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
                    if delta_min is not None and delta_min <= cluster_minutes:
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
                        landing_links[it["idx"]] = {
                            "flight": fref["flight"],
                            "flightIndex": fref["idx"],
                            "linkConfidence": "cluster-sequence",
                        }
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
                            landing_links[it["idx"]] = {
                                "flight": fref["flight"],
                                "flightIndex": fref["idx"],
                                "linkConfidence": "cluster-assigned",
                            }
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
        if 0 < ll_work == total_declared and total_declared > 0:
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

    app_state.landing_links = landing_links
    app_state.flight_to_landing_indices = flight_to_landing_indices


def broadcast_landing_update() -> None:
    """Broadcast landing rate data update to all connected clients."""
    try:
        payload = {
            "landings": app_state.cached_landings,
            "links": app_state.landing_links,
            "flight_to_landing_indices": app_state.flight_to_landing_indices,
            "summary": summarise_landings(app_state.cached_landings),
            "source": {
                "file": app_state.landing_rate_path,
                "folder": app_state.landing_watched_folder,
            }
        }
        socketio.emit("landing_rate_update", payload)
        logger.debug(f"Broadcasted landing update: {len(app_state.cached_landings)} landings")
    except Exception as e:
        logger.error(f"Failed to broadcast landing update: {e}")


def summarise_flights(flights: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute summary statistics for flights.
    
    Args:
        flights: List of flight records
        
    Returns:
        Dictionary with total_hours, flights_by_aircraft, count_by_aircraft,
        flights_by_route, and flights_by_date
    """
    total_hours: float = sum(f.get("hours", 0) for f in flights)
    flights_by_aircraft: Dict[str, float] = defaultdict(float)
    count_by_aircraft: Dict[str, int] = defaultdict(int)
    flights_by_route: Dict[str, int] = defaultdict(int)
    flights_by_date: Dict[str, float] = defaultdict(float)

    for f in flights:
        aircraft = f.get("aircraft", "Unknown")
        flights_by_aircraft[aircraft] += f.get("hours", 0)
        count_by_aircraft[aircraft] += 1
        dep = f.get("dep", "")
        arr = f.get("arr", "")
        flights_by_route[f"{dep} → {arr}"] += 1
        date = f.get("date", "")
        flights_by_date[date] += f.get("hours", 0)

    return {
        "total_hours": total_hours,
        "flights_by_aircraft": dict(flights_by_aircraft),
        "count_by_aircraft": dict(count_by_aircraft),
        "flights_by_route": dict(flights_by_route),
        "flights_by_date": dict(flights_by_date),
    }


def _safe_int(value: Optional[str], default: int, minimum: Optional[int] = None,
              maximum: Optional[int] = None) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return default
    if minimum is not None and parsed < minimum:
        return default
    if maximum is not None and parsed > maximum:
        return default
    return parsed


def _safe_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    token = str(value).strip().lower()
    if token in {"0", "false", "no", "off"}:
        return False
    if token in {"1", "true", "yes", "on"}:
        return True
    return default


def _parse_date(value: Optional[str]) -> Optional[datetime.date]:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_list(values: List[str]) -> List[str]:
    items: List[str] = []
    for entry in values:
        if entry is None:
            continue
        for token in str(entry).split(","):
            cleaned = token.strip()
            if cleaned:
                items.append(cleaned)
    return items


def get_map_config() -> Dict[str, Any]:
    return {
        "tile_url": os.getenv("MAP_TILE_URL", MAP_TILE_URL_DEFAULT),
        "tile_attribution": os.getenv("MAP_TILE_ATTRIBUTION", MAP_TILE_ATTRIBUTION_DEFAULT),
        "max_airports": _safe_int(os.getenv("MAP_MAX_AIRPORTS"), MAP_MAX_AIRPORTS_DEFAULT, minimum=10),
        "max_routes": _safe_int(os.getenv("MAP_MAX_ROUTES"), MAP_MAX_ROUTES_DEFAULT, minimum=10),
    }


def _collect_aircraft_options(flights: List[Dict[str, Any]]) -> List[str]:
    options: Set[str] = set()
    for f in flights:
        norm_ac = f.get("norm_ac") or normalize_aircraft(f.get("aircraft", ""))
        if norm_ac:
            options.add(norm_ac)
    return sorted(options)


def _map_filters_from_request(req) -> Dict[str, Any]:
    config = get_map_config()
    aircraft_values = _parse_list(req.args.getlist("aircraft"))
    tag_values = _parse_list(req.args.getlist("tags"))
    return {
        "start": req.args.get("start", "").strip(),
        "end": req.args.get("end", "").strip(),
        "aircraft": aircraft_values,
        "tags": tag_values,
        "show_routes": _safe_bool(req.args.get("show_routes"), True),
        "show_heatmap": _safe_bool(req.args.get("show_heatmap"), True),
        "max_airports": _safe_int(req.args.get("max_airports"), config["max_airports"], minimum=10),
        "max_routes": _safe_int(req.args.get("max_routes"), config["max_routes"], minimum=10),
    }


def _airport_db_signature() -> Tuple[Optional[float], Optional[float]]:
    bundle_mtime = AIRPORT_BUNDLE_PATH.stat().st_mtime if AIRPORT_BUNDLE_PATH.exists() else None
    override_mtime = AIRPORT_OVERRIDE_PATH.stat().st_mtime if AIRPORT_OVERRIDE_PATH.exists() else None
    return bundle_mtime, override_mtime


def _load_airport_csv(path: Path) -> Dict[str, Dict[str, Any]]:
    data: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return data
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader_obj = csv.DictReader(handle)
            for row in reader_obj:
                if not row:
                    continue
                icao = (row.get("icao") or row.get("ICAO") or "").strip().upper()
                if not icao:
                    continue
                try:
                    lat = float(row.get("lat", "") or row.get("LAT", ""))
                    lon = float(row.get("lon", "") or row.get("LON", ""))
                except Exception:
                    continue
                name = (row.get("name") or row.get("NAME") or "").strip()
                try:
                    elevation = float(row.get("elevation_ft", "") or row.get("ELEVATION_FT", "") or 0)
                except Exception:
                    elevation = 0.0
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    continue
                data[icao] = {
                    "icao": icao,
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "elevation_ft": elevation,
                }
    except OSError as e:
        logger.error(f"Failed to read airport dataset {path}: {e}")
    return data


def load_airport_db() -> Dict[str, Dict[str, Any]]:
    bundle_mtime, override_mtime = _airport_db_signature()
    cached_bundle = _AIRPORT_DB_CACHE.get("bundle_mtime")
    cached_override = _AIRPORT_DB_CACHE.get("override_mtime")
    if _AIRPORT_DB_CACHE.get("data") and bundle_mtime == cached_bundle and override_mtime == cached_override:
        return _AIRPORT_DB_CACHE["data"]

    data = _load_airport_csv(AIRPORT_BUNDLE_PATH)
    if AIRPORT_OVERRIDE_PATH.exists():
        override_data = _load_airport_csv(AIRPORT_OVERRIDE_PATH)
        if override_data:
            data.update(override_data)

    _AIRPORT_DB_CACHE["data"] = data
    _AIRPORT_DB_CACHE["bundle_mtime"] = bundle_mtime
    _AIRPORT_DB_CACHE["override_mtime"] = override_mtime
    return data


def airport_coords(icao: str) -> Optional[Tuple[float, float, str]]:
    if not icao:
        return None
    db = load_airport_db()
    record = db.get(str(icao).strip().upper())
    if not record:
        return None
    return record.get("lat"), record.get("lon"), record.get("name") or ""


def _flights_signature(flights: List[Dict[str, Any]]) -> int:
    sig = 0
    for f in flights:
        sig ^= hash((
            f.get("date"),
            f.get("dep"),
            f.get("arr"),
            f.get("aircraft"),
            f.get("landings"),
        ))
    return sig


def _write_map_index(airport_count: int, missing_airports: List[str]) -> None:
    try:
        MAP_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "airport_count": airport_count,
            "missing_airports": missing_airports,
        }
        with MAP_INDEX_PATH.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError as e:
        logger.error(f"Failed to write map index cache: {e}")


def build_map_data(flights: List[Dict[str, Any]], filters: Dict[str, Any]) -> Dict[str, Any]:
    start_date = _parse_date(filters.get("start"))
    end_date = _parse_date(filters.get("end"))
    raw_aircraft = _parse_list(filters.get("aircraft", []))
    aircraft_filters = {normalize_aircraft(a) or a for a in raw_aircraft if a}
    max_airports = filters.get("max_airports", MAP_MAX_AIRPORTS_DEFAULT)
    max_routes = filters.get("max_routes", MAP_MAX_ROUTES_DEFAULT)

    airport_db = load_airport_db()
    airport_signature = _airport_db_signature()
    cache_key = json.dumps({
        "start": filters.get("start"),
        "end": filters.get("end"),
        "aircraft": sorted(aircraft_filters),
        "max_airports": max_airports,
        "max_routes": max_routes,
        "sig": _flights_signature(flights),
        "airport_sig": airport_signature,
    }, sort_keys=True)

    with _MAP_CACHE_LOCK:
        cached = _MAP_DATA_CACHE.get(cache_key)
        if cached:
            cached_at = cached.get("ts")
            if cached_at and (time.time() - cached_at) <= MAP_CACHE_TTL_SECONDS:
                _MAP_DATA_CACHE.move_to_end(cache_key)
                return copy.deepcopy(cached.get("payload", {}))
            _MAP_DATA_CACHE.pop(cache_key, None)

    filtered_flights: List[Dict[str, Any]] = []
    for f in flights:
        date_str = f.get("date")
        if (start_date or end_date) and date_str:
            try:
                flight_date = datetime.strptime(str(date_str), "%Y-%m-%d").date()
            except Exception:
                continue
            if start_date and flight_date < start_date:
                continue
            if end_date and flight_date > end_date:
                continue

        norm_ac = f.get("norm_ac") or normalize_aircraft(f.get("aircraft", ""))
        if aircraft_filters and norm_ac not in aircraft_filters:
            continue
        filtered_flights.append(f)

    routes_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    airport_stats: Dict[str, Dict[str, Any]] = {}
    missing_airports: Set[str] = set()

    def _ensure_airport(icao_code: str) -> Dict[str, Any]:
        return airport_stats.setdefault(icao_code, {
            "icao": icao_code,
            "name": airport_db.get(icao_code, {}).get("name", ""),
            "arrivals": 0,
            "departures": 0,
            "visits": 0,
            "last_visit": None,
            "aircraft_counts": defaultdict(int),
        })

    for f in filtered_flights:
        dep = (f.get("dep") or "").strip().upper()
        arr = (f.get("arr") or "").strip().upper()
        if not dep or not arr:
            continue
        date_str = f.get("date")
        norm_ac = f.get("norm_ac") or normalize_aircraft(f.get("aircraft", ""))

        dep_stats = _ensure_airport(dep)
        dep_stats["departures"] += 1
        dep_stats["visits"] += 1
        if date_str and (dep_stats["last_visit"] is None or date_str > dep_stats["last_visit"]):
            dep_stats["last_visit"] = date_str
        if norm_ac:
            dep_stats["aircraft_counts"][norm_ac] += 1

        arr_stats = _ensure_airport(arr)
        arr_stats["arrivals"] += 1
        arr_stats["visits"] += 1
        if date_str and (arr_stats["last_visit"] is None or date_str > arr_stats["last_visit"]):
            arr_stats["last_visit"] = date_str
        if norm_ac:
            arr_stats["aircraft_counts"][norm_ac] += 1

        route_key = (dep, arr)
        routes_map.setdefault(route_key, {
            "dep": dep,
            "arr": arr,
            "count": 0,
            "aircrafts": set(),
        })
        routes_map[route_key]["count"] += 1
        if norm_ac:
            routes_map[route_key]["aircrafts"].add(norm_ac)

    top_route_overall = None
    if routes_map:
        top_route_overall = max(routes_map.values(), key=lambda r: r.get("count", 0))

    top_airport_overall = None
    if airport_stats:
        top_airport_overall = max(airport_stats.values(), key=lambda a: a.get("visits", 0))

    route_items: List[Dict[str, Any]] = []
    for (dep, arr), route in routes_map.items():
        dep_coords = airport_coords(dep)
        arr_coords = airport_coords(arr)
        if not dep_coords:
            missing_airports.add(dep)
        if not arr_coords:
            missing_airports.add(arr)
        if not dep_coords or not arr_coords:
            continue
        line = [[dep_coords[0], dep_coords[1]], [arr_coords[0], arr_coords[1]]]
        route_items.append({
            "dep": dep,
            "arr": arr,
            "count": route["count"],
            "aircrafts": sorted(route["aircrafts"]),
            "line": line,
        })

    airport_items: List[Dict[str, Any]] = []
    for icao_code, stats in airport_stats.items():
        coords = airport_coords(icao_code)
        if not coords:
            missing_airports.add(icao_code)
            continue
        aircraft_counts = stats.get("aircraft_counts", {})
        top_aircrafts = sorted(aircraft_counts, key=aircraft_counts.get, reverse=True)[:5]
        airport_items.append({
            "icao": icao_code,
            "name": stats.get("name") or coords[2],
            "lat": coords[0],
            "lon": coords[1],
            "visits": stats.get("visits", 0),
            "arrivals": stats.get("arrivals", 0),
            "departures": stats.get("departures", 0),
            "last_visit": stats.get("last_visit"),
            "top_aircrafts": top_aircrafts,
        })

    route_items.sort(key=lambda r: r.get("count", 0), reverse=True)
    airport_items.sort(key=lambda a: a.get("visits", 0), reverse=True)

    limited_routes = False
    limited_airports = False
    if max_routes and len(route_items) > max_routes:
        route_items = route_items[:max_routes]
        limited_routes = True
    if max_airports and len(airport_items) > max_airports:
        airport_items = airport_items[:max_airports]
        limited_airports = True

    top_route = top_route_overall or (route_items[0] if route_items else None)
    top_airport = top_airport_overall or (airport_items[0] if airport_items else None)

    result = {
        "routes": route_items,
        "airports": airport_items,
        "missing_airports": sorted(missing_airports),
        "stats": {
            "top_route": {
                "dep": top_route.get("dep"),
                "arr": top_route.get("arr"),
                "count": top_route.get("count", 0),
            } if top_route else None,
            "top_airport": {
                "icao": top_airport.get("icao"),
                "visits": top_airport.get("visits", 0),
            } if top_airport else None,
            "total_routes": len(route_items),
            "total_airports": len(airport_items),
        },
        "limits": {
            "max_airports": max_airports,
            "max_routes": max_routes,
            "limited_airports": limited_airports,
            "limited_routes": limited_routes,
        },
    }

    _write_map_index(len(airport_db), sorted(missing_airports))

    with _MAP_CACHE_LOCK:
        _MAP_DATA_CACHE[cache_key] = {"ts": time.time(), "payload": copy.deepcopy(result)}
        _MAP_DATA_CACHE.move_to_end(cache_key)
        while len(_MAP_DATA_CACHE) > MAP_CACHE_MAX_ENTRIES:
            _MAP_DATA_CACHE.popitem(last=False)

    return copy.deepcopy(result)


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
    persisted = _load_persisted_watched_folder()
    if persisted and os.path.isdir(persisted):
        logbook = os.path.join(persisted, LOGBOOK_FILENAME)
        if os.path.exists(logbook):
            app_state.watched_folder = persisted
            app_state.cached_flights = parse_logbook(logbook)
            app_state.cached_filename = os.path.basename(logbook)
            start_watcher(app_state.watched_folder)
    # Landing rates: try persisted, else try watched_folder, else logs/LandingRate.log
    try:
        persisted_lr = _load_persisted_landing_rate_path()
        if persisted_lr:
            if os.path.isdir(persisted_lr):
                app_state.landing_watched_folder = persisted_lr
                path = os.path.join(app_state.landing_watched_folder, LANDING_RATE_FILENAME)
                app_state.cached_landings = parse_landing_rates(path) if os.path.exists(path) else []
                start_landing_rate_watcher(app_state.landing_watched_folder)
            elif os.path.isfile(persisted_lr):
                app_state.landing_rate_path = persisted_lr
                app_state.landing_watched_folder = os.path.dirname(app_state.landing_rate_path)
                app_state.cached_landings = parse_landing_rates(app_state.landing_rate_path)
                start_landing_rate_watcher(app_state.landing_watched_folder)
        elif app_state.watched_folder:
            # auto: if LandingRate.log exists alongside logbook, use it
            candidate = os.path.join(app_state.watched_folder, LANDING_RATE_FILENAME)
            if os.path.exists(candidate):
                app_state.landing_watched_folder = app_state.watched_folder
                app_state.cached_landings = parse_landing_rates(candidate)
                start_landing_rate_watcher(app_state.landing_watched_folder)
        else:
            default_candidate = os.path.join("logs", LANDING_RATE_FILENAME)
            if os.path.exists(default_candidate):
                app_state.landing_watched_folder = "logs"
                app_state.cached_landings = parse_landing_rates(default_candidate)
                start_landing_rate_watcher(app_state.landing_watched_folder)
        # Load config and manual overrides
        try:
            _load_config()
            app_state.manual_overrides = _load_persisted_manual_links()
        except Exception as e:
            print('Failed to load manual overrides:', e)
        recompute_links()
    except Exception as e:
        print('Failed to initialize landing rate watcher:', e)


@app.before_request
def ensure_watcher_initialized():
    if app_state.watcher_initialized:
        return
    with watcher_init_lock:
        if app_state.watcher_initialized:
            return
        init_watcher_if_configured()
        app_state.watcher_initialized = True


@app.route("/", methods=['GET', 'POST'])
def dashboard():
    filename = app_state.cached_filename or DEFAULT_LOGBOOK_NAME

    if request.method == "POST":
        uploaded_file = request.files.get("logfile")
        if uploaded_file and uploaded_file.filename.endswith(".txt"):
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
            uploaded_file.save(temp_path)
            filename = uploaded_file.filename
            app_state.cached_flights = parse_logbook(temp_path)
            app_state.cached_filename = filename
            broadcast_update()

    flights = get_current_flights()
    # Ensure links are up to date and build a per-flight mapping to first landing index
    try:
        if app_state.cached_landings is None or not isinstance(app_state.cached_landings, list):
            app_state.cached_landings = get_current_landings()
        recompute_links()
        landing_index_for_flight = []
        for i, _ in enumerate(flights):
            indices = app_state.flight_to_landing_indices.get(i)
            landing_index_for_flight.append(indices[0] if indices else None)
    except Exception as e:
        print('Failed to compute landing indices for flights:', e)
        landing_index_for_flight = [None] * len(flights)
    data = summarise_flights(flights)

    landing_available = bool(
        (app_state.landing_rate_path or app_state.landing_watched_folder) or
        (app_state.cached_landings and len(app_state.cached_landings) > 0))
    return render_template(
        "dashboard.html",
        data=data,
        flights=flights,
        filename=filename,
        watched_folder=app_state.watched_folder,
        landing_index_for_flight=landing_index_for_flight,
        landing_available=landing_available,
    )


@app.route("/landing-rates", methods=['GET'])
def landing_rates_page():
    # Initial render, page will fetch live data via socket/API
    landings = app_state.cached_landings or get_current_landings()
    try:
        recompute_links()
    except Exception as e:
        print('Failed to recompute links for landing rates page:', e)
    return render_template(
        "landing_rates.html",
        summary=summarise_landings(landings),
        landings=landings,
        links=app_state.landing_links,
        source_file=app_state.landing_rate_path,
        source_folder=app_state.landing_watched_folder,
    )


@app.route("/map", methods=["GET"])
def map_page():
    flights = get_current_flights()
    map_filters = _map_filters_from_request(request)
    map_data = build_map_data(flights, map_filters)
    map_data["filters"] = {
        "aircraft_options": _collect_aircraft_options(flights),
        "tag_options": [],
        "tags_inert": True,
    }
    map_data["applied_filters"] = {
        "start": map_filters.get("start"),
        "end": map_filters.get("end"),
        "aircraft": _parse_list(map_filters.get("aircraft", [])),
    }
    config = get_map_config()
    tile_url = config.get("tile_url", MAP_TILE_URL_DEFAULT)
    tile_external = tile_url.startswith("http")
    return render_template(
        "map.html",
        map_data=map_data,
        map_config={
            "tile_url": tile_url,
            "tile_attribution": config.get("tile_attribution", MAP_TILE_ATTRIBUTION_DEFAULT),
            "max_airports": config.get("max_airports", MAP_MAX_AIRPORTS_DEFAULT),
            "max_routes": config.get("max_routes", MAP_MAX_ROUTES_DEFAULT),
            "tile_external": tile_external,
        },
        map_filters=map_filters,
    )


@app.route("/map/data", methods=["GET"])
def map_data():
    flights = get_current_flights()
    map_filters = _map_filters_from_request(request)
    payload = build_map_data(flights, map_filters)
    payload["filters"] = {
        "aircraft_options": _collect_aircraft_options(flights),
        "tag_options": [],
        "tags_inert": True,
    }
    payload["applied_filters"] = {
        "start": map_filters.get("start"),
        "end": map_filters.get("end"),
        "aircraft": _parse_list(map_filters.get("aircraft", [])),
    }
    if map_filters.get("tags"):
        payload["warnings"] = payload.get("warnings", [])
        payload["warnings"].append("Tag filtering is not available yet; tag filters are ignored.")
    return jsonify(payload)


@app.route("/set_folder", methods=["POST"])
def set_folder():
    folder_path = request.form.get("folder_path", "").strip()
    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({"error": "Invalid folder path"}), 400

    logbook_path = os.path.join(folder_path, LOGBOOK_FILENAME)
    if not os.path.exists(logbook_path):
        return jsonify({"error": f"{LOGBOOK_FILENAME} not found in folder"}), 400

    app_state.watched_folder = folder_path
    _persist_watched_folder(app_state.watched_folder)
    app_state.cached_flights = parse_logbook(logbook_path)
    app_state.cached_filename = os.path.basename(logbook_path)
    start_watcher(app_state.watched_folder)
    print(f"Watching folder set to: {app_state.watched_folder}")
    broadcast_update()
    return jsonify({"message": f"Watching {app_state.watched_folder}"})


@app.route("/data", methods=["GET"])
def get_data():
    flights = get_current_flights()
    summary = summarise_flights(flights)
    return jsonify({
        "summary": summary,
        "flights": flights,
        "filename": app_state.cached_filename or DEFAULT_LOGBOOK_NAME,
        "watched_folder": app_state.watched_folder,
    })


# --- Landing rate endpoints ---

@app.route("/landing-rates/data", methods=["GET"])
def get_landing_data():
    landings = app_state.cached_landings or get_current_landings()
    return jsonify({
        "landings": landings,
        "links": app_state.landing_links,
        "flight_to_landing_indices": app_state.flight_to_landing_indices,
        "summary": summarise_landings(landings),
        "source": {
            "file": app_state.landing_rate_path,
            "folder": app_state.landing_watched_folder,
        },
        "cluster_minutes": app_state.get_cluster_minutes(),
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
    app_state.manual_overrides[li] = fi
    _persist_manual_links(app_state.manual_overrides)
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
    landings = app_state.cached_landings or []
    if landing_index < 0 or landing_index >= len(landings):
        return jsonify({"error": "landing_index out of range"}), 400
    landing = landings[landing_index]
    # Basic candidate heuristic: same (date, norm_ac)
    same_group = []
    for idx, flight in enumerate(flights):
        if flight.get("date") == landing.get("date") and flight.get("norm_ac") == landing.get("norm_ac"):
            same_group.append({"flight_index": idx, "flight": flight})

    group_start_index = same_group[0]["flight_index"] if same_group else None
    group_end_index = same_group[-1]["flight_index"] if same_group else None

    if group_start_index is not None and group_start_index > 0:
        same_group.insert(
            0,
            {
                "flight_index": group_start_index - 1,
                "flight": flights[group_start_index - 1],
                "note": "Before group",
            },
        )
    if group_end_index is not None and group_end_index < len(flights) - 1:
        same_group.append({
            "flight_index": group_end_index + 1,
            "flight": flights[group_end_index + 1],
            "note": "After group",
        })
    return jsonify({"candidates": same_group})


@app.route("/links/list", methods=["GET"])
def list_overrides():
    flights = get_current_flights()
    landings = app_state.cached_landings or []
    items = []
    for li, fi in (app_state.manual_overrides or {}).items():
        landing = landings[li] if 0 <= li < len(landings) else None
        flight = flights[fi] if 0 <= fi < len(flights) else None
        items.append({"landing_index": li, "flight_index": fi, "landing": landing, "flight": flight})
    return jsonify({"overrides": items})


@app.route("/links/clear", methods=["POST"])
def clear_override():
    li_raw = request.form.get("landing_index", "").strip()
    if li_raw:
        try:
            li = int(li_raw)
        except Exception as e:
            print('Error parsing landing_index for clear_override:', e)
            return jsonify({"error": "Invalid landing_index"}), 400
        if li in app_state.manual_overrides:
            app_state.manual_overrides.pop(li, None)
    else:
        # Clear all
        app_state.manual_overrides = {}
    _persist_manual_links(app_state.manual_overrides)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Cleared"})


@app.route("/config/cluster", methods=["POST"])
def set_cluster_minutes():
    try:
        minutes = int(request.form.get("minutes", ""))
    except Exception as e:
        print('Error parsing minutes for set_cluster_minutes:', e)
        return jsonify({"error": "Invalid minutes"}), 400
    if minutes < 1 or minutes > 60:
        return jsonify({"error": "Minutes out of range (1-60)"}), 400
    app_state.set_cluster_minutes(minutes)
    _persist_config()
    # Recompute with new setting
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Updated", "cluster_minutes": app_state.get_cluster_minutes()})


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
    folder_path = request.form.get("folder_path", "").strip()
    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({"error": "Invalid folder path"}), 400
    log_path = os.path.join(folder_path, LANDING_RATE_FILENAME)
    if not os.path.exists(log_path):
        return jsonify({"error": f"{LANDING_RATE_FILENAME} not found in folder"}), 400
    app_state.landing_watched_folder = folder_path
    app_state.landing_rate_path = None
    _persist_landing_rate_path(app_state.landing_watched_folder)
    app_state.cached_landings = parse_landing_rates(log_path)
    start_landing_rate_watcher(app_state.landing_watched_folder)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": f"Watching {app_state.landing_watched_folder}"})


@app.route("/pick_landing_rate_file", methods=["POST"])
def pick_landing_rate_file():
    if not TKINTER_AVAILABLE:
        return jsonify({"error": "File picker not available on this system."}), 501
    try:
        root = Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        selected = filedialog.askopenfilename(
            title="Select LandingRate.log",
            filetypes=[("Log/CSV", "*.log *.csv"), ("All", "*.*")],
        )
        root.destroy()
        if not selected:
            return jsonify({"error": "No file selected"}), 400
        return jsonify({"file_path": selected})
    except Exception as e:
        return jsonify({"error": f"Failed to open file picker: {e}"}), 500


@app.route("/set_landing_rate_file", methods=["POST"])
def set_landing_rate_file():
    file_path = request.form.get("file_path", "").strip()
    if not file_path or not os.path.isfile(file_path):
        return jsonify({"error": "Invalid file path"}), 400
    app_state.landing_rate_path = file_path
    app_state.landing_watched_folder = os.path.dirname(file_path)
    _persist_landing_rate_path(app_state.landing_rate_path)
    app_state.cached_landings = parse_landing_rates(app_state.landing_rate_path)
    start_landing_rate_watcher(app_state.landing_watched_folder)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": f"Watching {app_state.landing_rate_path}"})


@app.route("/upload_landing_rate", methods=["POST"])
def upload_landing_rate():
    uploaded_file = request.files.get("landingrate")
    if not uploaded_file:
        return jsonify({"error": "No file uploaded"}), 400
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
    uploaded_file.save(temp_path)
    app_state.cached_landings = parse_landing_rates(temp_path)
    recompute_links()
    broadcast_landing_update()
    return jsonify({"message": "Landing rate file processed"})


if __name__ == "__main__":
    socketio.run(app, debug=True)
