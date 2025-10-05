import os
import tempfile
from collections import defaultdict
from datetime import datetime

from flask import Flask, render_template, request

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit

DEFAULT_LOG_FILE = "logs/X-Plane Pilot.txt"

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

@app.route("/", methods=['GET', 'POST'])
def dashboard():
    file_path = DEFAULT_LOG_FILE
    filename = "Default Logbook"

    if request.method == "POST":
        uploaded_file = request.files.get("logfile")
        if uploaded_file and uploaded_file.filename.endswith(".txt"):
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
            uploaded_file.save(temp_path)
            file_path = temp_path
            filename = uploaded_file.filename

    flights = parse_logbook(file_path)
    data = summarise_flights(flights)

    return render_template("dashboard.html", data=data, flights=flights, filename=filename)


if __name__ == "__main__":
    app.run(debug=True)
