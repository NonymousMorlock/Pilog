from flask import Flask, render_template
from datetime import datetime
from collections import defaultdict
import os

app = Flask(__name__)

LOG_FILE = "logs/X-Plane Pilot.txt"

def parse_logbook():
    flights = []
    if not os.path.exists(LOG_FILE):
        return flights

    with open(LOG_FILE) as f:
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


@app.route("/")
def dashboard():
    flights = parse_logbook()

    # Aggregate data
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

    data = {
        "total_hours": total_hours,
        "flights_by_aircraft": dict(flights_by_aircraft),
        "count_by_aircraft": dict(count_by_aircraft),
        "flights_by_route": dict(flights_by_route),
        "flights_by_date": dict(flights_by_date),
    }

    return render_template("dashboard.html", data=data, flights=flights)


if __name__ == "__main__":
    app.run(debug=True)
