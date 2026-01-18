# Pilog - X-Plane Pilot Logbook Analyzer

A Flask web application for analyzing X-Plane pilot flight logs and landing rates. Track flight hours, visualize landing patterns, and automatically link landing rate data to flights.

## Features

- 📊 **Dashboard**: View flight summaries, hours by aircraft, routes, and trends
- 🛬 **Landing Rate Analysis**: Import and analyze landing rate data with automatic linking to flights
- 🔄 **Real-time Sync**: Watch for changes to logbook files and auto-update the interface
- 🎨 **Dark Mode**: Built-in light/dark theme switching
- 🔗 **Smart Linking**: Multiple heuristics to match landing records to flights
- 📁 **Folder Monitoring**: Automatically watch folders for file updates

## Installation

### Prerequisites

- Python 3.11+
- pip

### Development Setup

1. **Clone and navigate to project:**
   ```bash
   cd pilog
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application:**
   ```bash
   python app.py
   ```

5. **Access the app:**
   Open your browser to `http://localhost:5000`

### Docker Deployment

1. **Build the image:**
   ```bash
   docker build -t pilog:latest .
   ```

2. **Run the container:**
   ```bash
   docker run -p 5000:5000 -v $(pwd)/uploads:/app/uploads pilog:latest
   ```

   Optionally mount your X-Plane logs directory:
   ```bash
   docker run -p 5000:5000 \
     -v $(pwd)/uploads:/app/uploads \
     -v /path/to/xplane/logs:/app/logs \
     pilog:latest
   ```

## Configuration

### Environment Variables

- **`LANDING_CLUSTER_MINUTES`** (default: `10`)
  - Time window in minutes for clustering nearby landing records
  - Range: 1-60 minutes
  - Used to group landings that occur close together in time

### File Locations

- **X-Plane Logbook**: `X-Plane Pilot.txt` - Standard X-Plane logbook file
- **Landing Rates**: `LandingRate.log` - CSV file with landing data
- **Configuration**: `uploads/` directory - Persistent settings and manual overrides

## Usage

### Dashboard

1. **Upload or Select Logbook**:
   - Click "Choose Folder" to select a folder containing `X-Plane Pilot.txt`
   - Or upload a logbook file directly

2. **View Statistics**:
   - Total flight hours
   - Hours per aircraft type
   - Routes flown
   - Flight hours by date

### Landing Rates

1. **Add Landing Rate Data**:
   - Click "Choose Folder" or "Choose File" to select your landing rate data
   - Or upload a `LandingRate.log` file

2. **Review Linked Landings**:
   - View automatically linked landings by flight
   - Quality indicators show landing ratings (Butter, Great, Acceptable, Hard, Very Hard)

3. **Manual Linking**:
   - Ambiguous landings can be manually linked to specific flights
   - Resolved overrides are persisted automatically

4. **Adjust Clustering**:
   - Use the cluster minutes setting to adjust how landing records are grouped
   - Useful when auto-linking produces unexpected results

## File Formats

### X-Plane Logbook (X-Plane Pilot.txt)

Space-separated format with fields:
- Record type (2 = flight)
- Date (YYMMDD)
- Departure airport code
- Arrival airport code
- Number of landings
- Flight time (hours)
- ... (additional fields)
- Tail number
- Aircraft type

### Landing Rate Log (LandingRate.log)

CSV format with headers:
```
time,Aircraft,VS,G,noserate,float,quality,Q,Qrad_abs
2024-01-15 14:30:45,C172,-250,1.2,-5,0,Good,95,12.3
```

Key fields:
- `time`: Timestamp (YYYY-MM-DD HH:MM:SS)
- `Aircraft`: Aircraft type/name
- `VS`: Vertical speed (ft/min) at touchdown
- `G`: G-force
- `quality`: Quality indicator
- `Qrad_abs`: Quality radius

## Project Structure

```
pilog/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── Dockerfile            # Docker configuration
├── static/
│   ├── app.js            # Theme and utility functions
│   ├── dashboard.js      # Dashboard UI logic
│   ├── landing_rates.js  # Landing rates UI logic
│   ├── chart.js          # Chart rendering utilities
│   ├── directory_picker.js # Folder/file picker handlers
│   └── theme.css         # Styling and theme
├── templates/
│   ├── base.html         # Base template
│   ├── dashboard.html    # Dashboard page
│   └── landing_rates.html # Landing rates page
├── logs/                 # Default log file location
└── uploads/              # Persistent configuration storage
```

## API Endpoints

### Logbook Management
- `GET /` - Dashboard page
- `POST /` - Upload logbook file
- `GET /data` - Get flight data (JSON)
- `POST /pick_folder` - Open folder picker dialog
- `POST /set_folder` - Set watched folder

### Landing Rates
- `GET /landing-rates` - Landing rates page
- `GET /landing-rates/data` - Get landing data (JSON)
- `POST /upload_landing_rate` - Upload landing rate file
- `POST /pick_landing_rate_folder` - Open folder picker for landing rates
- `POST /set_landing_rate_folder` - Set landing rate folder
- `POST /pick_landing_rate_file` - Open file picker for landing rate file
- `POST /set_landing_rate_file` - Set specific landing rate file

### Link Management
- `GET /links/list` - List manual link overrides
- `POST /links/resolve` - Manually link a landing to a flight
- `POST /links/candidates` - Get candidate flights for a landing
- `POST /links/clear` - Clear link overrides

### Configuration
- `POST /config/cluster` - Set landing clustering window

## WebSocket Events

The application uses Socket.IO for real-time updates:

- `log_update` - Emitted when flight data changes
- `landing_rate_update` - Emitted when landing rate data changes

## Development

### Running Tests

```bash
pytest tests/ -v --cov
```

### Type Checking

```bash
mypy app.py
```

### Code Style

Code follows PEP 8 conventions.

## Troubleshooting

### File not detecting changes
- Ensure the folder path is valid and accessible
- Try restarting the application
- Check file permissions

### Landing rates not linking
- Verify aircraft types match (normalization is applied)
- Check date/time alignment between logbook and landing rate files
- Adjust `LANDING_CLUSTER_MINUTES` if needed
- Manually resolve ambiguous landings

### "Folder picker not available"
- This message appears when running in containers or headless environments
- Use file upload instead or set paths via environment configuration

## License

MIT License - See LICENSE file

## Contributing

Contributions welcome! Areas for improvement:
- Additional aircraft type normalization
- Export functionality (PDF, CSV)
- Statistics and trend analysis
- Performance optimizations for large datasets
- Web-based file picker for containerized deployments

## Support

For issues, feature requests, or questions, please open an issue on the project repository.
