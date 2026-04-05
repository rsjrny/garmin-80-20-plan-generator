from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Garmin Data Hub", layout="wide")

# Add exit button in sidebar
with st.sidebar:
    st.divider()
    if st.button("🚪 Exit Application", use_container_width=True):
        st.write("To exit, close this browser tab or press Ctrl+W")
        components.html(
            """
            <script>
            window.open('', '_self', ''); 
            window.close();
            if (!window.closed) {
                alert('Please close this tab manually (Ctrl+W or click the X)');
            }
            </script>
            """,
            height=0,
        )

st.title("Garmin Data Hub")
st.write(
    "Use the pages in the left sidebar: **Garmin Sync**, **MCP Query**, **Activities**, **Build Plan**, **Charts**, and **Compliance**."
)
st.info(
    "Database is stored in Windows AppData by default. You can override with env var `GARMIN_DATA_HUB_DB`."
)

st.markdown(
    """
## User Guide

### Getting Started
1. **Download from Garmin** (Recommended): Use Garmin Backup to directly download activities from Garmin Connect - files are automatically imported into the database.
2. **Manual Import**: If you have FIT/CSV files saved locally, use the Import page to load them into the database.
3. **Review Activities**: Check the Activities page to see your imported workouts, with filtering and analysis options.
4. **Create Training Plans**: Use the Build Plan page to generate periodized training schedules based on your age, goals, and preferences.

### Training Philosophy: The 80/20 Principle

This application uses the **80/20 training method** developed by Dr. Stephen Seiler and popularized by Matt Fitzgerald. The core principle is simple but powerful:

- **80% of training** should be done at **low intensity** (Zone 1-2, easy conversational pace)
- **20% of training** should be done at **moderate to high intensity** (Zone 3-5, threshold and above)

**Why 80/20 Works:**
- **Reduces injury risk**: Most running injuries come from too much intensity
- **Builds aerobic base**: Low-intensity work develops mitochondrial density and capillary networks
- **Allows harder workouts**: When you do go hard, you're well-recovered and can truly push
- **Sustainable long-term**: Prevents burnout and overtraining
- **Proven by elites**: Studies of world-class endurance athletes show they naturally train this way

**How This App Implements 80/20:**
- Heart rate zones are calculated based on your LTHR (Lactate Threshold Heart Rate)
- Training plans automatically distribute volume: ~80% easy runs, ~20% quality workouts
- Long runs are kept in Zone 2 (easy) to build endurance without excessive stress
- Tempo and interval workouts make up the high-intensity 20%
- Masters athletes (50+) get additional recovery built into plans

**What This Means for You:**
- Most of your runs should feel **easy** - you should be able to hold a conversation
- If you're breathing hard on every run, you're training too hard
- Trust the process: Easy runs build the foundation for race-day speed
- Use heart rate as your guide, not pace (especially on hilly terrain)

### Import Page Details
- **Use Case**: For manually downloaded FIT/CSV files from Garmin Connect or other sources
- **File Selection**: Choose individual FIT/CSV files or entire folders
- **Safe Processing**: Each file is parsed in a separate process to prevent crashes
- **Duplicate Prevention**: Files are identified by SHA256 hash - reimporting the same file won't create duplicates
- **Progress Tracking**: Real-time progress bar shows import status
- **Error Handling**: Failed files are logged but don't stop the import process

### Build Plan Page Details
- **Athlete Info**: Enter your name, age, and physiological data (LTHR, HRMax)
- **HR Zone Calculation**: Automatic calculation from your activity data
  - **HRmax**: 99.5th percentile of all recorded heart rates (avoids spikes)
  - **LTHR**: Conservative estimate at 86% of HRmax
  - Click "Recalculate" to update from recent activities
  - Use "Save override" to manually set your tested values
- **Garmin Connect Setup**: Click the expandable "🎯 How to Set HR Zones in Garmin Connect" section
  - Shows your calculated HR values
  - Step-by-step instructions for Garmin Connect
  - Recommended zone percentages with calculated bpm values
  - Copy values directly into your Garmin device settings
- **Event Planning**: Set your race date, distance, and training start date
- **Training Preferences**: Choose run days per week (3-7) and which day for long runs
- **Periodization**: Automatic Base/Build/Peak/Taper phases based on race date
- **80/20 Distribution**: Plans automatically balance 80% easy/20% hard training
- **Masters Adjustments**: Age 50+ gets modified training loads and recovery
- **Export Options**: Generate Excel workbooks with detailed daily plans, nutrition guides, and workout libraries

### Activities Page Details
- **Data Overview**: View all imported activities with key metrics
- **Filtering**: Search by date range, activity type, or other criteria
- **Heart Rate Analysis**: Zone distribution shows if you're following 80/20 principles
- **Analysis**: Heart rate zones, pace analysis, and training load calculations
- **Export**: Individual activity data export options

### Garmin Backup Details (Recommended Method)
- **Direct Download**: Connect to Garmin Connect to download activities directly
- **Username Persistence**: Your username is saved and auto-filled on next use
- **Automatic Import**: Downloaded files are immediately imported into the database - no separate import step needed
- **Initial Backup Time**: First-time backup can take 1-2 hours depending on how many activities you have in Garmin Connect
- **Real-time Log**: Watch the download progress with live command output
- **Log Persistence**: Log remains visible after completion until you click "Clear Log"
- **Date Range Selection**: Choose specific time periods to download
- **MFA Support**: Handle multi-factor authentication if enabled on your account
- **Progress Tracking**: Real-time download progress with activity counts
- **No Manual Import Required**: Files go straight from Garmin Connect to your database

### Technical Details
- **Database**: SQLite database stores all activity data with full fidelity
- **File Storage**: Original FIT/CSV files are preserved alongside parsed data
- **Performance**: Optimized for large datasets with indexed searches
- **Backup**: Database can be copied/moved for backup purposes

### Database Location
By default: `%LOCALAPPDATA%\\GarminDataHub\\garmin.db`

Override with environment variable: `GARMIN_DATA_HUB_DB`

### Troubleshooting
- **Import Failures**: Check the error log on Import page for specific file issues
- **Missing Data**: Ensure FIT files contain the expected record types
- **Performance**: Large imports may take time - use the progress bar to monitor
- **Garmin Login**: Use app-specific passwords if you have 2FA enabled

### Recommended Reading
- **80/20 Running** by Matt Fitzgerald - The definitive guide to this training method
- **The Endurance Paradox** - Why training slower makes you faster
- Research by Dr. Stephen Seiler on intensity distribution in elite athletes
"""
)
