# Weekly Game History Update - Scheduling Setup

This directory contains scripts and configurations to automatically run the incremental game history update every Monday.

## 🚀 Quick Setup Options

### Option 1: GitHub Actions (Recommended)
- **File**: `.github/workflows/weekly-game-history.yml`
- **Schedule**: Every Monday at 6:00 AM UTC
- **Benefits**: Cloud-based, no local machine dependency, automatic git commits
- **Setup**: Just push to GitHub - no additional configuration needed

### Option 2: Windows Task Scheduler (Local)
- **Files**: `scripts/weekly_update.ps1`, `scripts/setup_weekly_task.bat`
- **Schedule**: Every Monday at 6:00 AM local time
- **Benefits**: Runs locally, works offline, full control
- **Setup**: Run `scripts/setup_weekly_task.bat` as Administrator

## 📅 Schedule Details

- **Frequency**: Every Monday
- **Time**: 6:00 AM (adjust timezone in GitHub Actions if needed)
- **Next Run**: October 20, 2025
- **Command**: `python -m src.scraper.build_game_history --providers gotsport --states AZ --genders M,F --ages U10 --incremental`

## 🔧 Manual Setup Instructions

### GitHub Actions Setup
1. Push the `.github/workflows/weekly-game-history.yml` file to your repository
2. Go to GitHub → Actions tab → Enable workflows
3. The task will automatically run every Monday

### Windows Task Scheduler Setup
1. **Run as Administrator**: Right-click `scripts/setup_weekly_task.bat` → "Run as administrator"
2. **Verify Setup**: Check Task Scheduler → Task Scheduler Library → "Weekly Game History Update"
3. **Test Run**: Right-click the task → "Run" to test

### Manual Task Creation (Alternative)
```cmd
schtasks /create /tn "Weekly Game History Update" /tr "powershell.exe -ExecutionPolicy Bypass -File \"C:\youth-soccer-master-index\scripts\weekly_update.ps1\"" /sc weekly /d MON /st 06:00
```

## 📊 Monitoring & Logs

### GitHub Actions
- Check the "Actions" tab in GitHub
- View logs for each run
- Automatic commit of new data

### Windows Task Scheduler
- Logs: `data/logs/weekly_update.log`
- Output: `data/logs/weekly_update_output.log`
- Errors: `data/logs/weekly_update_error.log`

## 🛠️ Troubleshooting

### GitHub Actions Issues
- Check repository permissions
- Verify Python dependencies in `requirements.txt`
- Review Actions logs for specific errors

### Windows Task Scheduler Issues
- Ensure Python is in PATH
- Check file permissions
- Verify PowerShell execution policy: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Common Issues
- **Permission Denied**: Run setup script as Administrator
- **Python Not Found**: Add Python to system PATH
- **File Locking**: Ensure no other processes are using the data files

## 🔄 Updating the Schedule

### Change Time
- **GitHub Actions**: Edit cron expression in `.github/workflows/weekly-game-history.yml`
- **Windows**: Use Task Scheduler GUI or `schtasks /change`

### Change Frequency
- **GitHub Actions**: Modify cron expression (e.g., `0 6 * * 1,3` for Mon/Wed)
- **Windows**: Use Task Scheduler GUI or recreate task

## ✅ Verification

After setup, verify the schedule:
- **GitHub**: Check Actions tab for scheduled runs
- **Windows**: `schtasks /query /tn "Weekly Game History Update"`

The next run will be **Monday, October 20, 2025 at 6:00 AM**.
