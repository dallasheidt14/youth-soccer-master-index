# Slack Integration Setup Guide

## Setting up Slack Webhook for Notifications

To enable Slack notifications in your Youth Soccer Master Index system, you need to configure a Slack webhook URL.

### Step 1: Create a Slack App
1. Go to https://api.slack.com/apps
2. Click "Create New App"
3. Choose "From scratch"
4. Enter app name: "Youth Soccer Master Index"
5. Select your workspace

### Step 2: Enable Incoming Webhooks
1. In your app settings, go to "Incoming Webhooks"
2. Toggle "Activate Incoming Webhooks" to On
3. Click "Add New Webhook to Workspace"
4. Choose the channel where you want notifications
5. Copy the webhook URL

### Step 3: Configure Environment Variable
Add the webhook URL to your environment:

**Windows (PowerShell):**
```powershell
$env:SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

**Windows (Command Prompt):**
```cmd
set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**Linux/Mac:**
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### Step 4: Test the Connection
Run this command to test your Slack integration:
```bash
python -m src.utils.notifier --test
```

### Step 5: Add to .env file (Optional)
You can also add the webhook URL to your `.env` file:
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

## Notification Types
The system will send notifications for:
- Pipeline start/completion
- Registry health checks
- Identity audit results
- Game integrity issues
- Manual notifications from the dashboard

## Troubleshooting
- Make sure the webhook URL is correct and active
- Check that the Slack app has permission to post to the channel
- Verify the environment variable is set correctly
- Test with: `python -m src.utils.notifier --test`
