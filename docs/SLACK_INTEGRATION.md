# Slack Integration Configuration

## Setup Instructions

1. **Create a Slack App**:
   - Go to https://api.slack.com/apps
   - Click "Create New App" → "From scratch"
   - Name: "Youth Soccer Master Index"
   - Choose your workspace

2. **Enable Incoming Webhooks**:
   - Go to "Features" → "Incoming Webhooks"
   - Toggle "Activate Incoming Webhooks" to On
   - Click "Add New Webhook to Workspace"
   - Choose the channel where you want notifications
   - Copy the webhook URL

3. **Configure Environment Variable**:
   ```bash
   # Add to your .env file or set as environment variable
   export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
   ```

4. **Test the Integration**:
   ```bash
   python -m src.utils.notifier --test
   ```

## Security Notes

- The webhook URL is a one-way key - it only allows sending messages to Slack
- It cannot read data or access your Slack account
- You can revoke it anytime from Slack App Management
- For multiple channels, create one webhook per channel

## Notification Types

The system sends notifications for:

- **Registry Health**: When registry health drops below 80%
- **Pipeline Start**: When weekly pipeline begins
- **Pipeline Complete**: When pipeline finishes (success/failure)
- **Identity Audit**: When team identity issues are found
- **Game Integrity**: When data corruption is detected

## Example Messages

- ✅ Registry healthy (93.8%) - 32 slices, v2.0.0
- 🚀 Starting pipeline: 4 slices (AZ,NV x M,F x U10,U11)
- ✅ Pipeline completed successfully: 4/4 slices
- ⚠️ Identity audit: 85/16021 entries flagged (0.5%) - manual review needed
- ⚠️ Game integrity issues detected: 2/32 slices need refresh
