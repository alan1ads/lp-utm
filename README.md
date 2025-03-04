# URL Checker Bot for Google Spreadsheets

This bot checks URLs in specified columns of a Google Spreadsheet and marks non-working URLs in red. It can perform checks on a regular schedule and optionally send notifications to Slack.

## Features

- Checks URLs in multiple columns of a Google Spreadsheet
- Marks non-working URLs in red directly in the spreadsheet
- Detects various failure modes:
  - HTTP errors (404, 403, 500, etc.)
  - Connection errors
  - Domain expiration
- Sends summary reports to Slack (optional)
- Configurable check schedule (testing mode: every 3 minutes, production mode: daily at 10 AM ET)

## Setup

### 1. Google Sheets API Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable the Google Sheets API and Google Drive API
4. Create a Service Account with the following roles:
   - Google Drive API > Drive Editor
   - Google Sheets API > Editor
5. Create and download a JSON key for this service account
6. Save the JSON key as `sheetscredentials.json` in the project directory
7. Share your Google Spreadsheet with the service account email (found in the JSON key)

### 2. Environment Configuration

1. Copy `.env.template` to `.env`:
   ```
   cp .env.template .env
   ```
2. Edit `.env` and update:
   - `SHEET_URL`: Your Google Sheet ID (from the URL of your spreadsheet)
   - `URL_COLUMNS`: Comma-separated list of columns to check for URLs (e.g., C,F,G,H)
   - `SLACK_WEBHOOK_URL`: (Optional) Webhook URL for Slack notifications
   - `TESTING_MODE`: Set to `true` for testing (checks every 3 minutes), `false` for production (checks at 10 AM daily)

### 3. Install Dependencies

```
pip install -r requirements.txt
```

### 4. Running the Bot

```
python url_checker_bot.py
```

## How It Works

1. The bot connects to your Google Spreadsheet using the provided credentials
2. It scans the specified columns for URLs
3. For each URL found, it:
   - Checks if the URL returns a valid response
   - Verifies that the domain hasn't expired
   - For failed URLs, marks the cell text in red
   - For working URLs, ensures the cell text is black
4. After checking all URLs, it sends a summary report to Slack (if configured)
5. In testing mode, it waits 3 minutes before the next check
6. In production mode, it waits until 10 AM ET the next day

## Troubleshooting

- **Authentication errors**: Make sure your `sheetscredentials.json` file is valid and the service account has been granted access to your spreadsheet
- **"Missing Packages" errors**: Run `pip install -r requirements.txt` to install all dependencies
- **Selenium errors**: Make sure you have Chrome installed, or configure the bot to use a different WebDriver

## Deployment Options

This bot can be deployed in several ways:

1. **Local machine**: Run the script on your local machine (will need to stay running)
2. **Server/VPS**: Deploy to a server for 24/7 operation
3. **Cloud service**: Deploy to a cloud service like Heroku, AWS, or Google Cloud Run

For cloud deployments, set the `GOOGLE_CREDENTIALS` environment variable with the contents of your credentials JSON instead of relying on the local file. 