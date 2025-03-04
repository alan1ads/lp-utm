# Deploying URL Checker Bot to Render.com

This guide explains how to deploy the URL Checker Bot to Render.com for continuous operation.

## Prerequisites

1. A Render.com account
2. Your Google Sheets API credentials
3. Your spreadsheet ID and configuration

## Deployment Steps

### 1. Prepare Your Google Credentials

Since you'll be deploying to the cloud, you need to convert your `sheetscredentials.json` file to an environment variable:

1. Open your `sheetscredentials.json` file
2. Copy all of its contents
3. You'll add this as an environment variable in Render.com

### 2. Deploy to Render.com

#### Option 1: Manual Deployment

1. Login to [Render.com](https://render.com)
2. Click "New" and select "Web Service"
3. Choose "Deploy from Git repository" or upload your files directly
4. Select "Docker" as the environment
5. Set the name to "url-checker-bot" (or any name you prefer)
6. Under "Environment Variables", add:
   - `SHEET_URL` = Your Google Sheet ID
   - `URL_COLUMNS` = Comma-separated list of columns to check (e.g., `A,B,C,D,E`)
   - `TESTING_MODE` = true or false
   - `GOOGLE_CREDENTIALS` = The entire contents of your `sheetscredentials.json` file (including curly braces)
7. Deploy the service

#### Option 2: Using render.yaml (Blueprint)

1. Login to [Render.com](https://render.com)
2. Go to "Blueprints" in the dashboard
3. Connect your Git repository
4. Render will detect the `render.yaml` file and prompt you to deploy the service
5. You'll need to fill in the environment variables as prompted

### 3. Post-Deployment

1. After deployment, check the logs to ensure the bot is running correctly
2. The bot should connect to your Google Sheet and start checking URLs
3. You can monitor the service in the Render.com dashboard

### 4. Switching to Production Mode

Once you've confirmed everything is working:

1. Go to your service in Render.com dashboard
2. Update the `TESTING_MODE` environment variable to `false`
3. This will make the bot check URLs daily at 10 AM ET instead of every 3 minutes

## Troubleshooting

- **Service crashes**: Check logs for errors in the Render.com dashboard
- **Credentials issues**: Verify that your `GOOGLE_CREDENTIALS` environment variable contains valid JSON
- **Spreadsheet access problems**: Make sure your service account has been granted access to the spreadsheet 