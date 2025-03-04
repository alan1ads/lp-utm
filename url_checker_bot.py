import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import time
import asyncio
import json
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gspread_formatting import *
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Load environment variables
load_dotenv()

# Set up Google Sheets credentials
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive',
         'https://www.googleapis.com/auth/spreadsheets']

# Modify the credentials setup
if os.getenv('GOOGLE_CREDENTIALS'):
    # Use credentials from environment variable
    import json
    credentials_dict = json.loads(os.getenv('GOOGLE_CREDENTIALS'))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
else:
    # Use local file for development
    credentials = ServiceAccountCredentials.from_json_keyfile_name('sheetscredentials.json', scope)

gc = gspread.authorize(credentials)

# After loading credentials
print("Service Account Email:", credentials._service_account_email)
try:
    # Try to list all spreadsheets to verify credentials
    all_sheets = gc.openall()
    print(f"Successfully authenticated. Can access {len(all_sheets)} sheets.")
except Exception as e:
    print(f"Authentication error: {str(e)}")

# Set up Slack webhook - get from environment variable
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
SHEET_URL = os.getenv('SHEET_URL')
# Define columns to check for URLs - can be configured in .env or hard-coded
URL_COLUMNS = os.getenv('URL_COLUMNS', 'C,F,G,H').split(',')
CHECK_INTERVAL = 180  # 3 minutes in seconds for testing

def send_slack_message(message):
    """Send notification to Slack channel"""
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook URL not configured, skipping notification")
        return
        
    payload = {'text': message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("Slack notification sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Slack message: {e}")

def is_valid_url(url):
    """Check if string is a valid URL format"""
    if not url or not isinstance(url, str):
        return False

    # First check with http:// if it doesn't have a protocol
    if not url.startswith(('http://', 'https://')):
        if is_valid_url('http://' + url):
            return True
            
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return bool(url_pattern.match(url))

def extract_urls_from_text(text):
    """Extract valid URLs from text content"""
    if not text or not isinstance(text, str):
        return []
        
    # Pattern to match URLs with or without protocol
    url_with_protocol_pattern = re.compile(
        r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^"\'\s<>]*)?'
    )
    
    # Pattern to match domains without protocol
    domain_pattern = re.compile(
        r'(?<!\S)(?:www\.)?(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?(?:/?|[/?]\S*)?(?!\S)'
    )
    
    urls = []
    
    # Find URLs with protocol
    matches = url_with_protocol_pattern.findall(text)
    for url in matches:
        if is_valid_url(url):
            urls.append(url)
    
    # Find domains without protocol and add http://
    matches = domain_pattern.findall(text)
    for domain in matches:
        url = 'http://' + domain if not domain.startswith('www.') else 'http://' + domain
        if is_valid_url(url):
            urls.append(url)
    
    return urls

def get_domain_expiration_indicators():
    """Get common patterns that indicate an expired domain"""
    return {
        # Exact patterns from common expired domain pages
        'exact_patterns': [
            'the domain has expired. is this your domain?',
            'the domain has expired. is this your domain? renew now',
            'domain has expired. renew now',
            'the domain has expired.',
        ],
        # Common registrar expiration pages
        'registrar_patterns': [
            'godaddy.com/expired',
            'expired.namecheap.com',
            'expired.domain',
            'domainexpired',
            'domain-expired',
        ],
        # Link text patterns that often appear with expired domains
        'link_patterns': [
            'renew now',
            'renew domain',
            'restore domain',
            'reactivate domain'
        ]
    }

def setup_selenium():
    """Configure and start a headless Chrome browser"""
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # New headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--disable-http2')  # Disable HTTP/2 to avoid protocol errors
    chrome_options.add_argument('--disable-javascript-harmony-shipping')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Add experimental options
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    return webdriver.Chrome(options=chrome_options)

def analyze_domain_status(content, domain, response_url, title, driver=None):
    """
    Analyze domain content to determine if it's truly expired.
    Checks for various common expiration message patterns.
    """
    try:
        # If we have a Selenium driver, get the JavaScript-rendered content
        if driver:
            try:
                print("\n=== Checking for domain expiration ===")
                driver.get(domain)
                
                # Wait for body to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Wait a moment for dynamic content
                time.sleep(5)  # Increased wait time for iframe load
                
                try:
                    # First make the target div visible
                    driver.execute_script("""
                        var target = document.getElementById('target');
                        if (target) {
                            target.style.opacity = '1';
                            target.style.visibility = 'visible';
                            target.style.display = 'block';
                        }
                    """)
                    
                    # Look specifically for plFrame
                    try:
                        iframe = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.ID, "plFrame"))
                        )
                        print("Found plFrame iframe")
                        
                        # Switch to the iframe
                        driver.switch_to.frame(iframe)
                        
                        # Wait for and get the content
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "span"))
                        )
                        
                        # Get all spans and their text
                        spans = driver.find_elements(By.TAG_NAME, "span")
                        for span in spans:
                            try:
                                text = span.text.strip().lower()
                                print(f"Found text in plFrame: {text}")
                                if "domain has expired" in text:
                                    driver.switch_to.default_content()
                                    return True, f"Found expired domain message: {text}"
                            except Exception as e:
                                print(f"Error reading span text: {e}")
                                continue
                        
                        driver.switch_to.default_content()
                    except Exception as e:
                        print(f"Error with plFrame: {e}")
                        driver.switch_to.default_content()
                
                except Exception as e:
                    print(f"Error making target visible: {e}")
                
                # Keep all existing checks (they're working for other cases)
                page_text = driver.page_source.lower()
                
                # Common expiration message patterns (keeping existing ones that work)
                expiration_patterns = [
                    # Exact matches from screenshot
                    "the domain has expired. is this your domain?",
                    "the domain has expired. is this your domain? renew now",
                    "domain has expired. renew now",
                    
                    # Common variations that were working
                    "this domain has expired",
                    "domain name has expired",
                    "domain registration has expired",
                    "domain expired",
                    "expired domain",
                    "domain is expired",
                    "domain has lapsed",
                    "domain registration expired",
                    "this domain is expired",
                    "this domain name has expired",
                    "domain has been expired",
                    "domain registration has lapsed",
                    "domain has expired and is pending renewal",
                    "expired domain name",
                    "domain expiration notice"
                ]
                
                # Check for patterns in the page source
                for pattern in expiration_patterns:
                    if pattern in page_text:
                        print(f"Found expiration message: {pattern}")
                        return True, f"Found domain expiration message: {pattern}"
                
                # Keep existing span checks that were working
                span_selectors = [
                    "span[style*='font-family:Arial']",
                    "span[style*='font-size']",
                    "span.expired-domain",
                    "span.domain-expired",
                    "div.expired-notice"
                ]
                
                for selector in span_selectors:
                    spans = driver.find_elements(By.CSS_SELECTOR, selector)
                    for span in spans:
                        text = span.text.strip().lower()
                        for pattern in expiration_patterns:
                            if pattern in text:
                                print(f"Found expiration message in styled element: {text}")
                                return True, f"Found domain expiration message: {text}"
                
            except Exception as e:
                print(f"Error checking JavaScript content: {e}")
        
        return False, None
        
    except Exception as e:
        print(f"Error in analyze_domain_status: {str(e)}")
        return False, None

def mark_cell_text_red(sheet, row, col):
    """Mark the cell text as red to indicate a problem with the URL"""
    cell_range = f"{col}{row}"
    fmt = CellFormat(
        textFormat=TextFormat(foregroundColor=Color(1, 0, 0))  # RGB for red
    )
    format_cell_range(sheet, cell_range, fmt)
    print(f"Marked cell {cell_range} as red")

def reset_cell_formatting(sheet, row, col):
    """Reset cell formatting to bright blue for working URLs"""
    cell_range = f"{col}{row}"
    fmt = CellFormat(
        textFormat=TextFormat(foregroundColor=Color(0.2, 0.6, 1.0))  # RGB for bright blue
    )
    format_cell_range(sheet, cell_range, fmt)
    print(f"Marked cell {cell_range} as bright blue (working URL)")

async def check_url(driver, url, sheet, row, col, retry_count=0):
    """Check a single URL and mark it red if it's not working"""
    print(f"\n=== Checking URL: {url} at cell {col}{row} ===")
    
    try:
        # Add timeout to prevent hanging on problematic URLs
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }, allow_redirects=True)
        
        print(f"Response status code: {response.status_code}")
        print(f"Final URL after redirects: {response.url}")
        
        # Initialize status variables
        url_is_down = False
        error_message = None
        
        if response.status_code == 200:
            # Check if domain is expired despite 200 status
            is_expired, reason = analyze_domain_status(response.text, url, response.url, None, driver)
            if is_expired:
                url_is_down = True
                error_message = f"Domain expired: {url}"
            else:
                print(f"✓ URL appears healthy: {url}")
        elif response.status_code in [403, 404, 400, 500, 502, 503, 504]:
            url_is_down = True
            error_message = f"HTTP {response.status_code}: {url}"
        else:
            # Check other non-200 codes
            url_is_down = True
            error_message = f"HTTP {response.status_code}: {url}"
                
        # Update cell formatting based on URL status
        if url_is_down:
            print(f"❌ {error_message}")
            mark_cell_text_red(sheet, row, col)
            return False, error_message
        else:
            # URL is working, make sure text is black (in case it was previously red)
            reset_cell_formatting(sheet, row, col)
            return True, None
            
    except requests.exceptions.RequestException as e:
        error_message = f"Connection Error: {url} - {str(e)}"
        print(f"❌ {error_message}")
        mark_cell_text_red(sheet, row, col)
        return False, error_message
        
    except Exception as e:
        error_message = f"Unexpected Error: {url} - {str(e)}"
        print(f"❌ {error_message}")
        
        # Try one more time if it's a Selenium-related error
        if retry_count == 0 and "Selenium" in str(e):
            print("Retrying with a fresh Selenium instance...")
            try:
                driver.quit()
            except:
                pass
                
            new_driver = setup_selenium()
            return await check_url(new_driver, url, sheet, row, col, retry_count=1)
            
        mark_cell_text_red(sheet, row, col)
        return False, error_message

def column_to_index(column_name):
    """Convert a column name (A, B, C, ..., Z, AA, AB, etc.) to a 0-based index"""
    index = 0
    for char in column_name:
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index - 1  # Convert to 0-based

def index_to_column(index):
    """Convert a 0-based index to a column name (A, B, C, ..., Z, AA, AB, etc.)"""
    index += 1  # Convert to 1-based
    column_name = ""
    while index > 0:
        remainder = (index - 1) % 26
        column_name = chr(ord('A') + remainder) + column_name
        index = (index - 1) // 26
    return column_name

async def check_links():
    """Check all URLs in the specified columns of the spreadsheet"""
    try:
        print("Setting up Selenium...")
        driver = setup_selenium()
        
        print("Attempting to connect to Google Sheet...")
        try:
            # Open the spreadsheet
            spreadsheet = gc.open_by_key(SHEET_URL)
            # Get the first worksheet
            sheet = spreadsheet.get_worksheet(0)
            
            # Get all values from the spreadsheet
            all_values = sheet.get_all_values()
            
            print(f"Retrieved {len(all_values)} rows from the spreadsheet")
            
            # Skip header row (row 1)
            failing_urls = []
            checked_count = 0
            total_urls = 0
            
            # Count URLs in relevant columns to provide progress updates
            print("Scanning for URLs in the spreadsheet...")
            for row_idx, row in enumerate(all_values, start=1):  # Start from row 1 (include header for completeness)
                if row_idx % 100 == 0:
                    print(f"Scanning row {row_idx}...")
                for col in URL_COLUMNS:
                    col_idx = column_to_index(col)  # Convert column letter to index
                    if col_idx < len(row):
                        cell_value = row[col_idx].strip()
                        if not cell_value:
                            continue
                            
                        if is_valid_url(cell_value):
                            total_urls += 1
                        else:
                            # Try to extract URLs from text content
                            extracted_urls = extract_urls_from_text(cell_value)
                            total_urls += len(extracted_urls)
            
            print(f"Found {total_urls} URLs to check across {len(URL_COLUMNS)} columns")
            
            # Process each URL in each specified column
            for row_idx, row in enumerate(all_values, start=1):  # Start from row 1 (include header)
                if row_idx % 50 == 0:
                    print(f"Processing row {row_idx} of {len(all_values)}...")
                    
                for col in URL_COLUMNS:
                    col_idx = column_to_index(col)  # Convert column letter to index
                    
                    # Skip if column index is out of range
                    if col_idx >= len(row):
                        continue
                        
                    cell_value = row[col_idx].strip()
                    if not cell_value:
                        continue
                    
                    # Extract URLs from the cell if it contains text
                    urls = []
                    if is_valid_url(cell_value):
                        urls = [cell_value]
                    else:
                        # Try to extract URLs from text content
                        urls = extract_urls_from_text(cell_value)
                    
                    # Check each URL in the cell
                    for url in urls:
                        checked_count += 1
                        print(f"\nChecking URL {checked_count}/{total_urls}: {url} in cell {col}{row_idx}")
                        
                        # Add http:// if missing
                        if not url.startswith(('http://', 'https://')):
                            url = 'http://' + url
                        
                        # Check the URL
                        is_working, error = await check_url(driver, url, sheet, row_idx, col)
                        
                        if not is_working:
                            failing_urls.append(f"Row {row_idx}, Col {col}: {error}")
                
                # Yield control to allow other tasks to run
                if row_idx % 10 == 0:
                    await asyncio.sleep(0.1)
            
            # Send summary report
            if failing_urls:
                print("\nFound failing URLs...")
                print("\n".join(failing_urls[:20]))
                if len(failing_urls) > 20:
                    print(f"...and {len(failing_urls) - 20} more.")
            else:
                print("\nAll URLs are healthy")
                
        finally:
            print("Closing Selenium browser...")
            try:
                driver.quit()
            except:
                pass
                
    except Exception as e:
        error_msg = f"⚠️ Critical error: {str(e)}"
        print(error_msg)
        send_slack_message(error_msg)

async def wait_until_next_interval(interval_seconds):
    """Wait until the next scheduled check time"""
    print(f"\nWaiting {interval_seconds} seconds until next check...")
    await asyncio.sleep(interval_seconds)

async def wait_until_next_run():
    """Wait until 10 AM for the daily scheduled run"""
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    target = now.replace(hour=10, minute=0, second=0, microsecond=0)
    
    # If it's already past 10 AM today, schedule for tomorrow
    if now >= target:
        target += timedelta(days=1)
    
    # Calculate wait time
    wait_seconds = (target - now).total_seconds()
    print(f"\nWaiting until {target.strftime('%Y-%m-%d %H:%M:%S %Z')} to run next check")
    await asyncio.sleep(wait_seconds)

# Define a simple HTTP server for Render.com health checks
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b'URL Checker Bot is running')
    
    def log_message(self, format, *args):
        # Silence the default logging to keep our console clean
        return

def start_health_check_server():
    """Start a simple HTTP server for health checks"""
    port = int(os.getenv('PORT', 10000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    print(f"Starting health check server on port {port}")
    httpd.serve_forever()

async def main():
    """Main execution function"""
    print("Starting URL checker service...")
    
    # Start the health check server in a separate thread
    health_check_thread = threading.Thread(target=start_health_check_server, daemon=True)
    health_check_thread.start()
    print("Health check server started")
    
    # Give deployment time to stabilize - significantly increased
    startup_delay = 120  # 2 minutes to ensure full deployment
    print(f"Waiting {startup_delay} seconds for deployment to fully complete...")
    await asyncio.sleep(startup_delay)
    
    print("Service started successfully!")
    print("🚀 URL checker service started - Running initial check...")
    
    # Always run an immediate check first, regardless of mode
    print("\nRunning initial URL check...")
    await check_links()
    print("Initial check completed.")
    
    # Check if we're in testing mode or production mode
    testing_mode = os.getenv('TESTING_MODE', 'false').lower() == 'true'
    
    if testing_mode:
        print(f"\nRunning in TESTING mode - checking URLs every {CHECK_INTERVAL} seconds")
        
        while True:
            print("\nStarting URL check cycle...")
            await wait_until_next_interval(CHECK_INTERVAL)
            await check_links()
    else:
        print("\nRunning in PRODUCTION mode - checking URLs daily at 10 AM Eastern Time")
        print("Now waiting for next scheduled check at 10 AM Eastern")
        
        # Wait for next 10 AM run
        await wait_until_next_run()
        
        while True:
            print("\nStarting URL check cycle...")
            await check_links()
            await wait_until_next_run()

if __name__ == "__main__":
    # Install any missing packages
    required_packages = [
        'gspread', 'oauth2client', 'requests', 'pytz', 'python-dotenv', 
        'beautifulsoup4', 'selenium', 'gspread-formatting'
    ]
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing required package: {package}")
            import subprocess
            subprocess.check_call(["pip", "install", package])
    
    # Run the main function
    asyncio.run(main()) 