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
import traceback

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
SHEET_URL = os.getenv('SHEET_URL', '14Yk8UnQviC29ascf4frQfAEDWzM2_bp1UloRcnW8ZCg')
WORKSHEET_ID = os.getenv('WORKSHEET_ID', '1795345169')  # Default to the worksheet ID from the URL
# Define columns to check for URLs - can be configured in .env or hard-coded
URL_COLUMNS = os.getenv('URL_COLUMNS', 'N,O,P,Q,R,S,T,U,V,W,X,Y,Z,AA,AB,AC,AD,AE,AF,AG,AH,AI,AJ,AK,AL,AM,AN,AO,AP,AQ,AR,AS,AT,AU,AV,AW,AX,AY,AZ,BA,BB,BC,BD,BE,BF,BG,BH,BI,BJ,BK,BL').split(',')
CHECK_INTERVAL = 180  # 3 minutes in seconds for testing

# Constants for batch processing
BATCH_SIZE = 500  # Process 500 URLs before restarting the browser
MAX_BROWSER_LIFETIME = 30  # Maximum minutes to keep a browser instance open

# Print important configuration for debugging
print("\n===== CONFIGURATION =====")
print(f"SHEET_URL: {SHEET_URL}")
print(f"WORKSHEET_ID: {WORKSHEET_ID}")
print(f"URL_COLUMNS: {','.join(URL_COLUMNS)}")
print(f"TESTING_MODE: {os.getenv('TESTING_MODE', 'false')}")
print("========================\n")

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
    """Check if a URL is working and mark it in the spreadsheet"""
    print(f"=== Checking URL: {url} at cell {col}{row} ===")
    
    # Track whether the URL is working
    is_working = False
    error_message = ""
    
    try:
        # Make the actual web request to check the URL
        try:
            timeout = 15  # Use a shorter timeout to avoid getting stuck
            response = requests.get(url, timeout=timeout, allow_redirects=True)
            
            # Check for various error conditions
            if response.status_code >= 400:
                error_message = f"HTTP Status {response.status_code}"
                print(f"❌ HTTP Error: {url} - {error_message}")
                mark_cell_text_red(sheet, row, col)
            else:
                # URL might be working - analyze content
                content = response.text
                
                # Analyze the domain status
                title = None
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    title_tag = soup.find('title')
                    if title_tag:
                        title = title_tag.text.strip()
                except:
                    title = None
                
                # Get the final URL after redirects
                response_url = response.url
                
                # Extract domain from URL
                domain_match = re.search(r'https?://([^/]+)', response_url)
                if domain_match:
                    domain = domain_match.group(1)
                else:
                    domain = response_url
                
                # Analyze the content to check domain status
                domain_status = analyze_domain_status(content, domain, response_url, title, driver)
                
                if domain_status == "error":
                    error_message = "Domain error detected"
                    print(f"❌ Domain Error: {url} - {error_message}")
                    mark_cell_text_red(sheet, row, col)
                else:
                    is_working = True
                    print(f"✅ URL is working: {url}")
                    reset_cell_formatting(sheet, row, col)
                    
        except requests.exceptions.RequestException as e:
            error_message = str(e)
            print(f"❌ Connection Error: {url} - {error_message}")
            mark_cell_text_red(sheet, row, col)
    except Exception as e:
        error_message = str(e)
        print(f"❌ Error checking URL: {url} - {error_message}")
        if retry_count < 1:  # Try one more time if there's an unexpected error
            print(f"Retrying URL: {url}")
            await asyncio.sleep(2)  # Wait 2 seconds before retry
            return await check_url(driver, url, sheet, row, col, retry_count + 1)
        else:
            mark_cell_text_red(sheet, row, col)
    
    return is_working, error_message

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
        driver = None  # Will initialize per batch
        
        print(f"Attempting to connect to Google Sheet with ID: {SHEET_URL}")
        try:
            # Open the spreadsheet
            spreadsheet = gc.open_by_key(SHEET_URL)
            print(f"Successfully opened spreadsheet: {spreadsheet.title}")
            
            # Get the specific worksheet by ID if possible, otherwise fall back to the first worksheet
            try:
                sheet = None
                
                # Try to get the worksheet by ID first
                if WORKSHEET_ID:
                    try:
                        # Try to get worksheet by gid 
                        worksheets = spreadsheet.worksheets()
                        for ws in worksheets:
                            if str(ws.id) == WORKSHEET_ID:
                                sheet = ws
                                print(f"Found worksheet by ID {WORKSHEET_ID}: {sheet.title}")
                                break
                    except Exception as e:
                        print(f"Error finding worksheet by ID {WORKSHEET_ID}: {str(e)}")
                
                # Fall back to first worksheet if needed
                if sheet is None:
                    sheet = spreadsheet.get_worksheet(0)
                    print(f"Using first worksheet: {sheet.title}")
            except Exception as e:
                print(f"Error getting worksheet, falling back to first worksheet: {str(e)}")
                sheet = spreadsheet.get_worksheet(0)
                print(f"Using first worksheet: {sheet.title}")
            
            # Get all values from the spreadsheet
            all_values = sheet.get_all_values()
            
            print(f"Retrieved {len(all_values)} rows from the spreadsheet")
            print(f"Using columns: {', '.join(URL_COLUMNS)}")
            
            # Skip header row (row 1)
            row_counter = 2  # Google Sheets is 1-indexed but we're starting with row 2 (after header)
            
            # Collect all URLs to check
            urls_to_check = []
            
            # Convert column letters to indices
            column_indices = [column_to_index(col) for col in URL_COLUMNS]
            
            # Loop through all rows to collect URLs
            for row_idx in range(1, len(all_values)):  # Start from index 1 (row 2)
                row_data = all_values[row_idx]
                for col_idx in column_indices:
                    # Make sure the row has enough columns
                    if col_idx < len(row_data):
                        cell_value = row_data[col_idx]
                        if cell_value:
                            # Extract URLs from the cell
                            urls = extract_urls_from_text(cell_value)
                            if urls:
                                # Add to the list of URLs to check
                                for url in urls:
                                    urls_to_check.append({
                                        'url': url,
                                        'row': row_idx + 1,  # +1 because we're 0-indexed but sheets are 1-indexed
                                        'col': index_to_column(col_idx)
                                    })
            
            print(f"Found {len(urls_to_check)} URLs to check")
            
            # Process URLs in batches
            batch_count = 0
            start_time = time.time()
            
            for i in range(0, len(urls_to_check), BATCH_SIZE):
                batch_count += 1
                batch = urls_to_check[i:i+BATCH_SIZE]
                
                print(f"\n===== Processing Batch {batch_count} ({len(batch)} URLs) =====")
                
                # Close previous driver if it exists
                if driver:
                    try:
                        print("Closing previous Selenium browser...")
                        driver.quit()
                    except Exception as e:
                        print(f"Error closing browser: {str(e)}")
                
                # Initialize a new driver for this batch
                print("Initializing a new Selenium browser...")
                driver = setup_selenium()
                
                # Process the batch
                for idx, url_data in enumerate(batch):
                    if time.time() - start_time > MAX_BROWSER_LIFETIME * 60:
                        print(f"Browser lifetime exceeded {MAX_BROWSER_LIFETIME} minutes. Reinitializing...")
                        try:
                            driver.quit()
                        except:
                            pass
                        driver = setup_selenium()
                        start_time = time.time()
                        
                    url = url_data['url']
                    row = url_data['row']
                    col = url_data['col']
                    
                    overall_index = i + idx + 1
                    print(f"Checking URL {overall_index}/{len(urls_to_check)}: {url} in cell {col}{row}")
                    
                    try:
                        await check_url(driver, url, sheet, row, col)
                    except Exception as e:
                        print(f"❌ Error checking URL {url}: {str(e)}")
                        traceback.print_exc()
                        try:
                            mark_cell_text_red(sheet, row, col)
                        except Exception as mark_err:
                            print(f"Error marking cell: {str(mark_err)}")
                
                # Give Google's API a break between batches
                print(f"Completed batch {batch_count}. Sleeping for 10 seconds to avoid API rate limits...")
                await asyncio.sleep(10)
            
            # Close the last driver
            if driver:
                try:
                    print("Closing Selenium browser...")
                    driver.quit()
                except Exception as e:
                    print(f"Error closing browser: {str(e)}")
            
            print("\nFinished checking all URLs!")
            
        except Exception as e:
            print(f"❌ Critical error processing spreadsheet: {str(e)}")
            traceback.print_exc()
    except Exception as e:
        print(f"⚠️ Critical error: {str(e)}")
        traceback.print_exc()
    finally:
        # Ensure the driver is closed
        if driver:
            try:
                driver.quit()
            except:
                pass

async def wait_until_next_interval(interval_seconds):
    """Wait until the next scheduled check time"""
    print(f"\nWaiting {interval_seconds} seconds until next check...")
    await asyncio.sleep(interval_seconds)

async def wait_until_next_run():
    """Wait until the next scheduled run time (7 AM Eastern Time)"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    # Schedule for 7 AM Eastern Time
    target_hour = 7
    target_minute = 0
    
    # Calculate the next run time
    if now.hour > target_hour or (now.hour == target_hour and now.minute >= target_minute):
        # If it's already past 7 AM, schedule for tomorrow
        next_run = now.replace(day=now.day+1, hour=target_hour, minute=target_minute, second=0, microsecond=0)
    else:
        # Schedule for today at 7 AM
        next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    # Calculate seconds until next run
    wait_seconds = (next_run - now).total_seconds()
    
    print(f"Next check scheduled for {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Waiting {wait_seconds/60/60:.2f} hours...")
    
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
        print("\nRunning in PRODUCTION mode - checking URLs daily at 7 AM Eastern Time")
        print("Now waiting for next scheduled check at 7 AM Eastern")
        
        # Wait for next 7 AM run
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