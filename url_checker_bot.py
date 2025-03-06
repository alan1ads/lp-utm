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
from urllib.parse import urlparse
import random
from time import sleep

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

# Add rate limiting constants
SHEETS_API_WRITES_PER_MINUTE = 60  # Google's quota limit
RATE_LIMIT_PAUSE_MIN = 180  # Minimum seconds to pause after hitting a rate limit (greatly increased)
RATE_LIMIT_PAUSE_MAX = 300  # Maximum seconds to pause after hitting a rate limit (greatly increased)
RATE_LIMIT_RETRIES = 5      # Maximum retries for rate-limited operations
MAX_PENDING_RETRIES = 10    # Maximum retries for processing pending formats (increased)
BATCH_WRITE_SIZE = 5        # Process this many pending formats at a time (smaller batches)
BATCH_WRITE_PAUSE = 180     # Seconds to pause between batches of pending formats (longer pauses)

# Create a queue of cells that need formatting but couldn't be formatted due to rate limits
pending_formats = []
successfully_formatted_cells = set()  # Track cells that were successfully formatted
failed_formatted_cells = set()  # Track cells that failed formatting after all retries

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
    """Check if a string is a valid URL format"""
    try:
        # Extract domain from URL
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # URL must have a domain and scheme
        if not domain:
            # Some URLs might be domain-only
            if '.' in url and not url.startswith(('http://', 'https://')):
                # Try adding a protocol and checking again
                return is_valid_url('http://' + url)
            return False
            
        # Basic domain validation
        if '.' not in domain:  # Must have at least one dot in domain
            return False
            
        # URL seems valid
        return True
    except:
        return False

def extract_urls_from_text(text):
    """Extract URLs from text content"""
    if not text or not isinstance(text, str):
        return []
        
    urls = []
    
    # Common URL patterns with http/https
    http_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    
    # Pattern for domains without protocol (www.example.com or example.com)
    domain_pattern = r'(?<!\S)(?:www\.)?(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?(?=\s|$|[,.;:!?)])'
    
    # Pattern for IP addresses
    ip_pattern = r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
    
    # Find all HTTP/HTTPS URLs
    http_urls = re.findall(http_pattern, text)
    for url in http_urls:
        if url not in urls and is_valid_url(url):
            urls.append(url)
    
    # Find all domain-style URLs without protocol
    domain_urls = re.findall(domain_pattern, text)
    for domain in domain_urls:
        url = 'http://' + domain
        if url not in urls and is_valid_url(url):
            urls.append(url)
    
    # Find all IP addresses as potential URLs
    ip_urls = re.findall(ip_pattern, text)
    for ip in ip_urls:
        url = 'http://' + ip
        if url not in urls and is_valid_url(url):
            urls.append(url)
    
    # Check for the entire text as a potential URL if it looks URL-like
    if '.' in text and ' ' not in text.strip() and len(text.strip()) > 3:
        # The entire text might be a URL without protocol
        if not text.startswith(('http://', 'https://')):
            potential_url = 'http://' + text.strip()
            if potential_url not in urls and is_valid_url(potential_url):
                urls.append(potential_url)
        else:
            # Text already has protocol
            if text not in urls and is_valid_url(text):
                urls.append(text)
    
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

def mark_cell_text_red(sheet, row, col, retry_count=0, backoff_seconds=1):
    """Mark cell text as red to indicate failing URL"""
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    # Check if this cell was already successfully formatted
    cell_id = f"{col}{row}"
    if cell_id in successfully_formatted_cells:
        print(f"Cell {cell_id} was already successfully formatted as red - skipping")
        return True
    
    try:
        cell_range = f"{col}{row}"
        # FIX: Create CellFormat properly with a new instance each time
        fmt = CellFormat(
            textFormat=TextFormat(foregroundColor=Color(1, 0, 0))  # RGB for red
        )
        
        # Debug output to help diagnose issues
        print(f"Applying red format to cell {cell_range}, format type: {type(fmt)}")
        
        # Apply the formatting
        format_cell_range(sheet, cell_range, fmt)
        print(f"Marked cell {cell_range} as red (failed URL)")
        
        # Track this successful formatting
        successfully_formatted_cells.add(cell_id)
        
        # If this was in the failed set, remove it
        if cell_id in failed_formatted_cells:
            failed_formatted_cells.remove(cell_id)
            
        return True
    except Exception as e:
        error_str = str(e)
        
        # Check if this is a rate limit error
        if "RESOURCE_EXHAUSTED" in error_str or "Quota exceeded" in error_str or "429" in error_str:
            # This is a rate limit error
            if retry_count < RATE_LIMIT_RETRIES:
                # Calculate backoff with exponential increase and jitter
                sleep_time = min(RATE_LIMIT_PAUSE_MAX, backoff_seconds + random.uniform(1, 20))
                print(f"🕒 Rate limit hit marking cell {col}{row} as red. Retrying after {sleep_time:.1f} seconds (attempt {retry_count+1}/{RATE_LIMIT_RETRIES})...")
                time.sleep(sleep_time)  # Use blocking sleep within this function
                # Recursive retry with increased backoff
                return mark_cell_text_red(sheet, row, col, retry_count + 1, backoff_seconds * 2)
            else:
                # Max retries reached, add to pending queue
                print(f"⚠️ Max retries reached for cell {col}{row}. Adding to pending formats queue.")
                pending_formats.append({
                    'sheet': sheet,
                    'row': row,
                    'col': col,
                    'type': 'red',
                    'format_key': f"{col}{row}:red",
                    'retry_count': 0,
                    'url': 'unknown'
                })
                return False
        elif "to_props" in error_str:
            # Specific fix for the 'dict' object has no attribute 'to_props' error
            print(f"⚠️ CellFormat error for cell {col}{row}. Trying alternative method...")
            try:
                # Try an alternative approach without using the CellFormat object directly
                sheet.format(cell_range, {"textFormat": {"foregroundColor": {"red": 1, "green": 0, "blue": 0}}})
                print(f"✅ Successfully marked cell {cell_range} as red using alternative method")
                successfully_formatted_cells.add(cell_id)
                return True
            except Exception as alt_e:
                print(f"❌ Alternative method also failed for cell {col}{row}: {str(alt_e)}")
                # Add to pending for later retry with escalated priority
                pending_formats.append({
                    'sheet': sheet,
                    'row': row,
                    'col': col,
                    'type': 'red',
                    'format_key': f"{col}{row}:red",
                    'retry_count': MAX_PENDING_RETRIES - 2,  # High priority retry
                    'url': 'unknown'
                })
                failed_formatted_cells.add(cell_id)
                return False
        else:
            # Some other error
            print(f"❌ Error marking cell {col}{row} as red: {error_str}")
            # Track this failed formatting
            failed_formatted_cells.add(cell_id)
            return False

def reset_cell_formatting(sheet, row, col, retry_count=0, backoff_seconds=1):
    """Reset cell formatting to bright blue for working URLs"""
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    # Check if this cell was already successfully formatted
    cell_id = f"{col}{row}"
    if cell_id in successfully_formatted_cells:
        print(f"Cell {cell_id} was already successfully formatted as blue - skipping")
        return True
    
    try:
        cell_range = f"{col}{row}"
        # FIX: Create CellFormat properly with a new instance each time
        fmt = CellFormat(
            textFormat=TextFormat(foregroundColor=Color(0.2, 0.6, 1.0))  # RGB for bright blue
        )
        
        # Debug output to help diagnose issues
        print(f"Applying blue format to cell {cell_range}, format type: {type(fmt)}")
        
        # Apply the formatting
        format_cell_range(sheet, cell_range, fmt)
        print(f"Marked cell {cell_range} as bright blue (working URL)")
        
        # Track this successful formatting
        successfully_formatted_cells.add(cell_id)
        
        # If this was in the failed set, remove it
        if cell_id in failed_formatted_cells:
            failed_formatted_cells.remove(cell_id)
            
        return True
    except Exception as e:
        error_str = str(e)
        
        # Check if this is a rate limit error
        if "RESOURCE_EXHAUSTED" in error_str or "Quota exceeded" in error_str or "429" in error_str:
            # This is a rate limit error
            if retry_count < RATE_LIMIT_RETRIES:
                # Calculate backoff with exponential increase and jitter
                sleep_time = min(RATE_LIMIT_PAUSE_MAX, backoff_seconds + random.uniform(1, 20))
                print(f"🕒 Rate limit hit marking cell {col}{row} as blue. Retrying after {sleep_time:.1f} seconds (attempt {retry_count+1}/{RATE_LIMIT_RETRIES})...")
                time.sleep(sleep_time)  # Use blocking sleep within this function
                # Recursive retry with increased backoff
                return reset_cell_formatting(sheet, row, col, retry_count + 1, backoff_seconds * 2)
            else:
                # Max retries reached, add to pending queue
                print(f"⚠️ Max retries reached for cell {col}{row}. Adding to pending formats queue.")
                pending_formats.append({
                    'sheet': sheet,
                    'row': row,
                    'col': col,
                    'type': 'blue',
                    'format_key': f"{col}{row}:blue",
                    'retry_count': 0,
                    'url': 'unknown'
                })
                return False
        elif "to_props" in error_str:
            # Specific fix for the 'dict' object has no attribute 'to_props' error
            print(f"⚠️ CellFormat error for cell {col}{row}. Trying alternative method...")
            try:
                # Try an alternative approach without using the CellFormat object directly
                sheet.format(cell_range, {"textFormat": {"foregroundColor": {"red": 0.2, "green": 0.6, "blue": 1.0}}})
                print(f"✅ Successfully marked cell {cell_range} as blue using alternative method")
                successfully_formatted_cells.add(cell_id)
                return True
            except Exception as alt_e:
                print(f"❌ Alternative method also failed for cell {col}{row}: {str(alt_e)}")
                # Add to pending for later retry with escalated priority
                pending_formats.append({
                    'sheet': sheet,
                    'row': row,
                    'col': col,
                    'type': 'blue',
                    'format_key': f"{col}{row}:blue",
                    'retry_count': MAX_PENDING_RETRIES - 2,  # High priority retry
                    'url': 'unknown'
                })
                failed_formatted_cells.add(cell_id)
                return False
        else:
            # Some other error
            print(f"❌ Error marking cell {col}{row} as blue: {error_str}")
            # Track this failed formatting
            failed_formatted_cells.add(cell_id)
            return False

async def check_url(driver, url, sheet, row, col, retry_count=0):
    """Check if a URL is working and mark it in the spreadsheet"""
    print(f"=== Checking URL: {url} at cell {col}{row} ===")
    
    # Track whether the URL is working
    is_working = False
    error_message = ""
    cell_marked = False  # Flag to track if we've marked the cell
    
    try:
        # Make the actual web request to check the URL
        try:
            timeout = 30  # Increased timeout to 30 seconds like in the original code
            
            # Add proper headers like in the original code
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, timeout=timeout, allow_redirects=True, headers=headers)
            
            print(f"Response status code: {response.status_code}")
            print(f"Final URL after redirects: {response.url}")
            
            # Check for special error conditions from the original code
            has_special_error = False
            response_text_lower = response.text.lower()
            
            # Check for Error 1000 or DNS errors in content
            if "error 1000" in response_text_lower:
                print(f"Found Error 1000 in response content for {url}")
                has_special_error = True
                error_message = "Cloudflare Error 1000"
            elif "dns points to prohibited ip" in response_text_lower and "cloudflare" in response_text_lower:
                print(f"Found Cloudflare Error 1000 indicators in response content for {url}")
                has_special_error = True
                error_message = "Cloudflare DNS Error"
            elif "dns_probe_finished_nxdomain" in response_text_lower:
                print(f"Found DNS_PROBE_FINISHED_NXDOMAIN in response content for {url}")
                has_special_error = True
                error_message = "DNS Probe Finished NXDOMAIN"
                
            # Check for errors that might only be visible in rendered content
            if not has_special_error and response.status_code == 200:
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    page_source = driver.page_source.lower()
                    
                    # Check for various error indicators in the rendered content
                    error_indicators = [
                        "error 1000", 
                        "dns_probe_finished", 
                        "this site can't be reached",
                        "404 not found",
                        "403 forbidden",
                        "page not found",
                        "domain expired",
                        "website expired",
                        "domain not found",
                        "server not found"
                    ]
                    
                    for indicator in error_indicators:
                        if indicator in page_source:
                            print(f"Found error indicator '{indicator}' in rendered content for {url}")
                            has_special_error = True
                            error_message = f"Error detected: {indicator}"
                            break
                            
                    # Additional Cloudflare specific checks
                    if "ray id:" in page_source and "cloudflare" in page_source:
                        if "dns points to" in page_source or "error" in page_source:
                            print(f"Found Cloudflare error indicators in rendered content for {url}")
                            has_special_error = True
                            error_message = "Cloudflare Error (rendered)"
                except Exception as e:
                    print(f"Error checking for special errors with Selenium: {e}")
                    # If we can't check with Selenium, we'll be conservative and mark as error
                    has_special_error = True
                    error_message = f"Selenium check failed: {str(e)}"
            
            # Mark the cell based on the error condition or response status
            if has_special_error:
                print(f"Special error detected: {error_message} for {url}")
                cell_marked = mark_cell_text_red(sheet, row, col)
            elif response.status_code >= 400:
                # ALWAYS mark any 4xx or 5xx status as red - no exceptions
                error_message = f"HTTP Status {response.status_code}"
                print(f"❌ HTTP Error: {url} - {error_message}")
                cell_marked = mark_cell_text_red(sheet, row, col)
            elif response.status_code == 301 or response.status_code == 302:
                # Check if redirects lead to a working page
                print(f"Redirect detected for {url} -> {response.url}")
                
                # Analyze the content after redirect to check domain status
                is_expired, reason = analyze_domain_status(response.text, url, response.url, None, driver)
                
                if is_expired:
                    error_message = f"Domain expired after redirect: {reason}"
                    print(f"❌ Domain Expired: {url} -> {response.url} - {error_message}")
                    cell_marked = mark_cell_text_red(sheet, row, col)
                else:
                    # Only mark as working if the redirect leads to a proper page
                    is_working = True
                    print(f"✅ URL redirects properly: {url} -> {response.url}")
                    cell_marked = reset_cell_formatting(sheet, row, col)
            else:
                # URL returned 200 - analyze content to check domain status
                content = response.text
                
                # Analyze the domain status
                title = None
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    title_tag = soup.find('title')
                    if title_tag:
                        title = title_tag.text.strip()
                        
                    # Additional checks for error indicators in HTML or title
                    error_title_indicators = [
                        "not found", "error", "forbidden", "expired", 
                        "404", "403", "500", "unavailable", "sorry"
                    ]
                    
                    if title and any(indicator in title.lower() for indicator in error_title_indicators):
                        print(f"Error indicator found in page title: '{title}' for {url}")
                        error_message = f"Error page title: {title}"
                        cell_marked = mark_cell_text_red(sheet, row, col)
                        # Skip further analysis as we've identified an error
                        return False, error_message
                except Exception as e:
                    print(f"Error analyzing page title: {str(e)}")
                
                # Get the final URL after redirects
                response_url = response.url
                
                # Extract domain from URL
                domain_match = re.search(r'https?://([^/]+)', response_url)
                if domain_match:
                    domain = domain_match.group(1)
                else:
                    domain = response_url
                
                # Analyze the content to check domain status
                is_expired, reason = analyze_domain_status(content, url, response_url, title, driver)
                
                if is_expired:
                    error_message = f"Domain expired: {reason}"
                    print(f"❌ Domain Expired: {url} - {error_message}")
                    cell_marked = mark_cell_text_red(sheet, row, col)
                else:
                    # Final check - make sure the page isn't a parked domain or empty
                    parked_domain_indicators = [
                        "domain is for sale", 
                        "buy this domain", 
                        "domain may be for sale", 
                        "parked domain",
                        "domain parking"
                    ]
                    
                    is_parked = False
                    for indicator in parked_domain_indicators:
                        if indicator in content.lower():
                            is_parked = True
                            error_message = f"Parked domain: {indicator}"
                            break
                    
                    # Check if the page is suspiciously small (common for error pages)
                    is_suspiciously_small = len(content) < 500 and "html" in content.lower()
                    
                    if is_parked:
                        print(f"❌ Parked Domain: {url} - {error_message}")
                        cell_marked = mark_cell_text_red(sheet, row, col)
                    elif is_suspiciously_small:
                        print(f"❌ Suspiciously small page: {url} - only {len(content)} bytes")
                        cell_marked = mark_cell_text_red(sheet, row, col)
                    else:
                        # URL is genuinely working!
                        is_working = True
                        print(f"✅ URL is working: {url}")
                        cell_marked = reset_cell_formatting(sheet, row, col)
                    
        except requests.exceptions.RequestException as e:
            # ALL connection errors should be marked red - no exceptions
            error_message = str(e)
            print(f"❌ Connection Error: {url} - {error_message}")
            cell_marked = mark_cell_text_red(sheet, row, col)
            
    except Exception as e:
        # ANY other exception should be marked red - no exceptions
        error_message = str(e)
        print(f"❌ Error checking URL: {url} - {error_message}")
        if retry_count < 1:  # Try one more time if there's an unexpected error
            print(f"Retrying URL: {url}")
            await asyncio.sleep(2)  # Wait 2 seconds before retry
            return await check_url(driver, url, sheet, row, col, retry_count + 1)
        else:
            try:
                cell_marked = mark_cell_text_red(sheet, row, col)
                if cell_marked:
                    print(f"Marked cell {col}{row} as red after retry failure")
                else:
                    print(f"Failed to mark cell {col}{row} as red after retry failure (likely rate limited)")
                    # Add to pending formats to ensure it gets marked eventually
                    pending_formats.append({
                        'sheet': sheet,
                        'row': row,
                        'col': col,
                        'type': 'red',
                        'url': url,
                        'retry_count': 0,
                        'format_key': f"{col}{row}:red"
                    })
            except Exception as mark_err:
                print(f"❌ Failed to mark cell {col}{row} as red: {str(mark_err)}")
                # Even if marking fails, still add to pending formats
                pending_formats.append({
                    'sheet': sheet,
                    'row': row,
                    'col': col,
                    'type': 'red',
                    'url': url,
                    'retry_count': 0,
                    'format_key': f"{col}{row}:red"
                })

    # Final safety check - ensure the cell was marked one way or the other
    if not cell_marked:
        try:
            print(f"⚠️ Cell {col}{row} was not marked during processing - added to pending formats queue with high priority")
            # Default to red unless we're ABSOLUTELY SURE the URL is working
            cell_type = 'blue' if is_working else 'red'
            # Add with higher priority to ensure it gets processed soon
            pending_formats.append({
                'sheet': sheet,
                'row': row,
                'col': col,
                'type': cell_type,
                'url': url,
                'retry_count': MAX_PENDING_RETRIES - 3,  # Higher priority
                'format_key': f"{col}{row}:{cell_type}"
            })
        except Exception as final_mark_err:
            print(f"❌ Final attempt to track cell {col}{row} failed: {str(final_mark_err)}")
            # Last resort - try to mark it red directly
            try:
                print(f"LAST RESORT: Attempting direct red marking for cell {col}{row}")
                mark_cell_text_red(sheet, row, col, retry_count=RATE_LIMIT_RETRIES-1)
            except:
                print(f"❌❌ ALL ATTEMPTS FAILED for cell {col}{row}. Will be caught by final safety check.")
    
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

async def process_pending_formats(final_attempt=False):
    """Process any cell formats that couldn't be applied due to rate limits"""
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    if not pending_formats:
        print("No pending cell formats to process")
        return
        
    total_to_process = len(pending_formats)
    print(f"\n===== Processing {total_to_process} pending cell formats =====")
    
    # Make a copy of the pending formats and clear the global list
    formats_to_process = pending_formats.copy()
    pending_formats = []
    
    successfully_processed = 0
    still_pending = 0
    
    # Sort formats by retry count (process those with fewer retries first)
    formats_to_process.sort(key=lambda x: x.get('retry_count', 0))
    
    # Process each pending format with pauses to avoid rate limits
    # Use a batching approach to be more aggressive about rate limits
    for batch_idx in range(0, len(formats_to_process), BATCH_WRITE_SIZE):
        batch = formats_to_process[batch_idx:batch_idx + BATCH_WRITE_SIZE]
        print(f"\nProcessing batch {batch_idx//BATCH_WRITE_SIZE + 1}/{(len(formats_to_process) + BATCH_WRITE_SIZE - 1)//BATCH_WRITE_SIZE}")
        
        for idx, format_data in enumerate(batch):
            sheet = format_data['sheet']
            row = format_data['row']
            col = format_data['col']
            format_type = format_data['type']
            format_key = format_data.get('format_key', f"{col}{row}:{format_type}")
            retry_count = format_data.get('retry_count', 0)
            url = format_data.get('url', 'unknown')
            
            # Skip if this cell was already successfully formatted
            cell_id = f"{col}{row}"
            if cell_id in successfully_formatted_cells:
                print(f"Skipping pending format for cell {cell_id} - already successfully formatted")
                successfully_processed += 1
                continue
                
            # Check if we've exceeded retries for this cell, but if this is a final attempt, try anyway
            if retry_count >= MAX_PENDING_RETRIES and not final_attempt:
                print(f"⚠️ Max retries exceeded for cell {cell_id}. Will not attempt further formatting.")
                failed_formatted_cells.add(cell_id)
                continue
            
            print(f"Processing format for cell {col}{row}: {format_type} (URL: {url}, retry: {retry_count+1}/{MAX_PENDING_RETRIES})")
            
            try:
                success = False
                if format_type == 'red':
                    success = mark_cell_text_red(sheet, row, col)
                else:  # blue
                    success = reset_cell_formatting(sheet, row, col)
                    
                if success:
                    successfully_processed += 1
                    print(f"✅ Successfully processed pending format for cell {col}{row}")
                else:
                    # Add back to the pending list if still failed, with incremented retry count
                    format_data['retry_count'] = retry_count + 1
                    pending_formats.append(format_data)
                    still_pending += 1
                    print(f"⚠️ Failed to process pending format for cell {col}{row} - will retry later")
                    
                # Pause between each format to avoid hitting rate limits - much longer pauses
                sleep_time = 60 / SHEETS_API_WRITES_PER_MINUTE * 3  # 3x safety factor (greatly increased)
                print(f"Pausing for {sleep_time:.1f} seconds to avoid rate limits...")
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                print(f"❌ Error processing pending format for cell {col}{row}: {str(e)}")
                format_data['retry_count'] = retry_count + 1
                pending_formats.append(format_data)
                still_pending += 1
        
        # Pause between batches with a longer pause (increased duration)
        if batch_idx + BATCH_WRITE_SIZE < len(formats_to_process):
            print(f"\nCompleted batch {batch_idx//BATCH_WRITE_SIZE + 1}. Taking a longer pause ({BATCH_WRITE_PAUSE} seconds) before next batch...")
            await asyncio.sleep(BATCH_WRITE_PAUSE)
    
    print(f"\n===== Pending Formats Processing Summary =====")
    print(f"✅ Successfully processed: {successfully_processed}/{total_to_process}")
    print(f"⚠️ Still pending: {still_pending}/{total_to_process}")
    print(f"Total successfully formatted cells: {len(successfully_formatted_cells)}")
    print(f"Total failed formatted cells: {len(failed_formatted_cells)}")
    
    remaining = len(pending_formats)
    if remaining > 0:
        print(f"⚠️ {remaining} cell formats still pending after processing")
    else:
        print("✅ All pending cell formats processed successfully")

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
            
            # Collect all URLs to check
            urls_to_check = []
            
            # Convert column letters to indices
            column_indices = [column_to_index(col) for col in URL_COLUMNS]
            
            # Loop through all rows to collect URLs
            for row_idx in range(1, len(all_values)):  # Start from index 1 (row 2)
                row_data = all_values[row_idx]
                
                for col_idx in column_indices:
                    # Make sure the row has enough columns
                    cell_content = ""
                    if col_idx < len(row_data):
                        cell_content = row_data[col_idx]
                    
                    if cell_content.strip():  # If cell is not empty
                        # Extract URLs from the cell
                        try:
                            urls = extract_urls_from_text(cell_content)
                            if urls:
                                # Add to the list of URLs to check
                                for url in urls:
                                    urls_to_check.append({
                                        'url': url,
                                        'row': row_idx + 1,  # +1 because we're 0-indexed but sheets are 1-indexed
                                        'col': index_to_column(col_idx),
                                        'original_content': cell_content
                                    })
                            else:
                                # If no valid URLs found but cell has content, mark it for checking anyway
                                possible_url = cell_content
                                if not possible_url.startswith(('http://', 'https://')):
                                    possible_url = 'http://' + possible_url
                                
                                urls_to_check.append({
                                    'url': possible_url,
                                    'row': row_idx + 1,
                                    'col': index_to_column(col_idx),
                                    'original_content': cell_content,
                                    'is_potential_url': True
                                })
                        except Exception as e:
                            # If URL extraction fails, still try to check it
                            print(f"❌ Error extracting URLs from cell {index_to_column(col_idx)}{row_idx+1}: {str(e)}")
                            try:
                                # Try to make a checkable URL from the content
                                possible_url = cell_content
                                if not possible_url.startswith(('http://', 'https://')):
                                    possible_url = 'http://' + possible_url
                                
                                urls_to_check.append({
                                    'url': possible_url,
                                    'row': row_idx + 1,
                                    'col': index_to_column(col_idx),
                                    'original_content': cell_content,
                                    'is_potential_url': True
                                })
                            except Exception as inner_e:
                                print(f"❌ Could not process cell {index_to_column(col_idx)}{row_idx+1}: {str(inner_e)}")
                                try:
                                    # Mark as red by default since we can't process it
                                    mark_cell_text_red(sheet, row_idx + 1, index_to_column(col_idx))
                                    print(f"Marked problematic cell {index_to_column(col_idx)}{row_idx+1} as red by default")
                                except Exception as mark_err:
                                    print(f"❌ Failed to mark problematic cell: {str(mark_err)}")
                                    # Add to pending formats with high priority
                                    pending_formats.append({
                                        'sheet': sheet,
                                        'row': row_idx + 1,
                                        'col': index_to_column(col_idx),
                                        'type': 'red',
                                        'format_key': f"{index_to_column(col_idx)}{row_idx+1}:red",
                                        'retry_count': MAX_PENDING_RETRIES - 3,  # High priority
                                        'url': cell_content
                                    })
            
            print(f"Found {len(urls_to_check)} URLs to check")
            
            # Prevent empty run
            if not urls_to_check:
                print("⚠️ Warning: No URLs found to check. Please verify spreadsheet content and column selection.")
                return
                
            # Process URLs in batches
            batch_count = 0
            start_time = time.time()
            total_cells_processed = 0
            
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
                    print(f"Checking URL {overall_index}/{len(urls_to_check)} [{total_cells_processed + 1}]: {url} in cell {col}{row}")
                    
                    try:
                        await check_url(driver, url, sheet, row, col)
                        total_cells_processed += 1
                    except Exception as e:
                        print(f"❌ Error checking URL {url}: {str(e)}")
                        traceback.print_exc()
                        try:
                            mark_cell_text_red(sheet, row, col)
                        except Exception as mark_err:
                            print(f"Error marking cell: {str(mark_err)}")
                            # Add to pending formats with high priority
                            pending_formats.append({
                                'sheet': sheet,
                                'row': row,
                                'col': col,
                                'type': 'red',
                                'format_key': f"{col}{row}:red",
                                'retry_count': MAX_PENDING_RETRIES - 3,  # High priority
                                'url': url
                            })
                        total_cells_processed += 1
                
                # Process any pending cell formats between batches
                if pending_formats:
                    print(f"Processing {len(pending_formats)} pending cell formats between batches...")
                    await process_pending_formats()
                
                # Give Google's API a break between batches
                print(f"Completed batch {batch_count}. Sleeping for 30 seconds to avoid API rate limits...")
                await asyncio.sleep(30)  # Increased from 10 to 30 seconds
            
            # Final summary
            print(f"\n===== URL CHECKING SUMMARY =====")
            print(f"Total cells processed: {total_cells_processed}")
            print(f"Total URLs checked: {len(urls_to_check)}")
            print(f"URLs per batch: {BATCH_SIZE}")
            print(f"Total batches: {batch_count}")
            print("=================================")
            
            # Final processing of any remaining pending formats
            if pending_formats:
                print(f"Final processing of {len(pending_formats)} pending cell formats...")
                await process_pending_formats()
                
                # If we still have pending formats, try multiple times with long pauses between attempts
                retry_attempts = 3
                for attempt in range(retry_attempts):
                    if not pending_formats:
                        break
                        
                    pause_time = 240 + (attempt * 120)  # 4 minutes, 6 minutes, 8 minutes
                    print(f"Taking a long pause ({pause_time} seconds) before attempt {attempt+1}/{retry_attempts} to process {len(pending_formats)} remaining pending formats...")
                    await asyncio.sleep(pause_time)
                    print(f"Attempt {attempt+1}/{retry_attempts} to process {len(pending_formats)} stubborn pending formats...")
                    await process_pending_formats(final_attempt=(attempt == retry_attempts-1))
                    
                # After all retries, try one final desperate attempt for any remaining cells
                if pending_formats:
                    print(f"⚠️ Still have {len(pending_formats)} pending formats after all retries")
                    print("Making one final ultra-conservative attempt with maximum pauses")
                    
                    # Make a copy and clear the pending formats
                    final_formats = pending_formats.copy()
                    pending_formats = []
                    
                    # Try each one individually with very long pauses
                    for idx, format_data in enumerate(final_formats):
                        sheet = format_data['sheet']
                        row = format_data['row']
                        col = format_data['col']
                        format_type = format_data['type']
                        
                        print(f"Final attempt {idx+1}/{len(final_formats)} for cell {col}{row}")
                        try:
                            if format_type == 'red':
                                mark_cell_text_red(sheet, row, col)
                            else:
                                reset_cell_formatting(sheet, row, col)
                            
                            # Ultra-long pause between each cell
                            await asyncio.sleep(120)
                        except Exception as e:
                            print(f"❌ Final attempt failed for cell {col}{row}: {str(e)}")
                            # At this point, we've tried everything, so just move on
            
            # Print final formatting statistics
            print("\n===== FINAL FORMATTING STATISTICS =====")
            print(f"Successfully formatted cells: {len(successfully_formatted_cells)}")
            print(f"Failed to format cells: {len(failed_formatted_cells)}")
            print(f"Cells still pending formatting: {len(pending_formats)}")
            
            if failed_formatted_cells:
                print("\nThe following cells could not be formatted after all retries:")
                for cell_id in sorted(list(failed_formatted_cells)):
                    print(f"- {cell_id}")
                    
            if pending_formats:
                print("\nThe following cells are still pending formatting:")
                for format_data in pending_formats:
                    col = format_data['col']
                    row = format_data['row']
                    url = format_data.get('url', 'unknown')
                    print(f"- {col}{row} (URL: {url})")
                    
            # SAFETY CHECK: Verify all URLs were processed and colored
            # This adds one final verification to make sure nothing was missed
            print("\n===== FINAL SAFETY CHECK =====")
            all_checked_cells = {f"{url_data['col']}{url_data['row']}" for url_data in urls_to_check}
            all_formatted_cells = successfully_formatted_cells.union(failed_formatted_cells)
            all_pending_cells = {f"{fmt['col']}{fmt['row']}" for fmt in pending_formats}
            
            missed_cells = all_checked_cells - all_formatted_cells - all_pending_cells
            
            if missed_cells:
                print(f"⚠️ WARNING: Found {len(missed_cells)} cells that were checked but not formatted!")
                print("Attempting emergency formatting for these cells:")
                
                for cell_id in missed_cells:
                    # Extract column and row from cell_id
                    col = ''.join(c for c in cell_id if c.isalpha())
                    row = int(''.join(c for c in cell_id if c.isdigit()))
                    
                    # Find original URL for this cell
                    original_url = "unknown"
                    for url_data in urls_to_check:
                        if url_data['col'] == col and url_data['row'] == row:
                            original_url = url_data['url']
                            break
                    
                    print(f"Emergency formatting for missed cell {cell_id} (URL: {original_url})")
                    
                    # Default to marking as red since we don't know the status
                    try:
                        print(f"Applying emergency red formatting to cell {cell_id}")
                        mark_cell_text_red(sheet, row, col)
                    except Exception as e:
                        print(f"❌ Emergency formatting failed for cell {cell_id}: {str(e)}")
                
                print(f"Emergency formatting attempted for {len(missed_cells)} missed cells")
            else:
                print("✅ All checked URLs were either successfully formatted or are in the pending queue")
                print(f"Total cells checked: {len(all_checked_cells)}")
                print(f"Total cells formatted: {len(all_formatted_cells)}")
                print(f"Total cells pending: {len(all_pending_cells)}")
                
            # End safety check
            
            # Report end time and overall success ratio
            success_rate = (len(successfully_formatted_cells) / (len(successfully_formatted_cells) + len(failed_formatted_cells) + len(pending_formats))) * 100 if (len(successfully_formatted_cells) + len(failed_formatted_cells) + len(pending_formats)) > 0 else 0
            print(f"\nSuccess rate: {success_rate:.2f}%")
            print("====================================")
            
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

async def wait_for_next_run(hours=24):
    """Wait for the specified number of hours before the next run"""
    wait_seconds = hours * 60 * 60
    
    # Get current time and calculate next run time
    now = datetime.now()
    next_run = now + timedelta(hours=hours)
    
    print(f"\nCurrent time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Next run scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {hours} hours)")
    print(f"Waiting {wait_seconds} seconds...")
    
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
    
    # Check if we're in testing mode or production mode
    testing_mode = os.getenv('TESTING_MODE', 'false').lower() == 'true'
    
    if testing_mode:
        print(f"\nRunning in TESTING mode - checking URLs every {CHECK_INTERVAL} seconds")
        
        while True:
            print("\nStarting URL check cycle...")
            await check_links()
            print("\nURL check cycle completed.")
            await wait_until_next_interval(CHECK_INTERVAL)
    else:
        print("\nRunning in PRODUCTION mode - checking URLs every 24 hours")
        
        while True:
            print("\nStarting URL check cycle...")
            start_time = time.time()
            
            # Run the check
            await check_links()
            
            end_time = time.time()
            duration = end_time - start_time
            print(f"\nURL check cycle completed. Duration: {duration/60/60:.2f} hours")
            
            # Wait 24 hours from when this run completed
            await wait_for_next_run(hours=24)

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