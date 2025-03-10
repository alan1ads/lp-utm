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
BATCH_SIZE = 300  # Process URLs in batches of 300 (reduced from 500)
MAX_BROWSER_LIFETIME = 20  # Restart browser every 20 minutes (reduced from 30)

# Add rate limiting constants
SHEETS_API_WRITES_PER_MINUTE = 60  # Google's quota limit
RATE_LIMIT_PAUSE_MIN = 180  # Minimum seconds to pause after hitting a rate limit
RATE_LIMIT_PAUSE_MAX = 300  # Maximum seconds to pause after hitting a rate limit
RATE_LIMIT_RETRIES = 5      # Maximum retries for rate-limited operations
MAX_PENDING_RETRIES = 10    # Maximum retries for processing pending formats
BATCH_WRITE_SIZE = 5        # Process 5 pending formats at a time
BATCH_WRITE_PAUSE = 180     # Pause 180 seconds between pending format batches
INTER_URL_PAUSE = 0.5       # Pause 0.5 seconds between individual URL checks
BATCH_COMPLETION_PAUSE = 60 # Pause 60 seconds between URL checking batches

# Browser management
browser_restart_count = 0  # Track browser restarts

# Time constants
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400

# Global tracking sets for cell formatting
successfully_formatted_cells = set()
failed_formatted_cells = set()

# List to track pending cell formats - made properly global
pending_formats = []

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
    """Extract URLs from text (preserving full URL structure including query parameters)"""
    if not text:
        return []
    
    # Use a URL pattern that includes query parameters and fragments
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^/\s]*)*(?:\?[^\s]*)?(?:#[^\s]*)?'
    matches = re.findall(url_pattern, text)
    
    # Filter out invalid URLs but preserve full structure
    valid_urls = []
    for url in matches:
        # Clean the URL (remove trailing punctuation but keep query parameters)
        url = url.strip()
        # Remove trailing punctuation but preserve query parameters
        if url.endswith(('.', ',', ')', ']', '}', ':', ';')):
            url = url[:-1]
        
        if is_valid_url(url):
            valid_urls.append(url)
    
    # Log extracted URLs
    if valid_urls:
        print(f"Extracted {len(valid_urls)} valid URLs: {valid_urls}")
    
    return valid_urls

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
    """Mark cell text as red for failed URLs"""
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    # Get the unique cell identifier
    cell_id = f"{col}{row}"
    
    # For marking RED, we'll still skip if already marked red to avoid unnecessary API calls.
    # But if a cell is currently blue (in successfully_formatted_cells), we SHOULD mark it red
    # if a bad URL is found after a good one.
    if cell_id in failed_formatted_cells and cell_id not in successfully_formatted_cells:
        print(f"Cell {cell_id} was already marked red - skipping")
        return True
    
    try:
        # Define the cell range for formatting
        cell_range = f"{col}{row}"
        
        # FIX: Create CellFormat properly with a new instance each time
        fmt = CellFormat(
            textFormat=TextFormat(foregroundColor=Color(0.95, 0.2, 0.1))  # Red
        )
        
        # Debug output to help diagnose issues
        print(f"Applying red format to cell {cell_range}, format type: {type(fmt)}")
        
        # Apply the formatting
        format_cell_range(sheet, cell_range, fmt)
        print(f"Marked cell {cell_range} as red (failed URL)")
        
        # Track this successful formatting
        failed_formatted_cells.add(cell_id)
        
        # If this was in the successfully formatted set, remove it as it's now failed
        if cell_id in successfully_formatted_cells:
            successfully_formatted_cells.remove(cell_id)
            
        return True
    except Exception as e:
        error_str = str(e)
        
        # Check for rate limit errors
        if "RESOURCE_EXHAUSTED" in error_str or "429" in error_str or "quota" in error_str.lower():
            print(f"⚠️ Rate limit hit while marking cell {cell_range} as red")
            if retry_count < RATE_LIMIT_RETRIES:
                # Calculate exponential backoff with jitter
                jitter = random.uniform(0.5, 1.5)
                retry_seconds = min(backoff_seconds * (2 ** retry_count) * jitter, RATE_LIMIT_PAUSE_MAX)
                print(f"Retrying in {retry_seconds:.1f} seconds (retry {retry_count+1}/{RATE_LIMIT_RETRIES})")
                time.sleep(retry_seconds)
                return mark_cell_text_red(sheet, row, col, retry_count + 1, backoff_seconds)
            
            # Add to pending formats to retry later
            print(f"Maximum retries reached for cell {cell_range}. Adding to pending formats queue.")
            pending_formats.append({
                'sheet': sheet,
                'row': row,
                'col': col,
                'type': 'red',
                'format_key': f"{col}{row}:red"
            })
            failed_formatted_cells.add(cell_id)
            return False
        
        # Check for the 'to_props' error
        elif "'dict' object has no attribute 'to_props'" in error_str:
            print(f"⚠️ Encountered 'to_props' error - trying alternative formatting method")
            try:
                # Try alternative formatting method
                worksheet = sheet
                worksheet.format(cell_range, {
                    "textFormat": {
                        "foregroundColor": {
                            "red": 0.95,
                            "green": 0.2,
                            "blue": 0.1
                        }
                    }
                })
                print(f"Marked cell {cell_range} as red using alternative method")
                
                # Track successful formatting
                failed_formatted_cells.add(cell_id)
                if cell_id in successfully_formatted_cells:
                    successfully_formatted_cells.remove(cell_id)
                    
                return True
            except Exception as inner_e:
                print(f"❌ Alternative formatting method also failed: {str(inner_e)}")
                failed_formatted_cells.add(cell_id)
                return False
        
        print(f"❌ Error marking cell {cell_range} as red: {error_str}")
        # Track this failed formatting
        failed_formatted_cells.add(cell_id)
        return False

def reset_cell_formatting(sheet, row, col, retry_count=0, backoff_seconds=1):
    """Reset cell formatting to bright blue for working URLs"""
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    # Get the unique cell identifier
    cell_id = f"{col}{row}"
    
    # CHANGED: ALWAYS reformat cells to ensure the new color is applied,
    # regardless of whether they were previously formatted
    # (temporarily disabled skipping already formatted cells)
    #if cell_id in successfully_formatted_cells and cell_id not in failed_formatted_cells:
    #    print(f"Cell {cell_id} was already successfully formatted as blue (#0000EE) - skipping")
    #    return True
    
    try:
        cell_range = f"{col}{row}"
        
        # FIX: Create CellFormat properly with a new instance each time
        # CHANGE: Updated color to #0000EE (0, 0, 238)
        fmt = CellFormat(
            textFormat=TextFormat(foregroundColor=Color(
                red=0/255,
                green=0/255,
                blue=238/255
            ))
        )
        
        # Debug output to help diagnose issues
        print(f"Applying blue (#0000EE) format to cell {cell_range}, format type: {type(fmt)}")
        
        # Apply the formatting
        format_cell_range(sheet, cell_range, fmt)
        print(f"Marked cell {cell_range} as blue (#0000EE) for working URL")
        
        # Track this successful formatting
        successfully_formatted_cells.add(cell_id)
        
        # If this was in the failed set, remove it
        if cell_id in failed_formatted_cells:
            failed_formatted_cells.remove(cell_id)
            
        return True
    except Exception as e:
        error_str = str(e)
        
        # Check for rate limit errors
        if "RESOURCE_EXHAUSTED" in error_str or "429" in error_str or "quota" in error_str.lower():
            print(f"⚠️ Rate limit hit while marking cell {cell_range} as blue (#0000EE)")
            if retry_count < RATE_LIMIT_RETRIES:
                # Calculate exponential backoff with jitter
                jitter = random.uniform(0.5, 1.5)
                retry_seconds = min(backoff_seconds * (2 ** retry_count) * jitter, RATE_LIMIT_PAUSE_MAX)
                print(f"Retrying in {retry_seconds:.1f} seconds (retry {retry_count+1}/{RATE_LIMIT_RETRIES})")
                time.sleep(retry_seconds)
                return reset_cell_formatting(sheet, row, col, retry_count + 1, backoff_seconds)
                
            # Add to pending formats to retry later
            print(f"Maximum retries reached for cell {cell_range}. Adding to pending formats queue.")
            pending_formats.append({
                'sheet': sheet,
                'row': row,
                'col': col,
                'type': 'blue',
                'retry_count': retry_count,
                'format_key': f"{col}{row}:blue"
            })
            failed_formatted_cells.add(cell_id)
            return False
            
        # Check for the 'to_props' error
        elif "'dict' object has no attribute 'to_props'" in error_str:
            print(f"⚠️ Encountered 'to_props' error - trying alternative formatting method")
            try:
                # Try alternative formatting method with updated color #0000EE
                worksheet = sheet
                worksheet.format(cell_range, {
                    "textFormat": {
                        "foregroundColor": {
                            "red": 0/255,
                            "green": 0/255,
                            "blue": 238/255
                        }
                    }
                })
                print(f"Marked cell {cell_range} as blue (#0000EE) using alternative method")
                
                # Track successful formatting
                successfully_formatted_cells.add(cell_id)
                if cell_id in failed_formatted_cells:
                    failed_formatted_cells.remove(cell_id)
                    
                return True
            except Exception as inner_e:
                print(f"❌ Alternative formatting method also failed: {str(inner_e)}")
                failed_formatted_cells.add(cell_id)
                return False
                
        print(f"❌ Error marking cell {cell_range} as blue (#0000EE): {error_str}")
        # Track this failed formatting
        failed_formatted_cells.add(cell_id)
        return False

async def check_url(driver, url, sheet, row, col, retry_count=0, is_last_url=False):
    """Check if a URL is working and mark it in the spreadsheet"""
    global browser_restart_count, pending_formats
    
    print(f"=== Checking URL: {url} at cell {col}{row} {'(FINAL URL in cell)' if is_last_url else ''} ===")
    
    # Track whether the URL is working
    is_working = False
    error_message = ""
    cell_marked = False  # Flag to track if we've marked the cell
    original_url = url  # Store the original URL for reference
    
    # Initialize has_error_indicators at the beginning to prevent UnboundLocalError
    has_error_indicators = False
    
    try:
        # Make the actual web request to check the URL
        try:
            timeout = 30  # Increased timeout to 30 seconds
            
            # Use proper headers to simulate a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Print the full URL we're checking (including query parameters)
            print(f"Checking full URL: {url}")
            
            # Handle potential request errors gracefully
            try:
                response = requests.get(url, timeout=timeout, allow_redirects=True, headers=headers)
                print(f"Response status code: {response.status_code}")
                print(f"Final URL after redirects: {response.url}")
                
                # Check HTTP status code first - quickest determination
                if response.status_code >= 400:
                    error_message = f"HTTP Status {response.status_code}"
                    print(f"❌ HTTP Error: {url} - {error_message}")
                    if is_last_url:
                        cell_marked = mark_cell_text_red(sheet, row, col)
                    else:
                        print(f"Not marking cell red yet since this is not the last URL in cell {col}{row}")
                    return False, error_message
                
                # For redirects, capture the final URL
                final_url = response.url
                response_text_lower = response.text.lower()
                
                # Track various indicators for better decision making
                has_template_vars = False
                has_parked_domain_indicators = False
                has_minimal_content = False
                has_real_content = False
                
                # Check for template variables - safe check without errors
                try:
                    if '{{' in response_text_lower and '}}' in response_text_lower:
                        print(f"ℹ️ Found handlebars template variables ({{var}}) in content")
                        has_template_vars = True
                    elif '{' in response_text_lower and '}' in response_text_lower and re.search(r'\{[a-zA-Z0-9_]+\}', response_text_lower):
                        print(f"ℹ️ Found curly brace placeholder parameters ({{var}}) in content")
                        has_template_vars = True
                except Exception as var_error:
                    print(f"Warning: Error checking for template variables (non-critical): {str(var_error)}")
                
                # Parse content with BeautifulSoup for deeper analysis
                try:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Extract important page elements
                    title = soup.find('title')
                    title_text = title.text.strip() if title else "No Title"
                    print(f"Page title: {title_text}")
                    
                    # Get counts of various content elements
                    paragraphs = soup.find_all('p')
                    headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    links = soup.find_all('a')
                    forms = soup.find_all('form')
                    images = soup.find_all('img')
                    
                    # Calculate content density metrics
                    content_elements = len(paragraphs) + len(headings) + len(forms) * 3 + len(images)
                    print(f"Content elements: {content_elements} (paragraphs: {len(paragraphs)}, " +
                          f"headings: {len(headings)}, forms: {len(forms)}, images: {len(images)})")
                    
                    # Check text content length
                    text_content = soup.get_text()
                    content_length = len(text_content.strip())
                    print(f"Text content length: {content_length} characters")
                    
                    # Analyze content quality
                    if content_elements >= 10 and content_length > 500:
                        print("✅ Page has substantial content elements and text")
                        has_real_content = True
                    elif content_elements < 3 and content_length < 200 and not has_template_vars:
                        print("⚠️ Page has very minimal content")
                        has_minimal_content = True

                    # Check for specific error phrases - focus on actual error messages
                    error_phrases = [
                        "404 not found", "403 forbidden", "500 server error", "502 bad gateway",
                        "dns_probe_finished_nxdomain", "page not found", "site can't be reached",
                        "connection refused", "site not found", "this page isn't working",
                        "this site can't be reached", "server not found", "website is unavailable"
                    ]
                    
                    for phrase in error_phrases:
                        if phrase in response_text_lower:
                            print(f"❌ Found specific error phrase: '{phrase}'")
                            has_error_indicators = True
                            error_message = f"Error content: {phrase}"
                            break
                    
                    # Check for parked domain indicators
                    parked_domain_phrases = [
                        "domain is for sale", "buy this domain", "purchase this domain", 
                        "domain expired", "renew your domain", "this domain may be for sale",
                        "domain parking", "parked domain", "this web page is parked"
                    ]
                    
                    for phrase in parked_domain_phrases:
                        if phrase in response_text_lower:
                            print(f"⚠️ Found parked domain indicator: '{phrase}'")
                            has_parked_domain_indicators = True
                            error_message = f"Parked domain: {phrase}"
                            break
                except Exception as soup_error:
                    print(f"Warning: Error during BeautifulSoup parsing (non-critical): {str(soup_error)}")
                    # If we can't parse but HTTP status is good, we'll still check with Selenium
                    if response.status_code < 400:
                        print("✅ HTTP status is good, continuing with Selenium check despite parsing error")
                
                # Always check with Selenium for a more accurate assessment
                # Especially important for JS-heavy sites and landing pages
                print("Performing thorough rendering check with Selenium")
                try:
                    # Add error handling for tab crashes
                    max_selenium_retries = 2
                    selenium_attempt = 0
                    
                    while selenium_attempt < max_selenium_retries:
                        try:
                            # Use the FULL original URL with all parameters
                            print(f"Loading URL in Selenium (attempt {selenium_attempt+1}): {original_url}")
                            driver.get(original_url)
                            
                            # Wait for page to load with longer timeout (15 seconds)
                            WebDriverWait(driver, 15).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            
                            # Analyze the rendered page
                            page_source = driver.page_source.lower()
                            rendered_body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                            
                            # Check for error indicators in the rendered content
                            selenium_error_found = False
                            for phrase in error_phrases:
                                if phrase in rendered_body_text:
                                    print(f"❌ Found error phrase in rendered content: '{phrase}'")
                                    selenium_error_found = True
                                    error_message = f"Rendered error: {phrase}"
                                    break
                                    
                            # Check for parked domain indicators in the rendered content
                            selenium_parked_found = False
                            for phrase in parked_domain_phrases:
                                if phrase in rendered_body_text:
                                    print(f"⚠️ Found parked domain indicator in rendered content: '{phrase}'")
                                    selenium_parked_found = True
                                    error_message = f"Rendered parked domain: {phrase}"
                                    break
                                    
                            # If Selenium found clear errors, mark the page as not working
                            if selenium_error_found or selenium_parked_found:
                                has_error_indicators = selenium_error_found
                                has_parked_domain_indicators = selenium_parked_found
                                if is_last_url:
                                    cell_marked = mark_cell_text_red(sheet, row, col)
                                else:
                                    print(f"Not marking cell red yet since this is not the last URL in cell {col}{row}")
                                return False, error_message
                            
                            # Analyze interactive elements in rendered page
                            try:
                                rendered_paragraphs = len(driver.find_elements(By.TAG_NAME, "p"))
                                rendered_headings = len(driver.find_elements(By.CSS_SELECTOR, "h1, h2, h3, h4, h5, h6"))
                                rendered_forms = len(driver.find_elements(By.TAG_NAME, "form"))
                                rendered_buttons = len(driver.find_elements(By.TAG_NAME, "button"))
                                rendered_inputs = len(driver.find_elements(By.TAG_NAME, "input"))
                                rendered_images = len(driver.find_elements(By.TAG_NAME, "img"))
                                
                                print(f"Rendered content: {rendered_paragraphs} paragraphs, " +
                                      f"{rendered_headings} headings, {rendered_forms} forms, " + 
                                      f"{rendered_buttons} buttons, {rendered_inputs} inputs, " +
                                      f"{rendered_images} images")
                                
                                # Evaluate content quality
                                rendered_text_length = len(rendered_body_text.strip())
                                print(f"Rendered text length: {rendered_text_length} characters")
                                
                                has_interactive_elements = rendered_forms > 0 or rendered_buttons > 0 or rendered_inputs > 0
                                
                                # Calculate a content quality score
                                content_quality = (
                                    rendered_paragraphs + 
                                    (rendered_headings * 2) + 
                                    (rendered_forms * 3) + 
                                    rendered_buttons + 
                                    rendered_inputs + 
                                    (rendered_images * 0.5)
                                )
                                
                                print(f"Content quality score: {content_quality}")
                                
                                # Make landing page specific assessments
                                is_landing_page = has_interactive_elements and ("{" in url or "{{" in url)
                                is_working_landing_page = False
                                
                                # Special case for landing pages with template variables
                                if is_landing_page:
                                    print("This appears to be a landing page with template variables")
                                    # Landing pages with forms are typically functional despite template vars
                                    if rendered_forms > 0 or (rendered_buttons > 0 and rendered_inputs > 0):
                                        print("✅ Landing page has functional form elements")
                                        is_working_landing_page = True
                                    # Landing pages often have minimal text content due to their nature
                                    if content_quality >= 5:
                                        print("✅ Landing page has sufficient quality score")
                                        is_working_landing_page = True
                                
                                # Regular page assessment
                                if content_quality >= 8 or has_real_content:
                                    print("✅ Page has high quality content")
                                    is_working = True
                                elif is_working_landing_page:
                                    print("✅ Working landing page detected")
                                    is_working = True
                                elif rendered_text_length < 50 and content_quality < 3 and not has_interactive_elements:
                                    print("⚠️ Page has extremely minimal content and no interactive elements")
                                    has_minimal_content = True
                                    if not is_landing_page:
                                        # Empty pages without interactive elements are probably errors
                                        error_message = "Empty or minimal content page"
                                else:
                                    # Default to working if we passed all error checks and the HTTP status was 200
                                    print("ℹ️ Page passed basic content checks with HTTP 200")
                                    is_working = True
                            except Exception as element_error:
                                print(f"Warning: Error analyzing page elements (non-critical): {str(element_error)}")
                                # Since we got to this point with a 200 status, the page is likely working
                                print("✅ HTTP status is good, considering page working despite element analysis error")
                                is_working = True
                                
                            # Success - break out of retry loop
                            break
                                
                        except Exception as selenium_error:
                            selenium_attempt += 1
                            error_str = str(selenium_error)
                            
                            print(f"Selenium error on attempt {selenium_attempt}: {error_str}")
                            
                            # If it's a tab crash, try to reset the driver
                            if "tab crashed" in error_str:
                                print("Tab crashed - attempting to restart browser")
                                try:
                                    driver.quit()
                                    browser_restart_count += 1
                                    driver = setup_selenium()
                                    await asyncio.sleep(3)  # Give browser a moment to start up
                                    
                                    # If this is our last retry and it failed, use request success as fallback
                                    if selenium_attempt >= max_selenium_retries - 1:
                                        print("Max Selenium retries reached after tab crash. Falling back to request analysis.")
                                        # Fall back to request-based analysis - if HTTP status was good, consider it working
                                        print("✅ HTTP status was good, considering page working despite Selenium issues")
                                        is_working = True
                                        break
                                except Exception as restart_error:
                                    print(f"Error restarting browser: {restart_error}")
                            
                            # If this is our last retry with Selenium, use HTTP request result as fallback
                            if selenium_attempt >= max_selenium_retries:
                                print("Max Selenium retries reached. Falling back to request analysis.")
                                # Since HTTP status was good, consider it working
                                print("✅ HTTP status was good, considering page working despite Selenium issues")
                                is_working = True
                            else:
                                # Pause before next attempt
                                await asyncio.sleep(3)

                except Exception as outer_selenium_error:
                    print(f"❌ Outer Selenium check failed: {str(outer_selenium_error)}")
                    # If HTTP status was good and there are no clear error indicators, consider it working
                    if response.status_code < 400 and not has_error_indicators and not has_parked_domain_indicators:
                        print("✅ HTTP status was good, considering page working despite Selenium issues")
                        is_working = True

            except requests.exceptions.RequestException as req_error:
                # Connection errors are handled via Selenium fallback
                error_message = str(req_error)
                print(f"❌ Connection Error with requests: {url} - {error_message}")
                
                # Try a fallback with Selenium for connectivity issues
                try:
                    print(f"Attempting fallback check with Selenium for {url}")
                    driver.get(original_url)  # Use original full URL
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    # If we got here, the page loaded in Selenium despite the request error
                    print(f"✅ Selenium fallback succeeded for {url} despite request error")
                    
                    # Do a quick content check
                    rendered_text = driver.find_element(By.TAG_NAME, "body").text
                    if len(rendered_text.strip()) > 100:
                        print("✅ Selenium found reasonable content despite request error")
                        is_working = True
                    else:
                        print("⚠️ Page loaded in Selenium but has minimal content")
                        # For landing pages, minimal content might be valid
                        if "{" in url or "{{" in url:
                            print("ℹ️ Minimal content might be valid for landing page with template variables")
                            is_working = True
                        else:
                            is_working = False
                            error_message = "Minimal content despite successful load"
                    
                    if is_working and is_last_url:
                        cell_marked = reset_cell_formatting(sheet, row, col)
                    elif not is_working and is_last_url:
                        cell_marked = mark_cell_text_red(sheet, row, col)
                    else:
                        print(f"Not marking cell yet since this is not the last URL in cell {col}{row}")
                except Exception as selenium_error:
                    # If both requests and Selenium fail, the URL is definitely not working
                    print(f"❌ Both requests and Selenium failed for {url}")
                    # Only mark the cell if this is the last URL in the cell
                    if is_last_url:
                        cell_marked = mark_cell_text_red(sheet, row, col)
                    else:
                        print(f"Not marking cell red yet since this is not the last URL in cell {col}{row}")
        
        except Exception as general_check_error:
            # Handle general errors in the checking process
            print(f"Warning: Error during URL checking process: {str(general_check_error)}")
            # For landing pages with template variables, be lenient
            if "{" in url or "{{" in url:
                print("⚠️ Error processing URL with template variables, but these are typically valid landing pages")
                is_working = True
            else:
                is_working = False
                error_message = f"Error checking URL: {str(general_check_error)}"
            
            # Mark the cell if this is the last URL
            if is_last_url:
                if is_working:
                    cell_marked = reset_cell_formatting(sheet, row, col)
                else:
                    cell_marked = mark_cell_text_red(sheet, row, col)
            
            # Return result
            return is_working, error_message
                
        # Final decision based on all collected evidence
        if has_error_indicators or has_parked_domain_indicators:
            is_working = False
            
        # Mark the cell based on our decision
        if is_working:
            print(f"✅ URL is working: {url}")
            if is_last_url:
                cell_marked = reset_cell_formatting(sheet, row, col)
            else:
                print(f"Not marking cell blue yet since this is not the last URL in cell {col}{row}")
        else:
            error_message = error_message or "Failed content quality checks"
            print(f"❌ URL is not working properly: {url} - {error_message}")
            if is_last_url:
                cell_marked = mark_cell_text_red(sheet, row, col)
            else:
                print(f"Not marking cell red yet since this is not the last URL in cell {col}{row}")
            
    except Exception as e:
        # ANY other exception at the top level
        error_message = str(e)
        print(f"❌ Top-level error checking URL: {url} - {error_message}")
        
        # For landing pages with template variables, be more lenient
        if "{" in url or "{{" in url:
            print("Special handling for URL with template variables - this is likely a landing page")
            print("Checking if HTTP request would succeed...")
            
            try:
                # Try a basic request to see if the URL is accessible
                test_response = requests.get(url, timeout=10, allow_redirects=True)
                if test_response.status_code < 400:
                    print(f"✅ HTTP request succeeded with status {test_response.status_code} - considering landing page working")
                    is_working = True
                else:
                    print(f"❌ HTTP request failed with status {test_response.status_code}")
                    is_working = False
            except:
                print("❌ HTTP request also failed")
                is_working = False
                
        if retry_count < 1:  # Try one more time if there's an unexpected error
            print(f"Retrying URL: {url}")
            await asyncio.sleep(2)  # Wait 2 seconds before retry
            return await check_url(driver, url, sheet, row, col, retry_count + 1, is_last_url)
        else:
            # After retries, make a final decision
            try:
                # Only mark the cell if this is the last URL in the cell
                if is_last_url:
                    if is_working:
                        cell_marked = reset_cell_formatting(sheet, row, col)
                        print(f"Marked cell {col}{row} as blue (#0000EE) for working URL")
                    else:
                        cell_marked = mark_cell_text_red(sheet, row, col)
                        print(f"Marked cell {col}{row} as red after retry failure")
                    
                    if not cell_marked:
                        print(f"Failed to mark cell {col}{row} (likely rate limited)")
                        # Add to pending formats to ensure it gets marked eventually
                        pending_formats.append({
                            'sheet': sheet,
                            'row': row,
                            'col': col,
                            'type': 'blue' if is_working else 'red',
                            'url': url,
                            'retry_count': 0,
                            'format_key': f"{col}{row}:{'blue' if is_working else 'red'}"
                        })
                else:
                    print(f"Not marking cell yet since this is not the last URL in cell {col}{row}")
            except Exception as mark_err:
                print(f"❌ Failed to mark cell {col}{row}: {str(mark_err)}")
                # Even if marking fails, still add to pending formats if this is the last URL
                if is_last_url:
                    pending_formats.append({
                        'sheet': sheet,
                        'row': row,
                        'col': col,
                        'type': 'blue' if is_working else 'red',
                        'url': url,
                        'retry_count': 0,
                        'format_key': f"{col}{row}:{'blue' if is_working else 'red'}"
                    })

    # Final safety check - ensure the cell was marked one way or the other but ONLY for the final URL
    if is_last_url and not cell_marked:
        try:
            print(f"⚠️ Cell {col}{row} was not marked during processing - added to pending formats queue with high priority")
            # Default to blue if we're ABSOLUTELY SURE the URL is working
            cell_type = 'blue' if is_working else 'red'
            # Add with higher priority to ensure it gets processed soon
            pending_formats.append({
                'sheet': sheet,
                'row': row,
                'col': col,
                'type': cell_type,
                'url': url,
                'retry_count': 0,
                'format_key': f"{col}{row}:{cell_type + ' (#0000EE)' if cell_type == 'blue' else cell_type}"
            })
            
        except Exception as e:
            print(f"❌ Failed to add cell {col}{row} to pending formats: {str(e)}")
            
    return is_working

def column_to_index(column_name):
    """Convert column name (A, B, C, ..., AA, AB, etc.) to 0-based index"""
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
    global pending_formats, successfully_formatted_cells, failed_formatted_cells
    
    # ADDED: Clear the successful formatting tracking to force reformatting of all cells
    successfully_formatted_cells = set()
    failed_formatted_cells = set()
    
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
            
            # Track URLs already processed per cell to handle multiple URLs in a cell
            processed_cell_urls = {}
            
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
                            # Get the cell identifier for tracking
                            col_name = index_to_column(col_idx)
                            cell_id = f"{col_name}{row_idx + 1}"
                            
                            # Extract all URLs from the cell
                            urls = extract_urls_from_text(cell_content)
                            
                            # Initialize tracking for this cell if needed
                            if cell_id not in processed_cell_urls:
                                processed_cell_urls[cell_id] = []
                            
                            if urls:
                                # Process the URLs in reverse order (IMPORTANT: to prioritize the last URL)
                                # This helps when multiple URLs are in a cell - we want the most recent/updated one
                                for url in reversed(urls):
                                    # Skip if we've already processed this exact URL for this cell
                                    if url in processed_cell_urls[cell_id]:
                                        print(f"Skipping duplicate URL {url} in cell {cell_id}")
                                        continue
                                        
                                    # Add to the list of URLs to check
                                    urls_to_check.append({
                                        'url': url,
                                        'row': row_idx + 1,  # +1 because we're 0-indexed but sheets are 1-indexed
                                        'col': col_name,
                                        'original_content': cell_content,
                                        'is_last_url': (url == urls[-1])  # Flag if this is the last URL in the cell
                                    })
                                    
                                    # Track that we've processed this URL for this cell
                                    processed_cell_urls[cell_id].append(url)
                            else:
                                # If no valid URLs found but cell has content, mark it for checking anyway
                                possible_url = cell_content
                                if not possible_url.startswith(('http://', 'https://')):
                                    possible_url = 'http://' + possible_url
                                
                                # Skip if we've already processed this exact URL for this cell
                                if possible_url in processed_cell_urls[cell_id]:
                                    print(f"Skipping duplicate URL {possible_url} in cell {cell_id}")
                                    continue
                                    
                                urls_to_check.append({
                                    'url': possible_url,
                                    'row': row_idx + 1,
                                    'col': col_name,
                                    'original_content': cell_content,
                                    'is_potential_url': True,
                                    'is_last_url': True  # This is the only URL for this cell
                                })
                                
                                # Track that we've processed this URL for this cell
                                processed_cell_urls[cell_id].append(possible_url)
                        except Exception as e:
                            # If URL extraction fails, still try to check it
                            print(f"❌ Error extracting URLs from cell {index_to_column(col_idx)}{row_idx+1}: {str(e)}")
                            try:
                                # Try to make a checkable URL from the content
                                possible_url = cell_content
                                if not possible_url.startswith(('http://', 'https://')):
                                    possible_url = 'http://' + possible_url
                                
                                # Get the cell identifier for tracking
                                col_name = index_to_column(col_idx)
                                cell_id = f"{col_name}{row_idx + 1}"
                                
                                # Initialize tracking for this cell if needed
                                if cell_id not in processed_cell_urls:
                                    processed_cell_urls[cell_id] = []
                                
                                # Skip if we've already processed this exact URL for this cell    
                                if possible_url in processed_cell_urls[cell_id]:
                                    print(f"Skipping duplicate URL {possible_url} in cell {cell_id}")
                                    continue
                                
                                urls_to_check.append({
                                    'url': possible_url,
                                    'row': row_idx + 1,
                                    'col': col_name,
                                    'original_content': cell_content,
                                    'is_potential_url': True,
                                    'is_last_url': True  # This is the only URL for this cell
                                })
                                
                                # Track that we've processed this URL for this cell
                                processed_cell_urls[cell_id].append(possible_url)
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
                    is_last_url = url_data.get('is_last_url', False)  # Get the flag that indicates if this is the last URL in the cell
                    
                    overall_index = i + idx + 1
                    print(f"Checking URL {overall_index}/{len(urls_to_check)} [{total_cells_processed + 1}]: {url} in cell {col}{row}")
                    
                    try:
                        # Pass is_last_url parameter to check_url
                        await check_url(driver, url, sheet, row, col, is_last_url=is_last_url)
                        total_cells_processed += 1
                        
                        # Add a small pause between individual URL checks to reduce system strain
                        if INTER_URL_PAUSE > 0:
                            await asyncio.sleep(INTER_URL_PAUSE)
                            
                    except Exception as e:
                        print(f"❌ Error checking URL {url}: {str(e)}")
                        traceback.print_exc()
                        try:
                            # Only mark cell red if this is the last URL in the cell
                            if is_last_url:
                                mark_cell_text_red(sheet, row, col)
                            else:
                                print(f"Not marking cell red yet since this is not the last URL in cell {col}{row}")
                        except Exception as mark_err:
                            print(f"Error marking cell: {str(mark_err)}")
                            # Add to pending formats with high priority but only if this is the last URL
                            if is_last_url:
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
                        
                        # Add a small pause after errors to let the system recover
                        await asyncio.sleep(INTER_URL_PAUSE * 2)
                
                # Process any pending cell formats between batches
                if pending_formats:
                    print(f"Processing {len(pending_formats)} pending cell formats between batches...")
                    await process_pending_formats()
                
                # Give Google's API a break between batches - use the new BATCH_COMPLETION_PAUSE constant
                print(f"Completed batch {batch_count}. Pausing for {BATCH_COMPLETION_PAUSE} seconds before next batch...")
                await asyncio.sleep(BATCH_COMPLETION_PAUSE)
            
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