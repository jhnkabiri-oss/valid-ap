import sys
import re
import logging
import threading
import time
from functools import partial
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QPushButton, QLabel, QFrame, 
                               QTextEdit, QListWidget, QListWidgetItem, QProgressBar, QSpinBox,
                               QMessageBox, QFileDialog)
from PySide6.QtCore import Qt, QThread, Signal, QTimer
from PySide6.QtGui import QFont, QColor

# Import backends with auto-reload
import importlib
try:
    import ap
    importlib.reload(ap)  # Auto-reload to get latest changes
    from ap import AfterPayBatchProcessor, AfterPayBatchValidator
    AP_AVAILABLE = True
    print("✅ ap.py auto-reloaded successfully")
except Exception as e:
    AP_AVAILABLE = False
    print(f"❌ ap.py import failed: {e}")

try:
    import conservative_batch
    importlib.reload(conservative_batch)  # Auto-reload 
    from conservative_batch import PeopleDataLabsLookup, extract_person_info, format_output, append_to_lookup_file
    LOOKUP_AVAILABLE = True
    print("✅ conservative_batch.py auto-reloaded successfully")
except Exception as e:
    LOOKUP_AVAILABLE = False
    print(f"❌ conservative_batch.py import failed: {e}")

# --- Custom Styles ---
STYLE_MAIN_BG = "background-color: #191b21;"  # Background Navy Gelap
BTN_GREEN = "background-color: #50c878; color: black; font-weight: bold; border-radius: 6px; padding: 8px;"
BTN_RED = "background-color: #d9534f; color: white; font-weight: bold; border-radius: 6px; padding: 8px;"
BTN_GREY = "background-color: #e0e0e0; color: black; font-weight: bold; border-radius: 6px; padding: 8px;"
BTN_YELLOW = "background-color: #f0c040; color: black; font-weight: bold; border-radius: 6px; padding: 8px;"
BTN_TAB_ACTIVE = "background-color: #e0e0e0; color: black; font-weight: bold; border-radius: 6px; padding: 8px;"
BTN_TAB_INACTIVE = "background-color: #3a3d46; color: white; font-weight: bold; border-radius: 6px; padding: 8px;"

class StyledButton(QPushButton):
    def __init__(self, text, style_sheet, width=None):
        super().__init__(text)
        self.setStyleSheet(style_sheet)
        self.setCursor(Qt.PointingHandCursor)
        if width:
            self.setFixedWidth(width)
        self.setFixedHeight(38)


class GuiLogHandler(logging.Handler):
    """Logging handler that emits messages to a PySide signal"""
    def __init__(self, signal=None):
        super().__init__()
        self.signal = signal

    def emit(self, record):
        try:
            message = self.format(record)
            if self.signal:
                self.signal.emit(message)
        except Exception:
            pass


class ValidationThread(QThread):
    progress = Signal(int, int, int)  # processed, valid, invalid
    log = Signal(str)
    email_processed = Signal(str, bool)
    email_processing_started = Signal(str, int)  # email, browser_id
    browser_ready = Signal(int, int)  # browser_id, pid
    restart_event = Signal(int, int, str, object)  # browser_id, attempt, reason, old_pid
    summary = Signal(object)
    finished = Signal()

    def __init__(self, emails, num_browsers=1, headless=False):
        super().__init__()
        self.emails = emails
        self.num_browsers = num_browsers
        self.headless = headless
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()

        self._processed = 0
        self._valid = 0
        self._invalid = 0
        self._processing = 0

    def run(self):
        # Attach our logging handler to 'ap' logger if available
        ap_logger = logging.getLogger('ap')
        handler = GuiLogHandler(self.log)
        ap_logger.setLevel(logging.INFO)
        ap_logger.addHandler(handler)

        try:
            # Progress callback called by AfterPayBatchProcessor after each email
            def progress_callback(result):
                self._processed += 1
                if result.get('valid'):
                    self._valid += 1
                else:
                    self._invalid += 1
                self.email_processed.emit(result.get('email'), result.get('valid'))
                self.progress.emit(self._processed, self._valid, self._invalid)

            # processing callback called when an email is dequeued and processing starts
            def processing_callback(email, browser_id):
                self._processing += 1
                self.email_processing_started.emit(email, browser_id)

            processor = AfterPayBatchProcessor(num_browsers=self.num_browsers, headless=self.headless, progress_callback=progress_callback, summary_callback=lambda s: self.summary.emit(s), stop_event=self._stop_event)
            # Provide a restart callback so GUI can log and display browser restarts
            try:
                def _restart_cb(browser_id, attempt, reason, old_pid=None):
                    try:
                        # Emit the signal for the GUI to handle
                        self.restart_event.emit(browser_id, attempt, reason, old_pid)
                        # Also log
                        self.log.emit(f"🔁 Browser {browser_id} restarted (attempt {attempt}) reason: {reason} old_pid:{old_pid}")
                    except Exception:
                        pass
                processor.restart_callback = _restart_cb
            except Exception:
                pass
            # Attach processing callback
            try:
                processor.processing_callback = processing_callback
            except Exception:
                pass
            # Keep a reference to the processor so we can pause/resume from outside
            self._processor = processor
            def _ready_cb(browser_id, pid=None):
                try:
                    self.browser_ready.emit(browser_id, pid if pid else -1)
                    self.log.emit(f"✅ Browser {browser_id} ready (pid: {pid})")
                except Exception:
                    pass
            processor.ready_callback = _ready_cb
            processor.process_emails(self.emails)
        except Exception as e:
            self.log.emit(f"❌ Validation thread error: {e}")
        finally:
            ap_logger.removeHandler(handler)
            self.finished.emit()

    def stop(self):
        self._stop_event.set()

    def pause(self):
        # Pause the underlying processor threads
        try:
            self._pause_event.set()
            if hasattr(self, '_processor') and self._processor:
                self._processor.pause()
            self.log.emit("⏸️ Validation paused")
        except Exception:
            pass

    def resume(self):
        try:
            self._pause_event.clear()
            if hasattr(self, '_processor') and self._processor:
                self._processor.resume()
            self.log.emit("▶️ Validation resumed")
        except Exception:
            pass


class LookupThread(QThread):
    log = Signal(str)
    lookup_result = Signal(str, str)  # email, formatted_result
    finished = Signal()

    def __init__(self, emails, api_key=None):
        super().__init__()
        self.emails = emails
        self.api_key = api_key
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()

    def run(self):
        try:
            api_key = self.api_key
            if not api_key:
                api_key = "af4f887ea97581b4bd22d61bc2be713116e27753f44897d107e2b02d43297601"
            lookup = PeopleDataLabsLookup(api_key)

            for i, email in enumerate(self.emails, 1):
                if self._stop_event.is_set():
                    break
                # Pause handling: block while paused
                while self._pause_event.is_set() and not self._stop_event.is_set():
                    time.sleep(0.2)
                self.log.emit(f"🔍 Looking up {i}/{len(self.emails)}: {email}")
                res = lookup.search_by_email(email, show_rate_info=(i==1))
                if res == "RATE_LIMITED":
                    self.log.emit(f"⚠️ Rate limited for {email}")
                    continue
                if res:
                    person_info = extract_person_info(res)
                    formatted = format_output(person_info, email)
                    # Save lookup results into lookup.txt (do not mix with valid.txt)
                    append_to_lookup_file(formatted)
                    self.lookup_result.emit(email, formatted)
                    self.log.emit(f"✅ Lookup done for {email}")
                else:
                    formatted = format_output(None, email)
                    # Save lookup results into lookup.txt even if no data
                    append_to_lookup_file(formatted)
                    self.lookup_result.emit(email, formatted)
                    self.log.emit(f"⚠️ No data for {email}")
                # conservative wait
                if i < len(self.emails):
                    self.log.emit("⏳ Waiting 10 seconds for rate limit...")
                    time.sleep(10)
        except Exception as e:
            self.log.emit(f"❌ Lookup thread error: {e}")
        finally:
            self.finished.emit()

    def stop(self):
        self._stop_event.set()

    def pause(self):
        self._pause_event.set()

    def resume(self):
        self._pause_event.clear()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Email Validator Pro V4")
        self.resize(1280, 850)
        self.setStyleSheet(STYLE_MAIN_BG)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(25, 25, 25, 25)
        main_layout.setSpacing(25)

        # ===========================
        #       LEFT PANEL
        # ===========================
        left_layout = QVBoxLayout()
        
        # Top Buttons
        top_left_btns = QHBoxLayout()
        top_left_btns.setSpacing(10)
        self.btn_load_email = StyledButton("Load Email", BTN_GREEN)
        self.btn_save_list = StyledButton("Save List", BTN_GREEN)
        self.btn_clear_email = StyledButton("Clear Email", BTN_RED)
        top_left_btns.addWidget(self.btn_load_email)
        top_left_btns.addWidget(self.btn_save_list)
        top_left_btns.addWidget(self.btn_clear_email)
        
        # List Container
        self.list_frame = QFrame()
        self.list_frame.setStyleSheet("""
            QFrame {
                background-color: #23262f;
                border: 1px solid #555;
                border-radius: 12px;
            }
        """)
        self.list_frame.setFixedWidth(320)
        
        list_layout = QVBoxLayout(self.list_frame)
        
        self.email_list = QListWidget()
        self.email_list.setStyleSheet("""
            QListWidget {
                background-color: transparent;
                color: white;
                font-size: 14px;
                border: none;
                outline: none;
            }
            QListWidget::item { padding: 4px 0; }
            QListWidget::item:selected { background-color: #3a3d46; color: #50c878; }
        """)
        
        emails = [
            "missyharveyrn@yahoo.com", "juliaanguyen2011@Yahoo.com", "aprilclark333@yahoo.com",
            "burden.derrick@yahoo.com", "doryl_martin56@yahoo.com", "goldie2207@yahoo.com",
            "divababy200003@yahoo.com", "madisonrcathey@yahoo.com", "liannyortiztabares19@yahoo.com",
            "tsitiridisg0812@yahoo.com", "zachhayden10@yahoo.com", "dlyric82@yahoo.com",
            "johnswlj3@yahoo.com", "grneyz.4310@yahoo.com", "phil_allmoneyin@yahoo.com"
        ]
        self.email_list.addItems(emails)
        list_layout.addWidget(self.email_list)

        lbl_email_list = QLabel("Email List")
        lbl_email_list.setStyleSheet("color: white; font-weight: bold; margin-top: 5px;")
        
        left_col = QVBoxLayout()
        left_col.addLayout(top_left_btns)
        left_col.addSpacing(10)
        left_col.addWidget(self.list_frame)
        left_col.addWidget(lbl_email_list)
        
        main_layout.addLayout(left_col)

        # ===========================
        #       RIGHT PANEL
        # ===========================
        right_col = QVBoxLayout()
        
        # Top Tabs
        tabs_layout = QHBoxLayout()
        self.btn_logger = StyledButton("Logger", BTN_TAB_ACTIVE, width=110)
        self.btn_valid_email = StyledButton("Valid Email", BTN_TAB_INACTIVE, width=110)
        self.btn_invalid_email = StyledButton("Invalid Email", BTN_TAB_INACTIVE, width=120)
        self.btn_lookup_email = StyledButton("Lookup Email", BTN_YELLOW, width=130)
        tabs_layout.addWidget(self.btn_logger)
        tabs_layout.addWidget(self.btn_valid_email)
        tabs_layout.addWidget(self.btn_invalid_email)
        tabs_layout.addWidget(self.btn_lookup_email)
        tabs_layout.addStretch()
        
        right_col.addLayout(tabs_layout)

        # Restart counters status row
        self.restart_status_layout = QHBoxLayout()
        self.restart_status_layout.setSpacing(8)
        self.lbl_restart_title = QLabel("Restarts:")
        self.lbl_restart_title.setStyleSheet("color: white; font-weight: bold; margin-left: 10px;")
        self.restart_status_layout.addWidget(self.lbl_restart_title)
        self.restart_status_layout.addStretch()
        self.restart_labels = []
        self.browser_restart_counts = {}
        right_col.addLayout(self.restart_status_layout)

        # Content Console
        self.log_frame = QFrame()
        self.log_frame.setStyleSheet("""
            QFrame {
                background-color: #23262f;
                border: 1px solid #555;
                border-radius: 12px;
            }
        """)
        log_layout = QVBoxLayout(self.log_frame)
        
        # Logger console
        self.log_console = QTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setStyleSheet("border: none; background-color: transparent;")
        self.log_console.setFont(QFont("Consolas", 11))
        
        html_log = """
        <style>
            .d { color: #888; } .g { color: #50c878; font-weight:bold; } 
            .r { color: #ff5555; font-weight:bold; } .w { color: white; }
        </style>
        <div class='w' style='line-height: 1.4;'>
        <span class='d'>[INFO]</span> <span class='g'>✅ Undetected Chrome driver created successfully</span><br>
        <span class='w'>📊 Progress: 1 processed | <span class='g'>✅ 0 valid</span> | <span class='r'>❌ 1 invalid</span></span><br>
        <span class='d'>[INFO]</span> [Browser-1] test2@gmail.com → <span class='r'>❌ INVALID (1/2)</span><br>
        <span class='d'>[INFO]</span> 🚪 Browser closed<br>
        <span class='w'>📊 Progress: 2 processed | <span class='g'>✅ 0 valid</span> | <span class='r'>❌ 2 invalid</span></span><br>
        <span class='d'>[INFO]</span> [Browser-2] invalid@xyz.com → <span class='r'>❌ INVALID (2/2)</span><br>
        <span class='d'>[INFO]</span> 🏁 Fast batch validation complete<br>
        </div>
        """
        self.log_console.setHtml(html_log.strip())
        log_layout.addWidget(self.log_console)

        # Valid console (for valid emails) and Lookup console
        self.valid_console = QTextEdit()
        self.valid_console.setReadOnly(True)
        self.valid_console.setStyleSheet(self.log_console.styleSheet())
        self.valid_console.hide()

        self.lookup_console = QTextEdit()
        self.lookup_console.setReadOnly(True)
        self.lookup_console.setStyleSheet(self.log_console.styleSheet())
        self.lookup_console.hide()

        self.invalid_console = QTextEdit()
        self.invalid_console.setReadOnly(True)
        self.invalid_console.setStyleSheet(self.log_console.styleSheet())
        self.invalid_console.hide()

        log_layout.addWidget(self.valid_console)
        log_layout.addWidget(self.invalid_console)
        log_layout.addWidget(self.lookup_console)
        
        right_col.addWidget(self.log_frame)
        
        # --- MIDDLE CONTROLS (WINDOWS TAB & BUTTONS) ---
        controls_layout = QHBoxLayout()
        controls_layout.setContentsMargins(0, 10, 0, 10)
        
        # 1. WINDOWS TAB STEPPER (INI BAGIAN UTAMA YANG DIPERBAIKI)
        win_layout = QVBoxLayout()
        win_layout.setSpacing(5)
        
        lbl_win = QLabel("Windows Tab")
        lbl_win.setStyleSheet("color: white; font-weight: bold; font-size: 13px;")
        
        self.spin_windows = QSpinBox()
        self.spin_windows.setRange(1, 100)
        self.spin_windows.setValue(5)
        self.spin_windows.setFixedSize(80, 45)
        
        # QSS BLACK MAGIC UNTUK BIKIN STEPPER KAYAK GAMBAR
        self.spin_windows.setStyleSheet("""
            QSpinBox {
                background-color: #d0d0d0; 
                color: #111; 
                font-weight: 900; 
                font-size: 20px;
                border-radius: 6px;
                padding-left: 10px;
                border: 2px solid #d0d0d0;
            }
            /* Tombol Panah Atas */
            QSpinBox::up-button {
                subcontrol-origin: border;
                subcontrol-position: top right;
                width: 25px; 
                background-color: #c0c0c0;
                border-top-right-radius: 6px;
                border-left: 1px solid #999; /* Garis Pemisah */
                border-bottom: 1px solid #999;
            }
            /* Tombol Panah Bawah */
            QSpinBox::down-button {
                subcontrol-origin: border;
                subcontrol-position: bottom right;
                width: 25px;
                background-color: #c0c0c0;
                border-bottom-right-radius: 6px;
                border-left: 1px solid #999; /* Garis Pemisah */
            }
            QSpinBox::up-button:hover, QSpinBox::down-button:hover {
                background-color: #b0b0b0;
            }
            /* Panah Segitiga Hitam */
            QSpinBox::up-arrow {
                width: 0; height: 0;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-bottom: 6px solid black; /* Segitiga Hitam */
            }
            QSpinBox::down-arrow {
                width: 0; height: 0;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 6px solid black; /* Segitiga Hitam */
            }
        """)
        
        win_layout.addWidget(lbl_win)
        win_layout.addWidget(self.spin_windows)
        
        controls_layout.addLayout(win_layout)
        
        # 2. ACTION BUTTONS
        controls_layout.addSpacing(20)
        controls_layout.addStretch() # Push buttons to right
        
        # Biar tombol tingginya sama kayak input
        self.btn_copy_result = StyledButton("Copy Result", BTN_GREY, width=120)
        self.btn_clear_log = StyledButton("Clear Log", BTN_GREY, width=120)
        self.btn_download_result = StyledButton("Download Result", BTN_GREEN, width=150)
        controls_layout.addWidget(self.btn_copy_result)
        controls_layout.addWidget(self.btn_clear_log)
        controls_layout.addWidget(self.btn_download_result)
        
        right_col.addLayout(controls_layout)

        # PROGRESS BAR (Styled like grey pill)
        self.pbar = QProgressBar()
        self.pbar.setValue(10)
        self.pbar.setTextVisible(True)
        self.pbar.setFormat("Process 10%")
        self.pbar.setAlignment(Qt.AlignCenter)
        self.pbar.setFixedHeight(32)
        self.pbar.setStyleSheet("""
            QProgressBar {
                background-color: #d0d0d0;
                border-radius: 6px;
                color: black;
                font-weight: bold;
                font-size: 12px;
            }
            QProgressBar::chunk {
                background-color: #6fd08c; 
                border-radius: 6px;
            }
        """)
        right_col.addWidget(self.pbar)
        right_col.addSpacing(10)

        # BOTTOM CONTROLS
        bottom_layout = QHBoxLayout()
        
        self.btn_start_validate = StyledButton("Start Validate", BTN_GREEN, width=140)
        self.btn_pause_validate = StyledButton("Pause Validate", BTN_GREY, width=140)
        self.btn_stop_validate = StyledButton("Stop Validate", BTN_RED, width=140)
        self.btn_pause_validate.setEnabled(False)
        bottom_layout.addWidget(self.btn_start_validate)
        bottom_layout.addWidget(self.btn_pause_validate)
        bottom_layout.addWidget(self.btn_stop_validate)
        
        # Garis Pemisah Vertikal
        line = QFrame()
        line.setFrameShape(QFrame.VLine)
        line.setStyleSheet("background-color: #555;")
        line.setFixedHeight(30)
        
        bottom_layout.addSpacing(20)
        bottom_layout.addWidget(line)
        bottom_layout.addSpacing(20)
        
        self.btn_lookup_data = StyledButton("Lookup Data", BTN_YELLOW, width=140)
        self.btn_pause_lookup = StyledButton("Pause Lookup", BTN_GREY, width=120)
        self.btn_stop_lookup = StyledButton("Stop Lookup", BTN_RED, width=120)
        bottom_layout.addWidget(self.btn_lookup_data)
        bottom_layout.addWidget(self.btn_pause_lookup)
        bottom_layout.addWidget(self.btn_stop_lookup)
        bottom_layout.addStretch()
        
        right_col.addLayout(bottom_layout)

        main_layout.addLayout(right_col, stretch=1)

        # Connect button signals
        self.btn_load_email.clicked.connect(self.load_email_list)
        self.btn_save_list.clicked.connect(self.save_email_list)
        self.btn_clear_email.clicked.connect(self.clear_email_list)
        self.btn_copy_result.clicked.connect(self.copy_result)
        self.btn_clear_log.clicked.connect(self.clear_log)
        self.btn_download_result.clicked.connect(self.download_result)
        
        self.btn_logger.clicked.connect(lambda: self.switch_tab("Logger"))
        self.btn_valid_email.clicked.connect(lambda: self.switch_tab("Valid Email"))
        self.btn_invalid_email.clicked.connect(lambda: self.switch_tab("Invalid Email"))
        self.btn_lookup_email.clicked.connect(lambda: self.switch_tab("Lookup Email"))
        
        self.btn_start_validate.clicked.connect(self.start_validation)
        self.btn_stop_validate.clicked.connect(self.stop_validation)
        self.btn_pause_validate.clicked.connect(self.pause_resume_validation)
        self.btn_lookup_data.clicked.connect(self.start_lookup)
        self.btn_pause_lookup.clicked.connect(self.pause_resume_lookup)
        self.btn_stop_lookup.clicked.connect(self.stop_lookup)

        # Validation/Lookup state
        self.validation_thread = None
        self.lookup_thread = None
        self.valid_emails = []
        self.invalid_emails = []
        self.lookup_results = []
        self.current_tab = "Logger"
        self.browser_restart_counts = {}
        self.restart_labels = []

        # Connect thread signals
        # Load existing invalid results from file into invalid console
        try:
            if not self.invalid_console.toPlainText().strip():
                with open('invalid.txt', 'r', encoding='utf-8') as f:
                    for line in f:
                        ln = line.strip()
                        if ln:
                            self.invalid_console.append(ln)
                            if ln.startswith('❌ INVALID - '):
                                email = ln.split('❌ INVALID - ')[1].strip()
                                if email not in self.invalid_emails:
                                    self.invalid_emails.append(email)
        except Exception:
            pass

        # (Will be connected dynamically when threads are created)

    def get_list_widget_emails(self):
        """Return list of emails from the QListWidget"""
        emails = []
        for i in range(self.email_list.count()):
            item_text = self.email_list.item(i).text()
            if '. ' in item_text:
                email = item_text.split('. ', 1)[1]
            else:
                email = item_text
            item = self.email_list.item(i)
            # Only include emails which are still enabled (not processed)
            if item.flags() & Qt.ItemIsEnabled:
                emails.append(email)
        return emails

    def start_validation(self):
        """Start validation using ap.py (AfterPayBatchProcessor)"""
        if not AP_AVAILABLE:
            QMessageBox.critical(self, "Error", "ap.py not available or failed to import")
            return

        emails = self.get_list_widget_emails()
        if not emails:
            QMessageBox.warning(self, "Warning", "Please load an email list first")
            return

        num_browsers = self.spin_windows.value()
        self.btn_start_validate.setEnabled(False)
        self.btn_stop_validate.setEnabled(True)
        self.btn_lookup_data.setEnabled(False)

        # Set progress bar to the original total emails count and reset value
        self.pbar.setMaximum(len(emails))
        self.pbar.setValue(0)

        self.validation_thread = ValidationThread(emails, num_browsers=num_browsers, headless=False)
        self.validation_thread.progress.connect(self.on_progress_updated)
        self.validation_thread.log.connect(self.log_message)
        self.validation_thread.email_processed.connect(self.on_email_processed)
        self.validation_thread.email_processing_started.connect(self.on_email_processing_started)
        self.validation_thread.browser_ready.connect(self.on_browser_ready)
        self.validation_thread.summary.connect(self.on_validation_summary)
        self.validation_thread.finished.connect(self.on_validation_finished)
        self.validation_thread.restart_event.connect(self.on_browser_restart)
        self.validation_thread.start()
        self.log_message(f"🚀 Starting validation with {num_browsers} browsers on {len(emails)} emails...")
        # Enable pause button
        self.btn_pause_validate.setEnabled(True)
        self.btn_pause_validate.setText("Pause Validate")
        # Initialize restart counters for N browsers
        try:
            self.browser_restart_counts = {i+1: 0 for i in range(num_browsers)}
            # Clear any existing labels
            for lbl in self.restart_labels:
                try:
                    self.restart_status_layout.removeWidget(lbl)
                    lbl.deleteLater()
                except Exception:
                    pass
            self.restart_labels = []
            # Create labels for each browser
            for i in range(num_browsers):
                lbl = QLabel(f"B{i+1}: 0")
                lbl.setStyleSheet("color: white; font-size: 11px; background-color: #333; padding: 4px; border-radius: 4px;")
                self.restart_labels.append(lbl)
                # insert before the stretch at index 1 of the layout: [title, stretch]; we place before stretch
                # find count of widgets in layout and insert near end before stretch
                self.restart_status_layout.addWidget(lbl)
            self.restart_status_layout.addStretch()
        except Exception:
            pass

    def stop_validation(self):
        """Request stop for validation"""
        if self.validation_thread:
            self.validation_thread.stop()
            self.log_message("⏹️ Stopping validation...")
            # Reset pause button state
            self.btn_pause_validate.setText("Pause Validate")
            self.btn_pause_validate.setEnabled(False)

    def pause_resume_validation(self):
        """Toggle pause/resume for validation"""
        if not self.validation_thread:
            self.log_message("⚠️ No validation thread running")
            return
        try:
            # Toggle pause/resume on ValidationThread which proxies to the processor
            if hasattr(self.validation_thread, '_pause_event') and not self.validation_thread._pause_event.is_set():
                self.validation_thread.pause()
                self.btn_pause_validate.setText("Resume Validate")
                self.log_message("⏸️ Validation paused")
            else:
                self.validation_thread.resume()
                self.btn_pause_validate.setText("Pause Validate")
                self.log_message("▶️ Validation resumed")
        except Exception as e:
            self.log_message(f"❌ Error toggling pause: {e}")

    def on_progress_updated(self, processed, valid_count, invalid_count):
        # Keep the total as the initial maximum set when validation started
        total = self.pbar.maximum()
        self.pbar.setMaximum(total if total > 0 else 1)
        self.pbar.setValue(processed)
        percentage = int((processed / total) * 100) if total > 0 else 0
        self.pbar.setFormat(f"Process {percentage}%")
        self.log_message(f"📊 Processed {processed}/{total} | ✅ {valid_count} | ❌ {invalid_count}")
        percentage = int((processed / total) * 100) if total > 0 else 0
        self.pbar.setFormat(f"Process {percentage}%")
        self.log_message(f"📊 Processed {processed}/{total} | ✅ {valid_count} | ❌ {invalid_count}")

    def on_email_processed(self, email, is_valid):
        # Always mark item as processed (grey/disabled)
        self.mark_list_item_processed(email, is_valid)

        if is_valid:
            self.valid_console.append(f"✅ VALID - {email}")
            if email not in self.valid_emails:
                self.valid_emails.append(email)
        else:
            self.log_message(f"❌ INVALID - {email}")
            if email not in self.invalid_emails:
                self.invalid_emails.append(email)
            try:
                self.invalid_console.append(f"❌ INVALID - {email}")
            except Exception:
                pass
        # Mark the item in the list as processed (disabled and greyed).
        try:
            self.mark_list_item_processed(email, is_valid)
        except Exception:
            pass

    def on_email_processing_started(self, email, browser_id):
        """Handle when GUI worker starts processing an email - remove from the list in real-time
        and log which browser started the job."""
        try:
            self.log_message(f"▶️ Browser {browser_id} starting: {email}")
            # Remove the item from the list when it starts processing; this reduces reprocessing
            for i in range(self.email_list.count() - 1, -1, -1):
                item = self.email_list.item(i)
                text = item.text()
                eid = text.split('. ', 1)[1] if '. ' in text else text
                if eid.strip().lower() == email.strip().lower():
                    # Remove item from list
                    self.email_list.takeItem(i)
                    # Keep progress bar maximum as-is (original total), don't reduce it
                    break
        except Exception as e:
            self.log_message(f"⚠️ Could not remove email {email} from list: {e}")

    def on_browser_restart(self, browser_id, attempt, reason, old_pid):
        """Update GUI when a browser was restarted via detection or other events."""
        try:
            # Increment counter
            cnt = self.browser_restart_counts.get(browser_id, 0) + 1
            self.browser_restart_counts[browser_id] = cnt
            # If label exists, update, else create it
            try:
                lbl = None
                if len(self.restart_labels) >= browser_id:
                    lbl = self.restart_labels[browser_id-1]
                else:
                    # create new label for this browser
                    lbl = QLabel(f"B{browser_id}: {cnt}")
                    lbl.setStyleSheet("color: white; font-size: 11px; background-color: #333; padding: 4px; border-radius: 4px;")
                    self.restart_labels.append(lbl)
                    self.restart_status_layout.addWidget(lbl)
                # Update label text
                if lbl:
                    lbl.setText(f"B{browser_id}: {cnt}")
            except Exception:
                pass
            # Log it in the GUI
            self.log_message(f"🔁 Browser {browser_id} restarted (attempt {attempt}) reason: {reason} old_pid:{old_pid}")
        except Exception:
            pass

    def on_validation_finished(self):
        self.log_message("✅ Validation complete")
        self.btn_start_validate.setEnabled(True)
        self.btn_stop_validate.setEnabled(False)
        self.btn_lookup_data.setEnabled(True)
        self.validation_thread = None
        # Reset pause button
        self.btn_pause_validate.setText("Pause Validate")
        self.btn_pause_validate.setEnabled(False)
        # Keep counters but you can reset them if desired — here we reset labels
        try:
            for lbl in self.restart_labels:
                try:
                    self.restart_status_layout.removeWidget(lbl)
                    lbl.deleteLater()
                except Exception:
                    pass
            self.restart_labels = []
            self.browser_restart_counts = {}
        except Exception:
            pass

    def on_browser_ready(self, browser_id, pid):
        """Handle GUI update when a browser is confirmed ready"""
        try:
            # If label exists, update text else create it
            lbl = None
            if len(self.restart_labels) >= browser_id:
                lbl = self.restart_labels[browser_id-1]
            else:
                lbl = QLabel(f"B{browser_id}: 0")
                lbl.setStyleSheet("color: white; font-size: 11px; background-color: #333; padding: 4px; border-radius: 4px;")
                self.restart_labels.append(lbl)
                self.restart_status_layout.addWidget(lbl)
            if lbl:
                # Display PID if provided
                if pid and pid > 0:
                    lbl.setText(f"B{browser_id}: {self.browser_restart_counts.get(browser_id, 0)} PID:{pid}")
                else:
                    lbl.setText(f"B{browser_id}: {self.browser_restart_counts.get(browser_id, 0)}")
            self.log_message(f"✅ Browser {browser_id} ready (pid:{pid})")
        except Exception:
            pass

    def get_valid_emails_for_lookup(self):
        """Return a deduplicated list of valid emails for lookup.
        Sources used (in priority):
          - explicit "✅ VALID - <email>" lines in `valid_console`
          - any email-like patterns found in `valid_console` (covers blocks)
          - emails found in `valid.txt` (file)
        This returns a stable order based on discovery (console -> file) with duplicates removed.
        """
        email_rx = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        seen = set()
        out = []

        # 1) Extract explicit console lines like "✅ VALID - email@domain"
        try:
            console_text = self.valid_console.toPlainText() or ""
            count_console_explicit = 0
            for line in console_text.splitlines():
                line = line.strip()
                if line.startswith('✅ VALID - '):
                    email = line.split('✅ VALID - ', 1)[1].strip()
                    if email and email not in seen:
                        seen.add(email)
                        out.append(email)
                        count_console_explicit += 1

            # 2) Scan the console for any other email-like patterns (e.g., blocks / appended text)
            for m in email_rx.findall(console_text):
                if m not in seen:
                    seen.add(m)
                    out.append(m)
        except Exception:
            count_console_explicit = 0

        # 3) Parse valid.txt and append any emails that are not already included
        file_count_total = 0
        try:
            with open('valid.txt', 'r', encoding='utf-8') as f:
                content = f.read()
                for m in email_rx.findall(content):
                    file_count_total += 1
                    if m not in seen:
                        seen.add(m)
                        out.append(m)
        except FileNotFoundError:
            file_count_total = 0
        except Exception:
            file_count_total = 0

        self.log_message(f"ℹ️ Lookup emails: console_explicit={count_console_explicit}, file_total_matches={file_count_total}, unique_total={len(out)}")
        return out

    def start_lookup(self):
        """Start lookup using conservative_batch.py for valid emails"""
        if not LOOKUP_AVAILABLE:
            QMessageBox.critical(self, "Error", "conservative_batch.py not available or failed to import")
            return

        # Gather valid emails from the valid console AND valid file (union), prefer unique addresses
        emails = self.get_valid_emails_for_lookup()

        if not emails:
            QMessageBox.information(self, "Info", "No valid emails found for lookup")
            return

        # Clear lookup.txt file before starting a fresh lookup session
        try:
            with open('lookup.txt', 'w', encoding='utf-8') as f:
                f.write("")
            self.log_message("✅ Cleared lookup.txt file")
        except Exception as e:
            self.log_message(f"❌ Error clearing lookup.txt: {e}")

        self.btn_lookup_data.setEnabled(False)
        self.btn_pause_lookup.setEnabled(True)
        self.btn_pause_lookup.setText("Pause Lookup")
        self.btn_stop_lookup.setEnabled(True)
        self.lookup_thread = LookupThread(emails)
        self.lookup_thread.log.connect(self.log_message)
        self.lookup_thread.lookup_result.connect(self.on_lookup_result)
        self.lookup_thread.finished.connect(self.on_lookup_finished)
        self.lookup_thread.start()
        self.log_message(f"🔍 Starting lookup for {len(emails)} emails...")
        self.log_message("💾 Lookup results will be saved to lookup.txt")

    def pause_resume_lookup(self):
        """Toggle pause/resume for lookup thread"""
        if not self.lookup_thread:
            self.log_message("⚠️ No lookup thread running")
            return
        try:
            if hasattr(self.lookup_thread, '_pause_event') and not self.lookup_thread._pause_event.is_set():
                self.lookup_thread.pause()
                self.btn_pause_lookup.setText("Resume Lookup")
                self.log_message("⏸️ Lookup paused")
            else:
                self.lookup_thread.resume()
                self.btn_pause_lookup.setText("Pause Lookup")
                self.log_message("▶️ Lookup resumed")
        except Exception as e:
            self.log_message(f"❌ Error toggling pause: {e}")

    def stop_lookup(self):
        """Stop the lookup thread"""
        if not self.lookup_thread:
            self.log_message("⚠️ No lookup thread running")
            return
        self.lookup_thread.stop()
        self.btn_lookup_data.setEnabled(True)
        self.btn_pause_lookup.setEnabled(False)
        self.btn_pause_lookup.setText("Pause Lookup")
        self.btn_stop_lookup.setEnabled(False)
        self.log_message("⏹️ Stopping lookup...")

    def on_lookup_result(self, email, formatted_result):
        # Wrap result with separator and append a blank line after it
        sep = "=" * 50
        block = f"{sep}\n{formatted_result}\n{sep}\n\n"
        self.lookup_console.append(block)
        self.lookup_results.append(block)
        # Also add to valid_console so user sees results on Valid tab
        try:
            self.valid_console.append(block)
        except Exception:
            pass

    def on_lookup_finished(self):
        self.log_message("✅ Lookup complete")
        self.btn_lookup_data.setEnabled(True)
        self.lookup_thread = None

    def on_validation_summary(self, summary: dict):
        """Handle validation summary and append to Logger in a block format"""
        try:
            sep = '=' * 60
            lines = [sep, '📋 BATCH VALIDATION RESULTS', sep]
            lines.append(f"⏰ Total time: {summary.get('total_time', 0):.1f} seconds")
            lines.append(f"📊 Total emails: {summary.get('total_emails', 0)}")
            lines.append(f"✅ Valid emails: {summary.get('valid_count', 0)}")
            lines.append(f"❌ Invalid emails: {summary.get('invalid_count', 0)}")
            lines.append(f"⚡ Speed: {summary.get('emails_per_minute', 0):.1f} emails/minute")
            lines.append("💾 Results saved to: valid.txt & invalid.txt")
            lines.append(sep)
            block = '\n'.join(lines)
            self.log_message(block)
        except Exception as e:
            self.log_message(f"❌ Error formatting summary: {e}")

    def copy_result(self):
        content = ''
        if self.current_tab == "Logger":
            content = self.log_console.toPlainText()
        elif self.current_tab == "Valid Email":
            content = self.valid_console.toPlainText()
        elif self.current_tab == "Invalid Email":
            content = self.invalid_console.toPlainText()
        elif self.current_tab == "Lookup Email":
            content = self.lookup_console.toPlainText()
        if content:
            QApplication.clipboard().setText(content)
            self.log_message("📋 Content copied to clipboard")
        else:
            self.log_message("❌ No content to copy")

    def clear_log(self):
        if self.current_tab == "Logger":
            self.log_console.clear()
        elif self.current_tab == "Valid Email":
            self.valid_console.clear()
            self.valid_emails = []
        elif self.current_tab == "Invalid Email":
            self.invalid_console.clear()
            self.invalid_emails = []
        elif self.current_tab == "Lookup Email":
            self.lookup_console.clear()
            self.lookup_results = []

    def download_result(self):
        content = ''
        if self.current_tab == "Logger":
            content = self.log_console.toPlainText()
        elif self.current_tab == "Valid Email":
            content = self.valid_console.toPlainText()
        elif self.current_tab == "Invalid Email":
            content = self.invalid_console.toPlainText()
        elif self.current_tab == "Lookup Email":
            content = self.lookup_console.toPlainText()
        if not content:
            QMessageBox.information(self, "Info", "No content to download")
            return
        file_path, _ = QFileDialog.getSaveFileName(self, "Download Results", "results.txt", "Text Files (*.txt);;All Files (*)")
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.log_message(f"💾 Results saved to {file_path}")
            except Exception as e:
                self.log_message(f"❌ Error saving file: {e}")

    def switch_tab(self, tab_name):
        self.current_tab = tab_name
        # Highlight the active tab
        self.btn_logger.setStyleSheet(BTN_TAB_ACTIVE if tab_name == "Logger" else BTN_TAB_INACTIVE)
        self.btn_valid_email.setStyleSheet(BTN_TAB_ACTIVE if tab_name == "Valid Email" else BTN_TAB_INACTIVE)
        self.btn_invalid_email.setStyleSheet(BTN_TAB_ACTIVE if tab_name == "Invalid Email" else BTN_TAB_INACTIVE)
        self.btn_lookup_email.setStyleSheet(BTN_TAB_ACTIVE if tab_name == "Lookup Email" else BTN_YELLOW)

        # Show appropriate content
        self.log_console.setVisible(tab_name == "Logger")
        self.valid_console.setVisible(tab_name == "Valid Email")
        self.invalid_console.setVisible(tab_name == "Invalid Email")
        self.lookup_console.setVisible(tab_name == "Lookup Email")
        # If user opens Invalid tab and it's empty, attempt to load from invalid.txt
        if tab_name == "Invalid Email" and not self.invalid_console.toPlainText().strip():
            try:
                with open('invalid.txt', 'r', encoding='utf-8') as f:
                    for line in f:
                        ln = line.strip()
                        if ln:
                            self.invalid_console.append(ln)
                            if ln.startswith('❌ INVALID - '):
                                email = ln.split('❌ INVALID - ')[1].strip()
                                if email not in self.invalid_emails:
                                    self.invalid_emails.append(email)
            except Exception:
                pass

    def log_message(self, message):
        """Add message to log with timestamp"""
        try:
            timestamp = time.strftime("%H:%M:%S")
            self.log_console.append(f"[{timestamp}] {message}")
        except Exception:
            print(f"[LOG] {message}")

    def load_initial_emails(self):
        try:
            with open('list.txt', 'r', encoding='utf-8') as f:
                emails = [line.strip() for line in f if line.strip() and '@' in line.strip()]
            self.set_email_list(emails)
            self.log_message(f"📧 Loaded {len(emails)} emails from list.txt")
        except FileNotFoundError:
            self.log_message("📝 No list.txt found - please load an email list")
        except Exception as e:
            self.log_message(f"❌ Error loading list.txt: {e}")

    def set_email_list(self, emails):
        self.email_list.clear()
        for i, email in enumerate(emails, 1):
            item = QListWidgetItem(f"{i:02d}. {email}")
            item.setData(Qt.UserRole, email)
            self.email_list.addItem(item)
        if emails:
            self.pbar.setMaximum(len(emails))
            self.pbar.setValue(0)

    def load_email_list(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Load Email List", "", "Text Files (*.txt);;All Files (*)")
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    emails = [line.strip() for line in f if line.strip() and '@' in line.strip()]
                self.set_email_list(emails)
                self.log_message(f"📧 Loaded {len(emails)} emails from {file_path}")
            except Exception as e:
                self.log_message(f"❌ Error loading file: {e}")

    def save_email_list(self):
        if self.email_list.count() == 0:
            QMessageBox.information(self, "Info", "No emails to save")
            return
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Email List", "list.txt", "Text Files (*.txt);;All Files (*)")
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    for i in range(self.email_list.count()):
                        item_text = self.email_list.item(i).text()
                        email = item_text.split('. ', 1)[1] if '. ' in item_text else item_text
                        f.write(email + '\n')
                self.log_message(f"💾 Saved {self.email_list.count()} emails to {file_path}")
            except Exception as e:
                self.log_message(f"❌ Error saving file: {e}")

    def clear_email_list(self):
        self.email_list.clear()
        self.pbar.setValue(0)
        self.pbar.setMaximum(1)
        self.log_message("🗑️ Email list cleared")

    def mark_list_item_processed(self, email, is_valid):
        """Mark a QListWidgetItem as disabled+greyed when processed.
        Marks the first enabled matching item.
        """
        for i in range(self.email_list.count()):
            item = self.email_list.item(i)
            text = item.text()
            if '. ' in text:
                item_email = text.split('. ', 1)[1]
            else:
                item_email = text
            if item_email.strip().lower() == email.strip().lower() and (item.flags() & Qt.ItemIsEnabled):
                # Disable item
                item.setFlags(item.flags() & ~Qt.ItemIsEnabled)
                # Set grey text color
                item.setForeground(QColor('#888888'))
                # Append status symbol
                stat = ' ✅' if is_valid else ' ❌'
                item.setText(text + stat)
                break



if __name__ == '__main__':
    # Prevent creating multiple QApplication or MainWindow instances if module is reloaded
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)

    # If a MainWindow already exists among top-level widgets, restore and focus it
    existing_main = None
    for w in app.topLevelWidgets():
        # Use class name check to avoid importing main.MainWindow again
        try:
            if w.__class__.__name__ == 'MainWindow':
                existing_main = w
                break
        except Exception:
            pass

    if existing_main is not None:
        window = existing_main
        try:
            print("GUI already running — reusing existing MainWindow")
            window.raise_()
            window.activateWindow()
        except Exception:
            pass
    else:
        window = MainWindow()
    window.show()
    sys.exit(app.exec())