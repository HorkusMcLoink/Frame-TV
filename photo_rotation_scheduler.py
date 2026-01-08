import os
import random
import json
import time
import threading
import signal
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta
from pathlib import Path
import logging
import heapq

try:
    from PIL import Image, ImageTk
    EXIF_AVAILABLE = True
    PIL_AVAILABLE = True
except ImportError:
    EXIF_AVAILABLE = False
    PIL_AVAILABLE = False

# File type and path constants
PHOTO_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')
LOG_FILE = "viewed_photos.json"
CACHE_FILE = "photo_metadata.json"

# GUI Configuration
BG_COLOR = "#c1b1f2"

# Limits and thresholds
MAX_PHOTO_COUNT = 10000
MIN_PHOTO_COUNT = 1
MAX_SWITCHES_PER_DAY = 100
CACHE_SAVE_INTERVAL = 5000  # Save cache every N photos scanned
THREAD_JOIN_TIMEOUT = 2.0


class TextHandler(logging.Handler):
    """Custom logging handler that writes to a tkinter Text widget"""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        
    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.see(tk.END)
        self.text_widget.after(0, append)


class PhotoScheduler:
    def __init__(self):
        self.setup_logging()
        self.setup_gui()
        self.viewed_photos = self.load_viewed_photos()
        self.metadata_cache = self.load_metadata_cache()
        self.cache_dirty = False
        self.viewed_photos_lock = threading.Lock()
        self.operation_lock = threading.Lock()
        self.operation_cancelled = threading.Event()
        self.current_thread = None
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        if not EXIF_AVAILABLE:
            self.logger.info("EXIF disabled (PIL not available - install: pip install Pillow)")
        
        self.update_next_switch()
        self.periodic_update()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (OSError, ValueError):
            # Signal handling may not work in all contexts (e.g., non-main thread)
            pass
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.on_closing()
        
    def setup_logging(self):
        class NoMillisecondsFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                return datetime.fromtimestamp(record.created).strftime('%m-%d %H:%M:%S')
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',
                          handlers=[logging.StreamHandler()])
        self.formatter = NoMillisecondsFormatter(fmt='%(asctime)s - %(message)s')
        for handler in logging.getLogger().handlers:
            handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(__name__)
    
    def load_metadata_cache(self):
        try:
            if Path(CACHE_FILE).exists():
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    # Handle backward compatibility with old format
                    if cache and isinstance(next(iter(cache.values()), None), (int, float)):
                        # Old format: {"photo.jpg": timestamp}
                        self.logger.info("Converting old cache format to new format")
                        cache = {k: {"date": v, "orientation": None} for k, v in cache.items()}
                        self.cache_dirty = True
                    return cache
        except (json.JSONDecodeError, IOError, OSError) as e:
            self.logger.error(f"Error loading metadata cache: {e}")
        return {}
    
    def save_metadata_cache(self):
        if not self.cache_dirty:
            return
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(self.metadata_cache, f)
            self.cache_dirty = False
        except (IOError, OSError) as e:
            self.logger.error(f"Error saving metadata cache: {e}")
        
    def get_photo_date(self, photo_path):
        """Get photo date with caching"""
        filename = photo_path.name
        
        # Check cache first
        if filename in self.metadata_cache:
            cached_date = self.metadata_cache[filename].get("date")
            if cached_date is not None:
                return cached_date
        
        # Calculate date using fallback hierarchy
        date = self._calculate_photo_date(photo_path)
        
        # Update cache
        if filename not in self.metadata_cache:
            self.metadata_cache[filename] = {}
        self.metadata_cache[filename]["date"] = date
        self.cache_dirty = True
        return date
    
    def _calculate_photo_date(self, photo_path):
        """Calculate photo date using fallback hierarchy"""
        try:
            if EXIF_AVAILABLE:
                try:
                    with Image.open(photo_path) as img:
                        exif = img.getexif()
                        if exif:
                            for tag in [36867, 36868, 306]:  # DateTimeOriginal, Digitized, DateTime
                                if tag in exif and exif[tag]:
                                    return datetime.strptime(exif[tag], '%Y:%m:%d %H:%M:%S').timestamp()
                except (IOError, OSError, ValueError):
                    pass
            
            stat = photo_path.stat()
            if hasattr(stat, 'st_birthtime'):
                return stat.st_birthtime
            elif os.name == 'nt':
                return stat.st_ctime
            return stat.st_mtime
        except (IOError, OSError) as e:
            self.logger.warning(f"Error getting date for {photo_path}: {e}")
            return time.time()
    
    def get_photo_orientation(self, photo_path):
        """Get photo orientation (portrait or landscape) with caching"""
        filename = photo_path.name
        
        # Check cache first
        if filename in self.metadata_cache:
            cached_orientation = self.metadata_cache[filename].get("orientation")
            if cached_orientation is not None:
                return cached_orientation
        
        # Calculate orientation
        orientation = self._calculate_photo_orientation(photo_path)
        
        # Update cache
        if filename not in self.metadata_cache:
            self.metadata_cache[filename] = {}
        self.metadata_cache[filename]["orientation"] = orientation
        self.cache_dirty = True
        return orientation
    
    def _calculate_photo_orientation(self, photo_path):
        """Calculate photo orientation from dimensions"""
        try:
            if EXIF_AVAILABLE:
                with Image.open(photo_path) as img:
                    width, height = img.size
                    # Square counts as portrait
                    return "landscape" if width > height else "portrait"
            else:
                # If PIL not available, default to both (no filtering)
                return None
        except (IOError, OSError) as e:
            self.logger.warning(f"Error getting orientation for {photo_path}: {e}")
            return None
    
    def setup_gui(self):
        self.root = tk.Tk()
        self.root.title("Photo Rotation Scheduler")
        self.root.configure(bg=BG_COLOR)
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Configure ttk style for the background color
        self.style = ttk.Style()
        self.style.configure("TFrame", background=BG_COLOR)
        self.style.configure("TLabel", background=BG_COLOR)
        self.style.configure("TLabelframe", background=BG_COLOR)
        self.style.configure("TLabelframe.Label", background=BG_COLOR)
        self.style.configure("Large.TButton", padding=(20, 10), font=('TkDefaultFont', 10, 'bold'))
        
        default_library = Path("/media/adam/DRPHOTOUSB")
        self.library_path = tk.StringVar(value=str(default_library))
        self.library_path.trace_add("write", self.update_gallery_path)
        self.gallery_path_display = tk.StringVar(value="")
        self.photo_count = tk.StringVar(value="50")
        self.switches_per_day = tk.StringVar(value="1")
        self.main_time = tk.StringVar(value="21:15")
        self.selection_mode = tk.StringVar(value="Random")
        self.orientation_filter = tk.StringVar(value="Landscape")
        self.status = tk.StringVar(value="Ready")
        self.next_switch = tk.StringVar(value="Calculating...")
        
        self.update_gallery_path()
        self.create_ui()
        
    def create_logo(self, parent, height=150):
        """Load frog.png logo and display on a label"""
        if not PIL_AVAILABLE:
            return None
        
        try:
            # Load image from same directory as script
            script_dir = Path(__file__).parent
            img_path = script_dir / "frog.png"
            
            img = Image.open(img_path).convert("RGBA")
            
            # Calculate width to maintain aspect ratio
            aspect = img.width / img.height
            width = int(height * aspect)
            img = img.resize((width, height), Image.LANCZOS)
            
            # Create background matching GUI color and composite
            bg_color = tuple(int(BG_COLOR[i:i+2], 16) for i in (1, 3, 5)) + (255,)
            background = Image.new("RGBA", img.size, bg_color)
            composite = Image.alpha_composite(background, img)
            
            # Convert to PhotoImage
            self.logo_image = ImageTk.PhotoImage(composite)
            
            # Create label with image
            label = tk.Label(parent, image=self.logo_image, bg=BG_COLOR)
            return label
        except Exception as e:
            self.logger.warning(f"Could not load logo: {e}")
            return None
    
    def create_ui(self):
        # Top section: Folder Settings
        top_frame = tk.Frame(self.root, bg=BG_COLOR)
        top_frame.pack(fill="x", padx=10, pady=(15, 5))
        
        # Folder Settings frame
        folder_settings = ttk.LabelFrame(top_frame, text="Folders", padding=10)
        folder_settings.pack(side="left", fill="x", expand=False)
        
        self.create_path_row(folder_settings, "Library:", self.library_path, 0)
        
        ttk.Label(folder_settings, text="Gallery:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(folder_settings, textvariable=self.gallery_path_display, width=55, state="readonly").grid(row=1, column=1, sticky="w", padx=5, pady=5)
        
        # Frog logo - placed absolutely so it doesn't affect layout
        logo_label = self.create_logo(self.root, height=120)
        if logo_label:
            logo_label.place(relx=1.0, x=-15, y=10, anchor="ne")
        
        # Photo Settings frame
        settings = ttk.LabelFrame(self.root, text="Swap Settings", padding=10)
        settings.pack(fill="x", padx=10, pady=5)
        
        photo_frame = ttk.Frame(settings)
        photo_frame.grid(row=0, column=0, columnspan=2, pady=5)
        ttk.Label(photo_frame, text="Photos:").pack(side="left")
        ttk.Entry(photo_frame, textvariable=self.photo_count, width=8).pack(side="left", padx=5)
        ttk.Label(photo_frame, text="Order:").pack(side="left", padx=(20,0))
        mode_combo = ttk.Combobox(photo_frame, textvariable=self.selection_mode, width=10, state="readonly")
        mode_combo['values'] = ("Random", "Newest", "Oldest")
        mode_combo.pack(side="left", padx=5)
        ttk.Label(photo_frame, text="Orientation:").pack(side="left", padx=(10,0))
        orientation_combo = ttk.Combobox(photo_frame, textvariable=self.orientation_filter, width=10, state="readonly")
        orientation_combo['values'] = ("Both", "Portrait", "Landscape")
        orientation_combo.pack(side="left", padx=5)
        
        schedule_frame = ttk.Frame(settings)
        schedule_frame.grid(row=1, column=0, columnspan=2, pady=5)
        ttk.Label(schedule_frame, text="Main Time:").pack(side="left")
        ttk.Entry(schedule_frame, textvariable=self.main_time, width=8).pack(side="left", padx=5)
        ttk.Label(schedule_frame, text="Switches/Day:").pack(side="left", padx=(20,0))
        ttk.Entry(schedule_frame, textvariable=self.switches_per_day, width=8).pack(side="left", padx=5)
        
        # buttons
        update_btn = ttk.Button(schedule_frame, text="Update Settings", 
                               command=self.update_settings, style="Large.TButton")
        update_btn.pack(side="left", padx=(20,0))
        
        status_frame = ttk.LabelFrame(self.root, text="Status", padding=10)
        status_frame.pack(fill="x", padx=10, pady=5)
        ttk.Label(status_frame, textvariable=self.status).pack(anchor="w")
        ttk.Label(status_frame, text="Next Switch:").pack(side="left")
        ttk.Label(status_frame, textvariable=self.next_switch, font=("TkDefaultFont", 9, "bold")).pack(side="left", padx=10)
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.pack(fill="x", pady=5)
        
        controls = ttk.Frame(self.root)
        controls.pack(pady=10)
        self.switch_btn = ttk.Button(controls, text="Switch Photos Now", command=self.switch_photos_async)
        self.switch_btn.pack(side="left", padx=5)
        self.clear_btn = ttk.Button(controls, text="Clear Gallery folder", command=self.clear_gallery_async)
        self.clear_btn.pack(side="left", padx=5)
        self.reset_btn = ttk.Button(controls, text="Reset View History", command=self.reset_history)
        self.reset_btn.pack(side="left", padx=5)
        
        # Console frame
        console_frame = ttk.LabelFrame(self.root, text="Console", padding=5)
        console_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10))
        
        # Console text widget with scrollbar
        console_scroll = ttk.Scrollbar(console_frame)
        console_scroll.pack(side="right", fill="y")
        
        self.console_text = tk.Text(console_frame, height=10, wrap=tk.WORD,
                                    yscrollcommand=console_scroll.set,
                                    bg="#2b2b2b", fg="#ffffff",
                                    font=("Consolas", 9))
        self.console_text.pack(fill="both", expand=True)
        self.console_text.configure(state='disabled')
        console_scroll.config(command=self.console_text.yview)
        
        # Add text handler to logger
        text_handler = TextHandler(self.console_text)
        text_handler.setFormatter(self.formatter)
        logging.getLogger().addHandler(text_handler)
        
        # Calculate window height: controls + 400px console
        self.root.update_idletasks()
        controls_height = self.root.winfo_reqheight()
        console_height = 400
        total_height = controls_height + console_height
        self.root.geometry(f"750x{total_height}+0+0")
        
        # Welcome text in the console
        self.console_text.configure(state='normal')
        self.console_text.insert(tk.END, "Frog:  I’m here to put a few of yer photos from yer Library folder in yer Gallery folder...\n")
        self.console_text.configure(state='disabled')
        
    def create_path_row(self, parent, label, variable, row):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=2)
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=1, sticky="ew", padx=5)
        ttk.Entry(frame, textvariable=variable, width=50).pack(side="left", fill="x", expand=True)
        ttk.Button(frame, text="...", width=3, command=lambda: self.browse_path(variable)).pack(side="left", padx=(5,0))
    
    def update_gallery_path(self, *args):
        library = self.library_path.get()
        if library:
            self.gallery_path_display.set(str(Path(library) / "Gallery"))
    
    def validate_settings(self):
        """Validate all settings and return list of errors"""
        errors = []
        
        # Validate photo count
        try:
            count = int(self.photo_count.get())
            if count < MIN_PHOTO_COUNT:
                errors.append(f"Photo count must be at least {MIN_PHOTO_COUNT}")
            elif count > MAX_PHOTO_COUNT:
                errors.append(f"Photo count cannot exceed {MAX_PHOTO_COUNT}")
        except ValueError:
            errors.append("Photo count must be a valid number")
        
        # Validate time format
        if not self.validate_time_format(self.main_time.get()):
            errors.append("Main time must be in HH:MM format (e.g., 21:15)")
        
        # Validate switches per day
        try:
            switches = int(self.switches_per_day.get())
            if switches <= 0:
                errors.append("Switches per day must be positive")
            elif switches > MAX_SWITCHES_PER_DAY:
                errors.append(f"Switches per day cannot exceed {MAX_SWITCHES_PER_DAY}")
        except ValueError:
            errors.append("Switches per day must be a valid number")
        
        return errors
    
    def update_settings(self):
        errors = self.validate_settings()
        if errors:
            messagebox.showerror("Invalid Settings", "\n".join(errors))
            return
        self.update_next_switch()
        self.logger.info("Settings updated")
    
    def get_gallery_path(self):
        return Path(self.library_path.get()) / "Gallery"
    
    def get_library_path(self):
        return Path(self.library_path.get())
        
    def browse_path(self, variable):
        path = filedialog.askdirectory()
        if path:
            variable.set(str(Path(path)))
    
    def validate_time_format(self, time_str):
        try:
            datetime.strptime(time_str, "%H:%M")
            return True
        except ValueError:
            return False
    
    def validate_paths(self):
        library = self.get_library_path().resolve()
        gallery = self.get_gallery_path().resolve()
        if library == gallery:
            raise ValueError("Gallery path cannot be the same as library path")
        return True
            
    def load_viewed_photos(self):
        try:
            if Path(LOG_FILE).exists():
                with open(LOG_FILE, 'r') as f:
                    data = json.load(f)
                    return set(data) if isinstance(data, list) else set()
        except (json.JSONDecodeError, IOError, OSError) as e:
            self.logger.error(f"Error loading viewed photos: {e}")
        return set()
    
    def save_viewed_photos(self):
        try:
            with self.viewed_photos_lock:
                with open(LOG_FILE, 'w') as f:
                    json.dump(list(self.viewed_photos), f)
        except (IOError, OSError) as e:
            self.logger.error(f"Error saving viewed photos: {e}")
            
    def get_switch_times(self):
        try:
            if not self.validate_time_format(self.main_time.get()):
                return []
            
            switches = int(self.switches_per_day.get())
            if switches <= 0 or switches > MAX_SWITCHES_PER_DAY:
                return []
            
            main_hour, main_min = map(int, self.main_time.get().split(':'))
            now = datetime.now()
            main_datetime = now.replace(hour=main_hour, minute=main_min, second=0, microsecond=0)
            
            if switches == 1:
                return [main_datetime]
            
            interval = timedelta(hours=24/switches)
            times = [main_datetime + (interval * i) for i in range(switches)]
            
            # Normalize all times to today for comparison
            today = now.date()
            times = [t.replace(year=today.year, month=today.month, day=today.day) for t in times]
            return sorted(times)
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error calculating switch times: {e}")
            return []
    
    def update_next_switch(self):
        switch_times = self.get_switch_times()
        if not switch_times:
            self.next_switch.set("No switches scheduled")
            return
        
        now = datetime.now()
        for switch_time in switch_times:
            if switch_time > now:
                self.next_switch.set(switch_time.strftime("%H:%M"))
                return
        
        self.next_switch.set(f"{switch_times[0].strftime('%H:%M')} (tomorrow)")
    
    def check_scheduled_switches(self):
        if self.operation_lock.locked():
            return
            
        switch_times = self.get_switch_times()
        if not switch_times:
            return
        
        now = datetime.now()
        current_time = now.replace(second=0, microsecond=0)
        
        for switch_time in switch_times:
            if switch_time.replace(second=0, microsecond=0) == current_time:
                self.logger.info(f"Scheduled switch at {switch_time.strftime('%H:%M')}")
                self.switch_photos_async()
                break
    
    def iter_photos(self, directory):
        """Iterate over photo files in directory with proper error handling"""
        try:
            directory_path = Path(directory)
            if not directory_path.exists():
                self.logger.warning(f"Directory does not exist: {directory_path}")
                return
            
            try:
                entries = list(directory_path.iterdir())
            except PermissionError as e:
                self.logger.error(f"Permission denied accessing {directory}: {e}")
                return
                
            for file_path in entries:
                try:
                    if file_path.is_file() and file_path.suffix.lower() in PHOTO_EXTENSIONS:
                        yield file_path
                except (PermissionError, OSError) as e:
                    self.logger.warning(f"Error accessing {file_path}: {e}")
                    continue
        except (IOError, OSError) as e:
            self.logger.error(f"Error reading directory {directory}: {e}")
    
    def _filter_by_orientation(self, photo, orientation_filter):
        """Check if photo matches orientation filter"""
        if orientation_filter == "Both":
            return True
        photo_orientation = self.get_photo_orientation(photo)
        if photo_orientation is None:
            # Can't determine orientation, include it
            return True
        return photo_orientation.lower() == orientation_filter.lower()
    
    def _reservoir_sample(self, iterator, k):
        """
        Reservoir sampling: select k random items from an iterator of unknown length.
        Memory efficient - only stores k items at a time.
        """
        reservoir = []
        for i, item in enumerate(iterator):
            if self.operation_cancelled.is_set():
                break
            if i < k:
                reservoir.append(item)
            else:
                # Randomly replace elements with decreasing probability
                j = random.randint(0, i)
                if j < k:
                    reservoir[j] = item
        return reservoir
    
    def select_photos(self, library_path, count, mode, orientation_filter="Both"):
        """Select photos from library based on mode and filters"""
        count = max(MIN_PHOTO_COUNT, min(count, MAX_PHOTO_COUNT))
        library_path = Path(library_path)
        
        def unviewed_photos():
            for photo in self.iter_photos(library_path):
                if self.operation_cancelled.is_set():
                    return
                with self.viewed_photos_lock:
                    if photo.name not in self.viewed_photos:
                        if self._filter_by_orientation(photo, orientation_filter):
                            yield photo
        
        def all_photos_filtered():
            for photo in self.iter_photos(library_path):
                if self.operation_cancelled.is_set():
                    return
                if self._filter_by_orientation(photo, orientation_filter):
                    yield photo
        
        if mode == "Random":
            # Use reservoir sampling for memory efficiency
            selected = self._reservoir_sample(unviewed_photos(), count)
            
            if len(selected) < count:
                self.logger.info("Resetting history - not enough unviewed photos")
                with self.viewed_photos_lock:
                    self.viewed_photos.clear()
                self.save_viewed_photos()
                selected = self._reservoir_sample(all_photos_filtered(), count)
            
            return selected
        
        else:
            # Newest/Oldest mode - need to scan with dates
            return self._select_by_date(library_path, count, mode, orientation_filter)
    
    def _select_by_date(self, library_path, count, mode, orientation_filter):
        """Select photos sorted by date (newest or oldest)"""
        self.logger.info(f"Selecting {count} {mode.lower()} photos (scanning library)...")
        processed = [0]  # Use list for closure modification
        
        def unviewed_photos_with_date():
            for photo in self.iter_photos(library_path):
                if self.operation_cancelled.is_set():
                    return
                with self.viewed_photos_lock:
                    if photo.name in self.viewed_photos:
                        continue
                if not self._filter_by_orientation(photo, orientation_filter):
                    continue
                processed[0] += 1
                if processed[0] % CACHE_SAVE_INTERVAL == 0:
                    self.logger.info(f"Scanned {processed[0]} photos...")
                    self.save_metadata_cache()
                yield (self.get_photo_date(photo), photo)
        
        heap_func = heapq.nlargest if mode == "Newest" else heapq.nsmallest
        selected = [photo for _, photo in heap_func(count, unviewed_photos_with_date())]
        
        self.logger.info(f"Scan complete: processed {processed[0]} photos")
        self.save_metadata_cache()
        
        if len(selected) < count:
            self.logger.info("Resetting history - not enough unviewed photos")
            with self.viewed_photos_lock:
                self.viewed_photos.clear()
            self.save_viewed_photos()
            
            processed[0] = 0
            
            def all_photos_with_date():
                for photo in self.iter_photos(library_path):
                    if self.operation_cancelled.is_set():
                        return
                    if not self._filter_by_orientation(photo, orientation_filter):
                        continue
                    processed[0] += 1
                    if processed[0] % CACHE_SAVE_INTERVAL == 0:
                        self.logger.info(f"Scanned {processed[0]} photos...")
                    yield (self.get_photo_date(photo), photo)
            
            selected = [photo for _, photo in heap_func(count, all_photos_with_date())]
            self.logger.info(f"Scan complete: processed {processed[0]} photos")
            self.save_metadata_cache()
        
        return selected
    
    def switch_photos_async(self):
        if not self.operation_lock.acquire(blocking=False):
            self.logger.warning("Operation already in progress")
            return
        
        self.operation_cancelled.clear()
        
        try:
            # Validate settings before starting
            errors = self.validate_settings()
            if errors:
                self.root.after(0, lambda: messagebox.showerror("Invalid Settings", "\n".join(errors)))
                self.operation_lock.release()
                return
            
            try:
                self.validate_paths()
            except ValueError as e:
                self.root.after(0, lambda: messagebox.showerror("Invalid Path", str(e)))
                self.operation_lock.release()
                return
            
            self.current_thread = threading.Thread(target=self._switch_photos_worker, daemon=True)
            self.root.after(0, self.start_operation)
            self.current_thread.start()
        except RuntimeError as e:
            self.logger.error(f"Error starting switch photos: {e}")
            self.operation_lock.release()
    
    def _switch_photos_worker(self):
        """Main worker for switching photos - orchestrates the process"""
        result_message = "Operation cancelled"
        try:
            self.logger.info("Starting photo switch...")
            
            library_path = self.get_library_path()
            gallery_path = self.get_gallery_path()
            
            if not library_path.exists():
                raise FileNotFoundError(f"Library path does not exist: {library_path}")
            
            gallery_path.mkdir(parents=True, exist_ok=True)
            
            # STEP 1: Select new photos
            count = int(self.photo_count.get())
            mode = self.selection_mode.get()
            orientation = self.orientation_filter.get()
            self.logger.info("Selecting new photos...")
            selected = self.select_photos(library_path, count, mode, orientation)
            selected_names = {photo.name for photo in selected}
            
            if self.operation_cancelled.is_set():
                return
            
            # STEP 2: Get current Gallery contents
            current_gallery_photos = list(self.iter_photos(gallery_path))
            
            # STEP 3: Move new photos to gallery
            moved_to_gallery = self._move_photos_to_gallery(selected, gallery_path)
            
            if self.operation_cancelled.is_set():
                return
            
            # STEP 4: Remove old photos from gallery
            removed_count, deleted_dupes = self._remove_old_photos_from_gallery(
                current_gallery_photos, selected_names, library_path
            )
            
            if deleted_dupes:
                with self.viewed_photos_lock:
                    for filename in deleted_dupes:
                        self.viewed_photos.discard(filename)
                self.logger.info(f"Removed {len(deleted_dupes)} duplicate(s)")
            
            if removed_count > 0:
                self.logger.info(f"Moved {removed_count} old photos back to library")
            
            self.save_viewed_photos()
            self.logger.info(f"Switch complete: {len(selected_names)} photos now in gallery")
            result_message = f"Switched to {len(selected_names)} photos"
            
        except FileNotFoundError as e:
            self.logger.error(f"Path error: {e}")
            result_message = f"Error: {str(e)}"
        except (IOError, OSError) as e:
            self.logger.error(f"File operation error: {e}")
            result_message = f"Error: {str(e)}"
        finally:
            self.root.after(0, lambda msg=result_message: self.end_operation(msg))
            self.operation_lock.release()
    
    def _move_photos_to_gallery(self, selected_photos, gallery_path):
        """Move selected photos to gallery, handling duplicates"""
        moved_count = 0
        for photo in selected_photos:
            if self.operation_cancelled.is_set():
                break
            new_path = gallery_path / photo.name
            try:
                # Use atomic rename, handle FileExistsError (TOCTOU fix)
                try:
                    photo.rename(new_path)
                    moved_count += 1
                except FileExistsError:
                    # Photo already in Gallery (duplicate in library)
                    photo.unlink()
                    self.logger.debug(f"Deleted library duplicate: {photo.name}")
                
                with self.viewed_photos_lock:
                    self.viewed_photos.add(photo.name)
            except PermissionError as e:
                self.logger.error(f"Permission denied moving {photo}: {e}")
            except (IOError, OSError) as e:
                self.logger.error(f"Error moving {photo}: {e}")
        
        self.logger.info(f"Moved {moved_count} new photos to gallery")
        return moved_count
    
    def _remove_old_photos_from_gallery(self, gallery_photos, selected_names, library_path):
        """Remove photos from gallery that aren't in selection"""
        removed_count = 0
        deleted_dupes = []
        
        for photo in gallery_photos:
            if self.operation_cancelled.is_set():
                break
            if photo.name not in selected_names:
                new_path = library_path / photo.name
                try:
                    # Use atomic rename, handle FileExistsError (TOCTOU fix)
                    try:
                        photo.rename(new_path)
                        removed_count += 1
                    except FileExistsError:
                        # Duplicate exists in library, just delete from Gallery
                        photo.unlink()
                        deleted_dupes.append(photo.name)
                except PermissionError as e:
                    self.logger.error(f"Permission denied removing {photo}: {e}")
                except (IOError, OSError) as e:
                    self.logger.error(f"Error removing {photo}: {e}")
        
        return removed_count, deleted_dupes
    
    def clear_gallery_async(self):
        if not self.operation_lock.acquire(blocking=False):
            self.logger.warning("Operation already in progress")
            return
        
        self.operation_cancelled.clear()
        
        try:
            try:
                self.validate_paths()
            except ValueError as e:
                self.root.after(0, lambda: messagebox.showerror("Invalid Path", str(e)))
                self.operation_lock.release()
                return
            
            self.current_thread = threading.Thread(target=self._clear_gallery_worker, daemon=True)
            self.root.after(0, self.start_operation)
            self.current_thread.start()
        except RuntimeError as e:
            self.logger.error(f"Error starting clear gallery: {e}")
            self.operation_lock.release()
    
    def _clear_gallery_worker(self):
        result_message = "Operation cancelled"
        try:
            self.logger.info("Starting clear gallery...")
            
            library_path = self.get_library_path()
            gallery_path = self.get_gallery_path()
            
            if not gallery_path.exists():
                result_message = "Gallery directory does not exist"
                return
            
            count = 0
            deleted_dupes = []
            
            for photo in self.iter_photos(gallery_path):
                if self.operation_cancelled.is_set():
                    return
                new_path = library_path / photo.name
                try:
                    # Use atomic rename, handle FileExistsError (TOCTOU fix)
                    try:
                        photo.rename(new_path)
                        count += 1
                    except FileExistsError:
                        photo.unlink()
                        deleted_dupes.append(photo.name)
                except PermissionError as e:
                    self.logger.error(f"Permission denied moving {photo}: {e}")
                except (IOError, OSError) as e:
                    self.logger.error(f"Error moving {photo}: {e}")
            
            if deleted_dupes:
                with self.viewed_photos_lock:
                    for filename in deleted_dupes:
                        self.viewed_photos.discard(filename)
                self.save_viewed_photos()
                self.logger.info(f"Removed {len(deleted_dupes)} duplicate(s)")
            
            self.logger.info(f"Clear complete: moved {count} photos back")
            result_message = f"Moved {count} photos back"
        except (IOError, OSError) as e:
            self.logger.error(f"Error clearing gallery: {e}")
            result_message = f"Error: {str(e)}"
        finally:
            self.root.after(0, lambda msg=result_message: self.end_operation(msg))
            self.operation_lock.release()
    
    def reset_history(self):
        if self.operation_lock.locked():
            messagebox.showwarning("Operation in Progress", "Cannot reset during operation")
            return
        
        with self.viewed_photos_lock:
            self.viewed_photos.clear()
        self.save_viewed_photos()
        
        # Also clear metadata cache
        self.metadata_cache.clear()
        self.cache_dirty = True
        self.save_metadata_cache()
        
        self.logger.info("History and cache reset")
    
    def start_operation(self):
        self.status.set("Working...")
        self.progress.start()
        self.switch_btn.config(state="disabled")
        self.clear_btn.config(state="disabled")
        self.reset_btn.config(state="disabled")
    
    def end_operation(self, message):
        self.status.set(message)
        self.progress.stop()
        self.switch_btn.config(state="normal")
        self.clear_btn.config(state="normal")
        self.reset_btn.config(state="normal")
        self.update_next_switch()
    
    def periodic_update(self):
        self.check_scheduled_switches()
        self.update_next_switch()
        self.root.after(60000, self.periodic_update)
    
    def on_closing(self):
        self.operation_cancelled.set()
        if self.current_thread and self.current_thread.is_alive():
            self.current_thread.join(timeout=THREAD_JOIN_TIMEOUT)
        self.save_metadata_cache()  # Save cache on exit
        self.root.destroy()
    
    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = PhotoScheduler()
    app.run()
