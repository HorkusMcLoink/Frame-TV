import os
import random
import json
import time
import shutil
import subprocess
import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta
import threading
from pathlib import Path
from typing import List, Optional
import psutil
import logging
import signal

# Try to import PIL for EXIF data, fallback gracefully if not available
try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    EXIF_AVAILABLE = True
except ImportError:
    EXIF_AVAILABLE = False

# Constants
PHOTO_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')
CHUNK_SIZE = 1000
LOG_FILE = "viewed_photos.json"

class PhotoScheduler:
    def __init__(self):
        self.setup_logging()
        self.setup_gui()
        self.viewed_photos = self.load_viewed_photos()
        self.viewed_photos_lock = threading.Lock()
        self.operation_lock = threading.Lock()  # Single operation lock
        self.operation_cancelled = threading.Event()
        self.current_thread = None
        self.is_operation_running = False  # Simple flag for UI state
        
        # Log EXIF availability
        if EXIF_AVAILABLE:
            self.logger.info("EXIF date detection enabled (PIL available)")
        else:
            self.logger.info("EXIF date detection disabled (PIL not available - install with: pip install Pillow)")
        
        # Schedule periodic updates
        self.update_next_switch()
        self.periodic_update()
        
    def setup_logging(self):
        # Custom formatter to remove milliseconds
        class NoMillisecondsFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created)
                return dt.strftime('%m-%d %H:%M:%S')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        
        # Apply custom formatter to all handlers
        formatter = NoMillisecondsFormatter(fmt='%(asctime)s - %(message)s')
        for handler in logging.getLogger().handlers:
            handler.setFormatter(formatter)
            
        self.logger = logging.getLogger(__name__)
        
    def get_photo_date(self, photo_path):
        """Get photo date using fallback hierarchy"""
        try:
            # Priority 1: EXIF DateTimeOriginal (most reliable)
            if EXIF_AVAILABLE:
                try:
                    with Image.open(photo_path) as img:
                        exif = img._getexif()
                        if exif:
                            # Try DateTimeOriginal first (when photo was taken)
                            for tag_id, value in exif.items():
                                tag = TAGS.get(tag_id, tag_id)
                                if tag == 'DateTimeOriginal' and value:
                                    return datetime.strptime(value, '%Y:%m:%d %H:%M:%S').timestamp()
                            
                            # Priority 2: Other EXIF dates
                            for tag_id, value in exif.items():
                                tag = TAGS.get(tag_id, tag_id)
                                if tag in ['DateTimeDigitized', 'DateTime'] and value:
                                    return datetime.strptime(value, '%Y:%m:%d %H:%M:%S').timestamp()
                except Exception:
                    pass  # Fall through to file timestamps
            
            # Priority 3: File creation time (Windows/some filesystems)
            stat = photo_path.stat()
            if hasattr(stat, 'st_birthtime'):  # macOS
                return stat.st_birthtime
            elif os.name == 'nt':  # Windows
                return stat.st_ctime
            
            # Priority 4: File modification time (last resort)
            return stat.st_mtime
            
        except Exception as e:
            self.logger.warning(f"Error getting date for {photo_path}: {e}")
            # Final fallback to current time
            return time.time()
    
    def setup_gui(self):
        self.root = tk.Tk()
        self.root.title("Photo Rotation Scheduler")
        self.root.geometry("600x350")
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Variables - Use Path objects for cross-platform compatibility
        if os.name == 'nt':  # Windows
            default_library = Path("/media/adam/DRPHOTOUSB")  # Windows-friendly default
        else:  # Linux/Mac
            default_library = Path("/media/adam/DRPHOTOUSB")
        self.library_path = tk.StringVar(value=str(default_library))
        self.library_path.trace_add("write", self.update_gallery_path)
        self.gallery_path_display = tk.StringVar(value="")
        self.photo_count = tk.StringVar(value="50")
        self.switches_per_day = tk.StringVar(value="1")
        self.main_time = tk.StringVar(value="21:15")
        self.selection_mode = tk.StringVar(value="Newest")
        self.status = tk.StringVar(value="Ready")
        self.next_switch = tk.StringVar(value="Calculating...")
        
        # Initialize gallery path
        self.update_gallery_path()
        
        # Create UI
        self.create_ui()
        
    def create_ui(self):
        # Settings Frame
        settings = ttk.LabelFrame(self.root, text="Settings", padding=10)
        settings.pack(fill="x", padx=10, pady=5)
        
        # Library Path
        self.create_path_row(settings, "Library:", self.library_path, 0)
        
        # Gallery Path Display
        ttk.Label(settings, text="Gallery:").grid(row=1, column=0, sticky="w", pady=2)
        gallery_display = ttk.Entry(settings, textvariable=self.gallery_path_display, width=45, state="readonly")
        gallery_display.grid(row=1, column=1, sticky="w", padx=5)
        
        # Photo settings
        photo_frame = ttk.Frame(settings)
        photo_frame.grid(row=2, column=0, columnspan=2, pady=5)
        
        ttk.Label(photo_frame, text="Photos:").pack(side="left")
        ttk.Entry(photo_frame, textvariable=self.photo_count, width=8).pack(side="left", padx=5)
        
        ttk.Label(photo_frame, text="Mode:").pack(side="left", padx=(20,0))
        mode_combo = ttk.Combobox(photo_frame, textvariable=self.selection_mode, width=10, state="readonly")
        mode_combo['values'] = ("Random", "Newest", "Oldest")
        mode_combo.pack(side="left", padx=5)
        
        # Schedule settings
        schedule_frame = ttk.Frame(settings)
        schedule_frame.grid(row=3, column=0, columnspan=2, pady=5)
        
        ttk.Label(schedule_frame, text="Main Time:").pack(side="left")
        ttk.Entry(schedule_frame, textvariable=self.main_time, width=8).pack(side="left", padx=5)
        
        ttk.Label(schedule_frame, text="Switches/Day:").pack(side="left", padx=(20,0))
        ttk.Entry(schedule_frame, textvariable=self.switches_per_day, width=8).pack(side="left", padx=5)
        
        # Update Settings button
        ttk.Button(schedule_frame, text="Update Settings", command=self.update_settings).pack(side="left", padx=(20,0))
        
        # Status Frame
        status_frame = ttk.LabelFrame(self.root, text="Status", padding=10)
        status_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(status_frame, textvariable=self.status).pack(anchor="w")
        ttk.Label(status_frame, text="Next Switch:").pack(side="left")
        ttk.Label(status_frame, textvariable=self.next_switch, font=("TkDefaultFont", 9, "bold")).pack(side="left", padx=10)
        
        # Progress bar
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.pack(fill="x", pady=5)
        
        # Control buttons
        controls = ttk.Frame(self.root)
        controls.pack(pady=10)
        
        self.switch_btn = ttk.Button(controls, text="Switch Photos Now", command=self.switch_photos_async)
        self.switch_btn.pack(side="left", padx=5)
        
        self.clear_btn = ttk.Button(controls, text="Reset Gallery folder", command=self.clear_gallery_async)
        self.clear_btn.pack(side="left", padx=5)
        
        self.reset_btn = ttk.Button(controls, text="Reset View History", command=self.reset_history)
        self.reset_btn.pack(side="left", padx=5)
        
    def create_path_row(self, parent, label, variable, row):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=2)
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=1, sticky="ew", padx=5)
        ttk.Entry(frame, textvariable=variable, width=40).pack(side="left", fill="x", expand=True)
        ttk.Button(frame, text="...", width=3, 
                  command=lambda: self.browse_path(variable)).pack(side="left", padx=(5,0))
    
    def update_gallery_path(self, *args):
        """Update the gallery path based on library path - cross-platform compatible"""
        library = self.library_path.get()
        if library:
            # Use Path to ensure cross-platform compatibility
            gallery_path = Path(library) / "Gallery"
            # Convert to string with appropriate separators for display
            self.gallery_path_display.set(str(gallery_path))
    
    def update_settings(self):
        """Update settings and recalculate next switch time"""
        self.update_next_switch()
        self.logger.info("Settings updated")
    
    def get_gallery_path(self):
        """Get the actual gallery path as Path object"""
        return Path(self.library_path.get()) / "Gallery"
    
    def get_library_path(self):
        """Get the library path as Path object"""
        return Path(self.library_path.get())
        
    def browse_path(self, variable):
        path = filedialog.askdirectory()
        if path:
            # Normalize path using Path to ensure cross-platform compatibility
            normalized_path = Path(path)
            variable.set(str(normalized_path))
    
    def validate_time_format(self, time_str):
        """Validate HH:MM format"""
        try:
            datetime.strptime(time_str, "%H:%M")
            return True
        except ValueError:
            return False
            
    def load_viewed_photos(self):
        try:
            log_file = Path(LOG_FILE)
            if log_file.exists():
                with open(log_file, 'r') as f:
                    data = json.load(f)
                    # Normalize paths in loaded data for cross-platform compatibility
                    if isinstance(data, list):
                        return [str(Path(path)) for path in data]
        except Exception as e:
            self.logger.error(f"Error loading viewed photos: {e}")
        return []
    
    def save_viewed_photos(self):
        try:
            with self.viewed_photos_lock:
                # Normalize paths before saving
                normalized_photos = [str(Path(path)) for path in self.viewed_photos]
                with open(LOG_FILE, 'w') as f:
                    json.dump(normalized_photos, f)
        except Exception as e:
            self.logger.error(f"Error saving viewed photos: {e}")
            
    def get_switch_times(self):
        """Calculate switch times including today"""
        try:
            # Validate time format first
            if not self.validate_time_format(self.main_time.get()):
                return []
                
            switches = int(self.switches_per_day.get())
            if switches <= 0:
                return []
            
            main_hour, main_min = map(int, self.main_time.get().split(':'))
            main_datetime = datetime.now().replace(hour=main_hour, minute=main_min, second=0)
            
            if switches == 1:
                return [main_datetime]
            
            # Calculate evenly spaced times throughout 24 hours
            interval = timedelta(hours=24/switches)
            times = []
            
            for i in range(switches):
                switch_time = main_datetime + (interval * i)
                # Normalize to same day for comparison
                switch_time = switch_time.replace(year=datetime.now().year, 
                                                month=datetime.now().month, 
                                                day=datetime.now().day)
                times.append(switch_time)
            
            return sorted(times)
        except Exception as e:
            self.logger.error(f"Error calculating switch times: {e}")
            return []
    
    def update_next_switch(self):
        """Update next switch time display"""
        switch_times = self.get_switch_times()
        if not switch_times:
            self.next_switch.set("No switches scheduled")
            return
        
        now = datetime.now()
        # Find next switch time (including today)
        for switch_time in switch_times:
            if switch_time > now:
                self.next_switch.set(switch_time.strftime("%H:%M"))
                return
        
        # If all times passed today, show first time tomorrow
        self.next_switch.set(f"{switch_times[0].strftime('%H:%M')} (tomorrow)")
    
    def check_scheduled_switches(self):
        """Check if it's time to switch photos"""
        # Don't trigger scheduled switches if an operation is already running
        if self.is_operation_running:
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
        """Yield photo paths from directory using pathlib"""
        try:
            directory_path = Path(directory)
            if not directory_path.exists():
                self.logger.warning(f"Directory does not exist: {directory_path}")
                return
                
            for file_path in directory_path.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in PHOTO_EXTENSIONS:
                    yield file_path
        except Exception as e:
            self.logger.error(f"Error reading directory {directory}: {e}")
    
    def select_photos(self, library_path, count, mode):
        """Select photos based on mode with enhanced date detection"""
        unviewed = []
        library_path = Path(library_path)
        
        for photo in self.iter_photos(library_path):
            if self.operation_cancelled.is_set():
                return []
            with self.viewed_photos_lock:
                # Normalize path for comparison
                photo_str = str(photo)
                if photo_str not in self.viewed_photos:
                    unviewed.append(photo)
        
        if len(unviewed) < count:
            self.logger.info("resetting history")
            with self.viewed_photos_lock:
                self.viewed_photos = []
            self.save_viewed_photos()
            unviewed = list(self.iter_photos(library_path))
        
        if mode == "Random":
            return random.sample(unviewed, min(count, len(unviewed)))
        else:
            # Sort by date using enhanced date detection
            try:
                self.logger.info(f"Sorting {len(unviewed)} photos by date...")
                unviewed.sort(key=self.get_photo_date, reverse=(mode == "Newest"))
            except Exception as e:
                self.logger.error(f"Error sorting photos: {e}")
                # Fallback to unsorted list
            return unviewed[:count]
    
    def switch_photos_async(self):
        """Switch photos in background thread"""
        # Use the operation lock to prevent multiple operations
        if not self.operation_lock.acquire(blocking=False):
            return  # Another operation is already running
        
        try:
            # Update UI state
            self.is_operation_running = True
            self.root.after(0, self.start_operation)
            
            self.current_thread = threading.Thread(target=self._switch_photos_worker, daemon=True)
            self.current_thread.start()
        except Exception as e:
            self.logger.error(f"Error starting switch photos thread: {e}")
            self.is_operation_running = False
            self.operation_lock.release()
    
    def _switch_photos_worker(self):
        """Worker thread for switching photos"""
        try:
            self.operation_cancelled.clear()
            self.logger.info("Starting photo switch operation...")
            
            library_path = self.get_library_path()
            gallery_path = self.get_gallery_path()
            
            if not library_path.exists():
                raise Exception(f"Library path does not exist: {library_path}")
            
            # Create gallery directory if it doesn't exist
            gallery_path.mkdir(parents=True, exist_ok=True)
            
            # Move existing photos back using pathlib
            moved_back_count = 0
            for photo_path in self.iter_photos(gallery_path):
                if self.operation_cancelled.is_set():
                    return
                new_path = library_path / photo_path.name
                try:
                    # Handle potential file conflicts
                    counter = 1
                    original_new_path = new_path
                    while new_path.exists():
                        stem = original_new_path.stem
                        suffix = original_new_path.suffix
                        new_path = library_path / f"{stem}_{counter}{suffix}"
                        counter += 1
                    
                    photo_path.rename(new_path)
                    moved_back_count += 1
                except Exception as e:
                    self.logger.error(f"Error moving {photo_path} back to library: {e}")
            
            if moved_back_count > 0:
                self.logger.info(f"Moved {moved_back_count} photos back to library")
            
            # Select and move new photos
            count = int(self.photo_count.get())
            mode = self.selection_mode.get()
            selected = self.select_photos(library_path, count, mode)
            
            moved_to_gallery_count = 0
            for photo_path in selected:
                if self.operation_cancelled.is_set():
                    return
                new_path = gallery_path / photo_path.name
                try:
                    photo_path.rename(new_path)
                    with self.viewed_photos_lock:
                        self.viewed_photos.append(str(photo_path))
                    moved_to_gallery_count += 1
                except Exception as e:
                    self.logger.error(f"Error moving {photo_path} to gallery: {e}")
            
            self.save_viewed_photos()
            
            self.logger.info(f"Photo switch operation completed. Moved {moved_to_gallery_count} photos to gallery.")
            self.root.after(0, lambda: self.end_operation(f"Switched {moved_to_gallery_count} photos successfully"))
            
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.logger.error(f"Error switching photos: {e}")
            self.root.after(0, lambda: self.end_operation(error_msg))
        finally:
            # Always release the lock and reset state
            self.is_operation_running = False
            self.operation_lock.release()
    
    def clear_gallery_async(self):
        """Clear gallery in background"""
        # Use the operation lock to prevent multiple operations
        if not self.operation_lock.acquire(blocking=False):
            return  # Another operation is already running
        
        try:
            # Update UI state
            self.is_operation_running = True
            self.root.after(0, self.start_operation)
            
            self.current_thread = threading.Thread(target=self._clear_gallery_worker, daemon=True)
            self.current_thread.start()
        except Exception as e:
            self.logger.error(f"Error starting clear gallery thread: {e}")
            self.is_operation_running = False
            self.operation_lock.release()
    
    def _clear_gallery_worker(self):
        """Worker thread for clearing gallery"""
        try:
            self.logger.info("Starting clear gallery operation...")
            
            library_path = self.get_library_path()
            gallery_path = self.get_gallery_path()
            
            if not gallery_path.exists():
                self.root.after(0, lambda: self.end_operation("Gallery directory does not exist"))
                return
            
            count = 0
            for photo_path in self.iter_photos(gallery_path):
                if self.operation_cancelled.is_set():
                    return
                new_path = library_path / photo_path.name
                try:
                    # Handle potential file conflicts
                    counter = 1
                    original_new_path = new_path
                    while new_path.exists():
                        stem = original_new_path.stem
                        suffix = original_new_path.suffix
                        new_path = library_path / f"{stem}_{counter}{suffix}"
                        counter += 1
                    
                    photo_path.rename(new_path)
                    count += 1
                except Exception as e:
                    self.logger.error(f"Error moving {photo_path}: {e}")
            
            self.logger.info(f"Clear gallery operation completed. Moved {count} photos back to library.")
            self.root.after(0, lambda: self.end_operation(f"Moved {count} photos back to library"))
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.logger.error(f"Error in clear gallery operation: {e}")
            self.root.after(0, lambda: self.end_operation(error_msg))
        finally:
            # Always release the lock and reset state
            self.is_operation_running = False
            self.operation_lock.release()
    
    def reset_history(self):
        """Reset viewed photos history"""
        # Don't allow during operations
        if self.is_operation_running:
            messagebox.showwarning("Operation in Progress", "Cannot reset history while an operation is running.")
            return
            
        with self.viewed_photos_lock:
            self.viewed_photos = []
        self.save_viewed_photos()
        self.logger.info("Viewing history reset")
        # Removed the popup messagebox - now just logs the action
    
    def start_operation(self):
        """Start operation UI feedback"""
        self.status.set("Working...")
        self.progress.start()
        # Disable all operation buttons
        self.switch_btn.config(state="disabled")
        self.clear_btn.config(state="disabled")
        self.reset_btn.config(state="disabled")
    
    def end_operation(self, message):
        """End operation UI feedback"""
        self.status.set(message)
        self.progress.stop()
        # Re-enable all operation buttons
        self.switch_btn.config(state="normal")
        self.clear_btn.config(state="normal")
        self.reset_btn.config(state="normal")
        self.update_next_switch()
    
    def periodic_update(self):
        """Check for scheduled switches every minute"""
        self.check_scheduled_switches()
        self.update_next_switch()
        # Schedule next check
        self.root.after(60000, self.periodic_update)
    
    def on_closing(self):
        """Handle window closing"""
        self.operation_cancelled.set()
        # Wait briefly for operations to complete
        if self.current_thread and self.current_thread.is_alive():
            self.current_thread.join(timeout=2.0)
        self.root.destroy()
    
    def run(self):
        """Start the application"""
        self.root.mainloop()

if __name__ == "__main__":
    app = PhotoScheduler()
    app.run()
