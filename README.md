# Frame-TV

Keeps a Samsung Frame TV showing a nightly-rotated set of photos in Art Mode.
Four parts share one folder on the Pi.

| Part | File | What it does |
|---|---|---|
| Photo picker | `example/photo_rotation_scheduler.py` | Desktop GUI that must stay open on a logged-in desktop. At the set time each day it moves the night's photos from the library into `Gallery/` and moves the previous batch back. Written by Mike ([Barkelew/Frame-TV](https://github.com/Barkelew/Frame-TV)). |
| Slideshow | `example/async_art_update_from_directory.py` | Mirrors `Gallery/` to the TV: uploads new photos, deletes removed ones, rotates the displayed art. From [NickWaterton/samsung-tv-ws-api](https://github.com/NickWaterton/samsung-tv-ws-api). |
| Art Mode enforcer | `example/async_art_ensure_art_mode.py` | Checks the TV every few seconds and presses power if it leaves Art Mode. From Nick's repo. |
| Orphan sweep | `example/async_art_sweep_orphans.py` | Deletes photos on the TV that the slideshow script does not track. Added in this repo. |

Nick's scripts use the `samsungframetv` library, pinned to 4.1.4.

## How the parts connect

The scheduler writes the night's photos into `Gallery/`. The slideshow script watches that folder
and mirrors it to the TV, recording each upload in `uploaded_files.json`. The sweep compares the
TV's photo list with that record. The enforcer runs independently.

## How the photo picker behaves

- **Photos are moved, not copied.** The library on the USB drive is the home for every photo.
  `Gallery/` is a temporary holding area for the current batch.
- **The schedule is a timer inside the open window**, not a system job. It fires when the clock
  matches the configured minute (default 21:15). If the window is closed, or the Pi is off or
  rebooting at that minute, that night's switch is skipped and nothing catches up.
- **Settings are not saved.** Library path, photo count, order, orientation, time, and switches
  per day return to the defaults in the code (`/media/adam/DRPHOTOUSB`, 50, Random, Landscape,
  21:15, 1) each time it launches. Re-enter any changes after a restart.
- **Only the top level of the library is read.** Photos in subfolders are never picked.
- **Random mode** avoids photos it has shown before and clears its history when too few remain.
- **No log file.** Messages appear only in the window's console.
- **Optional logo:** the window shows `example/frog.png` if present. It is not in this repo.

## Services and files

| File | Installed as | Notes |
|---|---|---|
| `systemd/slideshow.service` | system service | Restarts itself on failure. |
| `systemd/force-art.service` | system service | Restarts itself every 5 minutes (`RuntimeMaxSec=300`) on purpose. |
| `systemd/sweep-orphans.service` + `.timer` | system timer | Hourly. |
| `autostart/photo-rotation.desktop` | `~/.config/autostart/` | Starts the scheduler GUI at desktop login. It is not a service. |

## Before you install

The unit files and the autostart file contain values from the original setup. Change every match of:
grep -rn -E “adam|192.168.5.81|DRPHOTOUSB” systemd autostart example/async_art_sweep_orphans.py example/photo_rotation_scheduler.py


- `adam`: the Linux user and the `/home/adam/...` paths
- `192.168.5.81`: your TV's IP address
- `DRPHOTOUSB`: the USB drive name (`/media/<user>/<drive>/Gallery`)

## Requirements

- Python 3 with a venv at `~/frame-venv`:
  `python3 -m venv ~/frame-venv && ~/frame-venv/bin/pip install "samsungframetv[async,encrypted]==4.1.4" Pillow`
- `tkinter` for the scheduler GUI. If it is missing: `sudo apt install python3-tk`.
- A paired TV: the first script run asks you to press "Allow" on the TV and creates
  `example/token_file.txt`. Pair with `python3 async_art_simple.py <tv-ip>` from `example/`.

## Install (untested on a fresh Pi)
sudo cp systemd/{slideshow.service,sweep-orphans.service,sweep-orphans.timer} /etc/systemd/system/
sudo ln -s “$PWD/systemd/force-art.service” /etc/systemd/system/force-art.service
sudo systemctl daemon-reload
sudo systemctl enable --now force-art.service slideshow.service sweep-orphans.timer
mkdir -p ~/.config/autostart && ln -s “$PWD/autostart/photo-rotation.desktop” ~/.config/autostart/


Check: `systemctl is-active force-art slideshow` and `systemctl list-timers sweep-orphans.timer`.

## Running by hand (testing only)

Stop the matching service first so two copies do not manage the TV at once
(`sudo systemctl stop slideshow`), then from `example/`:
~/frame-venv/bin/python3 async_art_update_from_directory.py <tv-ip> -f <Gallery path> -u 1 -m flexible_black -c 60
~/frame-venv/bin/python3 async_art_ensure_art_mode.py <tv-ip> -p 5
~/frame-venv/bin/python3 async_art_sweep_orphans.py <tv-ip> # dry run; add --delete to remove


`-u` is the rotation interval in minutes and `-c` is the folder-check interval in seconds.

## The sweep

Deletes anything in the TV's My Photos that is not listed in `uploaded_files.json`.
It runs only when all of these hold:

- the TV is on
- every file in `Gallery/` has been uploaded and matches the record (the slideshow is "settled")
- the same photos are untracked in two scans 60 seconds apart
- the TV's photo list is not empty and at least half of the tracked photos are on it

Otherwise it logs why and skips. Anything you add to the TV by hand will be deleted.

## State files (not in Git)

- `example/token_file.txt`: the TV pairing key. Keep it private.
- `example/uploaded_files.json`: which photo on the TV matches which file.
- `example/viewed_photos.json`: the scheduler's history of shown photos.
- `example/photo_metadata.json`: a date and orientation cache the scheduler writes when the
  window closes, or when Newest/Oldest ordering is used.

## Troubleshooting

- Logs: `journalctl -u slideshow --since "-10min"` (same for `force-art`, `sweep-orphans`).
- Slideshow restarts repeatedly with `OSError: broken data stream`: a damaged photo is in
  `Gallery/`. Move it out and the service recovers. The scheduler only reads image headers, so
  a damaged file can still reach `Gallery/`.
- The scheduler accepts `.gif` and `.tiff`, but the slideshow only accepts
  `jpg, jpeg, png, bmp, tif`. Other types are moved into `Gallery/` and never uploaded.
- Pi reaches the router but nothing beyond it: check that the connection's gateway and DNS
  match the router.

## Updating Nick's scripts

This repo shares no Git history with Nick's, so `git merge` does not work. Add his repo as a
remote and compare files directly:

git remote add upstream https://github.com/NickWaterton/samsung-tv-ws-api.git && git fetch upstream
git show upstream/master:example/async_art_update_from_directory.py | diff - example/async_art_update_from_directory.py


Update the pinned `samsungframetv` version with the scripts, and test before restarting services.
