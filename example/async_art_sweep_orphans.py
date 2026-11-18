#!/usr/bin/env python3
# Remove photos from the TV's My Photos that are not listed in uploaded_files.json.
# Dry run by default. Pass --delete to actually delete.
# Only photos untracked in two scans (--scan-gap seconds apart) are removed.
# If the TV is off or busy, retries, then exits cleanly (exit 0). Hard safety failures exit 1.
import argparse, asyncio, json, logging, os, sys, warnings
from samsungframetv.async_art import SamsungTVAsyncArt

warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.realpath(__file__))
log = logging.getLogger("sweep")

class Abort(Exception):
    pass

class Retry(Exception):
    pass

def tracked_ids(path):
    with open(path) as f:
        data = json.load(f)
    return {v["content_id"] for v in data.get("uploaded_files", data).values()}

def num(cid):
    return int(cid[4:]) if cid.startswith("MY_F") and cid[4:].isdigit() else 0

ALLOWED = ("jpg", "jpeg", "png", "bmp", "tif")

def settled(folder, record):
    """Raise Retry unless the slideshow script has nothing left to add, remove or replace."""
    try:
        files = {f for f in os.listdir(folder)
                 if os.path.isfile(os.path.join(folder, f)) and f.rsplit(".", 1)[-1].lower() in ALLOWED}
    except OSError as e:
        raise Retry("cannot read folder %s: %s" % (folder, e))
    with open(record) as f:
        data = json.load(f)
    rec = data.get("uploaded_files", data)
    new, gone = files - set(rec), set(rec) - files
    if new or gone:
        raise Retry("not settled: %d files not yet uploaded, %d removed from folder but still recorded" % (len(new), len(gone)))
    changed = [n for n in files if rec[n].get("modified") != os.path.getmtime(os.path.join(folder, n))]
    if changed:
        raise Retry("not settled: %d files modified since upload" % len(changed))

async def read_tv(tv):
    items = await tv.available("MY-C0002", timeout=60)
    return {i["content_id"] for i in items}

async def scan(tv, record):
    before = tracked_ids(record)
    if not before:
        raise Abort("record is empty, refusing to continue")
    try:
        on_tv = await read_tv(tv)
    except AssertionError:
        raise Abort("TV returned no photo list")
    if not on_tv:
        raise Abort("TV photo list is empty")
    if len(before & on_tv) < len(before) / 2:
        raise Abort("fewer than half of tracked photos found on TV (partial read?)")
    after = tracked_ids(record)
    if after != before:
        raise Retry("record changed during the scan (upload in progress?)")
    return on_tv, after, on_tv - after

async def attempt(a):
    settled(a.folder, a.record)
    tv = SamsungTVAsyncArt(host=a.ip, port=8002, token_file=a.token_file)
    try:
        if not await tv.on():
            raise Retry("TV is off")
        try:
            await tv.start_listening()
        except Exception as e:
            raise Retry("could not connect to TV: %s" % e)
        on_tv, tracked, first = await scan(tv, a.record)
        log.info("scan 1: on TV %d | tracked %d | untracked %d", len(on_tv), len(tracked), len(first))
        if not first:
            log.info("nothing to remove")
            return
        log.info("waiting %ds for second scan", a.scan_gap)
        await asyncio.sleep(a.scan_gap)
        on_tv2, tracked2, second = await scan(tv, a.record)
        log.info("scan 2: on TV %d | tracked %d | untracked %d", len(on_tv2), len(tracked2), len(second))
        orphans = sorted(first & second, key=num)
        skipped = first ^ second
        if skipped:
            log.info("%d photos changed status between scans, leaving them alone", len(skipped))
        if not orphans:
            log.info("nothing to remove")
            return
        log.info("untracked in both scans: %s", orphans)
        if len(orphans) > a.max:
            raise Abort("%d to delete exceeds --max %d" % (len(orphans), a.max))
        if not a.delete:
            log.info("DRY RUN: nothing deleted. Use --delete to remove these.")
            return
        for i in range(0, len(orphans), 50):
            await tv.delete_list(orphans[i:i + 50])
        try:
            left = sorted((await read_tv(tv)) - tracked_ids(a.record), key=num)
            log.info("deleted %d. untracked remaining: %s", len(orphans), left)
        except AssertionError:
            log.warning("deleted %d, but could not re-read the TV list to verify", len(orphans))
    finally:
        try:
            await tv.close()
        except Exception:
            pass

async def main():
    p = argparse.ArgumentParser(description="Sweep untracked photos from a Frame TV")
    p.add_argument("ip")
    p.add_argument("-t", "--token_file", default=os.path.join(HERE, "token_file.txt"))
    p.add_argument("-r", "--record", default=os.path.join(HERE, "uploaded_files.json"))
    p.add_argument("--folder", default="/media/adam/DRPHOTOUSB/Gallery")
    p.add_argument("--max", type=int, default=100000, help="abort if more than this many would be deleted")
    p.add_argument("--scan-gap", type=int, default=60, help="seconds between the two scans")
    p.add_argument("--retries", type=int, default=6, help="attempts if the TV is off or busy")
    p.add_argument("--retry-wait", type=int, default=600, help="seconds between attempts")
    p.add_argument("--delete", action="store_true", help="actually delete (default is dry run)")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout)
    for n in range(1, a.retries + 1):
        try:
            await attempt(a)
            return 0
        except Retry as e:
            log.info("attempt %d of %d: %s", n, a.retries, e)
            if n < a.retries:
                await asyncio.sleep(a.retry_wait)
        except Abort as e:
            log.error("ABORT: %s", e)
            return 1
    log.info("TV not ready after %d attempts, skipping this run", a.retries)
    return 0

sys.exit(asyncio.run(main()))
