# Created by J A Said
    # Removes devices from a Meraki Organisation
    # Selects only devices that are not connected to a network
    # devices can be filtered by date/or model if required
 

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from getpass import getpass
import csv
import re
import time
from datetime import datetime, timedelta, UTC
import logging
import traceback
import sys

# ---------- Logging Setup ----------
log_filename = f"meraki_inventory_script_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_and_print(message, level='info'):
    print(message)
    if level == 'debug':
        logging.debug(message)
    elif level == 'info':
        logging.info(message)
    elif level == 'error':
        logging.error(message)
    elif level == 'warning':
        logging.warning(message)

def safe_exit(code=0, msg=None):
    if msg:
        log_and_print(msg, level='info')
    log_and_print("Done. Full log written to: " + log_filename, level='info')
    sys.exit(code)

# ---------- Helpers ----------
def parse_meraki_ts(ts: str) -> datetime:
    """
    Parse Meraki/RFC3339 timestamps like:
    2023-07-21T10:11:36Z / .625090Z / +00:00
    Returns an aware datetime in UTC.
    """
    if not ts:
        raise ValueError("empty timestamp")
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", s)
        if not m:
            raise
        s2 = m.group(1) + "+00:00"
        dt = datetime.fromisoformat(s2)
    return dt.astimezone(UTC)

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

# ---------- API Setup ----------
def validate_api_key(key):
    # Allow upper/lower hex (Meraki keys are 40 chars)
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', key):
        logging.error("Invalid API key format")
        return False
    return True

MAX_API_KEY_ATTEMPTS = 4
attempts = 0
while attempts < MAX_API_KEY_ATTEMPTS:
    API_KEY = getpass("Enter your Meraki API key (hidden): ")
    if validate_api_key(API_KEY):
        break
    else:
        attempts += 1
        print(f"❌ This API key is invalid. Please double check and input the correct API key. ({MAX_API_KEY_ATTEMPTS - attempts} attempt(s) left)")
        logging.error(f"Invalid API key attempt {attempts}")
else:
    print("❌ Maximum attempts reached. Exiting.")
    sys.exit(1)

BASE_URL = "https://api.meraki.com/api/v1"
HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Requests Session with retries and backoff
session = requests.Session()
retry = Retry(
    total=8,
    backoff_factor=1.5,  # exponential: 1.5s, 3s, 4.5s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    respect_retry_after_header=True
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

def log_rate_headers(resp):
    for h in ["X-Rate-Limit-Remaining", "X-Rate-Limit-Reset", "X-Request-Id"]:
        if h in resp.headers:
            logging.info(f"{h}: {resp.headers[h]}")

def api_request(method, endpoint, params=None, json=None, timeout=30):
    """
    Makes an API request with retry/backoff and gentle client-side throttling.
    Respects server Retry-After on 429.
    """
    url = f"{BASE_URL}{endpoint}"
    while True:
        resp = session.request(method, url, headers=HEADERS, params=params, json=json, timeout=timeout)
        log_rate_headers(resp)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", "2"))
            log_and_print(f"Hit rate limit. Waiting {wait}s then retrying {url}", level='warning')
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            rate_rem = resp.headers.get("X-Rate-Limit-Remaining")
            rate_res = resp.headers.get("X-Rate-Limit-Reset")
            log_and_print(f"API error at {url}: {e} (remaining={rate_rem}, reset={rate_res})", level='error')
            logging.error(traceback.format_exc())
            raise
        # Gentle throttle after a successful call to avoid hammering
        time.sleep(0.15)  # ~6–7 req/s max
        # Some endpoints may return no body (204); handle gracefully
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

# ---------- Step 1: API and org selection ----------
logging.info("Script started")

try:
    orgs = api_request('GET', '/organizations')
except Exception as e:
    log_and_print(f"Error fetching organizations: {e}", level='error')
    sys.exit(1)

if not isinstance(orgs, list) or not orgs:
    safe_exit(1, "No organizations returned or invalid response structure.")

print("Organizations:")
for idx, org in enumerate(orgs, 1):
    print(f"{idx}. {org.get('name','<no name>')} (ID: {org.get('id','<no id>')})")

while True:
    try:
        org_choice = int(input("Select organization by number: "))
        if 1 <= org_choice <= len(orgs):
            orgid = orgs[org_choice - 1]['id']
            break
        else:
            print("Please select a valid number.")
    except ValueError:
        print("Invalid input. Please enter a number.")

# ---------- Step 2: Fetch inventory devices ----------
def get_all_inventory_devices(orgid):
    devices = []
    starting_after = None
    while True:
        params = {"perPage": 1000}
        if starting_after:
            params["startingAfter"] = starting_after
        resp = api_request('GET', f"/organizations/{orgid}/inventoryDevices", params=params)
        if not isinstance(resp, list):
            logging.error("Unexpected response when fetching inventory devices (not a list).")
            break
        devices.extend(resp)
        if len(resp) < 1000:
            break
        starting_after = resp[-1].get('serial')
        # Pacing between pages to reduce burstiness
        time.sleep(0.2)
    return devices

try:
    print("Retrieving inventory devices...")
    devices = get_all_inventory_devices(orgid)
    log_and_print(f"Found {len(devices)} devices in inventory.")
except Exception as e:
    log_and_print(f"Error fetching inventory devices: {e}", level='error')
    sys.exit(1)

# ---------- Step 3: Filter by claimed date (optional) ----------
user_input = input("Show only devices claimed more than how many months ago? (press Enter to skip): ").strip()
months = None
if user_input:
    try:
        months = int(user_input)
    except ValueError:
        print("Invalid number. Showing all unassigned devices.")
        months = None

unassigned_devices = []
cutoff_date = None
if months:
    # ~30-day months heuristic; avoids extra dependencies
    cutoff_date = datetime.now(UTC) - timedelta(days=months * 30)

for device in devices:
    network_id = device.get('networkId')
    if network_id:  # assigned to a network, skip
        continue

    if months:
        claimed_at = device.get('claimedAt')
        if not claimed_at:
            logging.warning(f"No claimedAt for device {device.get('serial','N/A')}")
            continue
        try:
            claimed_dt = parse_meraki_ts(claimed_at)
        except Exception:
            logging.warning(f"Date parsing failed for device {device.get('serial','N/A')}: {claimed_at}")
            continue
        if claimed_dt < cutoff_date:
            unassigned_devices.append(device)
    else:
        # No age filter: include all unassigned
        unassigned_devices.append(device)

log_and_print(
    f"Found {len(unassigned_devices)} unassigned devices" +
    (f" claimed more than {months} months ago." if months else ".")
)

# ---- EARLY EXIT #1: nothing unassigned after age filter ----
if not unassigned_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria (unassigned/age filter). Exiting without changes.")

# ---------- Step 3b: Model filter (optional) ----------
all_models = sorted(set([dev.get('model', 'N/A') for dev in unassigned_devices]))
print(f"\nAvailable models in filtered inventory: {', '.join(all_models) if all_models else '(none)'}")
model_input = input("Enter model(s) to remove (comma separated, blank for all): ").strip().upper()
selected_models = set([m.strip() for m in model_input.split(",") if m.strip()]) if model_input else set()

filtered_devices = []
for device in unassigned_devices:
    dev_model = device.get('model', '').upper()
    if not selected_models or dev_model in selected_models:
        filtered_devices.append(device)

if selected_models:
    log_and_print(f"User selected models for removal: {', '.join(selected_models)}")
log_and_print(f"Devices eligible for removal after model filter: {len(filtered_devices)}")

# ---- EARLY EXIT #2: nothing after model filter ----
if not filtered_devices:
    safe_exit(0, "Note: there are no devices that fit the criteria for removal after model filtering. Exiting without changes.")

# ---------- Step 4: Export preview to CSV (optional) ----------
export = input("Export these devices to CSV? (y/n): ").lower()
if export == 'y':
    filename = f"meraki_unassigned_inventory_{orgid}.csv"
    if filtered_devices:
        keys = sorted(set().union(*(d.keys() for d in filtered_devices)))  # robust header
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(filtered_devices)
        log_and_print(f"Filtered inventory exported to {filename}")
    else:
        log_and_print("No devices to export.", level='warning')

# ---------- Step 5: Dry-Run Preview ----------
print("\n--- DRY RUN: Devices that WOULD be removed ---")
for device in filtered_devices:
    print(
        f"Model: {device.get('model', 'N/A')}, "
        f"Serial: {device.get('serial', 'N/A')}, "
        f"MAC: {device.get('mac', 'N/A')}, "
        f"Name: {device.get('name', 'N/A')}, "
        f"Claimed At: {device.get('claimedAt', 'N/A')}"
    )
log_and_print(f"Dry-run previewed {len(filtered_devices)} devices for removal.")
confirm = input("\nProceed with ACTUAL removal from the org? (y/n): ").strip().lower()

# ---------- Step 6: Removal and logging ----------
if confirm != 'y':
    safe_exit(0, "Cancelled by user. Exiting without changes.")

removal_log = []

def remove_devices_from_org(orgid, serials):
    endpoint = f"/organizations/{orgid}/inventory/release"
    payload = {"serials": serials}
    return api_request('POST', endpoint, json=payload)

print("\nRemoving devices...")
serials_all = [d.get('serial') for d in filtered_devices if d.get('serial')]
# Batch size kept modest to reduce server load; adjust as needed
for batch in chunked(serials_all, 100):
    try:
        remove_devices_from_org(orgid, batch)
        removed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        index = {d.get('serial'): d for d in filtered_devices}
        for serial in batch:
            dev = index.get(serial, {})
            mac = dev.get('mac', 'N/A')
            dev_model = dev.get('model', 'N/A')
            dev_name = dev.get('name', 'N/A')
            msg = f"Removed {serial} (Model: {dev_model}, Name: {dev_name}, MAC: {mac}) at {removed_at}"
            log_and_print(msg)
            removal_log.append({
                "serial": serial,
                "mac": mac,
                "model": dev_model,
                "name": dev_name,
                "removed_at": removed_at
            })
    except Exception as e:
        error_details = traceback.format_exc()
        log_and_print(f"Failed to remove batch of {len(batch)}: {e}", level='error')
        logging.error(error_details)
    time.sleep(0.5)

# Output removal log to CSV
if removal_log:
    removal_filename = f"meraki_removed_inventory_{orgid}.csv"
    with open(removal_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["serial", "mac", "model", "name", "removed_at"])
        writer.writeheader()
        writer.writerows(removal_log)
    log_and_print(f"\nRemoval log exported to {removal_filename}")
else:
    log_and_print("No devices available for removal, so no removal log was created.")

safe_exit(0)

