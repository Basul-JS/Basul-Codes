# Created by J A Said
# Rebinds network to a New Template
# Template is auto selected based on the model of MX that is been added 
    # Template selection logic 
    
import requests
import logging
import re
import json
from datetime import datetime
from getpass import getpass
import csv
import os
import time
import signal
import sys
from typing import Any, Dict, List, Optional, Tuple, Set, Union

# =====================
# Config & Constants
# =====================
EXCLUDED_VLANS = {100, 110, 210, 220, 230, 235, 240}
REQUEST_TIMEOUT = 30  # seconds
BASE_URL = "https://api.meraki.com/api/v1"
MAX_RETRIES = 5

# Logging setup
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    filename=f"meraki_script_{timestamp}.log",
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
CSV_LOGFILE = f"meraki_techboost25_rebind_{timestamp}.csv"

# =====================
# Utility: CSV audit log
# =====================
def log_change(
    event: str,
    details: str,
    *,
    username: Optional[str] = None,
    device_serial: Optional[str] = None,
    device_name: Optional[str] = None,
    misc: Optional[str] = None,
    org_id: Optional[str] = None,
    org_name: Optional[str] = None,
    network_id: Optional[str] = None,
    network_name: Optional[str] = None,
) -> None:
    file_exists = os.path.isfile(CSV_LOGFILE)
    with open(CSV_LOGFILE, mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow([
                'timestamp', 'event', 'details', 'user',
                'device_serial', 'device_name', 'misc',
                'org_id', 'org_name', 'network_id', 'network_name'
            ])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            event,
            details,
            username or OPERATOR,
            device_serial or '',
            device_name or '',
            misc or '',
            org_id or '',
            org_name or '',
            network_id or '',
            network_name or ''
        ])

# =====================
# Prompts
# =====================
OPERATOR = input("Enter your name or initials for audit logs: ")
DRY_RUN = input("Run in dry-run mode? (yes/no): ").strip().lower() in {'yes', 'y'}
print(f"{'DRY RUN: ' if DRY_RUN else ''}Actions will {'not ' if DRY_RUN else ''}be executed.")

# =====================
# Time-of-day warning (PROMINENT)
# =====================
now = datetime.now()
cutoff_hour = 18
cutoff_minute = 15
if not DRY_RUN and ((now.hour < cutoff_hour) or (now.hour == cutoff_hour and now.minute < cutoff_minute)):
    print("\n" + "="*80)
    print("⚠️  WARNING: YOU ARE ABOUT TO MAKE LIVE CHANGES TO THE NETWORK ⚠️")
    print("This may bring down the network if applied during business hours.")
    print(f"Current time: {now.strftime('%H:%M')}")
    print("Recommended run time: AFTER 18:15.")
    print("="*80 + "\n")
    confirm = input("❗ Type 'YES' to proceed, or anything else to abort: ").strip()
    if confirm.upper() != "YES":
        print("❌ Aborting script.")
        raise SystemExit(1)

# =====================
# API auth
# =====================
def validate_api_key(key: str) -> bool:
    return bool(re.fullmatch(r'[A-Fa-f0-9]{40}', key or ''))

MAX_API_KEY_ATTEMPTS = 4
attempts = 0
API_KEY = None
while attempts < MAX_API_KEY_ATTEMPTS:
    API_KEY = getpass("Enter your Meraki API key (hidden): ")
    if validate_api_key(API_KEY):
        break
    attempts += 1
    print(f"❌ Invalid API key. ({MAX_API_KEY_ATTEMPTS - attempts} attempt(s) left)")
else:
    print("❌ Maximum attempts reached. Exiting.")
    raise SystemExit(1)

HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Graceful abort
_aborted = False
def _handle_sigint(signum, frame):
    global _aborted
    _aborted = True
    print("\nReceived Ctrl+C — attempting graceful shutdown...")
    log_change('workflow_abort', 'User interrupted with SIGINT')
signal.signal(signal.SIGINT, _handle_sigint)

# =====================
# HTTP layer
# =====================
class MerakiAPIError(Exception):
    def __init__(self, status_code: int, text: str, json_body: Optional[Any], url: str):
        super().__init__(f"Meraki API error: {status_code} {text}")
        self.status_code = status_code
        self.text = text
        self.json_body = json_body
        self.url = url

def _request(method: str, path: str, *, params=None, json_data=None) -> Any:
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == 'GET':
                resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            elif method == 'POST':
                resp = requests.post(url, headers=HEADERS, json=json_data, timeout=REQUEST_TIMEOUT)
            elif method == 'PUT':
                resp = requests.put(url, headers=HEADERS, json=json_data, timeout=REQUEST_TIMEOUT)
            elif method == 'DELETE':
                resp = requests.delete(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            else:
                raise ValueError("Unknown HTTP method")

            if resp.status_code == 429:
                wait = min(2 ** (attempt - 1), 30)
                logging.warning(f"429 rate limit for {url}. Sleeping {wait}s and retrying...")
                time.sleep(wait)
                continue

            if not resp.ok:
                try:
                    body = resp.json()
                except Exception:
                    body = None
                logging.error(f"{method} {url} -> {resp.status_code} {resp.text}")
                raise MerakiAPIError(resp.status_code, resp.text, body, url)

            if resp.text:
                try:
                    return resp.json()
                except Exception:
                    return resp.text
            return None
        except MerakiAPIError:
            raise
        except Exception as e:
            if attempt == MAX_RETRIES:
                logging.exception(f"HTTP error for {url}: {e}")
                raise
            wait = min(2 ** attempt, 30)
            logging.warning(f"HTTP exception {e} for {url}. Retrying in {wait}s...")
            time.sleep(wait)

def meraki_get(path, params=None):
    return _request('GET', path, params=params)

def meraki_post(path, data=None):
    return _request('POST', path, json_data=data)

def meraki_put(path, data=None):
    return _request('PUT', path, json_data=data)

def meraki_delete(path):
    return _request('DELETE', path)

def do_action(func, *args, **kwargs):
    if DRY_RUN:
        logging.debug(f"DRY RUN: {func.__name__} args={args} kwargs={kwargs}")
        return None
    return func(*args, **kwargs)

# =====================
# VLAN error detector (robust)
# =====================
def is_vlans_disabled_error(exc: Exception) -> bool:
    needle = "VLANs are not enabled for this network"
    try:
        if isinstance(exc, MerakiAPIError):
            if exc.status_code == 400:
                if exc.json_body and isinstance(exc.json_body, dict):
                    errs = exc.json_body.get("errors")
                    if errs and any(needle in str(e) for e in errs):
                        return True
                if needle in (exc.text or ""):
                    return True
        return needle in str(exc)
    except Exception:
        return False

# =====================
# Switch port helpers (diff + apply)
# =====================
_PORT_FIELDS = [
    "enabled", "name", "tags", "type", "vlan", "voiceVlan", "allowedVlans",
    "poeEnabled", "isolationEnabled", "rstpEnabled", "stpGuard",
    "linkNegotiation", "udld", "accessPolicyType", "accessPolicyNumber",
    "portScheduleId"
]

def _normalize_tags(value):
    if isinstance(value, list):
        return sorted(value)
    if isinstance(value, str):
        return sorted([t for t in value.split() if t])
    return []

def _port_dict_by_number(ports: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for p in ports:
        pid = p.get("portId") or p.get("number") or p.get("name")
        if pid is None:
            continue
        out[str(pid)] = p
    return out

def compute_port_overrides(live_ports: List[Dict[str, Any]], tmpl_ports: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    live = _port_dict_by_number(live_ports)
    tmpl = _port_dict_by_number(tmpl_ports)
    for pid, lp in live.items():
        tp = tmpl.get(pid)
        if not tp:
            continue
        for fld in _PORT_FIELDS:
            lv = lp.get(fld)
            tv = tp.get(fld)
            if fld == "tags":
                lv = _normalize_tags(lv)
                tv = _normalize_tags(tv)
            if lv is not None and lv != tv:
                overrides.setdefault(pid, {})[fld] = lv
    return overrides

def apply_port_overrides(serial: str, overrides: Dict[str, Dict[str, Any]]) -> None:
    for pid, patch in overrides.items():
        try:
            do_action(meraki_put, f"/devices/{serial}/switch/ports/{pid}", data=patch)
            logging.debug(f"Applied port overrides on {serial} port {pid}: {patch}")
            log_change(
                'switch_port_override',
                f"Applied port overrides on port {pid}",
                device_serial=serial,
                misc=json.dumps(patch)
            )
        except Exception:
            logging.exception(f"Failed applying port overrides on {serial} port {pid}")

# =====================
# Domain helpers (raw API)
# =====================
def fetch_matching_networks(org_id: str, partial: str) -> List[Dict[str, Any]]:
    nets = meraki_get(f"/organizations/{org_id}/networks")
    partial_lower = partial.lower()
    matches = [n for n in nets if partial_lower in n.get('name', '').lower()]
    logging.debug(f"Found {len(matches)} networks matching '{partial}'")
    return matches

def fetch_devices(
    org_id: str,
    network_id: str,
    template_id: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    devs = meraki_get(f"/networks/{network_id}/devices")

    def _mk(d):
        tags = d.get('tags', [])
        if not isinstance(tags, list):
            tags = (tags or '').split()
        return {
            'serial': d['serial'],
            'model': d['model'],
            'tags': tags,
            'address': d.get('address', ''),
            'name': d.get('name', ''),
            'switchProfileId': d.get('switchProfileId'),
            'switchProfileName': d.get('switchProfileName'),
        }

    mx = [_mk(d) for d in devs if d['model'].startswith('MX')]
    ms = [_mk(d) for d in devs if d['model'].startswith('MS')]
    mr = [_mk(d) for d in devs if d['model'].startswith('MR') or d['model'].startswith('CW916')]

    # Per-MS port overrides vs current template profile (if known)
    if template_id:
        for sw in ms:
            profile_id = sw.get('switchProfileId')
            if not profile_id:
                sw['port_overrides'] = {}
                continue
            try:
                live_ports = meraki_get(f"/devices/{sw['serial']}/switch/ports")
                tmpl_ports = meraki_get(f"/organizations/{org_id}/configTemplates/{template_id}/switch/profiles/{profile_id}/ports")
                sw['port_overrides'] = compute_port_overrides(live_ports, tmpl_ports)
                logging.debug(f"Computed {len(sw['port_overrides'])} port overrides for {sw['serial']}")
            except Exception:
                logging.exception(f"Failed computing port overrides for {sw['serial']}")
                sw['port_overrides'] = {}
    else:
        for sw in ms:
            sw['port_overrides'] = {}

    logging.debug(f"Fetched devices: MX={len(mx)}, MS={len(ms)}, MR={len(mr)}")
    log_change(
        event='fetch_devices',
        details=f"Fetched devices for network {network_id}",
        network_id=network_id,
        misc=f"mx={json.dumps(mx)}, ms={json.dumps(ms)}, mr={json.dumps(mr)}"
    )
    return mx, ms, mr

def fetch_vlan_details(network_id: str) -> List[Dict[str, Any]]:
    try:
        vlans = meraki_get(f"/networks/{network_id}/appliance/vlans")
        filtered = [v for v in vlans if int(v.get('id')) not in EXCLUDED_VLANS]
        logging.debug(f"Fetched VLANs: {len(filtered)} (excluded {len(vlans) - len(filtered)})")
        return filtered
    except MerakiAPIError as e:
        if is_vlans_disabled_error(e):
            logging.warning("VLAN endpoints unavailable because VLANs are disabled on this network (returning empty list).")
            return []
        logging.exception("Failed to fetch VLANs")
        return []
    except Exception:
        logging.exception("Failed to fetch VLANs")
        return []

def vlans_enabled(network_id: str) -> Optional[bool]:
    try:
        settings = meraki_get(f"/networks/{network_id}/appliance/vlans/settings")
        return bool(settings.get("vlansEnabled"))
    except Exception:
        logging.exception("Could not read VLANs settings")
        return None

def update_vlans(network_id: str, network_name: str, vlan_list: List[Dict[str, Any]]):
    for v in vlan_list:
        payload = {
            'applianceIp': v.get('applianceIp'),
            'subnet': v.get('subnet'),
            'dhcpHandling': v.get('dhcpHandling'),
            'fixedIpAssignments': v.get('fixedIpAssignments', {}),
            'reservedIpRanges': v.get('reservedIpRanges', []),
        }
        try:
            do_action(meraki_put, f"/networks/{network_id}/appliance/vlans/{v['id']}", data=payload)
            logging.debug(f"Updated VLAN {v['id']}")
            log_change(
                'vlan_update',
                f"Updated VLAN {v['id']}",
                device_name=f"Network: {network_id}",
                network_id=network_id,
                network_name=network_name,
                misc=json.dumps(payload)
            )
        except MerakiAPIError as e:
            if is_vlans_disabled_error(e):
                raise
            logging.exception(f"Failed to update VLAN {v.get('id')}")
        except Exception:
            logging.exception(f"Failed to update VLAN {v.get('id')}")

def classify_serials_for_binding(org_id: str, net_id: str, serials: List[str]):
    already, elsewhere, avail = [], [], []
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            nid = inv.get('networkId')
            if nid == net_id:
                already.append(s)
            elif nid:
                elsewhere.append((s, inv.get('networkName') or nid))
            else:
                avail.append(s)
        except MerakiAPIError as e:
            if e.status_code == 404:
                avail.append(s)
            else:
                logging.error(f"Error checking inventory for {s}: {e}")
        except Exception as e:
            logging.error(f"Error checking inventory for {s}: {e}")
    return already, elsewhere, avail

# ---------- Clear & remove by model (org-aware) ----------
def _clear_and_remove_models(org_id: str, network_id: str, models: Tuple[str, ...]) -> bool:
    mx, ms, mr = fetch_devices(org_id, network_id)
    all_devs = mx + ms + mr
    to_remove = [d['serial'] for d in all_devs if d['model'] in models]
    if not to_remove:
        return True
    for serial in to_remove:
        try:
            do_action(meraki_put, f"/devices/{serial}", data={"name": "", "address": ""})
            log_change('device_clear', f"Cleared config for {serial}", device_serial=serial)
        except Exception:
            logging.exception(f"Error clearing {serial}")
    try:
        for serial in to_remove:
            do_action(meraki_post, f"/networks/{network_id}/devices/remove", data={"serial": serial})
            log_change('device_removed', f"Removed device from network", device_serial=serial)
    except Exception:
        logging.exception("Error removing devices")
    return True

def remove_existing_mx64_devices(org_id: str, network_id: str) -> bool:
    return _clear_and_remove_models(org_id, network_id, ("MX64",))

def remove_existing_MR33_devices(org_id: str, network_id: str) -> bool:
    return _clear_and_remove_models(org_id, network_id, ("MR33",))

# ---------- Prompt + claim into ORG (before selecting network) ----------
def prompt_and_validate_serials(org_id: str) -> List[str]:
    """
    Ask for intended device count, then collect serials via:
      - one comma-separated line, OR
      - multiple lines (press Enter on a blank line to finish)

    - If provided count != entered count, prompt to re-enter or proceed anyway.
    - Validate each serial individually:
        * Format XXXX-XXXX-XXXX (up to 4 attempts per serial)
        * If not in org inventory (404), attempt to claim into org
    - Dedupes within the same entry (preserving order) and warns.
    Exits gracefully after 4 consecutive blank attempts at entering serials.

    NOTE: In multi-line mode, pressing Enter after entering at least one serial
    simply finishes input and DOES NOT count as a "blank attempt". The blank
    attempt limit only applies when no serials were entered at all for that try.
    """
    MAX_SERIAL_ATTEMPTS = 4
    MAX_BLANK_ATTEMPTS = 4
    serial_pattern = re.compile(r"[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}")

    # 1) Ask intended count
    while True:
        count_raw = input("How many devices/serials will you add to this org? (Enter to skip): ").strip()
        if not count_raw:
            return []
        try:
            intended_count = int(count_raw)
            if intended_count <= 0:
                print("ℹ️  Count must be a positive integer.")
                continue
            break
        except ValueError:
            print("ℹ️  Please enter a whole number (e.g., 3).")

    # 2) Outer loop to re-enter the full list on mismatch
    blank_attempts = 0
    while True:
        print("\nEnter serial numbers:")
        print(" - You can paste them all at once (comma-separated),")
        print(" - OR enter one per line and press Enter on a blank line to finish.\n")
        first_line = input("Enter serial(s): ").strip().upper()

        # Collect raw serials (either CSV or multiline)
        raw_serials: List[str] = []
        if "," in first_line:
            # CSV mode (single line)
            raw_serials = [s.strip().upper() for s in first_line.split(",") if s.strip()]
        else:
            # Multiline mode
            if first_line:
                raw_serials.append(first_line)
            # IMPORTANT: blank after at least one serial just finishes input and is NOT a "blank attempt"
            while True:
                nxt = input("Enter next serial (or blank to finish): ").strip().upper()
                if not nxt:
                    break
                raw_serials.append(nxt)

        if not raw_serials:
            blank_attempts += 1
            remaining = MAX_BLANK_ATTEMPTS - blank_attempts
            if remaining <= 0:
                print("\n❌ No serial number(s) entered after 4 attempts -----------")
                print("   Please retry when serial(s) are known *******")
                sys.exit(1)
            print(f"ℹ️  No serials provided. Try again. (attempt {blank_attempts}/{MAX_BLANK_ATTEMPTS})")
            continue

        # Split/trim already done; now dedupe preserving order
        seen: Set[str] = set()
        serial_list: List[str] = []
        for s in raw_serials:
            if s in seen:
                print(f"ℹ️  Duplicate serial '{s}' removed from input.")
                continue
            seen.add(s)
            serial_list.append(s)

        entered_count = len(serial_list)
        if entered_count != intended_count:
            print(f"⚠️  You said {intended_count} device(s) but entered {entered_count}.")
            choice = input("Proceed anyway? (yes to proceed / no to re-enter): ").strip().lower()
            if choice not in {"y", "yes"}:
                # reset blank attempts when user is actively re-entering
                blank_attempts = 0
                continue  # re-enter list

        # 3) Validate each serial
        collected: List[str] = []
        for idx, original_serial in enumerate(serial_list, start=1):
            attempts = 0
            serial = original_serial
            while attempts < MAX_SERIAL_ATTEMPTS:
                # Format check
                if not re.fullmatch(r"[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}", serial or ""):
                    attempts += 1
                    if attempts >= MAX_SERIAL_ATTEMPTS:
                        print(f"❌ Maximum attempts reached for serial #{idx} ({original_serial}). Skipping.")
                        break
                    serial = input(
                        f"Serial #{idx} '{serial}' is invalid. Re-enter (attempt {attempts+1}/{MAX_SERIAL_ATTEMPTS}): "
                    ).strip().upper()
                    continue

                # Inventory check
                try:
                    meraki_get(f"/organizations/{org_id}/inventoryDevices/{serial}")
                    print(f"✅ {serial} found in org inventory.")
                    collected.append(serial)
                    break
                except MerakiAPIError as e:
                    # Not found in org -> claim
                    if getattr(e, "status_code", None) == 404:
                        try:
                            do_action(meraki_post, f"/organizations/{org_id}/claim", data={"serials": [serial]})
                            print(f"✅ Serial '{serial}' successfully claimed into org inventory.")
                            log_change('device_claimed_inventory', "Claimed serial into org inventory", device_serial=serial)
                            collected.append(serial)
                            break
                        except Exception as claim_ex:
                            attempts += 1
                            print(f"❌ Error claiming '{serial}' into org inventory: {claim_ex}")
                            if attempts >= MAX_SERIAL_ATTEMPTS:
                                print(f"❌ Maximum attempts reached for serial #{idx}. Skipping.")
                                break
                            serial = input(
                                f"Re-enter serial #{idx} (attempt {attempts+1}/{MAX_SERIAL_ATTEMPTS}): "
                            ).strip().upper()
                            continue
                    else:
                        print(f"API Error for serial '{serial}': {e}")
                        break
                except Exception as e:
                    print(f"API Error for serial '{serial}': {e}")
                    break

        # 4) Final check vs intended
        if len(collected) != intended_count:
            print(f"⚠️  Intended: {intended_count}, Entered: {entered_count}, Validated: {len(collected)}.")
            choice = input("Proceed with validated devices anyway? (yes to proceed / no to re-enter all): ").strip().lower()
            if choice in {"y", "yes"}:
                return collected
            else:
                # reset blank attempts when user is actively re-entering
                blank_attempts = 0
                print("Okay, let's re-enter the serial list.")
                continue

        return collected

def summarize_devices_in_org(org_id: str, serials: List[str]) -> Set[str]:
    detected_mx_models: Set[str] = set()
    if not serials:
        print("No serials to summarize.")
        return detected_mx_models

    print("\nValidated / added to organization:")
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            model = inv.get('model') or 'Unknown'
            ptypes = inv.get('productTypes') or []
            ptype = ptypes[0] if isinstance(ptypes, list) and ptypes else inv.get('productType') or 'Unknown'
            name = inv.get('name') or ''
            print(f" - {s}: {model} ({ptype}){f' — {name}' if name else ''}")

            if model.startswith('MX67'):
                detected_mx_models.add('MX67')
            elif model.startswith('MX75'):
                detected_mx_models.add('MX75')
        except Exception as e:
            print(f" - {s}: (lookup failed: {e})")

    return detected_mx_models

# ---------- Claim into network using prevalidated serials ----------
def claim_devices(org_id: str, network_id: str, prevalidated_serials: Optional[List[str]] = None) -> List[str]:
    if prevalidated_serials is not None:
        valids = prevalidated_serials
    else:
        valids = prompt_and_validate_serials(org_id)

    if not valids:
        print("❌ No valid serials.")
        return []

    already, elsewhere, avail = classify_serials_for_binding(org_id, network_id, valids)
    if elsewhere:
        print("⚠️ In use elsewhere:")
        for s, name in elsewhere:
            print(f" - {s} in {name}")

    mx_models: List[str] = []
    for s in avail:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            if (inv.get('model') or '').startswith('MX'):
                mx_models.append(inv['model'])
        except Exception:
            pass
    if len(set(mx_models)) > 1:
        print("❌ MX warm spare models mismatch. Aborting.")
        return []
    if not avail:
        print("ℹ️ No newly available devices to claim to the network (perhaps they’re already in this network).")
        return already

    try:
        remove_existing_mx64_devices(org_id, network_id)
        do_action(meraki_post, f"/networks/{network_id}/devices/claim", data={"serials": avail})
        for s in avail:
            log_change('device_claimed', f"Claimed device to network", device_serial=s)
        return avail
    except Exception:
        logging.exception("Failed to claim/bind")
        return []

# ---------- ORDERING HELPERS ----------
def select_primary_mx(org_id: str, serials: List[str]) -> Optional[str]:
    """
    If multiple MX are present, ask which should be PRIMARY (mx-01).
    If user presses Enter or types 'skip'/'cancel', auto-select the MX with the lowest serial.
    """
    mx_candidates: List[Tuple[str, str]] = []
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            model = (inv.get('model') or '').upper()
            if model.startswith('MX'):
                mx_candidates.append((s, model))
        except Exception:
            logging.exception(f"Unable to read inventory for {s}")

    if len(mx_candidates) == 0:
        return None
    if len(mx_candidates) == 1:
        return mx_candidates[0][0]

    auto_choice = sorted([s for s, _ in mx_candidates])[0]

    print("\nMultiple MX devices detected in the claimed list:")
    for idx, (s, m) in enumerate(mx_candidates, 1):
        print(f" {idx}. {s}  ({m})")
    sel = input("Select which MX should be PRIMARY (mx-01). "
                "Enter number, or press Enter / type 'skip'/'cancel' to auto-select: ").strip().lower()

    if not sel or sel in {'skip', 'cancel'}:
        print(f"ℹ️  No explicit choice made. Auto-selecting PRIMARY MX: {auto_choice}")
        return auto_choice

    if sel.isdigit():
        i = int(sel)
        if 1 <= i <= len(mx_candidates):
            return mx_candidates[i-1][0]

    print(f"ℹ️  Invalid selection. Auto-selecting PRIMARY MX: {auto_choice}")
    return auto_choice

def select_device_order(org_id: str, serials: List[str], kind: str) -> List[str]:
    """
    Choose an explicit order for devices of a given type (MR/CW916 or MS).
    If user presses Enter / 'skip'/'cancel', auto-order by serial (alphanumeric).
    kind must be 'MR' or 'MS'.
    Returns ordered list of serials for that kind.
    """
    filtered: List[Tuple[str, str]] = []  # (serial, model)
    for s in serials:
        try:
            inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
            model = (inv.get('model') or '').upper()
            if kind == 'MR' and (model.startswith('MR') or model.startswith('CW916')):
                filtered.append((s, model))
            elif kind == 'MS' and model.startswith('MS'):
                filtered.append((s, model))
        except Exception:
            logging.exception(f"Unable to read inventory for {s}")

    if len(filtered) <= 1:
        return [s for s, _ in filtered]

    auto_order = sorted([s for s, _ in filtered])

    print(f"\nSelect ordering for {kind} devices (enter a comma-separated list of indices).")
    for idx, (s, m) in enumerate(filtered, 1):
        print(f" {idx}. {s}  ({m})")
    raw = input(f"Desired order for {kind} (e.g. 2,1,3). "
                "Press Enter / type 'skip'/'cancel' to auto-order: ").strip().lower()

    if not raw or raw in {'skip', 'cancel'}:
        print(f"ℹ️  Auto-ordering {kind} devices by serial: {', '.join(auto_order)}")
        return auto_order

    parts = [p.strip() for p in raw.split(',') if p.strip()]
    if all(p.isdigit() and 1 <= int(p) <= len(filtered) for p in parts) and len(parts) == len(filtered):
        return [filtered[int(p)-1][0] for p in parts]

    print(f"ℹ️  Invalid list. Auto-ordering {kind} devices by serial: {', '.join(auto_order)}")
    return auto_order

# ---------- Warm spare primary enforcement ----------
def ensure_primary_mx(network_id: str, desired_primary_serial: Optional[str]) -> None:
    """
    Ensure the warm spare primary equals the serial selected as mx-01.
    If warm spare is enabled and the current primary != desired, perform a swap.

    Uses:
      GET  /networks/{networkId}/appliance/warmSpare
      POST /networks/{networkId}/appliance/warmSpare/swap
    """
    if not desired_primary_serial:
        return

    try:
        status = meraki_get(f"/networks/{network_id}/appliance/warmSpare") or {}
        enabled = bool(status.get("enabled"))
        current_primary = status.get("primarySerial")

        if not enabled:
            print("ℹ️  Warm spare is not enabled on this network; cannot swap primary automatically.")
            log_change('mx_warmspare_not_enabled',
                       "Warm spare not enabled; no primary swap performed",
                       network_id=network_id)
            return

        if current_primary and current_primary.upper() == desired_primary_serial.upper():
            print(f"✅ Warm spare already has the correct primary ({desired_primary_serial}).")
            return

        print(f"🔁 Swapping warm spare primary to {desired_primary_serial} ...")
        do_action(meraki_post, f"/networks/{network_id}/appliance/warmSpare/swap")
        log_change('mx_warmspare_swap',
                   f"Swapped warm spare primary to {desired_primary_serial}",
                   device_serial=desired_primary_serial,
                   network_id=network_id)
        print("✅ Warm spare primary swap requested.")

    except Exception as e:
        logging.exception("Failed to ensure warm spare primary")
        print(f"❌ Failed to verify/swap warm spare primary: {e}")

# ---------- Naming & configuration (with ordering) ----------
def name_and_configure_claimed_devices(
    org_id: str,
    network_id: str,
    network_name: str,
    serials: List[str],
    ms_list: List[Dict[str, Any]],
    mr_list: List[Dict[str, Any]],
    tpl_profile_map: Dict[str, str],
    old_mx_devices: Optional[List[Dict[str, Any]]] = None,
    old_mr_devices: Optional[List[Dict[str, Any]]] = None,
    primary_mx_serial: Optional[str] = None,
    mr_order: Optional[List[str]] = None,
    ms_order: Optional[List[str]] = None,
):
    """
    Renames and configures newly-claimed devices using optional ordering.
    - primary_mx_serial: that MX becomes ...-mx-01 (others mx-02, mx-03, ...)
    - mr_order: explicit AP order (first -> ...-ap-01)
    - ms_order: explicit switch order (first -> ...-ms-01)
    """
    prefix = '-'.join(network_name.split('-')[:2]).lower()
    counts = {'MX': 1, 'MR': 1, 'MS': 1}
    old_mr33s = sorted([d for d in (old_mr_devices or []) if d['model'] == 'MR33'], key=lambda x: x.get('name', ''))
    old_mxs_sorted = sorted((old_mx_devices or []) if old_mx_devices else [], key=lambda x: x.get('name', ''))

    # Lookup models once
    inv_by_serial: Dict[str, Dict[str, Any]] = {}
    for s in serials:
        try:
            inv_by_serial[s] = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
        except Exception:
            logging.exception(f"Failed inventory lookup for {s}")
            inv_by_serial[s] = {}

    # Partition
    mx_serials = [s for s in serials if (inv_by_serial.get(s, {}).get('model') or '').upper().startswith('MX')]
    mr_serials = [s for s in serials if any((inv_by_serial.get(s, {}).get('model') or '').upper().startswith(p) for p in ('MR', 'CW916'))]
    ms_serials = [s for s in serials if (inv_by_serial.get(s, {}).get('model') or '').upper().startswith('MS')]

    # MX ordering: selected primary first
    if primary_mx_serial and primary_mx_serial in mx_serials:
        mx_serials = [primary_mx_serial] + [s for s in mx_serials if s != primary_mx_serial]

    # MR / MS ordering override
    if mr_order:
        mr_serials = [s for s in mr_order if s in mr_serials]
    if ms_order:
        ms_serials = [s for s in ms_order if s in ms_serials]

    # --- MX ---
    mx_idx = 0
    for s in mx_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {}
        data['name'] = f"{prefix}-mx-{counts['MX']:02}"
        if mx_idx < len(old_mxs_sorted):
            data['address'] = old_mxs_sorted[mx_idx].get('address', '')
            data['tags'] = old_mxs_sorted[mx_idx].get('tags', [])
        else:
            data['address'] = ''
            data['tags'] = []
        mx_idx += 1
        counts['MX'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and reconfigured device {s} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''),
                       misc=f"tags={data.get('tags', [])}, address={data.get('address', '')}")
        except Exception:
            logging.exception(f"Failed configuring {s} (MX)")

    # --- MR ---
    ap_idx = 0
    for s in mr_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {'name': f"{prefix}-ap-{counts['MR']:02}"}
        if ap_idx < len(old_mr33s):
            data['tags'] = old_mr33s[ap_idx].get('tags', [])
            data['address'] = old_mr33s[ap_idx].get('address', '')
        else:
            data['tags'] = []
            data['address'] = ''
        ap_idx += 1
        counts['MR'] += 1
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and reconfigured device {s} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''),
                       misc=f"tags={data.get('tags', [])}, address={data.get('address', '')}")
        except Exception:
            logging.exception(f"Failed configuring {s} (MR)")

    # --- MS ---
    for s in ms_serials:
        mdl = (inv_by_serial.get(s, {}).get('model') or '')
        data: Dict[str, Any] = {'name': f"{prefix}-ms-{counts['MS']:02}"}
        counts['MS'] += 1
        prof_name = ms_list[0].get('switchProfileName') if ms_list else None
        prof_id = tpl_profile_map.get(prof_name) if prof_name else None
        if prof_id:
            data['switchProfileId'] = prof_id
        try:
            do_action(meraki_put, f"/devices/{s}", data=data)
            log_change('device_update', f"Renamed and reconfigured device {s} ({mdl})",
                       device_serial=s, device_name=data.get('name', ''),
                       misc=f"tags={data.get('tags', [])}, address={data.get('address', '')}")
        except Exception:
            logging.exception(f"Failed configuring {s} (MS)")

def remove_recently_added_tag(network_id: str):
    devs = meraki_get(f"/networks/{network_id}/devices")
    for d in devs:
        tags = d.get('tags', [])
        if not isinstance(tags, list):
            tags = (tags or '').split()
        if 'recently-added' in tags:
            updated_tags = [t for t in tags if t != 'recently-added']
            print(f"Removing 'recently-added' tag from {d['model']} {d['serial']}")
            try:
                do_action(meraki_put, f"/devices/{d['serial']}", data={"tags": updated_tags})
                log_change(
                    'tag_removed',
                    "Removed 'recently-added' tag",
                    device_serial=d['serial'],
                    device_name=d.get('name', ''),
                    misc=f"old_tags={tags}, new_tags={updated_tags}"
                )
            except Exception:
                logging.exception(f"Failed to remove 'recently-added' from {d['serial']}")

# ---------- Template rebind helpers (with rollback) ----------

# NEW: Helper to choose a template by VLAN count
def _pick_template_by_vlan_count(templates: List[Dict[str, Any]], vlan_count: Optional[int]) -> Optional[Dict[str, Any]]:
    """
    Auto-pick a template based on VLAN count.
      - 3 VLANs  -> name like '*NoLegacy*MX75'
      - 5 VLANs  -> name like '*3 X DATA_VLAN*MX75'
    Returns the first matching template dict, or None if no match.
    """
    if vlan_count not in (3, 5):
        return None

    patterns = []
    if vlan_count == 3:
        # e.g. 'something-NoLegacy-something-MX75'
        patterns = [r'NO\s*LEGACY.*MX*\b']
    elif vlan_count == 5:
        # e.g. 'something-3 X DATA_VLAN-something-MX75'
        patterns = [r'3\s*X\s*DATA[_\s-]*VLAN.*MX75\b']

    for pat in patterns:
        rx = re.compile(pat, re.IGNORECASE)
        for t in templates:
            name = (t.get('name') or '')
            if rx.search(name):
                return t
    return None

def list_and_rebind_template(
    org_id: str,
    network_id: str,
    current_id: Optional[str],
    network_name: str,
    *,
    pre_change_devices: Optional[List[Dict[str, Any]]] = None,
    pre_change_vlans: Optional[List[Dict[str, Any]]] = None,
    pre_change_template: Optional[str] = None,
    claimed_serials: Optional[List[str]] = None,
    removed_serials: Optional[List[str]] = None,
    ms_list: Optional[List[Dict[str, Any]]] = None,
    mx_model_filter: Optional[str] = None,
    vlan_count: Optional[int] = None,   # <-- NEW
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Interactive template selection & (re)bind with robust rollback behavior.
    Now proposes an auto-picked template based on VLAN count; you can accept or reject it.
    Returns: (new_template_id, new_template_name, rolled_back)
    """
    skip_attempts = 0

    while True:
        print(f"\nCurrent network: {network_name} (ID: {network_id})")
        log_change('current_network_info', f"Current network: {network_name}",
                   org_id=org_id, network_id=network_id, network_name=network_name)

        if current_id:
            try:
                curr = meraki_get(f"/organizations/{org_id}/configTemplates/{current_id}")
                print(f"Bound template: {curr.get('name','<unknown>')} (ID: {current_id})\n")
                log_change('bound_template_info',
                           f"Bound template {curr.get('name','<unknown>')} ({current_id})",
                           network_id=network_id, network_name=network_name)
            except Exception:
                print(f"Bound template ID: {current_id}\n")
                log_change('bound_template_info', f"Bound template ID: {current_id}",
                           network_id=network_id, network_name=network_name)
        else:
            print("No template bound.\n")

        # Fetch and optionally filter templates by suffix (MX67/MX75)
        temps = meraki_get(f"/organizations/{org_id}/configTemplates")
        filtered = temps
        if mx_model_filter in {'MX67', 'MX75'}:
            suffix = mx_model_filter.upper()
            filtered = [t for t in temps if (t.get('name') or '').strip().upper().endswith(suffix)]
            if not filtered:
                print(f"(No templates ending with {suffix}; showing all templates instead.)")
                filtered = temps

        # --- VLAN-count suggestion with confirmation ---
        offered_auto = False
        if not offered_auto:
            offered_auto = True
            auto_choice = _pick_template_by_vlan_count(filtered, vlan_count) or _pick_template_by_vlan_count(temps, vlan_count)
            if auto_choice:
                print(f"Suggested template based on VLAN count ({vlan_count}): {auto_choice['name']} (ID: {auto_choice['id']})")
                resp = input("Use this template? (Y/n): ").strip().lower()
                if resp in {"", "y", "yes"}:
                    # proceed to bind the suggested template
                    try:
                        if current_id:
                            do_action(meraki_post, f"/networks/{network_id}/unbind")

                        do_action(meraki_post, f"/networks/{network_id}/bind", data={"configTemplateId": auto_choice['id']})
                        log_change('template_bind',
                                   f"Auto-bound (confirmed) to template {auto_choice['name']} (ID: {auto_choice['id']})",
                                   device_name=network_name, network_id=network_id, network_name=network_name)
                        print(f"✅ Bound to {auto_choice['name']}")
                        return auto_choice['id'], auto_choice['name'], False

                    except MerakiAPIError as e:
                        logging.error(f"Error binding suggested template: {e}")
                        must_rollback = True if current_id else False
                        if is_vlans_disabled_error(e):
                            print("❌ VLANs are disabled for this network. Binding failed.")
                            must_rollback = True
                        if must_rollback:
                            print("🚨 Initiating rollback due to failed bind...")
                            rollback_all_changes(
                                network_id=network_id,
                                pre_change_devices=pre_change_devices or [],
                                pre_change_vlans=pre_change_vlans or [],
                                pre_change_template=pre_change_template,
                                org_id=org_id,
                                claimed_serials=claimed_serials or [],
                                removed_serials=removed_serials or [],
                                ms_list=ms_list or [],
                                network_name=network_name,
                            )
                            return current_id, None, True
                        print("Auto-bind failed; falling back to manual selection.\n")

                    except Exception as e:
                        logging.error(f"Unexpected error during auto-bind: {e}")
                        if current_id:
                            print("🚨 Unexpected error after unbind — initiating rollback...")
                            rollback_all_changes(
                                network_id=network_id,
                                pre_change_devices=pre_change_devices or [],
                                pre_change_vlans=pre_change_vlans or [],
                                pre_change_template=pre_change_template,
                                org_id=org_id,
                                claimed_serials=claimed_serials or [],
                                removed_serials=removed_serials or [],
                                ms_list=ms_list or [],
                                network_name=network_name,
                            )
                            return current_id, None, True
                        print("Auto-bind failed; falling back to manual selection.\n")
                else:
                    print("Okay — we’ll choose a different template from the list below.\n")

        # --- Manual selection list ---
        for i, t in enumerate(filtered, 1):
            print(f"{i}. {t['name']} (ID: {t['id']})")

        sel = input(
            "Select template # (or press Enter / type 'skip'/'cancel' to cancel — a second cancel will ROLLBACK): "
        ).strip().lower()

        # Skip/cancel handling with rollback on second time
        if sel in {"", "skip", "cancel"}:
            skip_attempts += 1
            if skip_attempts == 1:
                print("⚠️  You chose to cancel template selection.")
                print("If you cancel again, the process will be ROLLED BACK immediately.")
                continue
            # Second cancel -> rollback entire session state using provided snapshots
            print("🚨 Cancelled twice — initiating rollback...")
            log_change('rollback_trigger', 'User cancelled twice during template selection')
            rollback_all_changes(
                network_id=network_id,
                pre_change_devices=pre_change_devices or [],
                pre_change_vlans=pre_change_vlans or [],
                pre_change_template=pre_change_template,
                org_id=org_id,
                claimed_serials=claimed_serials or [],
                removed_serials=removed_serials or [],
                ms_list=ms_list or [],
                network_name=network_name,
            )
            return current_id, None, True  # rolled_back=True

        if not sel.isdigit():
            print("Invalid selection. Please enter a valid number or press Enter to cancel.")
            continue

        idx = int(sel) - 1
        if idx < 0 or idx >= len(filtered):
            print("Invalid template number.")
            continue

        chosen = filtered[idx]
        if chosen['id'] == current_id:
            print("No change (already bound to that template).")
            return current_id, chosen['name'], False

        # Attempt to unbind (if currently bound) and bind the new template
        try:
            # Unbind (safe: if not bound, skip)
            if current_id:
                do_action(meraki_post, f"/networks/{network_id}/unbind")

            # Bind the new template
            do_action(meraki_post, f"/networks/{network_id}/bind", data={"configTemplateId": chosen['id']})
            log_change('template_bind',
                       f"Bound to template {chosen['name']} (ID: {chosen['id']})",
                       device_name=network_name, network_id=network_id, network_name=network_name)
            print(f"✅ Bound to {chosen['name']}")
            return chosen['id'], chosen['name'], False

        except MerakiAPIError as e:
            # If we already unbound and bind failed, or VLANs disabled, roll back
            logging.error(f"Error binding template: {e}")
            must_rollback = True if current_id else False  # we unbound; network is mid-change
            if is_vlans_disabled_error(e):
                print("❌ VLANs are not enabled for this network. Binding failed and state may be partial.")
                must_rollback = True

            if must_rollback:
                print("🚨 Initiating rollback due to failed bind...")
                rollback_all_changes(
                    network_id=network_id,
                    pre_change_devices=pre_change_devices or [],
                    pre_change_vlans=pre_change_vlans or [],
                    pre_change_template=pre_change_template,
                    org_id=org_id,
                    claimed_serials=claimed_serials or [],
                    removed_serials=removed_serials or [],
                    ms_list=ms_list or [],
                    network_name=network_name,
                )
                return current_id, None, True  # rolled_back=True

            print(f"❌ Failed to bind template: {e}. You can try again or cancel.")
            continue

        except Exception as e:
            logging.error(f"Unexpected error during bind: {e}")
            if current_id:
                print("🚨 Unexpected error after unbind — initiating rollback...")
                rollback_all_changes(
                    network_id=network_id,
                    pre_change_devices=pre_change_devices or [],
                    pre_change_vlans=pre_change_vlans or [],
                    pre_change_template=pre_change_template,
                    org_id=org_id,
                    claimed_serials=claimed_serials or [],
                    removed_serials=removed_serials or [],
                    ms_list=ms_list or [],
                    network_name=network_name,
                )
                return current_id, None, True
            print(f"❌ Unexpected error: {e}. You can try again or cancel.")
            continue

def bind_network_to_template(
    org_id: str,
    network_id: str,
    tpl_id: Optional[str],
    vlan_list: List[Dict[str, Any]],
    network_name: str,
    *,
    pre_change_devices,
    pre_change_vlans,
    pre_change_template,
    claimed_serials,
    removed_serials,
    ms_list
):
    if not tpl_id:
        return
    time.sleep(5)

    enabled = vlans_enabled(network_id)
    if enabled is False:
        print("❌ VLANs are disabled on this network after binding. Rolling back immediately...")
        rollback_all_changes(
            network_id=network_id,
            pre_change_devices=pre_change_devices or [],
            pre_change_vlans=pre_change_vlans or [],
            pre_change_template=pre_change_template,
            org_id=org_id,
            claimed_serials=claimed_serials or [],
            removed_serials=removed_serials or [],
            ms_list=ms_list or [],
            network_name=network_name,
        )
        log_change('workflow_end', 'Exited after rollback due to VLANs disabled (pre-check)')
        raise SystemExit(1)

    try:
        update_vlans(network_id, network_name, vlan_list)
    except MerakiAPIError as e:
        if is_vlans_disabled_error(e):
            print("❌ VLANs disabled error during VLAN update. Rolling back immediately...")
            rollback_all_changes(
                network_id=network_id,
                pre_change_devices=pre_change_devices or [],
                pre_change_vlans=pre_change_vlans or [],
                pre_change_template=pre_change_template,
                org_id=org_id,
                claimed_serials=claimed_serials or [],
                removed_serials=removed_serials or [],
                ms_list=ms_list or [],
                network_name=network_name,
            )
            log_change('workflow_end', 'Exited after rollback due to VLANs disabled during VLAN update')
            raise SystemExit(1)
        raise

def select_switch_profile_interactive_by_model(tpl_profiles: List[Dict[str, Any]], tpl_profile_map: Dict[str, str], switch_model: str) -> Optional[str]:
    candidates = [p for p in tpl_profiles if switch_model in p.get('model', [])]
    if not candidates:
        print(f"No switch profiles in template support {switch_model}.")
        return None
    print(f"\nAvailable switch profiles for {switch_model}:")
    for idx, p in enumerate(candidates, 1):
        print(f"{idx}. {p['name']}")
    profile_names = [p['name'] for p in candidates]
    while True:
        choice = input("Select switch profile by number (or Enter to skip): ").strip()
        if not choice:
            return None
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(profile_names):
                return tpl_profile_map[profile_names[idx]]
        print("Invalid selection. Please try again.")

def device_in_inventory(org_id: str, serial: str) -> bool:
    try:
        inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{serial}")
        return inv.get('networkId') is None
    except Exception:
        return False

# =====================
# Rollback
# =====================
def rollback_all_changes(
    network_id: str,
    pre_change_devices: List[Dict[str, Any]],
    pre_change_vlans: List[Dict[str, Any]],
    pre_change_template: Optional[str],
    org_id: str,
    *,
    claimed_serials: Optional[List[str]] = None,
    removed_serials: Optional[List[str]] = None,
    ms_list: Optional[List[Dict[str, Any]]] = None,
    network_name: str,
):
    print("=== Starting rollback to previous network state ===")

    if claimed_serials:
        for serial in claimed_serials:
            print(f"Removing claimed device: {serial}")
            try:
                do_action(meraki_post, f"/networks/{network_id}/devices/remove", data={"serial": serial})
                log_change('rollback_device_removed', f"Removed claimed device in rollback", device_serial=serial)
            except Exception:
                logging.exception(f"Failed to remove claimed device {serial}")

    if removed_serials:
        for serial in removed_serials:
            print(f"Re-adding previously removed device: {serial}")
            try:
                do_action(meraki_post, f"/networks/{network_id}/devices/claim", data={"serials": [serial]})
                log_change('rollback_device_reclaimed', f"Re-claimed previously removed device", device_serial=serial)
            except Exception:
                logging.exception(f"Failed to re-claim device {serial}")

    print("Restoring config template binding...")
    try:
        do_action(meraki_post, f"/networks/{network_id}/unbind")
        if pre_change_template:
            do_action(meraki_post, f"/networks/{network_id}/bind", data={"configTemplateId": pre_change_template})
        log_change('rollback_template', f"Restored template binding {pre_change_template}", device_name=f"Network: {network_id}")
    except Exception:
        logging.exception("Failed to restore original template binding")

    print("Waiting for template binding to take effect (sleeping 15 seconds)...")
    time.sleep(15)

    current_devices = meraki_get(f"/networks/{network_id}/devices")
    current_serials = {d['serial'] for d in current_devices}
    for dev in pre_change_devices:
        if dev["serial"] not in current_serials:
            try:
                inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{dev['serial']}")
                if not inv.get('networkId'):
                    print(f"Re-adding device {dev['serial']} ({dev['model']}) to network...")
                    do_action(meraki_post, f"/networks/{network_id}/devices/claim", data={"serials": [dev["serial"]]})
                    log_change('rollback_device_readded', f"Device re-added during rollback", device_serial=dev['serial'])
                else:
                    print(f"Device {dev['serial']} is assigned elsewhere. Skipping.")
            except Exception as e:
                print(f"Could not check/claim device {dev['serial']}: {e}")

    current_devices = meraki_get(f"/networks/{network_id}/devices")
    current_serials = {d['serial'] for d in current_devices}

    try:
        restored_tpl_profiles = meraki_get(f"/organizations/{org_id}/configTemplates/{pre_change_template}/switch/profiles") if pre_change_template else []
        profile_id_set = {p['switchProfileId'] for p in restored_tpl_profiles}
        profile_name_to_id = {p['name']: p['switchProfileId'] for p in restored_tpl_profiles}
    except Exception:
        logging.exception("Could not fetch switch profiles for restored template")
        restored_tpl_profiles = []
        profile_id_set = set()
        profile_name_to_id = {}

    for dev in pre_change_devices:
        if dev["serial"] not in current_serials:
            continue

        update_args: Dict[str, Any] = {"name": dev.get("name", ""), "address": dev.get("address", ""), "tags": dev.get("tags", [])}
        if dev["model"].startswith("MS"):
            serial = dev["serial"]
            orig_profile_id = dev.get('switchProfileId')
            if orig_profile_id and orig_profile_id in profile_id_set:
                print(f"Auto-restoring MS {serial} to profile ID {orig_profile_id}")
                update_args["switchProfileId"] = orig_profile_id
            else:
                orig_profile_name = dev.get('switchProfileName')
                new_profile_id = profile_name_to_id.get(orig_profile_name)
                if new_profile_id:
                    print(f"Auto-restoring MS {serial} to profile '{orig_profile_name}' (ID: {new_profile_id})")
                    update_args["switchProfileId"] = new_profile_id

        try:
            do_action(meraki_put, f"/devices/{dev['serial']}", data=update_args)
            log_change(
                'rollback_device_update',
                f"Restored device config during rollback",
                device_serial=dev['serial'],
                device_name=dev.get('name', ''),
                misc=f"tags={dev.get('tags', [])}, address={dev.get('address', '')}"
            )
        except Exception:
            logging.exception(f"Failed to update device {dev['serial']} during rollback")
            continue

        if dev["model"].startswith("MS"):
            try:
                preserved = (dev.get('port_overrides') or {})
                if preserved:
                    apply_port_overrides(dev['serial'], preserved)
            except Exception:
                logging.exception(f"Failed applying preserved port overrides during rollback for {dev['serial']}")

    print("Restoring VLANs and DHCP assignments...")
    time.sleep(5)
    update_vlans(network_id, network_name, pre_change_vlans)
    log_change('rollback_vlans', "Restored VLANs and DHCP assignments", device_name=f"Network: {network_id}")

    print("=== Rollback complete ===")

# =====================
# Step Summary helpers (✅ / ❌ and skip N/A)
# =====================
StatusVal = Union[bool, str]  # True/False/"NA"

def _fmt(val: StatusVal) -> str:
    if val is True:
        return "✅ Success"
    if val is False:
        return "❌ Failed"
    return str(val)

def print_summary(step_status: Dict[str, StatusVal]) -> None:
    order = [
        'template_bound',
        'vlans_updated',
        'devices_claimed',
        'mx_removed',
        'mr33_removed',
        'configured',
        'old_mx',
        'old_mr33',
    ]
    print("\nStep Summary:")
    for step in order:
        val = step_status.get(step, "NA")
        if isinstance(val, str) and val.upper() == "NA":
            continue  # skip N/A lines entirely
        print(f" - {step}: {_fmt(val)}")

# =====================
# Robust network selector
# =====================
def select_network_interactive(org_id: str) -> Tuple[str, str]:
    """
    Robust interactive network selector.

    - Prompts for a partial name (case-insensitive contains).
    - If no matches: offer to search again; otherwise exit.
    - If one match: ask to confirm; if no, loop and let them search again.
    - If multiple: show a numbered list and prompt for a choice.
    - Enter on the first prompt cancels and exits.
    """
    while True:
        partial = input("Enter partial network name to search (or press Enter to cancel): ").strip()
        if not partial:
            print("\n❌ No Network selected -----------\n   Please retry when Network is known *******")
            sys.exit(1)

        networks = fetch_matching_networks(org_id, partial)

        # No matches
        if not networks:
            print("\n❌ No matching networks found -----------")
            retry = input("Search again? (y/N): ").strip().lower()
            if retry == 'y':
                continue
            print("\n❌ No Network selected -----------\n   Please retry when Network is known *******")
            sys.exit(1)

        # Single match
        if len(networks) == 1:
            only = networks[0]
            print(f"\n1 match: {only['name']} (ID: {only['id']})")
            confirm = input("Use this network? (Y/n): ").strip().lower()
            if confirm in {"", "y", "yes"}:
                print(f"Selected network: {only['name']} (ID: {only['id']})")
                return only['id'], only['name']
            # let them search again
            continue

        # Multiple matches: show list and pick one
        print("\nMultiple networks found:")
        for idx, net in enumerate(networks, 1):
            print(f"{idx}. {net['name']} (ID: {net['id']})")

        while True:
            raw = input("Select the network by number (or press Enter to cancel): ").strip()
            if not raw:
                print("\n❌ No Network selected -----------\n   Please retry when Network is known *******")
                sys.exit(1)
            if raw.isdigit():
                choice = int(raw)
                if 1 <= choice <= len(networks):
                    chosen = networks[choice - 1]
                    print(f"Selected network #{choice}: {chosen['name']} (ID: {chosen['id']})")
                    return chosen['id'], chosen['name']
            print("❌ Invalid selection. Please enter a valid number from the list.")

# =====================
# Org selector
# =====================
def select_org() -> str:
    orgs = meraki_get("/organizations")
    if not orgs:
        print("\n❌ No Organisations returned from API -----------")
        print("   Please retry when Org is known *******")
        sys.exit(1)

    print("Organizations:")
    for idx, org in enumerate(orgs, 1):
        print(f"{idx}. {org['name']} (ID: {org['id']})")

    raw = input("Select organization by number (or press Enter to cancel): ").strip()
    if not raw:
        print("\n❌ No Organisation selected -----------")
        print("   Please retry when Org is known *******")
        sys.exit(1)

    try:
        org_idx = int(raw)
        if org_idx < 1 or org_idx > len(orgs):
            raise ValueError("out of range")
    except Exception:
        print("\n❌ Invalid Organisation selection -----------")
        print("   Please retry when Org is known *******")
        sys.exit(1)

    return orgs[org_idx - 1]['id']

# =====================
# Main
# =====================
if __name__ == '__main__':
    log_change('workflow_start', 'Script started')

    step_status: Dict[str, StatusVal] = {}

    # -------- Select Org (graceful cancel/invalid) --------
    org_id = select_org()

    # -------- Prompt/validate serials now (org-level), then summarize --------
    prevalidated_serials = prompt_and_validate_serials(org_id)
    detected_mx_models = summarize_devices_in_org(org_id, prevalidated_serials)

    mx_model_filter = None
    if detected_mx_models == {'MX67'}:
        mx_model_filter = 'MX67'
    elif detected_mx_models == {'MX75'}:
        mx_model_filter = 'MX75'

    # -------- Select Network (robust) --------
    network_id, network_name = select_network_interactive(org_id)

    net_info = meraki_get(f"/networks/{network_id}")
    old_template = net_info.get('configTemplateId')

    # Pre-change snapshot incl. MS port overrides
    mx, ms, mr = fetch_devices(org_id, network_id, template_id=old_template)
    pre_change_devices = mx + ms + mr
    pre_change_vlans = fetch_vlan_details(network_id)
    pre_change_template = old_template
    pre_change_serials = {d['serial'] for d in pre_change_devices}

    # MX gate: if current MX model is NOT MX64, skip rebind/VLAN path
    current_mx_models = sorted({d['model'] for d in mx})
    is_mx64_present = any(m.startswith('MX64') for m in current_mx_models)

    if current_mx_models and not is_mx64_present:
        print(f"\nCurrent network: {network_name} (ID: {network_id})")
        if old_template:
            try:
                curr_tpl = meraki_get(f"/organizations/{org_id}/configTemplates/{old_template}")
                print(f"Bound template: {curr_tpl.get('name','<unknown>')} (ID: {old_template})")
            except Exception:
                print(f"Bound template ID: {old_template}")
        else:
            print("No template bound.")
        print(f"Detected MX model(s): {', '.join(current_mx_models)}")

        step_status['template_bound'] = "NA"
        step_status['vlans_updated'] = "NA"
        step_status['mx_removed'] = "NA"

        claimed = claim_devices(org_id, network_id, prevalidated_serials=prevalidated_serials)
        step_status['devices_claimed'] = bool(claimed)

        # Choose primary/order (auto if Enter/skip/cancel)
        primary_mx_serial = select_primary_mx(org_id, claimed)
        ensure_primary_mx(network_id, primary_mx_serial)  # ensure warm spare primary
        mr_order = select_device_order(org_id, claimed, 'MR')
        ms_order = select_device_order(org_id, claimed, 'MS')

        try:
            tpl_profiles = []
            if old_template:
                tpl_profiles = meraki_get(f"/organizations/{org_id}/configTemplates/{old_template}/switch/profiles")
                tpl_profile_map = {p['name']: p['switchProfileId'] for p in tpl_profiles}
            else:
                tpl_profile_map = {}
                tpl_profiles = []
        except Exception:
            logging.exception("Failed fetch template switch profiles")
            tpl_profile_map = {}
            tpl_profiles = []

        try:
            name_and_configure_claimed_devices(
                org_id,
                network_id,
                network_name,
                claimed,
                ms,
                mr,
                tpl_profile_map,
                old_mx_devices=mx,
                old_mr_devices=mr,
                primary_mx_serial=primary_mx_serial,
                mr_order=mr_order,
                ms_order=ms_order,
            )
            step_status['configured'] = True
        except Exception:
            logging.exception("Configuration of claimed devices failed")
            step_status['configured'] = False

        try:
            removed_mr33_ok = remove_existing_MR33_devices(org_id, network_id)
            step_status['mr33_removed'] = removed_mr33_ok
            if removed_mr33_ok:
                log_change('mr33_removed', "Removed old MR33 after new AP claim", misc=f"claimed_serials={claimed}")
        except Exception:
            logging.exception("MR33 removal failed")
            step_status['mr33_removed'] = False

        step_status.setdefault('old_mx', "NA")
        step_status.setdefault('old_mr33', "NA")

        remove_recently_added_tag(network_id)
        print_summary(step_status)

        # -------- Enhanced rollback prompt --------
        rollback_choice = input(
            "\n⚠️  Rollback option available.\n"
            "Type 'yes' to rollback changes, 'no' to continue without rollback, or just press Enter to skip.\n"
            "IMPORTANT: If you skip (press Enter), rollback will no longer be available.\n"
            "Have you ensured the network is fully functional and all required checks have been carried out? (yes/no/Enter): "
        ).strip().lower()

        if rollback_choice in {'yes', 'y'}:
            print("\nRolling back all changes...")
            log_change('rollback_start', 'User requested rollback')
            post_change_devices = meraki_get(f"/networks/{network_id}/devices")
            post_change_serials = {d['serial'] for d in post_change_devices}
            claimed_serials_rb = list(post_change_serials - pre_change_serials)
            removed_serials_rb = list(pre_change_serials - post_change_serials)
            rollback_all_changes(
                network_id,
                pre_change_devices,
                pre_change_vlans,
                pre_change_template,
                org_id,
                claimed_serials=claimed_serials_rb,
                removed_serials=removed_serials_rb,
                ms_list=ms,
                network_name=network_name,
            )
            print("✅ Rollback complete.")
            log_change('rollback_end', 'Rollback completed')

        elif rollback_choice in {'no', 'n'}:
            print("\nProceeding without rollback. Rollback option will no longer be available.")
            log_change('workflow_end', 'Script finished (no rollback)')

        else:
            print("\n❌ No rollback selected (Enter pressed).")
            print("⚠️  Rollback is no longer available. Please ensure the network is functional and all required checks have been carried out.")
            log_change('workflow_end', 'Script finished (rollback skipped with Enter)')

        raise SystemExit(0)

    # ELSE: MX64 present -> proceed with full rebind/VLAN flow
    vlan_list = fetch_vlan_details(network_id)
    old_mx, prebind_ms_devices, old_mr = fetch_devices(org_id, network_id, template_id=old_template)
    ms_serial_to_profileid = {sw['serial']: sw.get('switchProfileId') for sw in prebind_ms_devices}

    if old_template:
        try:
            old_tpl_profiles = meraki_get(f"/organizations/{org_id}/configTemplates/{old_template}/switch/profiles")
            old_profileid_to_name = {p['switchProfileId']: p['name'] for p in old_tpl_profiles}
        except Exception:
            logging.exception("Failed fetching old template switch profiles")
            old_profileid_to_name = {}
    else:
        old_profileid_to_name = {}

    try:
        new_template, _, rolled_back = list_and_rebind_template(
            org_id,
            network_id,
            old_template,
            network_name,
            pre_change_devices=pre_change_devices,
            pre_change_vlans=pre_change_vlans,
            pre_change_template=pre_change_template,
            claimed_serials=[],
            removed_serials=[],
            ms_list=ms,
            mx_model_filter=mx_model_filter,
            vlan_count=len(vlan_list),   # <-- pass VLAN count for suggestion
        )
        if rolled_back:
            log_change('workflow_end', 'Exited after rollback during template stage')
            print("Rollback complete. Exiting.")
            raise SystemExit(1)
        step_status['template_bound'] = (new_template is not None) and (new_template != old_template)
    except SystemExit:
        raise
    except Exception:
        logging.exception("Template bind failed")
        new_template = old_template
        step_status['template_bound'] = False

    try:
        bind_network_to_template(
            org_id, network_id, new_template, vlan_list, network_name,
            pre_change_devices=pre_change_devices,
            pre_change_vlans=pre_change_vlans,
            pre_change_template=pre_change_template,
            claimed_serials=[],
            removed_serials=[],
            ms_list=ms
        )
        step_status['vlans_updated'] = True
    except SystemExit:
        raise
    except Exception:
        logging.exception("VLAN update failed")
        step_status['vlans_updated'] = False

    try:
        tpl_profiles = meraki_get(f"/organizations/{org_id}/configTemplates/{new_template}/switch/profiles") if new_template else []
        tpl_profile_map = {p['name']: p['switchProfileId'] for p in tpl_profiles}
    except Exception:
        logging.exception("Failed fetch template switch profiles")
        tpl_profile_map = {}
        tpl_profiles = []

    _, postbind_ms_devices, _ = fetch_devices(org_id, network_id)
    for sw in postbind_ms_devices:
        old_profile_id = ms_serial_to_profileid.get(sw['serial'])
        old_profile_name = old_profileid_to_name.get(old_profile_id)
        new_profile_id = tpl_profile_map.get(old_profile_name) if old_profile_name else None
        if not new_profile_id:
            new_profile_id = select_switch_profile_interactive_by_model(tpl_profiles, tpl_profile_map, sw['model']) if tpl_profiles else None
            if not new_profile_id:
                continue
        try:
            do_action(meraki_put, f"/devices/{sw['serial']}", data={"switchProfileId": new_profile_id})
            log_change(
                'switch_profile_assign',
                f"Assigned switchProfileId {new_profile_id} to {sw['serial']}",
                device_serial=sw['serial'],
                device_name=sw.get('name', ''),
                misc=f"profile_name={old_profile_name or ''}"
            )
            preserved = (sw.get('port_overrides') or {})
            if preserved:
                apply_port_overrides(sw['serial'], preserved)
        except Exception:
            logging.exception(f"Failed to assign profile to {sw['serial']}")

    claimed = claim_devices(org_id, network_id, prevalidated_serials=prevalidated_serials)
    step_status['devices_claimed'] = bool(claimed)

    # Choose orders for MR/MS and primary MX for naming after claim (auto if Enter/skip/cancel)
    primary_mx_serial = select_primary_mx(org_id, claimed)
    ensure_primary_mx(network_id, primary_mx_serial)  # ensure warm spare primary
    mr_order = select_device_order(org_id, claimed, 'MR')
    ms_order = select_device_order(org_id, claimed, 'MS')

    post_change_devices = meraki_get(f"/networks/{network_id}/devices")
    post_change_serials = {d['serial'] for d in post_change_devices}
    claimed_serials = list(post_change_serials - pre_change_serials)
    removed_serials = list(pre_change_serials - post_change_serials)

    if claimed:
        new_mx, ms_list, mr_list = fetch_devices(org_id, network_id)
        step_status['old_mx'] = bool([d['serial'] for d in old_mx])
        step_status['old_mr33'] = bool([d['serial'] for d in old_mr if d['model'] == 'MR33'])
        try:
            mx_models = []
            for s in claimed:
                try:
                    inv = meraki_get(f"/organizations/{org_id}/inventoryDevices/{s}")
                    mx_models.append(inv.get('model', ''))
                except Exception:
                    pass
            if any(m.startswith('MX67') or m.startswith('MX75') for m in mx_models):
                remove_existing_mx64_devices(org_id, network_id)
                log_change('mx_removed', "Removed old MX64 after new MX claim", misc=f"claimed_serials={claimed}")
            step_status['mx_removed'] = True
        except Exception:
            logging.exception("MX removal failed")
            step_status['mx_removed'] = False
        try:
            mr33_ok = remove_existing_MR33_devices(org_id, network_id)
            step_status['mr33_removed'] = mr33_ok
            if mr33_ok:
                log_change('mr33_removed', "Removed old MR33 after new AP claim", misc=f"claimed_serials={claimed}")
        except Exception:
            logging.exception("MR33 removal failed")
            step_status['mr33_removed'] = False
        try:
            name_and_configure_claimed_devices(
                org_id,
                network_id,
                network_name,
                claimed,
                ms_list,
                mr_list,
                tpl_profile_map,
                old_mx_devices=old_mx,
                old_mr_devices=old_mr,
                primary_mx_serial=primary_mx_serial,
                mr_order=mr_order,
                ms_order=ms_order,
            )
            remove_recently_added_tag(network_id)
            step_status['configured'] = True
        except Exception:
            logging.exception("Configuration of claimed devices failed")
            step_status['configured'] = False
    else:
        step_status.setdefault('mx_removed', "NA")
        step_status.setdefault('mr33_removed', "NA")
        step_status.setdefault('configured', "NA")
        step_status.setdefault('old_mx', "NA")
        step_status.setdefault('old_mr33', "NA")

    print_summary(step_status)

    # -------- Enhanced rollback prompt --------
    rollback_choice = input(
        "\n⚠️  Rollback option available.\n"
        "Type 'yes' to rollback changes, 'no' to continue without rollback, or just press Enter to skip.\n"
        "IMPORTANT: If you skip (press Enter), rollback will no longer be available.\n"
        "Have you ensured the network is fully functional and all required checks have been carried out? (yes/no/Enter): "
    ).strip().lower()

    if rollback_choice in {'yes', 'y'}:
        print("\nRolling back all changes...")
        log_change('rollback_start', 'User requested rollback')
        rollback_all_changes(
            network_id,
            pre_change_devices,
            pre_change_vlans,
            pre_change_template,
            org_id,
            claimed_serials=claimed_serials,
            removed_serials=removed_serials,
            ms_list=ms,
            network_name=network_name,
        )
        print("✅ Rollback complete.")
        log_change('rollback_end', 'Rollback completed')

    elif rollback_choice in {'no', 'n'}:
        print("\nProceeding without rollback. Rollback option will no longer be available.")
        log_change('workflow_end', 'Script finished (no rollback)')

    else:
        print("\n❌ No rollback selected (Enter pressed).")
        print("⚠️  Rollback is no longer available. Please ensure the network is functional and all required checks have been carried out.")
        log_change('workflow_end', 'Script finished (rollback skipped with Enter)')

