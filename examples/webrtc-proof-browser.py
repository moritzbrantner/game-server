#!/usr/bin/env python3
"""Explicit Chromium probe on the real setup deployment; requires Playwright."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument("lobby_id")
parser.add_argument("--relay", action="store_true")
parser.add_argument("--scoped", action="store_true")
args = parser.parse_args()
source = Path(__file__).with_name("webrtc-proof.js").read_text()
options = {"lobbyId": args.lobby_id, "relayOnly": args.relay, "scoped": args.scoped}
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://moenarch.com", wait_until="domcontentloaded")
    result = page.evaluate("async options => {" + source + "; return runWebRtcProof(options); }", options)
    print(json.dumps(result))
    browser.close()
