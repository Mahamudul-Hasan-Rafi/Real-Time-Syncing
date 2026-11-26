"""Docker healthcheck: healthy while the guardian's check loop has completed within the last 2 minutes."""
import re
import sys
import time
from urllib.request import urlopen

try:
    text = urlopen("http://localhost:9405/metrics", timeout=5).read().decode()
    last = float(re.search(r"^connect_guardian_last_check_timestamp_seconds (\S+)", text, re.M).group(1))
    sys.exit(0 if time.time() - last < 120 else 1)
except Exception:
    sys.exit(1)
