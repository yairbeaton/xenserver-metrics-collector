#!/usr/bin/env python3
"""xen_collector.py - collect performance metrics from a XenServer / Citrix
Hypervisor / XCP-ng pool over SSH, write CSVs and build an HTML dashboard.

Requirements
    Python 3.9+ and, for real (non --demo) runs only:  pip install paramiko

What it does
    1. SSH to the pool master, map hosts / VMs / SRs with ``xe``.
    2. SSH to every other pool host (same credentials, address from xe).
    3. Per host, read RRD data from xapi's local unix socket (``rrd_updates``),
       falling back to ``xe *-data-source-query`` through the master.
    4. Append CPU / memory / IOPS / throughput / latency to CSV files after
       every sample, then render ``dashboard.html`` from those CSVs.

Examples
    # interactive (asks for anything missing, password via getpass)
    python3 xen_collector.py

    # one-time snapshot with a key file
    python3 xen_collector.py --host 10.0.0.5 --key-file ~/.ssh/id_ed25519 --mode snapshot

    # 45 minutes of sampling every 150 s
    python3 xen_collector.py --host xen-master --mode continuous --interval 150 --duration 45

    # no SSH, synthetic pool (goes through the same CSV + dashboard code)
    python3 xen_collector.py --demo --mode snapshot

    # rebuild the dashboard from existing CSVs
    python3 xen_collector.py --dashboard-only ./xen_metrics_20250101_120000

Outputs (in --output-dir): inventory.csv, host_metrics.csv, vm_metrics.csv,
host_sr_metrics.csv, collector.log, run_meta.json, dashboard.html
"""

import argparse
import base64
import csv
import getpass
import hashlib
import html
import json
import logging
import math
import random
import re
import shlex
import sys
import threading
import time
import webbrowser
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ===========================================================================
# Constants
# ===========================================================================

LOGGER_NAME = "xen_collector"

INTERVAL_RANGE = (120, 180)   # seconds, continuous mode
DURATION_RANGE = (15, 60)     # minutes, continuous mode
SNAPSHOT_WINDOW = 120         # seconds of RRD history averaged in snapshot mode
RRD_STEP = 5                  # seconds; the 5 s archive holds ~10 min
RRD_SOCKETS = ("/var/lib/xcp/xapi", "/var/xapi/xapi")

# Host SR latency is assumed to be reported in MICROseconds. Some versions
# report milliseconds: use --host-latency-unit ms (divisor becomes 1.0).
HOST_LATENCY_DIVISOR = 1000.0
# VM VBD latency (read_latency / write_latency) is in microseconds.
VM_LATENCY_DIVISOR = 1000.0

KIB_PER_GIB = 1024.0 * 1024.0
BYTES_PER_GIB = 1024.0 ** 3

HOST_METRICS = [
    "cpu_pct", "mem_total_gib", "mem_used_gib", "mem_used_pct",
    "iops_read", "iops_write", "iops_total", "throughput_mib_s",
    "latency_ms_avg", "latency_ms_max", "read_latency_ms", "write_latency_ms",
]
VM_METRICS = [
    "cpu_pct", "mem_total_gib", "mem_free_gib", "mem_used_gib", "mem_used_pct",
    "iops_read", "iops_write", "iops_total", "throughput_mib_s",
    "read_latency_ms", "write_latency_ms", "latency_ms_max",
]
HOST_FIELDS = ["timestamp", "sample_no", "host_uuid", "host_name", "source"] + HOST_METRICS
VM_FIELDS = ["timestamp", "sample_no", "vm_uuid", "vm_name", "host_name", "source"] + VM_METRICS
SR_FIELDS = ["timestamp", "host", "sr_uuid8", "sr_name", "iops_read", "iops_write",
             "iops_total", "throughput_mib_s", "latency_ms"]
INV_FIELDS = ["type", "uuid", "name", "host_name", "address", "power_state",
              "vcpus", "memory_total_gib", "extra"]

CHARTJS_URL = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"

# Flag thresholds (amber, red)
CPU_FLAG = (75.0, 90.0)
MEM_FLAG = (80.0, 90.0)
LAT_FLAG = (10.0, 20.0)

log = logging.getLogger(LOGGER_NAME)

# ===========================================================================
# Data model
# ===========================================================================


@dataclass
class HostInfo:
    uuid: str
    name: str
    address: str
    hostname: str = ""
    enabled: bool = True
    cpus: int = 0
    mem_total_bytes: float = 0.0
    extra: str = ""
    is_master: bool = False
    status: str = "ok"  # ok | via-master | unreachable


@dataclass
class VmInfo:
    uuid: str
    name: str
    power_state: str
    host_uuid: str
    vcpus: int = 0
    mem_bytes: float = 0.0


@dataclass
class SrInfo:
    uuid: str
    name: str
    type: str = ""


@dataclass
class PoolInfo:
    name: str
    master_address: str
    hosts: List[HostInfo] = field(default_factory=list)
    vms: List[VmInfo] = field(default_factory=list)
    srs: List[SrInfo] = field(default_factory=list)

    def sr_names(self) -> Dict[str, str]:
        return {s.uuid[:8]: s.name for s in self.srs}

    def host_by_uuid(self, uuid: str) -> Optional[HostInfo]:
        for h in self.hosts:
            if h.uuid == uuid:
                return h
        return None


@dataclass
class RawHostSample:
    """Raw (already window-averaged) data-source values for one host."""
    host: Dict[str, Optional[float]]
    vms: Dict[str, Dict[str, Optional[float]]]
    source: str  # rrd | xe


# ===========================================================================
# Small numeric helpers
# ===========================================================================


def _finite(v: Optional[float]) -> bool:
    return v is not None and math.isfinite(v)


def _vals(values: Iterable[Optional[float]]) -> List[float]:
    return [v for v in values if _finite(v)]  # type: ignore[misc]


def mean_of(values: Iterable[Optional[float]]) -> Optional[float]:
    v = _vals(values)
    return sum(v) / len(v) if v else None


def sum_of(values: Iterable[Optional[float]]) -> Optional[float]:
    v = _vals(values)
    return sum(v) if v else None


def max_of(values: Iterable[Optional[float]]) -> Optional[float]:
    v = _vals(values)
    return max(v) if v else None


def scale(v: Optional[float], factor: float) -> Optional[float]:
    return v * factor if _finite(v) else None


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * pct / 100.0
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def to_float(text: Optional[str]) -> Optional[float]:
    if text is None:
        return None
    text = text.strip()
    if not text:
        return None
    try:
        v = float(text)
    except ValueError:
        return None
    return v if math.isfinite(v) else None


# ===========================================================================
# xe record parsing
# ===========================================================================

_XE_LINE = re.compile(r"^\s*([A-Za-z0-9_\-./]+)\s*\(\s*[A-Za-z]{2,3}\s*\)\s*:\s?(.*)$")


def parse_xe_records(text: str) -> List[Dict[str, str]]:
    """Parse ``xe ...-list`` output: blocks separated by blank lines, lines like
    ``uuid ( RO)    : value``. Values may contain ': ' (split on the first
    colon after the access marker only)."""
    records: List[Dict[str, str]] = []
    cur: Dict[str, str] = {}
    last: Optional[str] = None
    for line in text.splitlines():
        if not line.strip():
            if cur:
                records.append(cur)
            cur, last = {}, None
            continue
        m = _XE_LINE.match(line)
        if m:
            last = m.group(1)
            cur[last] = m.group(2).strip()
        elif last is not None:
            cur[last] += "\n" + line.strip()
    if cur:
        records.append(cur)
    return records


def parse_xe_map(value: str) -> Dict[str, str]:
    """Parse a xe map parameter such as ``cpu_count: 8; modelname: Xeon: X``."""
    out: Dict[str, str] = {}
    for part in value.split(";"):
        if ":" in part:
            k, v = part.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def _to_int(text: str) -> int:
    f = to_float(text)
    return int(f) if f is not None else 0


def hosts_from_records(records: List[Dict[str, str]]) -> List[HostInfo]:
    hosts = []
    for r in records:
        cpu = parse_xe_map(r.get("cpu_info", ""))
        sw = parse_xe_map(r.get("software-version", ""))
        extra = " ".join(x for x in (sw.get("product_brand", ""), sw.get("product_version", ""),
                                     cpu.get("modelname", "")) if x)
        hosts.append(HostInfo(
            uuid=r.get("uuid", ""), name=r.get("name-label", ""),
            address=r.get("address", ""), hostname=r.get("hostname", ""),
            enabled=r.get("enabled", "true").lower() == "true",
            cpus=_to_int(cpu.get("cpu_count", "0")),
            mem_total_bytes=to_float(r.get("memory-total", "")) or 0.0, extra=extra))
    return [h for h in hosts if h.uuid]


def vms_from_records(records: List[Dict[str, str]], host_uuids: Iterable[str]) -> List[VmInfo]:
    known = set(host_uuids)
    vms = []
    for r in records:
        res = r.get("resident-on", "")
        vms.append(VmInfo(
            uuid=r.get("uuid", ""), name=r.get("name-label", ""),
            power_state=r.get("power-state", "").lower(),
            host_uuid=res if res in known else "",
            vcpus=_to_int(r.get("VCPUs-number", "0")),
            mem_bytes=to_float(r.get("memory-static-max", "")) or 0.0))
    return [v for v in vms if v.uuid]


# ===========================================================================
# RRD (rrd_updates XML) parsing
# ===========================================================================


def _rrd_json_rows(text: str) -> Tuple[List[str], List[List[Any]]]:
    """Legend and value rows from a JSON rrd_updates document. Tolerant about
    the row shape: {"t":..,"values":[..]}, {"t":..,"v":[..]} or a bare list."""
    try:
        doc = json.loads(text)  # accepts NaN / Infinity literals
    except ValueError as exc:
        raise ValueError("unparsable rrd JSON: %s" % exc)
    meta = doc.get("meta") if isinstance(doc, dict) else None
    legend = [str(e).strip() for e in (meta or {}).get("legend") or []]
    rows: List[List[Any]] = []
    for row in doc.get("data") or []:
        if isinstance(row, dict):
            vals = row.get("values", row.get("v"))
        else:
            vals = row
        if isinstance(vals, list):
            rows.append(vals)
    return legend, rows


def parse_rrd_xml(text: str) -> Dict[Tuple[str, str], Dict[str, Optional[float]]]:
    """Parse an rrd_updates document. Returns {(kind, uuid): {data_source: mean}}
    where mean is over all finite values in the window (None if none finite).
    Raises ValueError when the document has no legend or no rows."""
    text = text.strip()
    legend: List[str]
    value_rows: List[List[Any]]
    if text.startswith("{"):
        # Newer XenServer / XCP-ng versions answer rrd_updates in JSON.
        legend, value_rows = _rrd_json_rows(text)
    else:
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise ValueError("unparsable rrd XML: %s" % exc)
        legend = [(e.text or "").strip() for e in root.findall("./meta/legend/entry")]
        value_rows = [[v.text for v in row.findall("v")] for row in root.findall("./data/row")]
    if not legend or not value_rows:
        raise ValueError("rrd document has no legend or no rows")
    sums = [0.0] * len(legend)
    counts = [0] * len(legend)
    for values in value_rows:
        for i, v in enumerate(values):
            if i >= len(legend):
                break
            f = to_float(None if v is None else str(v))  # NaN / inf -> None
            if f is not None:
                sums[i] += f
                counts[i] += 1
    out: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}
    for i, entry in enumerate(legend):
        parts = entry.split(":", 3)  # CF:kind:uuid:name
        if len(parts) != 4 or parts[1] not in ("host", "vm"):
            continue
        _, kind, uuid, name = parts
        out.setdefault((kind, uuid), {})[name] = sums[i] / counts[i] if counts[i] else None
    return out


# ===========================================================================
# Metric derivation
# ===========================================================================

_SR_METRIC_RE = re.compile(
    r"^(iops_read|iops_write|iops_total|io_throughput_total|io_throughput_read|"
    r"io_throughput_write|read_latency|write_latency|latency)_([0-9a-f]{8})$")
_VBD_RE = re.compile(
    r"^vbd_(.+)_(iops_read|iops_write|iops_total|io_throughput_total|read_latency|write_latency)$")
_CPUN_RE = re.compile(r"^cpu\d+$")

NEEDED_HOST_RE = re.compile(
    r"^(cpu_avg|cpu\d+|memory_total_kib|memory_free_kib)$|" + _SR_METRIC_RE.pattern)
NEEDED_VM_RE = re.compile(
    r"^(cpu_usage|cpu\d+|memory|memory_internal_free)$|" + _VBD_RE.pattern)


def _cpu_pct(ds: Dict[str, Optional[float]], primary: str) -> Optional[float]:
    v = ds.get(primary)
    if _finite(v):
        return v * 100.0  # type: ignore[operator]
    return scale(mean_of(val for k, val in ds.items() if _CPUN_RE.match(k)), 100.0)


def derive_host(ds: Dict[str, Optional[float]], lat_div: float,
                fallback_mem_total_gib: Optional[float] = None
                ) -> Tuple[Dict[str, Optional[float]], Dict[str, Dict[str, Optional[float]]]]:
    """Return (host metrics, per-SR metrics keyed by sr uuid first 8 chars)."""
    per_sr: Dict[str, Dict[str, Optional[float]]] = {}
    for k, v in ds.items():
        m = _SR_METRIC_RE.match(k)
        if m:
            per_sr.setdefault(m.group(2), {})[m.group(1)] = v
    sr_rows: Dict[str, Dict[str, Optional[float]]] = {}
    for sr8, m in per_sr.items():
        r, w = m.get("iops_read"), m.get("iops_write")
        total = m.get("iops_total")
        if not _finite(total):
            total = sum_of([r, w])
        tp = m.get("io_throughput_total")
        if not _finite(tp):
            tp = sum_of([m.get("io_throughput_read"), m.get("io_throughput_write")])
        rl = scale(m.get("read_latency"), 1.0 / lat_div)
        wl = scale(m.get("write_latency"), 1.0 / lat_div)
        lat = scale(m.get("latency"), 1.0 / lat_div)
        if lat is None:
            lat = mean_of([rl, wl])
        sr_rows[sr8] = dict(iops_read=r, iops_write=w, iops_total=total, throughput_mib_s=tp,
                            latency_ms=lat, read_latency_ms=rl, write_latency_ms=wl)

    total_gib = scale(ds.get("memory_total_kib"), 1.0 / KIB_PER_GIB)
    if total_gib is None:
        total_gib = fallback_mem_total_gib
    free_gib = scale(ds.get("memory_free_kib"), 1.0 / KIB_PER_GIB)
    used_gib = total_gib - free_gib if _finite(total_gib) and _finite(free_gib) else None  # type: ignore[operator]
    used_pct = used_gib / total_gib * 100.0 if _finite(used_gib) and total_gib else None  # type: ignore[operator]

    rows = sr_rows.values()
    iops_r = sum_of(x["iops_read"] for x in rows)
    iops_w = sum_of(x["iops_write"] for x in rows)
    iops_t = sum_of(x["iops_total"] for x in rows)
    if iops_t is None:
        iops_t = sum_of([iops_r, iops_w])
    metrics = dict(
        cpu_pct=_cpu_pct(ds, "cpu_avg"), mem_total_gib=total_gib, mem_used_gib=used_gib,
        mem_used_pct=used_pct, iops_read=iops_r, iops_write=iops_w, iops_total=iops_t,
        throughput_mib_s=sum_of(x["throughput_mib_s"] for x in rows),
        latency_ms_avg=mean_of(x["latency_ms"] for x in rows),
        latency_ms_max=max_of(x["latency_ms"] for x in rows),
        read_latency_ms=mean_of(x["read_latency_ms"] for x in rows),
        write_latency_ms=mean_of(x["write_latency_ms"] for x in rows))
    return metrics, sr_rows


def derive_vm(ds: Dict[str, Optional[float]], lat_div: float = VM_LATENCY_DIVISOR,
              fallback_mem_total_gib: Optional[float] = None) -> Dict[str, Optional[float]]:
    by_metric: Dict[str, List[Optional[float]]] = {}
    for k, v in ds.items():
        m = _VBD_RE.match(k)
        if m:
            by_metric.setdefault(m.group(2), []).append(v)
    rl = [scale(v, 1.0 / lat_div) for v in by_metric.get("read_latency", [])]
    wl = [scale(v, 1.0 / lat_div) for v in by_metric.get("write_latency", [])]
    iops_r = sum_of(by_metric.get("iops_read", []))
    iops_w = sum_of(by_metric.get("iops_write", []))
    iops_t = sum_of(by_metric.get("iops_total", []))
    if iops_t is None:
        iops_t = sum_of([iops_r, iops_w])

    total_gib = scale(ds.get("memory"), 1.0 / BYTES_PER_GIB)
    if total_gib is None:
        total_gib = fallback_mem_total_gib
    free_gib = scale(ds.get("memory_internal_free"), 1.0 / KIB_PER_GIB)
    used_gib = used_pct = None
    if _finite(total_gib) and _finite(free_gib):
        free_gib = min(free_gib, total_gib)  # type: ignore[type-var]
        used_gib = total_gib - free_gib  # type: ignore[operator]
        used_pct = used_gib / total_gib * 100.0 if total_gib else None  # type: ignore[operator]
    return dict(
        cpu_pct=_cpu_pct(ds, "cpu_usage"), mem_total_gib=total_gib, mem_free_gib=free_gib,
        mem_used_gib=used_gib, mem_used_pct=used_pct, iops_read=iops_r, iops_write=iops_w,
        iops_total=iops_t, throughput_mib_s=sum_of(by_metric.get("io_throughput_total", [])),
        read_latency_ms=mean_of(rl), write_latency_ms=mean_of(wl),
        latency_ms_max=max_of(rl + wl))


# ===========================================================================
# SSH layer (paramiko imported lazily)
# ===========================================================================


# ===========================================================================
# Banner (BBS-style block letters) - shown at launch and on top of the report
# ===========================================================================

BANNER_TEXT = "CITRIX-ISRAEL"
_GLYPHS: Dict[str, Tuple[str, ...]] = {
    "C": (" ####", "##   ", "##   ", "##   ", " ####"),
    "I": ("####", " ## ", " ## ", " ## ", "####"),
    "T": ("######", "  ##  ", "  ##  ", "  ##  ", "  ##  "),
    "R": ("#### ", "##  #", "#### ", "## # ", "##  #"),
    "X": ("##  ##", " #### ", "  ##  ", " #### ", "##  ##"),
    "-": ("    ", "    ", "####", "    ", "    "),
    "S": (" ####", "##   ", " ### ", "   ##", "#### "),
    "A": (" ### ", "##  #", "#####", "##  #", "##  #"),
    "E": ("#####", "##   ", "#### ", "##   ", "#####"),
    "L": ("##   ", "##   ", "##   ", "##   ", "#####"),
}


def banner_lines(text: str = BANNER_TEXT) -> List[str]:
    """Render ``text`` as 5 rows of block characters inside a double-line frame."""
    rows = [" ".join(_GLYPHS[ch][r] for ch in text).replace("#", "█") for r in range(5)]
    width = max(len(r) for r in rows)
    sub = "XenServer Pool Metrics Collector"
    out = ["╔" + "═" * (width + 2) + "╗"]
    out += ["║ " + r.ljust(width) + " ║" for r in rows]
    out.append("║ " + " " * width + " ║")
    out.append("║ " + "Author: Yair Biton".center(width) + " ║")
    out.append("║ " + sub.center(width) + " ║")
    out.append("╚" + "═" * (width + 2) + "╝")
    return out


def print_banner() -> None:
    lines = banner_lines()
    try:
        if sys.stdout.isatty():
            print("\033[1;36m" + "\n".join(lines) + "\033[0m\n")
        else:
            print("\n".join(lines) + "\n")
    except UnicodeEncodeError:  # consoles that cannot show block characters
        print("=== %s - XenServer Pool Metrics Collector ===\n" % BANNER_TEXT)


def import_paramiko() -> Any:
    try:
        import paramiko  # type: ignore
    except ImportError:
        print("paramiko is required for real SSH connections:  pip install paramiko")
        sys.exit(1)
    return paramiko


@dataclass
class Credentials:
    user: str
    port: int
    key_file: Optional[str] = None
    password: Optional[str] = None     # kept in memory only; never logged / written
    passphrase: Optional[str] = None


class HostKeyPrompt:
    """Missing-host-key policy: show key type + SHA256 fingerprint and ask the
    user (y / n / a = accept all for this run). Combined with
    paramiko.MissingHostKeyPolicy at runtime (see make_policy)."""

    def __init__(self) -> None:
        self.accept_all = False
        self._accepted: set = set()
        self._lock = threading.Lock()

    def missing_host_key(self, client: Any, hostname: str, key: Any) -> None:
        fp = "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
        with self._lock:
            if self.accept_all or (hostname, fp) in self._accepted:
                return
            print("\nUnknown host key for %s\n  %s %s" % (hostname, key.get_name(), fp))
            try:
                ans = input("Accept? [y]es / [n]o / [a]ccept all for this run: ").strip().lower()
            except EOFError:
                ans = "n"
            if ans in ("a", "all"):
                self.accept_all = True
            elif ans in ("y", "yes"):
                self._accepted.add((hostname, fp))
            else:
                paramiko = import_paramiko()
                raise paramiko.SSHException("host key for %s rejected by user" % hostname)


def make_policy(paramiko: Any) -> Any:
    cls = type("PromptHostKeyPolicy", (HostKeyPrompt, paramiko.MissingHostKeyPolicy), {})
    return cls()


class SshHost:
    """One SSH connection to one host; run() reconnects once on failure."""

    def __init__(self, address: str, creds: Credentials, policy: Any) -> None:
        self.address = address
        self.creds = creds
        self.policy = policy
        self.client: Any = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        paramiko = import_paramiko()
        self.close()
        c = self.creds
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(self.policy)
        use_default_keys = c.key_file is None and not c.password
        client.connect(
            self.address, port=c.port, username=c.user, password=c.password or None,
            key_filename=c.key_file, passphrase=c.passphrase, timeout=15, banner_timeout=15,
            auth_timeout=20, look_for_keys=use_default_keys, allow_agent=use_default_keys)
        transport = client.get_transport()
        if transport is not None:
            transport.set_keepalive(30)
        self.client = client

    def _exec(self, cmd: str, timeout: int) -> Tuple[int, str, str]:
        if self.client is None:
            raise OSError("not connected")
        _, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        return stdout.channel.recv_exit_status(), out, err

    def run(self, cmd: str, timeout: int = 60) -> Tuple[int, str, str]:
        try:
            return self._exec(cmd, timeout)
        except Exception as exc:  # reconnect once
            log.warning("%s: command failed (%s); reconnecting once", self.address, exc)
            with self._lock:
                self.connect()
            return self._exec(cmd, timeout)

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None


def describe_ssh_error(exc: Exception, address: str) -> str:
    """Short, actionable text for common SSH failures."""
    if type(exc).__name__ == "BadHostKeyException":
        return ("the saved SSH key for %s does not match the key the host presented. If the host "
                "was reinstalled, remove the old key with:  ssh-keygen -R %s" % (address, address))
    return str(exc)


def load_credentials(args: argparse.Namespace) -> Credentials:
    creds = Credentials(user=args.user, port=args.port, key_file=args.key_file)
    if args.key_file:
        creds.key_file = str(Path(args.key_file).expanduser())
        if _key_needs_passphrase(creds.key_file):
            creds.passphrase = getpass.getpass("Passphrase for %s: " % args.key_file)
    else:
        pw = getpass.getpass("SSH password for %s@%s (blank = try agent/default keys): "
                             % (args.user, args.host))
        creds.password = pw or None
    return creds


def _key_needs_passphrase(path: str) -> bool:
    paramiko = import_paramiko()
    for cls in (paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey):
        try:
            cls.from_private_key_file(path)
            return False
        except paramiko.PasswordRequiredException:
            return True
        except Exception:
            continue
    return False


# ===========================================================================
# Backends: real pool over SSH, and synthetic demo pool
# ===========================================================================

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.\-]+$")


def build_rrd_command(window: int, step: int) -> str:
    socks = " ".join(shlex.quote(s) for s in RRD_SOCKETS)
    return (
        "SOCK=; for s in %s; do [ -S \"$s\" ] && SOCK=$s && break; done; "
        "[ -n \"$SOCK\" ] || { echo 'no xapi socket' >&2; exit 3; }; "
        "S=$(( $(date +%%s) - %d )); "
        "curl -s --max-time 30 --unix-socket \"$SOCK\" "
        "\"http://localhost/rrd_updates?start=$S&cf=AVERAGE&interval=%d&host=true\"" % (socks, window, step))


class XenBackend:
    def __init__(self, args: argparse.Namespace, creds: Credentials) -> None:
        self.args = args
        self.creds = creds
        self.master: Optional[SshHost] = None
        self.sessions: Dict[str, SshHost] = {}
        self.policy: Any = None
        self.pool: Optional[PoolInfo] = None
        self.vm_uuid_names: Dict[str, str] = {}
        self._rrd_broken: set = set()
        self._xe_lock = threading.Lock()  # serialise xe fallback on the master connection

    # -- connection ---------------------------------------------------------
    def connect(self) -> None:
        paramiko = import_paramiko()
        self.policy = make_policy(paramiko)
        self.master = SshHost(self.args.host, self.creds, self.policy)
        print("Connecting to pool master %s ..." % self.args.host)
        self.master.connect()

    def _xe(self, cmd: str) -> str:
        assert self.master is not None
        code, out, err = self.master.run(cmd)
        if code != 0:
            raise RuntimeError("`%s` failed (%d): %s" % (cmd, code, err.strip()[:300]))
        return out

    # -- mapping ------------------------------------------------------------
    def map_pool(self) -> PoolInfo:
        pools = parse_xe_records(self._xe("xe pool-list params=uuid,name-label,master"))
        hosts = hosts_from_records(parse_xe_records(self._xe(
            "xe host-list params=uuid,name-label,address,hostname,enabled,cpu_info,"
            "memory-total,software-version")))
        srs = [SrInfo(r.get("uuid", ""), r.get("name-label", ""), r.get("type", ""))
               for r in parse_xe_records(self._xe("xe sr-list params=uuid,name-label,type"))]
        master_uuid = pools[0].get("master", "") if pools else ""
        for h in hosts:
            h.is_master = (h.uuid == master_uuid) or (not master_uuid and h.address == self.args.host)
        pool_name = (pools[0].get("name-label", "") if pools else "") or \
            next((h.name for h in hosts if h.is_master), "") or self.args.host
        pool = PoolInfo(pool_name, self.args.host, hosts, [], srs)
        self.pool = pool
        pool.vms = self.refresh_vms()
        self._connect_hosts(pool)
        return pool

    def refresh_vms(self) -> List[VmInfo]:
        out = self._xe(
            "xe vm-list is-control-domain=false is-a-template=false is-a-snapshot=false "
            "params=uuid,name-label,power-state,resident-on,VCPUs-number,memory-static-max")
        host_uuids = [h.uuid for h in self.pool.hosts] if self.pool else \
            [r.get("uuid", "") for r in parse_xe_records(self._xe("xe host-list params=uuid"))]
        return vms_from_records(parse_xe_records(out), host_uuids)

    def _connect_hosts(self, pool: PoolInfo) -> None:
        for h in pool.hosts:
            if h.is_master:
                assert self.master is not None
                self.sessions[h.uuid] = self.master
                continue
            print("Connecting to host %s (%s) ..." % (h.name, h.address))
            sess = SshHost(h.address, self.creds, self.policy)
            try:
                sess.connect()
                self.sessions[h.uuid] = sess
            except Exception as exc:
                h.status = "via-master"
                log.warning("host %s (%s): no direct SSH (%s); will query it through the master",
                            h.name, h.address, exc)
                print("  WARNING: no direct SSH to %s (%s): %s" % (h.name, h.address, describe_ssh_error(exc, h.address)))
                print("           Its metrics will be read through the pool master instead.")

    # -- collection ---------------------------------------------------------
    def collect(self, host: HostInfo, window: int, step: int,
                running_vms: Dict[str, VmInfo]) -> Optional[RawHostSample]:
        sess = self.sessions.get(host.uuid)
        if sess is None:
            # No direct SSH to this host: query its metrics through the master.
            return self._fallback(host, running_vms, direct=False)
        if host.uuid in self._rrd_broken:
            return self._fallback(host, running_vms, direct=True)
        try:
            code, out, err = sess.run(build_rrd_command(window, step), timeout=60)
            if code != 0 or not out.strip():
                raise RuntimeError("curl exit %d: %s" % (code, err.strip()[:200]))
            try:
                parsed = parse_rrd_xml(out)
            except ValueError:
                log.warning("%s: RRD response not understood; first 300 chars: %r; last 300: %r; stderr: %r",
                            host.name, out[:300], out[-300:], err[:300])
                raise
            hds = parsed.get(("host", host.uuid))
            if hds is None:
                raise ValueError("host uuid not found in RRD legend")
            vms = {u: ds for (kind, u), ds in parsed.items() if kind == "vm"}
            self._mark(host, "ok")
            return RawHostSample(hds, vms, "rrd")
        except Exception as exc:
            log.warning("%s: RRD path failed (%s); using xe data-source queries for the rest "
                        "of this run", host.name, exc)
            self._rrd_broken.add(host.uuid)
        return self._fallback(host, running_vms, direct=True)

    def _fallback(self, host: HostInfo, running_vms: Dict[str, VmInfo],
                  direct: bool) -> Optional[RawHostSample]:
        """xe data-source queries via the master. ``direct`` says whether we
        also hold an SSH session to the host itself (only affects status)."""
        try:
            with self._xe_lock:
                raw = self._collect_xe(host, running_vms)
            self._mark(host, "ok" if direct else "via-master")
            return raw
        except Exception as exc:
            log.warning("%s: xe fallback failed: %s", host.name, exc)
            self._mark(host, "unreachable")
            return None

    def _mark(self, host: HostInfo, status: str) -> None:
        if host.status != status:
            host.status = status
            msg = "host %s is now %s" % (host.name, status)
            log.warning(msg)
            print("  WARNING: " + msg)

    def _query_names(self, list_cmd: str, query_prefix: str, wanted: "re.Pattern[str]") -> Dict[str, Optional[float]]:
        """List data sources with xe, query those matching ``wanted`` in one remote shell loop."""
        assert self.master is not None
        code, out, _ = self.master.run(list_cmd)
        names: List[str] = []
        for line in out.splitlines():
            m = re.search(r"name_label\s*(?:\(\s*\w+\s*\))?\s*:\s*(\S+)", line)
            tokens = [m.group(1)] if m else (re.split(r"[,\s]+", line.strip()) if "," in line else [])
            names.extend(t for t in tokens if t)
        if not names:  # --minimal style: one comma separated line
            names = [t for t in re.split(r"[,\s]+", out.strip()) if t]
        names = sorted({n for n in names if _SAFE_NAME.match(n) and wanted.match(n)})
        if not names:
            return {}
        script = "for n in %s; do printf '%%s=%%s\\n' \"$n\" \"$(%s data-source=\"$n\" 2>/dev/null)\"; done" % (
            " ".join(shlex.quote(n) for n in names), query_prefix)
        code, out, err = self.master.run(script, timeout=120)
        result: Dict[str, Optional[float]] = {}
        for line in out.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                result[k] = to_float(v)
        return result

    def _collect_xe(self, host: HostInfo, running_vms: Dict[str, VmInfo]) -> RawHostSample:
        hq = shlex.quote(host.uuid)
        hds = self._query_names("xe host-data-source-list host=%s" % hq,
                                "xe host-data-source-query host=%s" % hq, NEEDED_HOST_RE)
        if not hds:
            raise RuntimeError("no host data sources returned by xe")
        vms: Dict[str, Dict[str, Optional[float]]] = {}
        for vm in running_vms.values():
            if vm.host_uuid != host.uuid:
                continue
            vq = shlex.quote(vm.uuid)
            vms[vm.uuid] = self._query_names("xe vm-data-source-list uuid=%s" % vq,
                                             "xe vm-data-source-query uuid=%s" % vq, NEEDED_VM_RE)
        return RawHostSample(hds, vms, "xe")

    def close(self) -> None:
        for s in list(self.sessions.values()):
            s.close()
        if self.master is not None:
            self.master.close()


class DemoBackend:
    """Synthetic pool: 3 hosts, 12 VMs, random-walk raw data-source values with
    the same names a real RRD would deliver."""

    def __init__(self, seed: int = 7) -> None:
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.state: Dict[str, float] = {}
        self.refreshes = 0
        self.pool: Optional[PoolInfo] = None
        self.srs = [SrInfo("a1b2c3d4-0000-4000-8000-000000000001", "NFS-VM-Store", "nfs"),
                    SrInfo("e5f60718-0000-4000-8000-000000000002", "iSCSI-LUN-01", "lvmoiscsi")]
        self.personality: Dict[str, Tuple[float, float, float]] = {}

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def map_pool(self) -> PoolInfo:
        hosts = []
        for i in range(3):
            uuid = "11111111-0000-4000-8000-00000000000%d" % (i + 1)
            hosts.append(HostInfo(uuid, "xen-demo-0%d" % (i + 1), "10.20.0.%d" % (11 + i),
                                  "xen-demo-0%d" % (i + 1), True, 16, 128 * BYTES_PER_GIB,
                                  "XCP-ng 8.2.1 Intel(R) Xeon(R) Silver 4210", is_master=(i == 0)))
        srs = list(self.srs)
        for h in hosts:
            srs.append(SrInfo("%08x-0000-4000-8000-0000000000%02d" % (0xB0000000 + int(h.uuid[-1]), int(h.uuid[-1])),
                              "Local storage (%s)" % h.name, "ext"))
        names = ["web-01", "web-02", "db-prod-01", "db-prod-02", "app-<b>01</b>: legacy", "file-srv",
                 "mail-01", "build-agent", "monitoring", "vdi-gold", "dc-01", "test-halted"]
        vms = []
        for i, n in enumerate(names):
            state = "halted" if n == "test-halted" else "running"
            host = hosts[i % 3].uuid if state == "running" else ""
            vms.append(VmInfo("22222222-0000-4000-8000-0000000000%02d" % (i + 1), n, state, host,
                              [2, 4, 8][i % 3], [4, 8, 16][i % 3] * BYTES_PER_GIB))
            self.personality[vms[-1].uuid] = (self.rng.uniform(0.05, 0.9),
                                              self.rng.uniform(50, 1500), self.rng.uniform(0.5, 14.0))
        self.pool = PoolInfo("demo-pool", "10.20.0.11", hosts, vms, srs)
        return self.pool

    def refresh_vms(self) -> List[VmInfo]:
        assert self.pool is not None
        self.refreshes += 1
        if self.refreshes % 6 == 0:  # simulate a live migration
            vm = self.pool.vms[3]
            others = [h.uuid for h in self.pool.hosts if h.uuid != vm.host_uuid]
            vm.host_uuid = others[self.refreshes // 6 % len(others)]
        return self.pool.vms

    def _walk(self, key: str, lo: float, hi: float, step: float, init: Optional[float] = None) -> float:
        cur = self.state.get(key, init if init is not None else self.rng.uniform(lo, hi))
        cur += self.rng.gauss(0, step * (hi - lo))
        cur = min(hi, max(lo, cur))
        self.state[key] = cur
        return cur

    def _nan(self, v: float, p: float = 0.02) -> Optional[float]:
        return None if self.rng.random() < p else v

    def _sr_metrics(self, ds: Dict[str, Optional[float]], key: str, sr8: str) -> None:
        r = self._walk("%s.r.%s" % (key, sr8), 20, 1800, 0.08)
        w = self._walk("%s.w.%s" % (key, sr8), 10, 1200, 0.08)
        lat = self._walk("%s.l.%s" % (key, sr8), 600, 16000, 0.12)
        if self.rng.random() < 0.03:
            lat *= 2.2
        ds["iops_read_" + sr8] = r
        ds["iops_write_" + sr8] = w
        ds["iops_total_" + sr8] = r + w
        ds["io_throughput_total_" + sr8] = (r + w) * 0.012
        ds["latency_" + sr8] = lat
        ds["read_latency_" + sr8] = lat * 0.8
        ds["write_latency_" + sr8] = lat * 1.3

    def collect(self, host: HostInfo, window: int, step: int,
                running_vms: Dict[str, VmInfo]) -> Optional[RawHostSample]:
        assert self.pool is not None
        with self.lock:
            k = "h." + host.uuid
            cpu = self._walk(k + ".cpu", 0.08, 0.95, 0.07)
            hds: Dict[str, Optional[float]] = {"cpu_avg": cpu}
            for c in range(16):
                hds["cpu%d" % c] = min(1.0, max(0.0, cpu + self.rng.gauss(0, 0.05)))
            hds["memory_total_kib"] = 128 * KIB_PER_GIB
            hds["memory_free_kib"] = self._walk(k + ".mem", 8, 60, 0.05) * KIB_PER_GIB
            for sr in self.srs + [s for s in self.pool.srs if s.name.endswith("(%s)" % host.name)]:
                self._sr_metrics(hds, k, sr.uuid[:8])
            vms: Dict[str, Dict[str, Optional[float]]] = {}
            for vm in running_vms.values():
                if vm.host_uuid != host.uuid:
                    continue
                base, io_base, lat_base = self.personality[vm.uuid]
                vk = "v." + vm.uuid
                ds: Dict[str, Optional[float]] = {}
                ds["cpu_usage"] = self._nan(self._walk(vk + ".cpu", max(0.01, base - 0.2), min(0.99, base + 0.15), 0.1))
                for c in range(vm.vcpus):
                    ds["cpu%d" % c] = ds["cpu_usage"]
                ds["memory"] = vm.mem_bytes
                if vm.name not in ("file-srv", "dc-01"):  # no guest tools -> no free memory
                    ds["memory_internal_free"] = self._walk(vk + ".mem", 0.1, 0.7, 0.05) * vm.mem_bytes / 1024.0
                for dev in ("xvda", "xvdb"):
                    f = 1.0 if dev == "xvda" else 0.4
                    r = self._walk("%s.%s.r" % (vk, dev), 1, io_base * f, 0.12)
                    w = self._walk("%s.%s.w" % (vk, dev), 1, io_base * f * 0.6, 0.12)
                    ds["vbd_%s_iops_read" % dev] = self._nan(r)
                    ds["vbd_%s_iops_write" % dev] = self._nan(w)
                    ds["vbd_%s_iops_total" % dev] = self._nan(r + w)
                    ds["vbd_%s_io_throughput_total" % dev] = (r + w) * 0.01
                    lat = self._walk("%s.%s.l" % (vk, dev), 400, lat_base * 1000 + 800, 0.15)
                    if self.rng.random() < 0.03:
                        lat *= 3
                    ds["vbd_%s_read_latency" % dev] = lat
                    ds["vbd_%s_write_latency" % dev] = lat * 1.4
                vms[vm.uuid] = ds
        return RawHostSample(hds, vms, "rrd")


# ===========================================================================
# Sampling + CSV output
# ===========================================================================


def _fmt(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, float):
        return round(v, 3) if math.isfinite(v) else ""
    return v


class CsvSink:
    """Append-mode CSV files; flushed after every write so a crash loses nothing."""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.files: Dict[str, Tuple[Any, Any]] = {}
        self._open("host_metrics.csv", HOST_FIELDS)
        self._open("vm_metrics.csv", VM_FIELDS)
        self._open("host_sr_metrics.csv", SR_FIELDS)

    def _open(self, name: str, fields: List[str]) -> None:
        path = self.out_dir / name
        new = not path.exists() or path.stat().st_size == 0
        fh = open(path, "a", newline="", encoding="utf-8")
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore", restval="")
        if new:
            w.writeheader()
            fh.flush()
        self.files[name] = (fh, w)

    def write(self, name: str, rows: List[Dict[str, Any]]) -> None:
        fh, w = self.files[name]
        for r in rows:
            w.writerow({k: _fmt(v) for k, v in r.items()})
        fh.flush()

    def write_inventory(self, pool: PoolInfo) -> None:
        names = {h.uuid: h.name for h in pool.hosts}
        with open(self.out_dir / "inventory.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=INV_FIELDS)
            w.writeheader()
            for h in pool.hosts:
                state = "unreachable" if h.status == "unreachable" else ("enabled" if h.enabled else "disabled")
                w.writerow({k: _fmt(v) for k, v in dict(
                    type="host", uuid=h.uuid, name=h.name, host_name="", address=h.address,
                    power_state=state, vcpus=h.cpus, memory_total_gib=h.mem_total_bytes / BYTES_PER_GIB,
                    extra=h.extra + (" [master]" if h.is_master else "")).items()})
            for v in pool.vms:
                w.writerow({k: _fmt(x) for k, x in dict(
                    type="vm", uuid=v.uuid, name=v.name, host_name=names.get(v.host_uuid, ""),
                    address="", power_state=v.power_state, vcpus=v.vcpus,
                    memory_total_gib=v.mem_bytes / BYTES_PER_GIB, extra="").items()})

    def close(self) -> None:
        for fh, _ in self.files.values():
            try:
                fh.close()
            except Exception:
                pass


def collect_sample(backend: Any, pool: PoolInfo, sample_no: int, ts: str, window: int,
                   host_lat_div: float, log_names: bool
                   ) -> Tuple[List[dict], List[dict], List[dict]]:
    running = {v.uuid: v for v in pool.vms if v.power_state == "running"}
    targets = list(pool.hosts)  # hosts without direct SSH are read via the master
    host_rows: List[dict] = []
    vm_rows: List[dict] = []
    sr_rows: List[dict] = []
    if not targets:
        return host_rows, vm_rows, sr_rows
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        futs = [(h, ex.submit(backend.collect, h, window, RRD_STEP, running)) for h in targets]
        results = []
        for h, f in futs:
            try:
                results.append((h, f.result()))
            except Exception as exc:
                log.warning("collection for %s raised: %s", h.name, exc)
                results.append((h, None))
    names = pool.sr_names()
    host_names: set = set()
    vm_names: set = set()
    for h, raw in results:
        if raw is None:
            continue
        host_names.update(raw.host.keys())
        metrics, per_sr = derive_host(raw.host, host_lat_div, h.mem_total_bytes / BYTES_PER_GIB or None)
        host_rows.append(dict(timestamp=ts, sample_no=sample_no, host_uuid=h.uuid, host_name=h.name,
                              source=raw.source, **metrics))
        for sr8, m in sorted(per_sr.items()):
            sr_rows.append(dict(timestamp=ts, host=h.name, sr_uuid8=sr8, sr_name=names.get(sr8, ""), **{
                k: m[k] for k in ("iops_read", "iops_write", "iops_total", "throughput_mib_s", "latency_ms")}))
        for vuuid, ds in raw.vms.items():
            vm = running.get(vuuid)
            if vm is None:  # control domain or not mapped as running
                continue
            vm_names.update(ds.keys())
            vm_rows.append(dict(timestamp=ts, sample_no=sample_no, vm_uuid=vuuid, vm_name=vm.name,
                                host_name=h.name, source=raw.source,
                                **derive_vm(ds, VM_LATENCY_DIVISOR, vm.mem_bytes / BYTES_PER_GIB or None)))
    if log_names:
        log.info("Host data sources seen: %s", ", ".join(sorted(host_names)))
        log.info("VM data sources seen: %s", ", ".join(sorted(vm_names)))
    return host_rows, vm_rows, sr_rows


def sleep_until(target: float) -> None:
    while True:
        rem = target - time.monotonic()
        if rem <= 0:
            return
        time.sleep(min(rem, 1.0))


def run_collection(backend: Any, pool: PoolInfo, sink: CsvSink, args: argparse.Namespace,
                   host_lat_div: float) -> int:
    continuous = args.mode == "continuous"
    total = args.duration * 60 // args.interval + 1 if continuous else 1
    window = args.interval + 10 if continuous else SNAPSHOT_WINDOW
    t0 = time.monotonic()
    now = datetime.now().astimezone()
    base = now - timedelta(seconds=(total - 1) * args.interval) if args.demo_fast else now
    done = 0
    inv_sig = None
    try:
        for n in range(total):
            target = t0 + n * args.interval
            if n > 0 and not args.demo_fast:
                sleep_until(target)
            ts_dt = base + timedelta(seconds=n * args.interval) if args.demo_fast else datetime.now().astimezone()
            if n > 0:
                pool.vms = backend.refresh_vms()  # follow migrations / starts
            sig = tuple((v.uuid, v.power_state, v.host_uuid) for v in pool.vms)
            if sig != inv_sig:
                sink.write_inventory(pool)
                inv_sig = sig
            hr, vr, sr = collect_sample(backend, pool, n + 1, ts_dt.isoformat(timespec="seconds"),
                                        window, host_lat_div, log_names=(n == 0))
            sink.write("host_metrics.csv", hr)
            sink.write("vm_metrics.csv", vr)
            sink.write("host_sr_metrics.csv", sr)
            done += 1
            nxt = ""
            if n + 1 < total:
                rem = args.interval if args.demo_fast else max(0, int(t0 + (n + 1) * args.interval - time.monotonic()))
                nxt = "  next in %ds" % rem
            print("[%*d/%d] %s  hosts=%d vms=%d%s" % (len(str(total)), n + 1, total,
                  ts_dt.strftime("%H:%M:%S"), len(hr), len(vr), nxt))
    except KeyboardInterrupt:
        print("\nInterrupted -- keeping the %d sample(s) collected so far." % done)
    sink.write_inventory(pool)
    return done


# ===========================================================================
# Console tree
# ===========================================================================


def print_tree(pool: PoolInfo) -> None:
    print("\nPool: %s  (master %s)" % (pool.name, pool.master_address))
    groups = [(h, [v for v in pool.vms if v.host_uuid == h.uuid]) for h in pool.hosts]
    for i, (h, vms) in enumerate(groups):
        tag = " [master]" if h.is_master else ""
        flags = "" if h.status == "ok" else " (%s)" % h.status
        print("  +- Host %s (%s)%s  %d CPUs, %.1f GiB%s" % (
            h.name, h.address, tag, h.cpus, h.mem_total_bytes / BYTES_PER_GIB, flags))
        for v in vms:
            print("  |    - %-28s %-9s %2d vCPU  %6.1f GiB" % (v.name, v.power_state, v.vcpus,
                                                               v.mem_bytes / BYTES_PER_GIB))
    orphans = [v for v in pool.vms if not v.host_uuid]
    if orphans:
        print("  +- Not resident on any host")
        for v in orphans:
            print("       - %-28s %-9s %2d vCPU  %6.1f GiB (not sampled)" % (
                v.name, v.power_state, v.vcpus, v.mem_bytes / BYTES_PER_GIB))
    running = sum(1 for v in pool.vms if v.power_state == "running")
    print("  %d hosts, %d VMs (%d running)\n" % (len(pool.hosts), len(pool.vms), running))


# ===========================================================================
# Dashboard
# ===========================================================================


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def num(row: Dict[str, str], key: str) -> Optional[float]:
    return to_float(row.get(key))


def esc(s: Any) -> str:
    return html.escape(str(s), quote=True)


def json_for_script(obj: Any) -> str:
    """JSON safe inside <script>: '<', '>' and '&' become \\u escapes."""
    return (json.dumps(obj, ensure_ascii=True)
            .replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026"))


def stat3(values: Iterable[Optional[float]]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    v = _vals(values)
    if not v:
        return None, None, None
    return sum(v) / len(v), max(v), percentile(v, 95)


def td(v: Optional[float], nd: int = 1, flag: Optional[Tuple[float, float]] = None) -> str:
    if v is None:
        return '<td class="num na" data-v="">&ndash;</td>'
    cls = "num"
    if flag is not None:
        if v >= flag[1]:
            cls += " crit"
        elif v >= flag[0]:
            cls += " warn"
    return '<td class="%s" data-v="%.6f">%s</td>' % (cls, v, ("{:,.%df}" % nd).format(v))


def load_meta(out_dir: Path) -> Dict[str, Any]:
    p = out_dir / "run_meta.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_meta(out_dir: Path, meta: Dict[str, Any]) -> None:
    (out_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _rows_by(rows: List[Dict[str, str]], key: str) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        out.setdefault(r.get(key, ""), []).append(r)
    return out


def build_dashboard(out_dir: Path) -> Optional[Path]:
    host_rows = read_csv(out_dir / "host_metrics.csv")
    vm_rows = read_csv(out_dir / "vm_metrics.csv")
    sr_rows = read_csv(out_dir / "host_sr_metrics.csv")
    inv = read_csv(out_dir / "inventory.csv")
    if not host_rows and not vm_rows:
        print("No samples in %s - dashboard not built." % out_dir)
        return None
    meta = load_meta(out_dir)

    # Timeline (one entry per sample number)
    ts_by_no: Dict[int, str] = {}
    for r in host_rows + vm_rows:
        try:
            ts_by_no.setdefault(int(r["sample_no"]), r["timestamp"])
        except (KeyError, ValueError):
            pass
    nos = sorted(ts_by_no)
    idx = {n: i for i, n in enumerate(nos)}
    labels = [ts_by_no[n][11:19] for n in nos]
    n_samples = len(nos)
    mode = meta.get("mode") or ("continuous" if n_samples > 1 else "snapshot")
    continuous = mode == "continuous" and n_samples > 1
    interval = meta.get("interval")
    if interval is None and n_samples > 1:
        try:
            interval = int((datetime.fromisoformat(ts_by_no[nos[1]]) - datetime.fromisoformat(ts_by_no[nos[0]])).total_seconds())
        except ValueError:
            interval = ""

    inv_hosts = [r for r in inv if r.get("type") == "host"]
    inv_vms = [r for r in inv if r.get("type") == "vm"]
    running_vms = sum(1 for r in inv_vms if r.get("power_state") == "running")
    host_names = [r["name"] for r in inv_hosts] or sorted({r["host_name"] for r in host_rows})
    hosts_by_name = _rows_by(host_rows, "host_name")

    def series(rows: List[Dict[str, str]], key: str) -> List[Optional[float]]:
        out: List[Optional[float]] = [None] * n_samples
        for r in rows:
            try:
                out[idx[int(r["sample_no"])]] = num(r, key)
            except (KeyError, ValueError):
                pass
        return out

    # --- KPIs ---
    by_sample = _rows_by(host_rows, "sample_no")
    tot_iops = [sum_of(num(r, "iops_total") for r in rs) for rs in by_sample.values()]
    mem_pcts = []
    for rs in by_sample.values():
        used = sum_of(num(r, "mem_used_gib") for r in rs)
        tot = sum_of(num(r, "mem_total_gib") for r in rs if num(r, "mem_used_gib") is not None)
        if used is not None and tot:
            mem_pcts.append(used / tot * 100.0)
    pool_cpu = mean_of(num(r, "cpu_pct") for r in host_rows)
    pool_mem = mean_of(mem_pcts)
    iops_avg, iops_peak = mean_of(tot_iops), max_of(tot_iops)
    worst_host_lat = max_of(num(r, "latency_ms_max") for r in host_rows)
    worst_vm_lat = max_of(num(r, "latency_ms_max") for r in vm_rows)

    def kpi(value: Optional[float], label: str, nd: int = 1, unit: str = "",
            flag: Optional[Tuple[float, float]] = None) -> str:
        cls = "kpi"
        if value is not None and flag is not None:
            cls += " crit" if value >= flag[1] else (" warn" if value >= flag[0] else "")
        txt = "&ndash;" if value is None else ("{:,.%df}" % nd).format(value) + unit
        return '<div class="%s"><div class="kv">%s</div><div class="kl">%s</div></div>' % (cls, txt, esc(label))

    iops_txt = "&ndash;" if iops_avg is None else "{:,.0f} / {:,.0f}".format(iops_avg, iops_peak)
    kpis = "".join([
        kpi(len(host_names), "Hosts", 0), kpi(running_vms, "Running VMs", 0),
        kpi(pool_cpu, "Pool avg CPU", 1, " %", CPU_FLAG), kpi(pool_mem, "Pool memory used", 1, " %", MEM_FLAG),
        '<div class="kpi"><div class="kv">%s</div><div class="kl">Total IOPS (avg / peak)</div></div>' % iops_txt,
        kpi(worst_host_lat, "Worst host latency", 1, " ms", LAT_FLAG),
        kpi(worst_vm_lat, "Worst VM latency", 1, " ms", LAT_FLAG)])

    # --- Host table ---
    metrics = [("cpu_pct", "CPU %", 1, CPU_FLAG), ("mem_used_pct", "Memory %", 1, MEM_FLAG),
               ("iops_total", "IOPS", 0, None), ("latency_ms_avg", "Latency ms", 2, LAT_FLAG)]
    if continuous:
        head1 = "".join('<th colspan="3">%s</th>' % esc(m[1]) for m in metrics)
        head2 = "".join("<th>avg</th><th>max</th><th>p95</th>" for _ in metrics)
    else:
        head1 = "".join("<th>%s</th>" % esc(m[1]) for m in metrics)
        head2 = ""
    host_body = []
    host_status = {r["name"]: r.get("power_state", "") for r in inv_hosts}
    for name in host_names:
        rs = hosts_by_name.get(name, [])
        cells = []
        for key, _, nd, flag in metrics:
            a, mx, p95 = stat3(num(r, key) for r in rs)
            cells.append(td(a, nd, flag) if not continuous else td(a, nd, flag) + td(mx, nd, flag) + td(p95, nd, flag))
        st = host_status.get(name, "")
        badge = ' <span class="badge">%s</span>' % esc(st) if st and st != "enabled" else ""
        host_body.append("<tr><td>%s%s</td>%s</tr>" % (esc(name), badge, "".join(cells)))
    host_table = ('<div class="scroll"><table><thead><tr><th rowspan="%d">Host</th>%s</tr>%s</thead><tbody>%s</tbody></table></div>'
                  % (2 if continuous else 1, head1, "<tr>%s</tr>" % head2 if continuous else "", "".join(host_body)))

    # --- VM table + series ---
    vms_by_uuid = _rows_by(vm_rows, "vm_uuid")
    vm_stats: List[Dict[str, Any]] = []
    vm_series: Dict[str, Dict[str, Any]] = {}
    for uuid, rs in vms_by_uuid.items():
        lat_mean = [mean_of([num(r, "read_latency_ms"), num(r, "write_latency_ms")]) for r in rs]
        vm_stats.append(dict(
            uuid=uuid, name=rs[-1]["vm_name"], host=rs[-1]["host_name"],
            cpu=stat3(num(r, "cpu_pct") for r in rs), mem=mean_of(num(r, "mem_used_pct") for r in rs),
            iops=stat3(num(r, "iops_total") for r in rs), lat_avg=mean_of(lat_mean),
            lat_max=max_of(num(r, "latency_ms_max") for r in rs)))
        if continuous:
            vm_series[uuid] = dict(name=rs[-1]["vm_name"], cpu=series(rs, "cpu_pct"), mem=series(rs, "mem_used_pct"),
                                   iops=series(rs, "iops_total"), lat_mean=_series_mean(rs, idx, n_samples),
                                   lat_max=series(rs, "latency_ms_max"))
    vm_stats.sort(key=lambda s: s["name"].lower())
    vm_body = []
    for s in vm_stats:
        vm_body.append(
            '<tr data-name="%s" data-host="%s"><td data-v="%s">%s</td><td data-v="%s">%s</td>%s%s%s%s%s%s%s</tr>' % (
                esc(s["name"].lower()), esc(s["host"]), esc(s["name"].lower()), esc(s["name"]),
                esc(s["host"].lower()), esc(s["host"]),
                td(s["cpu"][0], 1, CPU_FLAG), td(s["cpu"][1], 1, CPU_FLAG), td(s["mem"], 1, MEM_FLAG),
                td(s["iops"][0], 0), td(s["iops"][1], 0), td(s["lat_avg"], 2, LAT_FLAG), td(s["lat_max"], 2, LAT_FLAG)))
    vm_heads = [("VM", "text"), ("Host", "text"), ("CPU avg %", "num"), ("CPU max %", "num"), ("Mem used %", "num"),
                ("IOPS avg", "num"), ("IOPS max", "num"), ("Latency avg ms", "num"), ("Latency max ms", "num")]
    vm_table = ('<div class="scroll"><table id="vmtable"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>'
                % ("".join('<th class="sortable" data-type="%s">%s</th>' % (t, esc(h)) for h, t in vm_heads), "".join(vm_body)))
    host_opts = "".join('<option value="%s">%s</option>' % (esc(h), esc(h)) for h in sorted({s["host"] for s in vm_stats}))
    vm_opts = "".join('<option value="%s">%s</option>' % (esc(s["uuid"]), esc(s["name"])) for s in vm_stats)

    def top(key: Any, n: int = 10) -> Dict[str, List[Any]]:
        pairs = [(s["name"], key(s)) for s in vm_stats if key(s) is not None]
        pairs.sort(key=lambda p: -p[1])
        return {"labels": [p[0] for p in pairs[:n]], "values": [round(p[1], 3) for p in pairs[:n]]}

    # --- SR table ---
    sr_groups: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for r in sr_rows:
        sr_groups.setdefault((r["host"], r["sr_uuid8"]), []).append(r)
    sr_body = []
    for (h, sr8), rs in sorted(sr_groups.items()):
        ia, im, _ = stat3(num(r, "iops_total") for r in rs)
        la, lm, _ = stat3(num(r, "latency_ms") for r in rs)
        sr_body.append("<tr><td>%s</td><td>%s</td><td><code>%s</code></td>%s%s%s%s</tr>" % (
            esc(h), esc(rs[-1].get("sr_name", "")), esc(sr8), td(ia, 0), td(im, 0),
            td(la, 2, LAT_FLAG), td(lm, 2, LAT_FLAG)))
    sr_table = ('<div class="scroll"><table><thead><tr><th>Host</th><th>SR</th><th>SR id</th><th>IOPS avg</th><th>IOPS max</th>'
                '<th>Latency avg ms</th><th>Latency max ms</th></tr></thead><tbody>%s</tbody></table></div>' % "".join(sr_body))

    # --- chart data ---
    data: Dict[str, Any] = {"mode": "continuous" if continuous else "snapshot", "labels": labels,
                            "top": {"cpu": top(lambda s: s["cpu"][0]), "iops": top(lambda s: s["iops"][0]),
                                    "lat": top(lambda s: s["lat_max"])},
                            "vm_series": vm_series}
    if continuous:
        data["hosts"] = [dict(name=n, cpu=series(hosts_by_name.get(n, []), "cpu_pct"),
                              mem=series(hosts_by_name.get(n, []), "mem_used_pct"),
                              iops=series(hosts_by_name.get(n, []), "iops_total"),
                              lat=series(hosts_by_name.get(n, []), "latency_ms_avg")) for n in host_names]
    else:
        def avg(n: str, k: str) -> Optional[float]:
            return mean_of(num(r, k) for r in hosts_by_name.get(n, []))
        data["host_bars"] = dict(labels=host_names, cpu=[avg(n, "cpu_pct") for n in host_names],
                                 mem=[avg(n, "mem_used_pct") for n in host_names],
                                 iops=[avg(n, "iops_total") for n in host_names],
                                 lat=[avg(n, "latency_ms_avg") for n in host_names])

    pool_name = meta.get("pool_name") or "XenServer pool"
    header_items = [("Pool", pool_name), ("Master", meta.get("master", "")), ("Mode", mode),
                    ("Start", ts_by_no[nos[0]].replace("T", " ")), ("End", ts_by_no[nos[-1]].replace("T", " ")),
                    ("Samples", n_samples), ("Interval", "%ss" % interval if interval else "n/a")]
    header = "".join('<div><span class="hl">%s</span> %s</div>' % (esc(k), esc(v)) for k, v in header_items)

    host_charts = "".join('<div class="card"><h3>%s</h3><div class="chart"><canvas id="%s"></canvas></div></div>' % (t, i)
                          for i, t in (("h_cpu", "CPU %"), ("h_mem", "Memory used %"), ("h_iops", "IOPS (total)"),
                                       ("h_lat", "Disk latency (ms, avg over SRs)")))
    vm_charts = "".join('<div class="card"><h3>%s</h3><div class="chart"><canvas id="%s"></canvas></div></div>' % (t, i)
                        for i, t in (("t_cpu", "Top 10 VMs by avg CPU %"), ("t_iops", "Top 10 VMs by avg IOPS"),
                                     ("t_lat", "Top 10 VMs by max latency (ms)")))
    selector = ""
    if continuous:
        selector = ('<h3>Single VM over time</h3><p><select id="vmsel">%s</select></p><div class="grid">%s</div>' % (
            vm_opts, "".join('<div class="card"><h3>%s</h3><div class="chart"><canvas id="%s"></canvas></div></div>' % (t, i)
                             for i, t in (("v_cpu", "CPU %"), ("v_mem", "Memory used %"), ("v_iops", "IOPS"),
                                          ("v_lat", "Latency (ms)")))))

    page = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%(title)s</title>
<style>%(css)s</style></head>
<body><main>
<header><pre class="banner" aria-label="CITRIX-ISRAEL">%(banner)s</pre><h1>%(title)s</h1><div class="meta">%(header)s</div></header>
<p id="nochart" class="note" hidden>Charts need internet access to load Chart.js.</p>
<section class="kpis">%(kpis)s</section>
<section><h2>Hosts</h2>%(host_table)s<div class="grid">%(host_charts)s</div></section>
<section><h2>Virtual machines</h2>
<div class="filters"><input id="vmfilter" type="search" placeholder="Filter by VM name"><select id="hostfilter"><option value="">All hosts</option>%(host_opts)s</select>
<span id="vmcount" class="note"></span></div>
%(vm_table)s
<div class="grid">%(vm_charts)s</div>%(selector)s</section>
<section><h2>Storage repositories</h2>%(sr_table)s</section>
<footer class="note">Generated %(now)s by xen_collector.py. Amber/red cells: CPU &ge; 75/90 %%, memory &ge; 80/90 %%, latency &ge; 10/20 ms.</footer>
</main>
<script type="application/json" id="data">%(data)s</script>
<script src="%(chartjs)s"></script>
<script>%(js)s</script>
</body></html>
""" % dict(title=esc("%s - metrics" % pool_name), css=_CSS, banner=esc("\n".join(banner_lines())), header=header, kpis=kpis, host_table=host_table,
           host_charts=host_charts, host_opts=host_opts, vm_table=vm_table, vm_charts=vm_charts,
           selector=selector, sr_table=sr_table, now=esc(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
           data=json_for_script(data), chartjs=CHARTJS_URL, js=_JS)
    path = out_dir / "dashboard.html"
    path.write_text(page, encoding="utf-8")
    return path


def _series_mean(rows: List[Dict[str, str]], idx: Dict[int, int], n: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * n
    for r in rows:
        try:
            out[idx[int(r["sample_no"])]] = mean_of([num(r, "read_latency_ms"), num(r, "write_latency_ms")])
        except (KeyError, ValueError):
            pass
    return out


_CSS = """
:root{--bg:#f6f7f9;--card:#fff;--fg:#1c2330;--muted:#5d6878;--line:#dde2ea;--accent:#2f6fdb;
--warn:#fff1c9;--warnfg:#7a5300;--crit:#fcd9d6;--critfg:#8a1c14;--grid:#e6e9ef}
@media (prefers-color-scheme:dark){:root{--bg:#14171c;--card:#1d222a;--fg:#e6e9ee;--muted:#9aa5b5;--line:#2c333d;
--accent:#6ea0ff;--warn:#4a3a0c;--warnfg:#ffd77a;--crit:#4e1c1a;--critfg:#ffb4ad;--grid:#2c333d}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
main{max-width:1280px;margin:0 auto;padding:16px}
.banner{margin:0 0 14px;font:700 clamp(5px,1.15vw,12px)/1.15 ui-monospace,Menlo,Consolas,monospace;color:var(--accent,#0b7285);overflow-x:auto;white-space:pre}
h1{margin:0 0 6px;font-size:22px}h2{margin:28px 0 10px;font-size:18px}h3{margin:0 0 8px;font-size:14px;color:var(--muted);font-weight:600}
.meta{display:flex;flex-wrap:wrap;gap:6px 22px;color:var(--muted)}.hl{font-weight:600;color:var(--fg)}
.note{color:var(--muted);font-size:12px}#nochart{background:var(--warn);color:var(--warnfg);padding:6px 10px;border-radius:6px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:14px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.kv{font-size:22px;font-weight:600}.kl{color:var(--muted);font-size:12px}
.kpi.warn{background:var(--warn);color:var(--warnfg)}.kpi.crit{background:var(--crit);color:var(--critfg)}
.kpi.warn .kl,.kpi.crit .kl{color:inherit}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px;margin-top:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px;min-width:0}
.chart{position:relative;height:260px}
.scroll{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:8px}
table{border-collapse:collapse;width:100%}th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
thead th{background:var(--card);color:var(--muted);font-weight:600;position:sticky;top:0}
td.num{text-align:right;font-variant-numeric:tabular-nums}td.na{color:var(--muted)}
td.warn{background:var(--warn);color:var(--warnfg)}td.crit{background:var(--crit);color:var(--critfg)}
th.sortable{cursor:pointer;user-select:none}th.sortable.asc::after{content:" \\25B2"}th.sortable.desc::after{content:" \\25BC"}
.filters{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px}
input,select{background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:6px 8px;font:inherit}
.badge{background:var(--crit);color:var(--critfg);border-radius:4px;padding:1px 6px;font-size:11px}
code{font-size:12px}footer{margin-top:28px}
"""

_JS = """
(function(){
var D=JSON.parse(document.getElementById('data').textContent);
// ---- table sorting / filtering (no Chart.js needed) ----
var tbl=document.getElementById('vmtable');
if(tbl){
  var tb=tbl.tBodies[0], ths=tbl.tHead.rows[0].cells;
  Array.prototype.forEach.call(ths,function(th,i){
    th.addEventListener('click',function(){
      var asc=!th.classList.contains('asc');
      Array.prototype.forEach.call(ths,function(t){t.classList.remove('asc','desc');});
      th.classList.add(asc?'asc':'desc');
      var isnum=th.dataset.type==='num', rows=Array.prototype.slice.call(tb.rows);
      function key(r){var v=r.cells[i].dataset.v; if(isnum){return v===''?null:parseFloat(v);} return v||'';}
      rows.sort(function(a,b){var x=key(a),y=key(b);
        if(x===null&&y===null)return 0; if(x===null)return 1; if(y===null)return -1;
        var c=x<y?-1:(x>y?1:0); return asc?c:-c;});
      rows.forEach(function(r){tb.appendChild(r);});
    });
  });
  var f=document.getElementById('vmfilter'), hf=document.getElementById('hostfilter'), cnt=document.getElementById('vmcount');
  function apply(){var q=f.value.trim().toLowerCase(),h=hf.value,n=0;
    Array.prototype.forEach.call(tb.rows,function(r){
      var ok=(!q||r.dataset.name.indexOf(q)>=0)&&(!h||r.dataset.host===h);
      r.style.display=ok?'':'none'; if(ok)n++;});
    cnt.textContent=n+' of '+tb.rows.length+' VMs';}
  f.addEventListener('input',apply); hf.addEventListener('change',apply); apply();
}
// ---- charts ----
if(!window.Chart){document.getElementById('nochart').hidden=false;return;}
var css=getComputedStyle(document.documentElement), col=function(n){return css.getPropertyValue(n).trim();};
Chart.defaults.color=col('--muted'); Chart.defaults.borderColor=col('--grid');
var PAL=['#2f6fdb','#e0702b','#2a9d6f','#b04fd0','#d6a100','#d6456b','#4a9bd4','#7f8c2a','#8b6b4a','#5b6bd6'];
var live=[];
function lineChart(id,datasets,unit){
  var c=new Chart(document.getElementById(id),{type:'line',
    data:{labels:D.labels,datasets:datasets.map(function(d,i){return {label:d.name,data:d.data,borderColor:PAL[i%PAL.length],
      backgroundColor:PAL[i%PAL.length],borderWidth:2,pointRadius:2,spanGaps:true,tension:.2};})},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
      scales:{y:{beginAtZero:true,title:{display:!!unit,text:unit}}},plugins:{legend:{display:datasets.length>1}}}});
  return c;
}
function barChart(id,labels,values,unit){
  return new Chart(document.getElementById(id),{type:'bar',
    data:{labels:labels,datasets:[{data:values,backgroundColor:PAL[0]}]},
    options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,
      scales:{x:{beginAtZero:true,title:{display:!!unit,text:unit}}},plugins:{legend:{display:false}}}});
}
if(D.mode==='continuous'){
  [['h_cpu','cpu','%'],['h_mem','mem','%'],['h_iops','iops','IOPS'],['h_lat','lat','ms']].forEach(function(s){
    lineChart(s[0],D.hosts.map(function(h){return {name:h.name,data:h[s[1]]};}),s[2]);});
}else{
  var b=D.host_bars;
  [['h_cpu','cpu','%'],['h_mem','mem','%'],['h_iops','iops','IOPS'],['h_lat','lat','ms']].forEach(function(s){
    barChart(s[0],b.labels,b[s[1]],s[2]);});
}
barChart('t_cpu',D.top.cpu.labels,D.top.cpu.values,'%');
barChart('t_iops',D.top.iops.labels,D.top.iops.values,'IOPS');
barChart('t_lat',D.top.lat.labels,D.top.lat.values,'ms');
var sel=document.getElementById('vmsel');
if(sel){
  function drawVm(){
    live.forEach(function(c){c.destroy();}); live=[];
    var v=D.vm_series[sel.value]; if(!v)return;
    live.push(lineChart('v_cpu',[{name:'CPU %',data:v.cpu}],'%'));
    live.push(lineChart('v_mem',[{name:'Memory used %',data:v.mem}],'%'));
    live.push(lineChart('v_iops',[{name:'IOPS',data:v.iops}],'IOPS'));
    live.push(lineChart('v_lat',[{name:'mean r/w',data:v.lat_mean},{name:'max',data:v.lat_max}],'ms'));
  }
  sel.addEventListener('change',drawVm); drawVm();
}
})();
"""

# ===========================================================================
# CLI
# ===========================================================================


def _ranged(lo: int, hi: int, what: str) -> Any:
    def conv(text: str) -> int:
        try:
            v = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError("%r is not an integer" % text)
        if not lo <= v <= hi:
            raise argparse.ArgumentTypeError("%s must be between %d and %d" % (what, lo, hi))
        return v
    return conv


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect XenServer/XCP-ng pool metrics over SSH and build an HTML dashboard.")
    p.add_argument("--host", help="pool master address")
    p.add_argument("--user", default="root")
    p.add_argument("--port", type=int, default=22)
    p.add_argument("--key-file", help="SSH private key (otherwise a password is requested)")
    p.add_argument("--mode", choices=["snapshot", "continuous"])
    p.add_argument("--interval", type=_ranged(*INTERVAL_RANGE, "interval (seconds)"),
                   help="sampling interval in seconds, %d-%d (continuous mode)" % INTERVAL_RANGE)
    p.add_argument("--duration", type=_ranged(*DURATION_RANGE, "duration (minutes)"),
                   help="total duration in minutes, %d-%d (continuous mode)" % DURATION_RANGE)
    p.add_argument("--output-dir", help="default ./xen_metrics_<YYYYmmdd_HHMMSS>")
    p.add_argument("--host-latency-unit", choices=["us", "ms"], default="us",
                   help="unit of host SR latency data sources (default us)")
    p.add_argument("--demo", action="store_true", help="synthetic pool, no SSH")
    p.add_argument("--demo-fast", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--dashboard-only", metavar="DIR", help="rebuild dashboard.html from CSVs in DIR")
    p.add_argument("--no-open", action="store_true", help="do not open the dashboard in a browser")
    return p.parse_args(argv)


def ask_int(prompt: str, lo: int, hi: int, default: int) -> int:
    while True:
        raw = input("%s (%d-%d, default %d): " % (prompt, lo, hi, default)).strip()
        if not raw:
            return default
        if raw.isdigit() and lo <= int(raw) <= hi:
            return int(raw)
        print("  Please enter a number between %d and %d." % (lo, hi))


def resolve_interactive(args: argparse.Namespace) -> None:
    if args.mode is None:
        print("Collection mode: [1] One-time snapshot  [2] Continuous sampling")
        while True:
            c = input("Choice [1/2]: ").strip()
            if c in ("1", "2"):
                args.mode = "snapshot" if c == "1" else "continuous"
                break
            print("  Please enter 1 or 2.")
    if args.mode == "continuous":
        if args.interval is None:
            args.interval = ask_int("Sampling interval in seconds", *INTERVAL_RANGE, 120)
        if args.duration is None:
            args.duration = ask_int("Duration in minutes", *DURATION_RANGE, 30)
    else:
        args.interval = args.interval or INTERVAL_RANGE[0]
        args.duration = args.duration or 30
    if not args.demo and not args.host:
        args.host = input("Pool master address: ").strip()
        if not args.host:
            print("A pool master address is required.")
            sys.exit(2)


def finish(out_dir: Path, no_open: bool, sink_paths: bool = True) -> None:
    dash = build_dashboard(out_dir)
    print("\nOutput files:")
    for n in ("inventory.csv", "host_metrics.csv", "vm_metrics.csv", "host_sr_metrics.csv", "collector.log"):
        if (out_dir / n).exists():
            print("  %s" % (out_dir / n).resolve())
    if dash:
        print("  %s" % dash.resolve())
        if not no_open:
            try:
                webbrowser.open(dash.resolve().as_uri())
            except Exception as exc:
                print("Could not open browser: %s" % exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    print_banner()
    if args.dashboard_only:
        d = Path(args.dashboard_only)
        if not (d / "host_metrics.csv").exists() and not (d / "vm_metrics.csv").exists():
            print("No host_metrics.csv / vm_metrics.csv found in %s" % d)
            return 2
        finish(d, args.no_open)
        return 0

    resolve_interactive(args)
    out_dir = Path(args.output_dir or "xen_metrics_%s" % datetime.now().strftime("%Y%m%d_%H%M%S"))
    out_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(out_dir / "collector.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    host_lat_div = HOST_LATENCY_DIVISOR if args.host_latency_unit == "us" else 1.0
    backend: Any
    if args.demo:
        backend = DemoBackend()
    else:
        import_paramiko()
        backend = XenBackend(args, load_credentials(args))
    sink = CsvSink(out_dir)
    meta: Dict[str, Any] = {"mode": args.mode, "interval": args.interval if args.mode == "continuous" else None,
                            "duration_min": args.duration if args.mode == "continuous" else None,
                            "master": args.host or "demo", "started": datetime.now().astimezone().isoformat(timespec="seconds")}
    try:
        backend.connect()
        pool = backend.map_pool()
        meta.update(pool_name=pool.name, master=pool.master_address)
        write_meta(out_dir, meta)
        print_tree(pool)
        log.info("pool=%s hosts=%d vms=%d mode=%s", pool.name, len(pool.hosts), len(pool.vms), args.mode)
        run_collection(backend, pool, sink, args, host_lat_div)
    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as exc:
        log.exception("fatal error")
        print("ERROR: %s" % describe_ssh_error(exc, args.host or ""))
        return 1
    finally:
        meta["finished"] = datetime.now().astimezone().isoformat(timespec="seconds")
        write_meta(out_dir, meta)
        backend.close()
        sink.close()
    finish(out_dir, args.no_open)
    return 0


if __name__ == "__main__":
    sys.exit(main())
