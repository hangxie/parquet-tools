#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill test coverage into coverage.csv, then generate an HTML and PNG chart.

For each day in the target range:
  - If already recorded in coverage.csv, skip it.
  - If a commit exists for that day, check it out and run go test.
  - If no commit exists, carry the previous day's coverage forward.
  - Days before the first commit with meaningful coverage (> 0%) are skipped.

New entries are written only after git HEAD is restored, so mid-loop
checkouts never corrupt coverage.csv.

The chart displays weekly data points (Mondays) to avoid clutter, while
coverage.csv retains the full daily history.

Usage:
    python3 scripts/coverage-history.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                        [output.html] [coverage.csv]

    --start     First day to collect (default: 7 days ago)
    --end       Last day to collect (default: today)
"""

import argparse
import csv
import math
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = "hangxie/parquet-tools"

# ---------------------------------------------------------------------------
# Coverage collection
# ---------------------------------------------------------------------------

def repo_root():
    return Path(subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True
    ).strip())


def current_ref():
    """Return the current branch name, or commit SHA if HEAD is detached."""
    try:
        return subprocess.check_output(
            ["git", "symbolic-ref", "--short", "HEAD"],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()


def git_checkout(ref):
    result = subprocess.run(
        ["git", "checkout", ref, "--quiet"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git checkout {ref} failed: {result.stderr.strip()}")


def git_stash():
    """Stash uncommitted changes; return True if anything was stashed."""
    result = subprocess.run(
        ["git", "stash", "--quiet"], capture_output=True, text=True,
    )
    return "No local changes to save" not in result.stdout


def git_stash_pop():
    subprocess.run(["git", "stash", "pop", "--quiet"], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def load_commits_by_day():
    """Return {date_str: sha} mapping each UTC day to its latest commit SHA.

    Uses Unix timestamps so day attribution is consistent regardless of the
    commit author's timezone or the system's local timezone.
    """
    out = subprocess.check_output(["git", "log", "--format=%H %at"], text=True)
    by_day = {}
    for line in out.splitlines():
        sha, ts_str = line.split()
        d = datetime.fromtimestamp(int(ts_str), tz=timezone.utc).strftime("%Y-%m-%d")
        if d not in by_day:  # git log is newest-first; first seen = latest
            by_day[d] = sha
    return by_day


def run_coverage(root, build_dir):
    """Run go test at current HEAD and return total coverage as a float.

    Test failures (e.g. flaky network tests hitting dead S3/GCS URLs) do not
    prevent coverage from being measured — go test still writes the profile.
    Only a build error (no profile produced) raises an exception.
    """
    build_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "CGO_ENABLED": "1"}
    tmp = build_dir / "coverage.out.tmp"
    out = build_dir / "coverage.out"

    subprocess.run(
        ["go", "test", "-parallel", "4", "-count", "1", "-trimpath",
         f"-coverprofile={tmp}", "./..."],
        cwd=str(root), env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if not tmp.exists():
        raise RuntimeError("go test produced no coverage profile (build error?)")

    with open(tmp) as f, open(out, "w") as g:
        for line in f:
            if "cmd/internal/testutils" not in line and "parquet-go" not in line:
                g.write(line)

    result = subprocess.run(
        ["go", "tool", "cover", f"-func={out}"],
        capture_output=True, text=True, check=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("total:"):
            return float(line.split()[-1].rstrip("%"))
    raise RuntimeError("no total coverage line in go tool cover output")


def load_existing(csv_path):
    """Return {date_str: coverage} from coverage.csv."""
    if not csv_path.exists():
        return {}
    entries = {}
    with open(csv_path, newline="") as f:
        for row in csv.reader(f):
            if len(row) < 2:
                continue
            d = datetime.fromtimestamp(int(row[0].strip()), tz=timezone.utc).strftime("%Y-%m-%d")
            entries[d] = float(row[1].strip())
    return entries


def write_entries(csv_path, all_entries):
    """Rewrite coverage.csv with all entries sorted chronologically."""
    with open(csv_path, "w") as f:
        for date_str in sorted(all_entries):
            d = date.fromisoformat(date_str)
            ts = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
            f.write(f"{ts},{all_entries[date_str]:.1f}\n")


def collect(csv_path, start, end):
    root = repo_root()
    build_dir = root / "build" / "test"
    commits_by_day = load_commits_by_day()

    print(f"Collecting coverage from {start} to {end}...")

    existing = load_existing(csv_path)
    ref = current_ref()
    new_entries = {}
    prev_cov = None

    for d_str in sorted(existing):
        if d_str < start.isoformat():
            prev_cov = existing[d_str]

    stashed = git_stash()
    try:
        cur = start
        while cur <= end:
            d_str = cur.isoformat()

            if d_str in existing:
                print(f"  {d_str}: already recorded, skipping")
                prev_cov = existing[d_str]
                cur += timedelta(days=1)
                continue

            commit = commits_by_day.get(d_str, "")

            if commit:
                print(f"  {d_str}: {commit[:8]}", end=" ... ", flush=True)
                git_checkout(commit)
                try:
                    cov = run_coverage(root, build_dir)
                    print(f"{cov:.1f}%")
                    if cov > 0:
                        prev_cov = cov
                        new_entries[d_str] = cov
                    else:
                        print(f"  {d_str}: skipping 0% (no tests yet)")
                except Exception as e:
                    print(f"failed ({e}), skipping")
            elif prev_cov is not None:
                print(f"  {d_str}: no commit, carrying forward {prev_cov:.1f}%")
                new_entries[d_str] = prev_cov
            else:
                print(f"  {d_str}: no commit and no prior data, skipping")

            cur += timedelta(days=1)

    finally:
        git_checkout(ref)
        if stashed:
            git_stash_pop()

    if new_entries:
        merged = {**existing, **new_entries}
        write_entries(csv_path, merged)
        for date_str in sorted(new_entries):
            print(f"  recorded {date_str}: {new_entries[date_str]:.1f}%")
        print(f"Done. Added {len(new_entries)} entries.")
    else:
        print("Nothing new to record.")

# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

def filter_weekly(points, weekday=0):
    """Return a subset of points keeping one entry per week on the given weekday.

    weekday follows Python's date.weekday() convention: 0=Monday … 6=Sunday.
    The most recent point is always included so the chart reflects latest data.
    """
    if not points:
        return points
    result = [
        (d, c) for d, c in points
        if datetime.strptime(d, "%Y-%m-%d").weekday() == weekday
    ]
    if not result or result[-1] != points[-1]:
        result.append(points[-1])
    return result


def load_coverage(csv_path):
    """Return sorted list of (date_string, coverage_float), one entry per day."""
    seen = {}
    try:
        with open(csv_path, newline="") as f:
            for row in csv.reader(f):
                if len(row) < 2:
                    continue
                ts = int(row[0].strip())
                cov = float(row[1].strip())
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                seen[date_str] = cov
    except FileNotFoundError:
        print(f"Coverage CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    return sorted(seen.items())


def pct_tick_step(y_min, y_max):
    """Return a tick step giving 3-6 gridlines across the y range."""
    r = y_max - y_min
    for step in (0.5, 1, 2, 5, 10, 20):
        if 3 <= r / step <= 6:
            return step
    return max(1.0, round(r / 4))


def generate_html(points, output_path):
    if not points:
        print("No coverage data to chart.", file=sys.stderr)
        sys.exit(1)

    dates = [d for d, _ in points]
    covs = [c for _, c in points]
    min_cov, max_cov = min(covs), max(covs)

    y_min = max(0.0, math.floor(min_cov - 2))
    y_max = min(100.0, math.ceil(max_cov + 1))
    y_range = y_max - y_min
    step = pct_tick_step(y_min, y_max)

    W, H = 900, 420
    ML, MR, MT, MB = 65, 30, 30, 50
    cw, ch = W - ML - MR, H - MT - MB

    first = datetime.strptime(dates[0], "%Y-%m-%d")
    last = datetime.strptime(dates[-1], "%Y-%m-%d")
    span = (last - first).days or 1

    def xp(d):
        return ML + (datetime.strptime(d, "%Y-%m-%d") - first).days / span * cw

    def yp(c):
        return MT + ch - (c - y_min) / y_range * ch

    y_els = []
    tick = y_min
    while tick <= y_max + 1e-9:
        y = yp(tick)
        y_els.append(
            f'<line x1="{ML}" y1="{y:.1f}" x2="{ML+cw}" y2="{y:.1f}" '
            f'stroke="#eee" stroke-width="1"/>'
        )
        label = f"{tick:.1f}%" if step < 1 else f"{tick:.0f}%"
        y_els.append(
            f'<text x="{ML-8}" y="{y:.1f}" text-anchor="end" '
            f'dominant-baseline="middle" font-size="12" fill="#999">{label}</text>'
        )
        tick = round(tick + step, 10)

    x_els = []
    x_els.append(
        f'<text x="{xp(dates[0]):.1f}" y="{MT+ch+20}" text-anchor="middle" '
        f'font-size="12" fill="#999">{first.year}</text>'
    )
    year = first.year + 1
    while True:
        jan1 = datetime(year, 1, 1)
        if jan1 > last:
            break
        x = xp(jan1.strftime("%Y-%m-%d"))
        x_els.append(
            f'<line x1="{x:.1f}" y1="{MT}" x2="{x:.1f}" y2="{MT+ch}" '
            f'stroke="#eee" stroke-width="1" stroke-dasharray="3,3"/>'
        )
        x_els.append(
            f'<text x="{x:.1f}" y="{MT+ch+20}" text-anchor="middle" '
            f'font-size="12" fill="#999">{year}</text>'
        )
        year += 1

    svg_pts = [(xp(d), yp(c)) for d, c in points]
    line_d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in svg_pts)
    baseline_y = yp(y_min)
    area_d = (
        f"M{ML:.1f},{baseline_y:.1f} L"
        + " L".join(f"{x:.1f},{y:.1f}" for x, y in svg_pts)
        + f" L{svg_pts[-1][0]:.1f},{baseline_y:.1f} Z"
    )

    js_points = []
    for (d, cov), (x, _) in zip(points, svg_pts):
        label = datetime.strptime(d, "%Y-%m-%d").strftime("%b %d")
        js_points.append(f'{{x:{x:.1f},d:"{label}",n:{cov:.1f}}}')
    js_points_str = "[" + ",".join(js_points) + "]"

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    latest_cov = covs[-1]

    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Coverage History — {REPO}</title>
<style>
html {{
  min-height: 100%;
}}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  margin: 0; min-height: 100vh; background: #fff; color: #333;
}}
.chart-page {{
  box-sizing: border-box;
  min-height: 100vh;
  padding: clamp(12px, 2.5vw, 32px);
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 10px;
}}
h2 {{
  width: min(100%, 175vh);
  font-size: 16px; font-weight: 600; margin: 0 auto;
}}
.chart {{
  display: block;
  width: min(100%, 175vh);
  height: auto;
  margin: 0 auto;
}}
#tip {{
  position: fixed; background: #1e293b; color: #f8fafc;
  padding: 5px 10px; border-radius: 5px; font-size: 13px;
  pointer-events: none; display: none; white-space: nowrap;
  box-shadow: 0 2px 8px rgba(0,0,0,.25);
}}
p.meta {{
  width: min(100%, 175vh);
  font-size: 11px; color: #bbb; margin: 0 auto;
}}
@media (max-width: 640px) {{
  .chart-page {{ justify-content: flex-start; }}
}}
</style>
</head>
<body>
<main class="chart-page">
<h2>{REPO} — Test Coverage</h2>
<div id="tip"></div>
<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-labelledby="chart-title chart-desc">
  <title id="chart-title">{REPO} test coverage history</title>
  <desc id="chart-desc">Test coverage from {dates[0]} to {dates[-1]}.</desc>
  <defs>
    <linearGradient id="area-fill" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#16a34a" stop-opacity="0.18"/>
      <stop offset="100%" stop-color="#16a34a" stop-opacity="0.02"/>
    </linearGradient>
  </defs>
  {''.join(y_els)}
  {''.join(x_els)}
  <line x1="{ML}" y1="{MT}" x2="{ML}" y2="{MT+ch}" stroke="#ddd" stroke-width="1"/>
  <line x1="{ML}" y1="{MT+ch}" x2="{ML+cw}" y2="{MT+ch}" stroke="#ddd" stroke-width="1"/>
  <text x="14" y="{MT + ch//2}" text-anchor="middle" font-size="12" fill="#999"
        transform="rotate(-90,14,{MT + ch//2})">Coverage</text>
  <path d="{area_d}" fill="url(#area-fill)"/>
  <path d="{line_d}" fill="none" stroke="#16a34a" stroke-width="4"/>
  <path id="hover-zone" d="{line_d}" fill="none" stroke="transparent" stroke-width="20" style="cursor:default"/>
</svg>
<p class="meta">Updated {today} &middot; latest {latest_cov:.1f}%</p>
</main>
<script>
const tip = document.getElementById('tip');
const pts = {js_points_str};
const zone = document.getElementById('hover-zone');
const svg = zone.closest('svg');
zone.addEventListener('mousemove', e => {{
  const rect = svg.getBoundingClientRect();
  const viewBoxWidth = svg.viewBox.baseVal.width || rect.width;
  const mx = rect.width ? (e.clientX - rect.left) * viewBoxWidth / rect.width : 0;
  let best = null, minD = Infinity;
  for (const p of pts) {{
    const d = Math.abs(p.x - mx);
    if (d < minD) {{ minD = d; best = p; }}
  }}
  if (best) {{
    tip.textContent = best.d + ' — ' + best.n + '% covered';
    tip.style.display = 'block';
    tip.style.left = (e.clientX + 14) + 'px';
    tip.style.top = (e.clientY - 36) + 'px';
  }}
}});
zone.addEventListener('mouseleave', () => {{ tip.style.display = 'none'; }});
</script>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)
    print(f"Generated {output_path} ({len(points)} data points, latest: {latest_cov:.1f}%)")


def generate_png(points, output_path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    if not points:
        print("No coverage data to chart.", file=sys.stderr)
        sys.exit(1)

    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in points]
    covs = [c for _, c in points]
    latest_cov = covs[-1]

    y_min = max(0.0, math.floor(min(covs) - 2))
    y_max = min(100.0, math.ceil(max(covs) + 1))
    step = pct_tick_step(y_min, y_max)

    fig, ax = plt.subplots(figsize=(9, 4.2), dpi=100)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.plot(dates, covs, color="#16a34a", linewidth=2.5, solid_capstyle="round")
    ax.fill_between(dates, covs, alpha=0.12, color="#16a34a")

    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_major_locator(plt.MultipleLocator(step))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.grid(axis="y", color="#eeeeee", linewidth=1, linestyle="-", zorder=0)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.AutoDateLocator()))
    ax.grid(axis="x", color="#eeeeee", linewidth=1, linestyle="--", zorder=0)

    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#dddddd")
    ax.spines["bottom"].set_color("#dddddd")
    ax.tick_params(colors="#999999", labelsize=10)
    ax.set_ylabel("Coverage", color="#999999", fontsize=10)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fig.suptitle(
        f"{REPO} — Test Coverage",
        fontsize=13, fontweight="bold", color="#333333", y=0.97,
    )
    fig.text(
        0.5, 0.01,
        f"Updated {today} · latest {latest_cov:.1f}%",
        ha="center", va="bottom", fontsize=9, color="#bbbbbb",
    )

    plt.tight_layout(rect=[0, 0.04, 1, 0.93])
    plt.savefig(output_path, dpi=100, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Generated {output_path} ({len(points)} data points, latest: {latest_cov:.1f}%)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", metavar="YYYY-MM-DD",
                        help="first day to collect (default: 7 days ago)")
    parser.add_argument("--end", metavar="YYYY-MM-DD",
                        help="last day to collect (default: today)")
    parser.add_argument("output", nargs="?", default="coverage-history.html",
                        help="output HTML file (default: coverage-history.html)")
    parser.add_argument("csv", nargs="?", default="scripts/coverage.csv",
                        help="coverage CSV file (default: scripts/coverage.csv)")
    args = parser.parse_args()

    today = date.today()
    start = date.fromisoformat(args.start) if args.start else today - timedelta(days=6)
    end = date.fromisoformat(args.end) if args.end else today

    if start > end:
        parser.error(f"--start {start} is after --end {end}")

    collect(Path(args.csv), start, end)

    print(f"Loading coverage data from {args.csv}...", flush=True)
    points = load_coverage(args.csv)
    if not points:
        print("No data points found.", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {len(points)} data points")
    chart_points = filter_weekly(points)
    print(f"Using {len(chart_points)} weekly data points for chart")
    generate_html(chart_points, args.output)
    if args.output.endswith(".html"):
        generate_png(chart_points, args.output[:-5] + ".png")


if __name__ == "__main__":
    main()
