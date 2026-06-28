#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch star history for hangxie/parquet-tools and generate an interactive HTML chart
and a static PNG image (requires matplotlib).

Usage:
    python3 scripts/star-history.py [output.html]

Output:
    <output.html>          interactive chart
    <output.png>           static chart for wiki embedding (sibling of the HTML file)

Environment:
    GITHUB_TOKEN  GitHub personal access token (raises rate limit from 60 to 5000/hr)
"""

import json
import os
import sys
import urllib.request
import urllib.error
from collections import Counter
from datetime import datetime, timedelta, timezone

REPO = "hangxie/parquet-tools"
RECENT_DAYS = 30  # daily hover tips for this many days at the trailing end of the chart


def fetch_stars(token=None):
    """Return sorted list of star date strings ('YYYY-MM-DD') via GitHub API."""
    stars = []
    page = 1
    headers = {
        "Accept": "application/vnd.github.v3.star+json",
        "User-Agent": "star-history/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    while True:
        url = f"https://api.github.com/repos/{REPO}/stargazers?per_page=100&page={page}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"GitHub API error (page {page}): {e}", file=sys.stderr)
            sys.exit(1)

        if not data:
            break
        for item in data:
            stars.append(item["starred_at"][:10])  # "YYYY-MM-DD"
        if len(data) < 100:
            break
        page += 1

    return sorted(stars)


def cumulative_by_date(star_dates):
    """Return dict mapping date string -> cumulative star count."""
    daily = Counter(star_dates)
    result = {}
    running = 0
    for d in sorted(daily):
        running += daily[d]
        result[d] = running
    return result


def monthly_points(cumulative):
    """Return list of (date, count) using the last day with data in each month."""
    monthly = {}
    for date, count in sorted(cumulative.items()):
        monthly[date[:7]] = (date, count)
    return [v for _, v in sorted(monthly.items())]


def tick_step(max_val):
    """Return a tick interval giving 2-5 gridlines from 0 to max_val."""
    for step in [50, 100, 200, 250, 500, 1000, 2000, 5000, 10000, 25000]:
        if 2 <= max_val // step <= 5:
            return step
    return max(1, max_val // 4)


def generate_html(monthly, cumulative, output_path, recent_days=RECENT_DAYS):
    if not monthly:
        print("No star data to chart.", file=sys.stderr)
        sys.exit(1)

    max_count = monthly[-1][1]
    y_max = max_count
    y_step = tick_step(max_count)

    W, H = 900, 420
    ML, MR, MT, MB = 65, 30, 30, 50
    cw, ch = W - ML - MR, H - MT - MB

    first = datetime.strptime(monthly[0][0], "%Y-%m-%d")
    last = datetime.strptime(monthly[-1][0], "%Y-%m-%d")
    span = (last - first).days or 1

    def xp(d):
        return ML + (datetime.strptime(d, "%Y-%m-%d") - first).days / span * cw

    def yp(c):
        return MT + ch - c / y_max * ch

    # Y gridlines and labels
    y_els = []
    tick = 0
    while tick <= y_max:
        y = yp(tick)
        y_els.append(
            f'<line x1="{ML}" y1="{y:.1f}" x2="{ML+cw}" y2="{y:.1f}" '
            f'stroke="#eee" stroke-width="1"/>'
        )
        y_els.append(
            f'<text x="{ML-8}" y="{y:.1f}" text-anchor="end" '
            f'dominant-baseline="middle" font-size="12" fill="#999">{tick}</text>'
        )
        tick += y_step

    # X gridlines (Jan 1 of each year after the first) and year labels
    x_els = []
    x_els.append(
        f'<text x="{ML}" y="{MT+ch+20}" text-anchor="middle" '
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

    # SVG line and area paths (always monthly resolution)
    pts = [(xp(d), yp(c)) for d, c in monthly]
    line_d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area_d = (
        f"M{ML:.1f},{MT+ch:.1f} L"
        + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        + f" L{pts[-1][0]:.1f},{MT+ch:.1f} Z"
    )

    # Daily hit targets for the trailing recent_days period
    cutoff = last - timedelta(days=recent_days - 1)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    pre = {d: c for d, c in cumulative.items() if d < cutoff_str}
    running = cumulative[max(pre)] if pre else 0
    daily_recent = []
    for i in range(recent_days):
        d = (cutoff + timedelta(days=i)).strftime("%Y-%m-%d")
        if d in cumulative:
            running = cumulative[d]
        daily_recent.append((d, running))

    # JS point data: monthly labels outside recent window, daily labels inside
    js_points = []
    for (date, count), (x, _) in zip(monthly, pts):
        if date >= cutoff_str:
            continue
        label = datetime.strptime(date, "%Y-%m-%d").strftime("%b 01")
        js_points.append(f'{{x:{x:.1f},d:"{label}",n:{count}}}')
    for date, count in daily_recent:
        x = xp(date)
        label = datetime.strptime(date, "%Y-%m-%d").strftime("%b %d")
        js_points.append(f'{{x:{x:.1f},d:"{label}",n:{count}}}')
    js_points_str = "[" + ",".join(js_points) + "]"

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Star History — hangxie/parquet-tools</title>
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
<h2>hangxie/parquet-tools — Star History</h2>
<div id="tip"></div>
<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-labelledby="chart-title chart-desc">
  <title id="chart-title">hangxie/parquet-tools star history</title>
  <desc id="chart-desc">Cumulative GitHub stars from {monthly[0][0]} to {monthly[-1][0]}.</desc>
  <defs>
    <linearGradient id="area-fill" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#2563eb" stop-opacity="0.18"/>
      <stop offset="100%" stop-color="#2563eb" stop-opacity="0.02"/>
    </linearGradient>
  </defs>
  {''.join(y_els)}
  {''.join(x_els)}
  <line x1="{ML}" y1="{MT}" x2="{ML}" y2="{MT+ch}" stroke="#ddd" stroke-width="1"/>
  <line x1="{ML}" y1="{MT+ch}" x2="{ML+cw}" y2="{MT+ch}" stroke="#ddd" stroke-width="1"/>
  <text x="14" y="{MT + ch//2}" text-anchor="middle" font-size="12" fill="#999"
        transform="rotate(-90,14,{MT + ch//2})">Stars</text>
  <path d="{area_d}" fill="url(#area-fill)"/>
  <path d="{line_d}" fill="none" stroke="#2563eb" stroke-width="4"/>
  <path id="hover-zone" d="{line_d}" fill="none" stroke="transparent" stroke-width="20" style="cursor:default"/>
</svg>
<p class="meta">Updated {today} &middot; {max_count} total stars</p>
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
    tip.textContent = best.d + ' — ' + best.n + ' ★';
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
    print(f"Generated {output_path} ({max_count} stars, {len(monthly)} monthly + {recent_days} daily points)")


def generate_png(monthly, output_path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    if not monthly:
        print("No star data to chart.", file=sys.stderr)
        sys.exit(1)

    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in monthly]
    counts = [c for _, c in monthly]
    max_count = counts[-1]

    fig, ax = plt.subplots(figsize=(9, 4.2), dpi=100)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.plot(dates, counts, color="#2563eb", linewidth=2.5, solid_capstyle="round")
    ax.fill_between(dates, counts, alpha=0.12, color="#2563eb")

    y_step = tick_step(max_count)
    ax.yaxis.set_major_locator(plt.MultipleLocator(y_step))
    ax.grid(axis="y", color="#eeeeee", linewidth=1, linestyle="-", zorder=0)

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(axis="x", color="#eeeeee", linewidth=1, linestyle="--", zorder=0)

    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#dddddd")
    ax.spines["bottom"].set_color("#dddddd")
    ax.tick_params(colors="#999999", labelsize=10)
    ax.set_ylabel("Stars", color="#999999", fontsize=10)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fig.suptitle(
        "hangxie/parquet-tools — Star History",
        fontsize=13, fontweight="bold", color="#333333", y=0.97,
    )
    fig.text(
        0.5, 0.01,
        f"Updated {today} · {max_count} total stars",
        ha="center", va="bottom", fontsize=9, color="#bbbbbb",
    )

    plt.tight_layout(rect=[0, 0.04, 1, 0.93])
    plt.savefig(output_path, dpi=100, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Generated {output_path} ({max_count} stars, {len(monthly)} monthly points)")


def main():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    out = sys.argv[1] if len(sys.argv) > 1 else "star-history.html"
    recent_days = int(sys.argv[2]) if len(sys.argv) > 2 else RECENT_DAYS
    print(f"Fetching stars for {REPO}...", flush=True)
    stars = fetch_stars(token)
    print(f"Total: {len(stars)} stars")
    cumulative = cumulative_by_date(stars)
    pts = monthly_points(cumulative)
    generate_html(pts, cumulative, out, recent_days)
    if out.endswith(".html"):
        generate_png(pts, out[:-5] + ".png")


if __name__ == "__main__":
    main()
