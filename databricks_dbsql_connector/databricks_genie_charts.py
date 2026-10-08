"""
Render Genie visualisations as PNG images for the Genie chat dialogs.

Genie One describes each chart it draws with an AI/BI (Lakeview) widget spec,
the same ``renderSpec`` the Databricks UI renders, e.g.::

    {"widgetType": "bar",
     "encodings": {"x": {"fieldName": "month", "scale": {"type": "temporal"}},
                   "y": {"fieldName": "trips", "format": {"abbreviation": "compact"}},
                   "color": {"fieldName": "fare_band"}},
     "mark": {"colors": ["#077A9D"], "layout": "stack"},
     "frame": {"title": "Monthly trips"}}

``render_spec`` draws that spec from the query's columns/rows with matplotlib
(bundled with QGIS). ``infer_spec`` proposes a simple spec when Genie returned
data but no chart. Everything runs off the UI thread and returns PNG bytes.
"""
import io
import math
from datetime import datetime

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.ticker import FuncFormatter
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except Exception:  # matplotlib missing on this QGIS install
    MATPLOTLIB_AVAILABLE = False

# Databricks AI/BI default categorical palette.
PALETTE = ["#077A9D", "#FFAB00", "#00A972", "#FF3621", "#8BCAE7",
           "#AB4057", "#99DDB4", "#FCA4A1", "#919191", "#BF7080"]

SUPPORTED_WIDGETS = ("bar", "line", "area", "scatter", "pie")


# ---------------------------------------------------------------------------
# Value parsing and formatting
# ---------------------------------------------------------------------------

def _to_number(value):
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(num) else num


def _to_datetime(value):
    if value is None:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text[:10]):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.replace(tzinfo=None)
        except ValueError:
            continue
    return None


def _number_formatter(fmt):
    """Matplotlib tick formatter following an AI/BI number format."""
    fmt = fmt or {}
    places = (fmt.get("decimalPlaces") or {}).get("places")
    compact = fmt.get("abbreviation") == "compact"
    prefix = "$" if (fmt.get("type") == "number-currency"
                     and fmt.get("currencyCode", "USD") == "USD") else ""
    percent = fmt.get("type") == "number-percent"

    def _fmt(value, _pos=None):
        if value is None:
            return ""
        if value == 0:
            return "0"
        if percent:
            value *= 100
        suffix = "%" if percent else ""
        if compact:
            for div, unit in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
                if abs(value) >= div:
                    digits = 1 if places is None else min(places, 2)
                    text = f"{value / div:.{digits}f}".rstrip("0").rstrip(".")
                    return f"{prefix}{text}{unit}{suffix}"
        digits = places if places is not None else (0 if float(value).is_integer() else 2)
        return f"{prefix}{value:,.{digits}f}{suffix}"
    return _fmt


def _date_format(fmt):
    date_style = (fmt or {}).get("date", "")
    if "month" in date_style:
        return "%b %Y"
    if "year" in date_style:
        return "%Y"
    return "%Y-%m-%d"


# ---------------------------------------------------------------------------
# Spec helpers
# ---------------------------------------------------------------------------

def _col_index(columns, field):
    if not field:
        return None
    for i, name in enumerate(columns):
        if name == field:
            return i
    lower = field.lower()
    for i, name in enumerate(columns):
        if name.lower() == lower:
            return i
    return None


def _y_series(encodings):
    """Return ([(field, label), ...] primary, [...] secondary, primary_enc, secondary_enc)."""
    y = encodings.get("y") or {}
    if "primary" in y or "secondary" in y:
        prim = y.get("primary") or {}
        sec = y.get("secondary") or {}
        as_list = lambda enc: [(f.get("fieldName"), f.get("displayName") or f.get("fieldName"))
                               for f in enc.get("fields") or []]
        return as_list(prim), as_list(sec), prim, sec
    if "fields" in y:
        return ([(f.get("fieldName"), f.get("displayName") or f.get("fieldName"))
                 for f in y["fields"]], [], y, {})
    if y.get("fieldName"):
        return [(y["fieldName"], y.get("displayName") or y["fieldName"])], [], y, {}
    return [], [], y, {}


def _axis_title(enc, default):
    return ((enc or {}).get("axis") or {}).get("title") or (enc or {}).get("displayName") or default


def _style_axes(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#C8C8C8")
    ax.tick_params(colors="#444444", labelsize=8)
    ax.grid(axis="y", color="#EDEDED", linewidth=0.8)
    ax.set_axisbelow(True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def render_spec(spec, columns, rows, title=None):
    """Draw an AI/BI ``renderSpec`` from query results. Returns PNG bytes,
    or ``None`` if the chart type/columns can't be drawn here."""
    if not MATPLOTLIB_AVAILABLE or not spec or not columns or not rows:
        return None
    widget = (spec.get("widgetType") or "").lower()
    if widget not in SUPPORTED_WIDGETS:
        return None
    enc = spec.get("encodings") or {}
    colors = (spec.get("mark") or {}).get("colors") or PALETTE
    title = title or (spec.get("frame") or {}).get("title") or ""

    fig = Figure(figsize=(7.2, 3.8), dpi=110)
    FigureCanvasAgg(fig)
    ax = fig.add_subplot(111)

    if widget == "pie":
        drawn = _draw_pie(ax, enc, columns, rows, colors)
    else:
        drawn = _draw_xy(fig, ax, widget, spec, enc, columns, rows, colors)
    if not drawn:
        return None

    if title:
        has_legend = ax.get_legend() is not None
        ax.set_title(title, loc="left", fontsize=10, fontweight="bold",
                     color="#1B1B1B", pad=24 if has_legend else 10)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor="white")
    return buf.getvalue()


def _draw_pie(ax, enc, columns, rows, colors):
    vi = _col_index(columns, (enc.get("angle") or {}).get("fieldName"))
    li = _col_index(columns, (enc.get("color") or {}).get("fieldName"))
    if vi is None:
        return False
    values, labels = [], []
    for row in rows:
        num = _to_number(row[vi])
        if num is not None and num > 0:
            values.append(num)
            labels.append(str(row[li]) if li is not None else "")
    if not values:
        return False
    show = (enc.get("label") or {}).get("show", True)
    ax.pie(values, labels=labels if show else None, colors=colors[:len(values)] if len(colors) >= len(values) else None,
           autopct="%1.0f%%" if show else None, startangle=90, counterclock=False,
           wedgeprops={"linewidth": 1, "edgecolor": "white"},
           textprops={"fontsize": 8, "color": "#333333"})
    ax.axis("equal")
    return True


def _draw_xy(fig, ax, widget, spec, enc, columns, rows, colors):
    x_enc = enc.get("x") or {}
    xi = _col_index(columns, x_enc.get("fieldName"))
    primary, secondary, prim_enc, sec_enc = _y_series(enc)
    color_enc = enc.get("color") or {}
    ci = _col_index(columns, color_enc.get("fieldName"))
    if xi is None or not primary:
        return False

    temporal = (x_enc.get("scale") or {}).get("type") == "temporal"
    quantitative_x = (x_enc.get("scale") or {}).get("type") == "quantitative"
    parse_x = _to_datetime if temporal else (_to_number if quantitative_x else str)

    # Build series: {label: [(x, y), ...]}, keeping first-seen x order.
    def collect(fields):
        series = {}
        for field, label in fields:
            yi = _col_index(columns, field)
            if yi is None:
                continue
            for row in rows:
                xv, yv = parse_x(row[xi]), _to_number(row[yi])
                if xv is None or yv is None:
                    continue
                key = label
                if ci is not None:
                    key = str(row[ci]) if len(fields) == 1 else f"{label} · {row[ci]}"
                series.setdefault(key, []).append((xv, yv))
        return series

    prim_series = collect(primary)
    if not prim_series:
        return False
    sec_series = collect(secondary)
    layout = (spec.get("mark") or {}).get("layout", "")
    show_labels = (enc.get("label") or {}).get("show", False)
    _style_axes(ax)

    if widget == "bar":
        _draw_bars(ax, prim_series, colors, stacked=(layout == "stack"),
                   temporal=temporal, x_fmt=x_enc.get("format"),
                   y_fmt=prim_enc.get("format"), show_labels=show_labels)
    else:
        _draw_lines(ax, widget, prim_series, colors, temporal)
        if temporal:
            locator = mdates.AutoDateLocator(maxticks=8)
            ax.xaxis.set_major_locator(locator)
            if (x_enc.get("format") or {}).get("date"):
                ax.xaxis.set_major_formatter(
                    mdates.DateFormatter(_date_format(x_enc.get("format"))))
            else:
                ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    ax.yaxis.set_major_formatter(FuncFormatter(_number_formatter(prim_enc.get("format"))))
    ax.set_xlabel(_axis_title(x_enc, ""), fontsize=8, color="#444444")
    ax.set_ylabel(_axis_title(prim_enc, primary[0][1]), fontsize=8, color="#444444")

    if sec_series:   # dual axis (e.g. trips + average fare)
        ax2 = ax.twinx()
        offset = len(prim_series)
        _draw_lines(ax2, "line" if widget != "scatter" else "scatter",
                    sec_series, colors[offset:] + colors[:offset], temporal)
        ax2.yaxis.set_major_formatter(FuncFormatter(_number_formatter(sec_enc.get("format"))))
        ax2.set_ylabel(_axis_title(sec_enc, secondary[0][1]), fontsize=8, color="#444444")
        ax2.spines["top"].set_visible(False)
        ax2.tick_params(colors="#444444", labelsize=8)
        handles = ax.get_legend_handles_labels()
        handles2 = ax2.get_legend_handles_labels()
        _legend_above(ax, handles[0] + handles2[0], handles[1] + handles2[1])
    elif len(prim_series) > 1:
        handles, labels = ax.get_legend_handles_labels()
        _legend_above(ax, handles, labels,
                      title=(color_enc.get("legend") or {}).get("title")
                      or color_enc.get("displayName"))
    return True


def _legend_above(ax, handles, labels, title=None):
    """Legend in a row above the plot (like AI/BI charts), never over data."""
    if title:
        labels = [f"{title}:"] + list(labels)
        handles = [ax.plot([], [], " ")[0]] + list(handles)
    ax.legend(handles, labels, fontsize=8, frameon=False, loc="lower left",
              bbox_to_anchor=(0, 1.0), ncol=min(6, len(labels)),
              handlelength=1.2, columnspacing=1.2, borderaxespad=0.2)


def _draw_bars(ax, series, colors, stacked, temporal, x_fmt, y_fmt, show_labels):
    # Category axis in first-seen order (sorted for dates).
    cats = []
    for points in series.values():
        for xv, _ in points:
            if xv not in cats:
                cats.append(xv)
    if temporal:
        cats.sort()
    labels = ([c.strftime(_date_format(x_fmt)) for c in cats] if temporal
              else [str(c) for c in cats])
    index = {c: i for i, c in enumerate(cats)}
    n = len(series)
    width = 0.8 if (stacked or n == 1) else 0.8 / n
    bottoms = [0.0] * len(cats)
    fmt = _number_formatter(y_fmt)
    for s_i, (name, points) in enumerate(series.items()):
        vals = [0.0] * len(cats)
        for xv, yv in points:
            vals[index[xv]] += yv
        if stacked:
            xs = list(range(len(cats)))
            bars = ax.bar(xs, vals, width, bottom=bottoms, label=name,
                          color=colors[s_i % len(colors)])
            bottoms = [b + v for b, v in zip(bottoms, vals)]
        else:
            xs = [i - 0.4 + width * (s_i + 0.5) for i in range(len(cats))] if n > 1 else list(range(len(cats)))
            bars = ax.bar(xs, vals, width, label=name, color=colors[s_i % len(colors)])
        if show_labels and len(cats) <= 24:
            ax.bar_label(bars, labels=[fmt(v) if v else "" for v in vals],
                         fontsize=7, color="white" if stacked else "#444444",
                         label_type="center" if stacked else "edge",
                         padding=0 if stacked else 2)
    ax.set_xticks(range(len(cats)))
    ax.set_xticklabels(labels, rotation=0 if len(cats) <= 8 else 45,
                       ha="center" if len(cats) <= 8 else "right")


def _draw_lines(ax, widget, series, colors, temporal):
    for s_i, (name, points) in enumerate(series.items()):
        points = sorted(points, key=lambda p: p[0]) if (temporal or widget != "scatter") else points
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        color = colors[s_i % len(colors)]
        if widget == "scatter":
            ax.scatter(xs, ys, s=18, color=color, label=name, alpha=0.85)
        else:
            ax.plot(xs, ys, color=color, linewidth=1.8, label=name)
            if widget == "area":
                ax.fill_between(xs, ys, color=color, alpha=0.25)


def infer_spec(columns, rows, title=""):
    """Propose a simple chart when Genie returned data but no visualisation:
    a label/date column plus numeric column(s), 2-200 rows. Returns a
    renderSpec dict or ``None`` if the result isn't chart-shaped."""
    if not columns or not rows or not (2 <= len(rows) <= 200):
        return None
    sample = rows[:50]
    numeric, temporal, label = [], [], []
    for i, name in enumerate(columns):
        values = [r[i] for r in sample if i < len(r) and r[i] not in (None, "")]
        if not values:
            continue
        if all(_to_number(v) is not None for v in values):
            numeric.append(name)
        elif all(_to_datetime(v) is not None for v in values):
            temporal.append(name)
        elif not str(values[0]).upper().startswith(("POINT", "POLYGON", "LINESTRING", "MULTI", "SRID=")):
            label.append(name)
    x = (temporal or label or [None])[0]
    measures = [n for n in numeric if n != x]
    if x is None or not measures:
        # e.g. zip code + count: a numeric "label" column first, then measures
        if len(numeric) >= 2 and len(rows) <= 50:
            x, measures = numeric[0], numeric[1:2]
            return {"widgetType": "bar",
                    "encodings": {"x": {"fieldName": x, "scale": {"type": "categorical"}},
                                  "y": {"fieldName": measures[0]}},
                    "frame": {"title": title}}
        return None
    is_time = x in temporal
    return {"widgetType": "line" if is_time and len(rows) > 12 else "bar",
            "encodings": {"x": {"fieldName": x, "scale": {"type": "temporal" if is_time else "categorical"}},
                          "y": {"fields": [{"fieldName": m} for m in measures[:3]]},
                          "label": {"show": len(rows) <= 12}},
            "frame": {"title": title}}
