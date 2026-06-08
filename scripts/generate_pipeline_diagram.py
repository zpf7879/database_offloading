"""
Generates a visual pipeline diagram PNG for the database offloading POC.
Output: docs/pipeline_diagram.png

Usage:
    pip install matplotlib
    python scripts/generate_pipeline_diagram.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import os

# ── colour palette ───────────────────────────────────────────────────────────
C_MYSQL      = "#1565C0"   # dark blue     — MySQL
C_DEBEZIUM   = "#6A1B9A"   # purple        — Debezium
C_KAFKA      = "#E65100"   # orange        — Kafka
C_STREAMS    = "#2E7D32"   # dark green    — Kafka Streams
C_SINK       = "#00695C"   # teal          — MongoDB Kafka sink
C_MONGO      = "#1a6b3c"   # MongoDB green — MongoDB Atlas
C_TOPIC_BG   = "#FFF3E0"   # light orange  — topic boxes
C_TOPIC_BDR  = "#E65100"
C_WHITE      = "#FFFFFF"
C_LABEL      = "#FFFFFF"
C_SUBLABEL   = "#EEEEEE"
C_ARROW      = "#444444"
C_BG         = "#F8F9FA"

fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 16)
ax.set_ylim(0, 10)
ax.axis("off")
ax.set_facecolor(C_BG)
fig.patch.set_facecolor(C_BG)

# ── helpers ──────────────────────────────────────────────────────────────────

def box(ax, x, y, w, h, color, label, sublabel=None, radius=0.3):
    ax.add_patch(mpatches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0.05,rounding_size={radius}",
        facecolor=color, edgecolor="white", linewidth=1.5, zorder=3))
    ty = y + h / 2 + (0.15 if sublabel else 0)
    ax.text(x + w / 2, ty, label,
            ha="center", va="center",
            fontsize=9, fontweight="bold", color=C_LABEL, zorder=4)
    if sublabel:
        ax.text(x + w / 2, y + h / 2 - 0.2, sublabel,
                ha="center", va="center",
                fontsize=7, color=C_SUBLABEL, zorder=4)

def topic_box(ax, x, y, w, h, label):
    ax.add_patch(mpatches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.05,rounding_size=0.15",
        facecolor=C_TOPIC_BG, edgecolor=C_TOPIC_BDR,
        linewidth=1.2, zorder=3))
    ax.text(x + w / 2, y + h / 2, label,
            ha="center", va="center",
            fontsize=6.5, color="#BF360C",
            fontfamily="monospace", zorder=4)

def arrow(ax, x1, y1, x2, y2, label=None, style="->"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle=style, color=C_ARROW,
                        lw=1.5, connectionstyle="arc3,rad=0.0"), zorder=2)
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx, my + 0.18, label,
                ha="center", va="bottom",
                fontsize=6.5, color="#555555",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=C_BG,
                          edgecolor="none", alpha=0.85))

def fan_arrow(ax, x1, y1, x2, y2, rad=0.0):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="->", color=C_ARROW, lw=1.2,
                        connectionstyle=f"arc3,rad={rad}"), zorder=2)

# ── 1. MySQL ─────────────────────────────────────────────────────────────────
box(ax, 0.3, 4.1, 1.8, 1.8, C_MYSQL, "MySQL 8", "Source DB\n(binlog ON)")

# ── 2. Debezium ───────────────────────────────────────────────────────────────
box(ax, 2.6, 4.1, 1.8, 1.8, C_DEBEZIUM, "Debezium", "MySQL\nConnector")
arrow(ax, 2.1, 5.0, 2.6, 5.0, "binlog\nstream")

# ── 3. Kafka topics ──────────────────────────────────────────────────────────
topics = [
    "poc.offload_poc.customer",
    "poc.offload_poc.customer_address",
    "poc.offload_poc.customer_contact",
    "poc.offload_poc.customer_identification",
    "poc.offload_poc.customer_tax",
    "poc.offload_poc.relationship",
]
topic_x = 5.0
topic_w = 3.2
topic_h = 0.52
topic_gap = 0.62
total_h = len(topics) * topic_gap
top_y = 5.0 + total_h / 2

for i, t in enumerate(topics):
    ty = top_y - (i + 1) * topic_gap + 0.3
    topic_box(ax, topic_x, ty, topic_w, topic_h, t)
    fan_arrow(ax, 4.4, 5.0, topic_x, ty + topic_h / 2,
              rad=0.15 * (i - 2.5) / 3)

# Kafka label background
ax.add_patch(mpatches.FancyBboxPatch(
    (4.85, 1.55), 3.5, 7.0,
    boxstyle="round,pad=0.1,rounding_size=0.3",
    facecolor="#FFF8F0", edgecolor=C_TOPIC_BDR,
    linewidth=1.0, linestyle="--", zorder=1, alpha=0.6))
ax.text(6.55, 8.75, "Apache Kafka", ha="center", va="center",
        fontsize=8, color=C_KAFKA, fontweight="bold")

# ── 4. Kafka Streams ──────────────────────────────────────────────────────────
box(ax, 8.9, 3.9, 2.2, 2.2, C_STREAMS,
    "Kafka Streams", "Customer Profile\nAggregator")

# fan arrows from topics to Kafka Streams
for i, t in enumerate(topics):
    ty = top_y - (i + 1) * topic_gap + 0.3 + topic_h / 2
    fan_arrow(ax, topic_x + topic_w, ty, 8.9, 5.0,
              rad=-0.1 * (i - 2.5) / 3)

# label on the fan
ax.text(8.4, 5.0, "6 topics\n(CDC events)", ha="center", va="center",
        fontsize=6.5, color="#555555",
        bbox=dict(boxstyle="round,pad=0.25", facecolor=C_BG,
                  edgecolor="none", alpha=0.9))

# ── Kafka Streams detail box ──────────────────────────────────────────────────
detail_steps = [
    "① Re-key child topics by customer_id",
    "② Aggregate each child into Map<pk, record>",
    "③ Left-join all 6 KTables",
    "④ Emit merged document per customer",
]
dx, dy = 8.85, 1.6
ax.add_patch(mpatches.FancyBboxPatch(
    (dx, dy), 2.3, 2.1,
    boxstyle="round,pad=0.1,rounding_size=0.2",
    facecolor="#E8F5E9", edgecolor=C_STREAMS,
    linewidth=0.8, zorder=2))
for j, step in enumerate(detail_steps):
    ax.text(dx + 0.12, dy + 1.85 - j * 0.48, step,
            ha="left", va="center",
            fontsize=6, color="#1B5E20", zorder=3)
ax.annotate("", xy=(9.95, 3.9), xytext=(9.95, 3.7),
    arrowprops=dict(arrowstyle="->", color=C_STREAMS, lw=1.0), zorder=3)

# ── 5. poc.customer_profile topic ────────────────────────────────────────────
topic_box(ax, 8.9, 1.0, 2.2, 0.52, "poc.customer_profile")
arrow(ax, 10.0, 3.9, 10.0, 1.52, "merged\ndocument")

# ── 6. MongoDB Kafka Sink ─────────────────────────────────────────────────────
box(ax, 11.7, 3.9, 2.0, 2.2, C_SINK, "MongoDB\nKafka Sink", "Connector")
arrow(ax, 11.1, 5.0, 11.7, 5.0, "1 merged\ntopic")

# ── 7. MongoDB Atlas ──────────────────────────────────────────────────────────
box(ax, 11.7, 1.1, 2.0, 2.0, C_MONGO, "MongoDB Atlas", "customer_profile\ncollection")
arrow(ax, 12.7, 3.9, 12.7, 3.1, "upsert\n(ReplaceOne)")

# annotation on MongoDB
ax.text(13.85, 2.1,
        "{ customer_id,\n  first_name,\n  addresses: [...],\n  contacts: [...],\n  relationships: [...] }",
        ha="left", va="center", fontsize=6, color="#1a6b3c",
        fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#E8F5E9",
                  edgecolor=C_MONGO, linewidth=0.8))
ax.annotate("", xy=(13.7, 2.1), xytext=(13.85, 2.1),
    arrowprops=dict(arrowstyle="->", color=C_MONGO, lw=0.8), zorder=3)

# ── legend ────────────────────────────────────────────────────────────────────
legend_items = [
    (C_MYSQL,    "MySQL 8 (source)"),
    (C_DEBEZIUM, "Debezium CDC connector"),
    (C_KAFKA,    "Apache Kafka topics"),
    (C_STREAMS,  "Kafka Streams aggregator"),
    (C_SINK,     "MongoDB Kafka sink connector"),
    (C_MONGO,    "MongoDB Atlas"),
]
for i, (color, label) in enumerate(legend_items):
    lx, ly = 0.3, 3.4 - i * 0.42
    ax.add_patch(mpatches.FancyBboxPatch(
        (lx, ly - 0.13), 0.28, 0.28,
        boxstyle="round,pad=0.02", facecolor=color,
        edgecolor="white", linewidth=0.8, zorder=3))
    ax.text(lx + 0.4, ly + 0.01, label,
            ha="left", va="center", fontsize=7, color="#333333")

# ── title ─────────────────────────────────────────────────────────────────────
ax.text(8.0, 9.6,
        "Database Offloading POC — Real-Time CDC Pipeline",
        ha="center", va="center",
        fontsize=13, fontweight="bold", color="#1a1a1a")
ax.text(8.0, 9.2,
        "MySQL → Debezium → Kafka → Kafka Streams → MongoDB Atlas",
        ha="center", va="center",
        fontsize=8.5, color="#555555", style="italic")

plt.tight_layout(pad=0.5)

out_path = os.path.join(os.path.dirname(__file__), "..", "docs", "pipeline_diagram.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"Saved: {os.path.abspath(out_path)}")
