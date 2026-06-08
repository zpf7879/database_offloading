"""
Generates a visual ER diagram PNG for the offload POC source schema.
Output: docs/er_diagram.png

Usage:
    pip install matplotlib
    python scripts/generate_er_diagram.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import os

# ── colour palette ──────────────────────────────────────────────────────────
C_HEADER   = "#1a6b3c"   # MongoDB green  — table header
C_PK       = "#e8f5e9"   # light green    — PK row
C_FK       = "#fff9e6"   # light yellow   — FK row
C_NORMAL   = "#ffffff"   # white          — regular row
C_BORDER   = "#555555"
C_TEXT     = "#1a1a1a"
C_FK_TEXT  = "#b07d00"
C_PK_TEXT  = "#1a6b3c"
C_LINE     = "#666666"

# ── table definitions ────────────────────────────────────────────────────────
# Each entry: (column_name, type_label, "PK"|"FK"|"")
TABLES = {
    "customer": {
        "x": 3.5, "y": 7.5,
        "cols": [
            ("customer_id",      "VARCHAR(36)",  "PK"),
            ("external_ref",     "VARCHAR(64)",  ""),
            ("first_name",       "VARCHAR(100)", ""),
            ("last_name",        "VARCHAR(100)", ""),
            ("date_of_birth",    "DATE",         ""),
            ("gender",           "CHAR(1)",      ""),
            ("nationality",      "VARCHAR(3)",   ""),
            ("status",           "ENUM",         ""),
            ("created_at",       "DATETIME",     ""),
            ("updated_at",       "DATETIME",     ""),
        ],
    },
    "customer_address": {
        "x": 0.0, "y": 13.5,
        "cols": [
            ("address_id",   "VARCHAR(36)", "PK"),
            ("customer_id",  "VARCHAR(36)", "FK"),
            ("address_type", "ENUM",        ""),
            ("line1",        "VARCHAR(200)",""),
            ("city",         "VARCHAR(100)",""),
            ("state",        "VARCHAR(50)", ""),
            ("postcode",     "VARCHAR(20)", ""),
            ("country",      "VARCHAR(3)",  ""),
            ("is_primary",   "TINYINT",     ""),
            ("updated_at",   "DATETIME",    ""),
        ],
    },
    "customer_contact": {
        "x": 7.2, "y": 13.5,
        "cols": [
            ("contact_id",    "VARCHAR(36)",  "PK"),
            ("customer_id",   "VARCHAR(36)",  "FK"),
            ("contact_type",  "ENUM",         ""),
            ("contact_value", "VARCHAR(255)", ""),
            ("is_primary",    "TINYINT",      ""),
            ("is_verified",   "TINYINT",      ""),
            ("updated_at",    "DATETIME",     ""),
        ],
    },
    "customer_identification": {
        "x": 0.0, "y": 4.0,
        "cols": [
            ("id_record_id",      "VARCHAR(36)",  "PK"),
            ("customer_id",       "VARCHAR(36)",  "FK"),
            ("id_type",           "ENUM",         ""),
            ("id_number",         "VARCHAR(100)", ""),
            ("issuing_authority", "VARCHAR(100)", ""),
            ("issue_date",        "DATE",         ""),
            ("expiry_date",       "DATE",         ""),
            ("updated_at",        "DATETIME",     ""),
        ],
    },
    "customer_tax": {
        "x": 7.2, "y": 4.0,
        "cols": [
            ("tax_record_id", "VARCHAR(36)", "PK"),
            ("customer_id",   "VARCHAR(36)", "FK"),
            ("tax_country",   "VARCHAR(3)",  ""),
            ("tax_id",        "VARCHAR(50)", ""),
            ("tin_type",      "VARCHAR(50)", ""),
            ("updated_at",    "DATETIME",    ""),
        ],
    },
    "relationship": {
        "x": 3.5, "y": 0.2,
        "cols": [
            ("relationship_id",   "VARCHAR(36)", "PK"),
            ("party_id_from",     "VARCHAR(36)", "FK"),
            ("party_id_to",       "VARCHAR(36)", "FK"),
            ("relationship_type", "ENUM",        ""),
            ("valid_from",        "DATE",        ""),
            ("valid_to",          "DATE",        ""),
            ("status",            "ENUM",        ""),
            ("updated_at",        "DATETIME",    ""),
        ],
    },
}

ROW_H   = 0.38   # height of each row
COL_W   = 3.0    # table width
HDR_H   = 0.5    # header height

def table_height(name):
    return HDR_H + len(TABLES[name]["cols"]) * ROW_H

def table_bbox(name):
    """Returns (left, bottom, right, top) in data coords."""
    t  = TABLES[name]
    h  = table_height(name)
    return t["x"], t["y"], t["x"] + COL_W, t["y"] + h

def row_y(name, col_index):
    """Y-centre of a specific column row."""
    t   = TABLES[name]
    top = t["y"] + table_height(name)
    return top - HDR_H - (col_index + 0.5) * ROW_H

def draw_table(ax, name):
    t   = TABLES[name]
    x0  = t["x"]
    y0  = t["y"]
    h   = table_height(name)
    top = y0 + h

    # ── header ──────────────────────────────────────────────────
    ax.add_patch(mpatches.FancyBboxPatch(
        (x0, top - HDR_H), COL_W, HDR_H,
        boxstyle="square,pad=0", linewidth=0,
        facecolor=C_HEADER, zorder=2))
    ax.text(x0 + COL_W / 2, top - HDR_H / 2, name,
            ha="center", va="center", fontsize=8.5, fontweight="bold",
            color="white", zorder=3)

    # ── rows ────────────────────────────────────────────────────
    for i, (col, typ, flag) in enumerate(t["cols"]):
        ry = top - HDR_H - (i + 1) * ROW_H
        bg = C_PK if flag == "PK" else (C_FK if flag == "FK" else C_NORMAL)
        ax.add_patch(mpatches.FancyBboxPatch(
            (x0, ry), COL_W, ROW_H,
            boxstyle="square,pad=0", linewidth=0,
            facecolor=bg, zorder=2))

        # flag badge
        if flag:
            badge_c = C_PK_TEXT if flag == "PK" else C_FK_TEXT
            ax.text(x0 + 0.08, ry + ROW_H / 2, flag,
                    ha="left", va="center", fontsize=5.5,
                    color=badge_c, fontweight="bold", zorder=3)
            col_x = x0 + 0.38
        else:
            col_x = x0 + 0.08

        ax.text(col_x, ry + ROW_H / 2, col,
                ha="left", va="center", fontsize=6.5, color=C_TEXT, zorder=3)
        ax.text(x0 + COL_W - 0.08, ry + ROW_H / 2, typ,
                ha="right", va="center", fontsize=5.8,
                color="#777777", style="italic", zorder=3)

    # ── outer border ────────────────────────────────────────────
    ax.add_patch(mpatches.FancyBboxPatch(
        (x0, y0), COL_W, h,
        boxstyle="square,pad=0",
        edgecolor=C_BORDER, facecolor="none", linewidth=1.0, zorder=4))

    # ── row dividers ────────────────────────────────────────────
    for i in range(len(t["cols"]) + 1):
        ly = top - HDR_H - i * ROW_H
        ax.plot([x0, x0 + COL_W], [ly, ly],
                color="#dddddd", linewidth=0.4, zorder=3)


def fk_connection(ax, src_table, src_col_idx, dst_table, dst_col_idx=0):
    """Draw a crow's-foot FK line from src column to dst PK."""
    sx0, sy0, sx1, sy1 = table_bbox(src_table)
    dx0, dy0, dx1, dy1 = table_bbox(dst_table)

    sy = row_y(src_table, src_col_idx)
    dy = row_y(dst_table, dst_col_idx)

    # pick left or right edge based on relative position
    if sx0 > dx1:          # src is to the right of dst
        s_anchor = (sx0, sy)
        d_anchor = (dx1, dy)
    elif sx1 < dx0:        # src is to the left of dst
        s_anchor = (sx1, sy)
        d_anchor = (dx0, dy)
    elif sy > dy1:         # src is above dst (vertical)
        s_anchor = (sx0 + COL_W / 2, sy0)
        d_anchor = (dx0 + COL_W / 2, dy1)
    else:
        s_anchor = (sx0 + COL_W / 2, sy1)
        d_anchor = (dx0 + COL_W / 2, dy0)

    ax.annotate("",
        xy=d_anchor, xytext=s_anchor,
        arrowprops=dict(
            arrowstyle="-|>",
            color=C_LINE,
            lw=1.1,
            connectionstyle="arc3,rad=0.0",
        ),
        zorder=1,
    )


# ── build figure ─────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(13, 10))
ax.set_xlim(-0.3, 10.8)
ax.set_ylim(-0.3, 18.5)
ax.axis("off")
ax.set_facecolor("#f7f7f7")
fig.patch.set_facecolor("#f7f7f7")

for name in TABLES:
    draw_table(ax, name)

# FK → PK connections
# customer_address.customer_id  → customer.customer_id (col 0)
fk_connection(ax, "customer_address",       1, "customer", 0)
fk_connection(ax, "customer_contact",       1, "customer", 0)
fk_connection(ax, "customer_identification",1, "customer", 0)
fk_connection(ax, "customer_tax",           1, "customer", 0)
fk_connection(ax, "relationship",           1, "customer", 0)   # party_id_from
fk_connection(ax, "relationship",           2, "customer", 0)   # party_id_to

# ── title + legend ───────────────────────────────────────────────────────────
ax.set_title("Offload POC — Source Schema ER Diagram",
             fontsize=13, fontweight="bold", color=C_TEXT, pad=10)

legend_handles = [
    mpatches.Patch(facecolor=C_PK,     edgecolor=C_BORDER, label="Primary Key"),
    mpatches.Patch(facecolor=C_FK,     edgecolor=C_BORDER, label="Foreign Key"),
    mpatches.Patch(facecolor=C_NORMAL, edgecolor=C_BORDER, label="Column"),
]
ax.legend(handles=legend_handles, loc="lower right",
          fontsize=7.5, framealpha=0.9)

plt.tight_layout()

out_path = os.path.join(os.path.dirname(__file__), "..", "docs", "er_diagram.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"Saved: {os.path.abspath(out_path)}")
