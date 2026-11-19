
"""Professional instant PDF report for Pipeline Autopilot.

Uses live database records and ReportLab.
Compatible with: await build_instant_pdf(client=None)
"""

import io
from collections import Counter
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    KeepTogether,
)
from reportlab.graphics.shapes import (
    Drawing,
    Rect,
    String,
    Circle,
    Line,
    PolyLine,
    Wedge,
)

import db


# --------------------------------------------------
# DESIGN SYSTEM
# --------------------------------------------------

NAVY = colors.HexColor("#19184E")
PURPLE = colors.HexColor("#8054F7")
GREEN = colors.HexColor("#0FBF96")
BLUE = colors.HexColor("#4E8AF7")
ORANGE = colors.HexColor("#F2A34A")
GREY = colors.HexColor("#74718E")
PALE = colors.HexColor("#F4F0FF")
BORDER = colors.HexColor("#E5DEFA")
WHITE = colors.white

PAGE_WIDTH = 612
CONTENT_WIDTH = 512

PALETTE = [
    PURPLE,
    BLUE,
    GREEN,
    ORANGE,
    colors.HexColor("#B6A1F9"),
    colors.HexColor("#E76A92"),
]


def safe(value):
    return escape(str(value if value is not None else "—"))


def number(value):
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def labelize(value):
    return str(value or "Unknown").replace("_", " ").title()


# --------------------------------------------------
# TYPOGRAPHY
# --------------------------------------------------

styles = getSampleStyleSheet()

styles.add(ParagraphStyle(
    name="PA_Brand",
    fontName="Helvetica-Bold",
    fontSize=15,
    leading=19,
    textColor=NAVY,
    spaceAfter=4,
))

styles.add(ParagraphStyle(
    name="PA_Title",
    fontName="Helvetica-Bold",
    fontSize=23,
    leading=29,
    textColor=NAVY,
    spaceAfter=12,
))

styles.add(ParagraphStyle(
    name="PA_Section",
    fontName="Helvetica-Bold",
    fontSize=13,
    leading=18,
    textColor=NAVY,
    spaceBefore=12,
    spaceAfter=9,
))

styles.add(ParagraphStyle(
    name="PA_Body",
    fontName="Helvetica",
    fontSize=9.2,
    leading=15,
    textColor=NAVY,
    spaceAfter=7,
))

styles.add(ParagraphStyle(
    name="PA_Small",
    fontName="Helvetica",
    fontSize=8,
    leading=12,
    textColor=GREY,
    spaceAfter=5,
))

styles.add(ParagraphStyle(
    name="PA_CardLabel",
    fontName="Helvetica",
    fontSize=8,
    leading=11,
    textColor=GREY,
    alignment=TA_CENTER,
))

styles.add(ParagraphStyle(
    name="PA_CardValue",
    fontName="Helvetica-Bold",
    fontSize=20,
    leading=26,
    textColor=NAVY,
    alignment=TA_CENTER,
))

styles.add(ParagraphStyle(
    name="PA_Table",
    fontName="Helvetica",
    fontSize=8,
    leading=12,
    textColor=NAVY,
))


def paragraph(text, style="PA_Body"):
    return Paragraph(safe(text), styles[style])


def rich(text, style="PA_Body"):
    return Paragraph(text, styles[style])


# --------------------------------------------------
# REUSABLE DESIGN COMPONENTS
# --------------------------------------------------

def panel(rows, widths=None, background="#F6F3FF", padding=12):
    table = Table(
        rows,
        colWidths=widths,
        hAlign="LEFT",
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1),
         colors.HexColor(background)),
        ("BOX", (0, 0), (-1, -1), 0.7, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), padding),
        ("RIGHTPADDING", (0, 0), (-1, -1), padding),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))

    return table


def section(story, title):
    story.append(paragraph(title, "PA_Section"))


def kpi_cards(story, items):
    cards = []

    for name, value in items:
        card = Table(
            [
                [paragraph(name, "PA_CardLabel")],
                [paragraph(value, "PA_CardValue")],
            ],
            colWidths=[108],
        )

        card.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), PALE),
            ("BOX", (0, 0), (-1, -1), 0.6, BORDER),
            ("TOPPADDING", (0, 0), (-1, 0), 12),
            ("BOTTOMPADDING", (0, -1), (-1, -1), 12),
        ]))

        cards.append(card)

    outer = Table(
        [cards],
        colWidths=[128] * len(cards),
        hAlign="LEFT",
    )

    outer.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]))

    story.append(outer)
    story.append(Spacer(1, 12))


# --------------------------------------------------
# LINE GRAPH
# --------------------------------------------------

def recovery_trend(days):
    """Actual daily detected and recovered counts."""

    width = 485
    height = 195
    drawing = Drawing(width, height)

    maximum = max(
        [max(row[1], row[2]) for row in days] or [1]
    ) or 1

    left = 42
    bottom = 37
    plot_width = 420
    plot_height = 115

    for i in range(5):
        y = bottom + i * plot_height / 4

        drawing.add(Line(
            left, y,
            left + plot_width, y,
            strokeColor=BORDER,
            strokeWidth=0.7,
            strokeDashArray=[3, 4],
        ))

        drawing.add(String(
            5, y - 3,
            str(round(maximum * i / 4)),
            fontSize=8,
            fillColor=GREY,
        ))

    if days:
        xs = [
            left + i * plot_width / max(1, len(days) - 1)
            for i in range(len(days))
        ]

        for index, color in [
            (1, PURPLE),
            (2, GREEN),
        ]:
            coordinates = []

            for i, row in enumerate(days):
                x = xs[i]
                y = bottom + plot_height * row[index] / maximum

                coordinates.extend([x, y])

                drawing.add(Circle(
                    x, y, 3.3,
                    fillColor=color,
                    strokeColor=WHITE,
                    strokeWidth=0.8,
                ))

            if len(days) > 1:
                drawing.add(PolyLine(
                    coordinates,
                    strokeColor=color,
                    strokeWidth=2.8,
                    fillColor=None,
                ))

        for i, row in enumerate(days):
            drawing.add(String(
                xs[i] - 15,
                19,
                row[0],
                fontSize=8,
                fillColor=GREY,
            ))

    drawing.add(Line(
        140, 180, 156, 180,
        strokeColor=PURPLE,
        strokeWidth=3,
    ))
    drawing.add(String(
        162, 176,
        "Detected incidents",
        fontSize=9,
        fillColor=NAVY,
    ))

    drawing.add(Line(
        285, 180, 301, 180,
        strokeColor=GREEN,
        strokeWidth=3,
    ))
    drawing.add(String(
        307, 176,
        "Auto-recovered",
        fontSize=9,
        fillColor=NAVY,
    ))

    return drawing


# --------------------------------------------------
# INCIDENT DISTRIBUTION
# --------------------------------------------------

def incident_donut(items):
    drawing = Drawing(485, 180)

    total = sum(value for _, value in items)

    if total == 0:
        drawing.add(String(
            140, 90,
            "No incidents recorded in this period",
            fontSize=11,
            fillColor=GREY,
        ))
        return drawing

    center_x = 95
    center_y = 90
    radius = 66
    angle = 90

    for i, (name, value) in enumerate(items[:6]):
        sweep = 360 * value / total

        if sweep > 0:
            drawing.add(Wedge(
                center_x,
                center_y,
                radius,
                angle - sweep,
                angle,
                fillColor=PALETTE[i % len(PALETTE)],
                strokeColor=WHITE,
                strokeWidth=1,
            ))

        angle -= sweep

    drawing.add(Circle(
        center_x, center_y, 37,
        fillColor=WHITE,
        strokeColor=None,
    ))

    drawing.add(String(
        center_x,
        center_y + 3,
        number(total),
        fontName="Helvetica-Bold",
        fontSize=16,
        textAnchor="middle",
        fillColor=NAVY,
    ))

    drawing.add(String(
        center_x,
        center_y - 13,
        "Incidents",
        fontSize=9,
        textAnchor="middle",
        fillColor=GREY,
    ))

    for i, (name, value) in enumerate(items[:6]):
        y = 152 - i * 25

        drawing.add(Circle(
            205, y, 5,
            fillColor=PALETTE[i % len(PALETTE)],
            strokeColor=None,
        ))

        drawing.add(String(
            219, y - 3,
            f"{labelize(name)[:23]}: {value:,}",
            fontSize=9,
            fillColor=NAVY,
        ))

    return drawing


# --------------------------------------------------
# QUALITY VISUAL
# --------------------------------------------------

def quality_bar(share):
    drawing = Drawing(485, 52)

    value = max(0, min(100, share or 0))

    drawing.add(Rect(
        0, 14, 470, 17,
        rx=8,
        ry=8,
        fillColor=colors.HexColor("#EAE5F5"),
        strokeColor=None,
    ))

    if value > 0:
        drawing.add(Rect(
            0, 14, 470 * value / 100, 17,
            rx=8,
            ry=8,
            fillColor=GREEN,
            strokeColor=None,
        ))

    drawing.add(String(
        0, 39,
        "Usable data",
        fontSize=9,
        fillColor=NAVY,
    ))

    drawing.add(String(
        470, 39,
        f"{value:.1f}%",
        textAnchor="end",
        fontName="Helvetica-Bold",
        fontSize=11,
        fillColor=GREEN,
    ))

    return drawing


# --------------------------------------------------
# PDF GENERATOR
# --------------------------------------------------

async def build_instant_pdf(client=None):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)

    pool = await db.get_pool()

    async with pool.acquire() as conn:

        pipelines = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT id, name, status, dag_id
                FROM public.pipelines
                ORDER BY name
                """
            )
        ]

        runs = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT dag_id, status, started_at,
                       rows_processed, rows_quarantined
                FROM public.pipeline_runs
                ORDER BY started_at DESC
                LIMIT 30
                """
            )
        ]

        groups = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT error_type, status,
                       COUNT(*)::int AS total
                FROM public.audit_log
                WHERE created_at >= $1
                GROUP BY error_type, status
                """,
                start,
            )
        ]

        daily = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT
                    date_trunc('day', created_at) AS day,
                    COUNT(*)::int AS incidents,
                    COUNT(*) FILTER (
                        WHERE status = 'auto_fixed'
                    )::int AS recovered
                FROM public.audit_log
                WHERE created_at >= $1
                GROUP BY 1
                ORDER BY 1
                """,
                start,
            )
        ]

        highlights = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT error_type, status,
                       outcome, created_at
                FROM public.audit_log
                WHERE created_at >= $1
                ORDER BY created_at DESC
                LIMIT 5
                """,
                start,
            )
        ]

    # ----------------------------------------------
    # METRIC CALCULATIONS
    # ----------------------------------------------

    healthy = sum(
        p["status"] == "healthy"
        for p in pipelines
    )

    ingest_runs = [
        r for r in runs
        if "ingest" in str(r.get("dag_id", "")).lower()
    ]

    latest = ingest_runs[0] if ingest_runs else (
        runs[0] if runs else {}
    )

    usable = latest.get("rows_processed")
    quarantined = latest.get("rows_quarantined")

    if (
        isinstance(usable, int)
        and isinstance(quarantined, int)
        and usable + quarantined > 0
    ):
        quality = 100 * usable / (usable + quarantined)
    else:
        quality = None

    total = sum(row["total"] for row in groups)

    recovered = sum(
        row["total"]
        for row in groups
        if row["status"] == "auto_fixed"
    )

    failed = sum(
        row["total"]
        for row in groups
        if row["status"] == "recovery_failed"
    )

    recovery_rate = (
        100 * recovered / total
        if total > 0 else None
    )

    incident_types = Counter()

    for row in groups:
        incident_types[
            labelize(row.get("error_type"))
        ] += row["total"]

    daily_lookup = {
        row["day"].date(): row
        for row in daily
    }

    days = []

    for i in range(6, -1, -1):
        day = (now - timedelta(days=i)).date()
        record = daily_lookup.get(day, {})

        days.append((
            day.strftime("%b %d"),
            record.get("incidents", 0),
            record.get("recovered", 0),
        ))

    # ----------------------------------------------
    # DOCUMENT SETUP
    # ----------------------------------------------

    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=(8.5 * inch, 11 * inch),
        leftMargin=50,
        rightMargin=50,
        topMargin=48,
        bottomMargin=49,
        title="Pipeline Autopilot - Instant Report",
        author="Pipeline Autopilot",
    )

    story = []

    def report_header(title):
        story.append(paragraph(
            "PIPELINE AUTOPILOT",
            "PA_Brand",
        ))

        story.append(paragraph(
            "Autonomous ETL Monitoring & Recovery",
            "PA_Small",
        ))

        story.append(Spacer(1, 12))

        story.append(paragraph(
            title,
            "PA_Title",
        ))

        story.append(paragraph(
            f"Generated: {now:%d %b %Y, %H:%M} UTC  |  "
            "Source: Live pipeline records",
            "PA_Small",
        ))

        story.append(Spacer(1, 12))

    # ==============================================
    # PAGE 1 — EXECUTIVE OVERVIEW
    # ==============================================

    report_header(
        "Instant Pipeline Health & Recovery Report"
    )

    section(story, "Executive Summary")

    summary = (
        f"The ETL platform currently has {healthy} of "
        f"{len(pipelines)} monitored stages healthy. "
        f"Over the last seven days, the system recorded "
        f"{total:,} incidents and {recovered:,} "
        f"auto-recovery outcomes."
    )

    if failed:
        summary += (
            f" {failed:,} recovery outcomes were not "
            "verified as successful."
        )

    if quality is not None:
        summary += (
            f" The latest ingest batch achieved "
            f"{quality:.1f}% usable data."
        )

    story.append(panel(
        [[paragraph(summary)]],
        widths=[CONTENT_WIDTH],
        background="#EEE7FF",
        padding=17,
    ))

    story.append(Spacer(1, 14))

    kpi_cards(story, [
        ("Healthy Stages", f"{healthy}/{len(pipelines)}"),
        ("Rows Processed", number(usable)),
        (
            "Data Quality",
            f"{quality:.1f}%" if quality is not None else "N/A",
        ),
        ("Auto-Recoveries", number(recovered)),
    ])

    section(story, "Pipeline Health & Run Overview")

    stage_order = ["ingest", "transform", "validate"]

    ordered = sorted(
        pipelines,
        key=lambda p: next(
            (
                i for i, name in enumerate(stage_order)
                if name in str(p.get("dag_id", "")).lower()
            ),
            99,
        ),
    )

    stage_cells = []

    for stage in ordered[:3]:
        stage_cells.append(rich(
            f'<b>{safe(stage["name"])}</b><br/>'
            f'<font color="#087E63">'
            f'{safe(labelize(stage["status"]))}'
            "</font>"
        ))

    if stage_cells:
        story.append(panel(
            [stage_cells],
            widths=[CONTENT_WIDTH / len(stage_cells)] *
                   len(stage_cells),
            background="#EFFCF5",
        ))

    section(story, "Incident & Recovery Trend")

    story.append(recovery_trend(days))

    story.append(paragraph(
        "Last 7 days | Purple: detected incidents | "
        "Green: verified auto-recoveries",
        "PA_Small",
    ))

    story.append(Spacer(1, 8))

    section(story, "Operational Performance")

    if recovery_rate is not None:
        performance = (
            f"{recovery_rate:.1f}% of incident records "
            "in the reporting window have an "
            "auto-recovered outcome."
        )
    else:
        performance = (
            "No incidents were recorded during "
            "the reporting window."
        )

    story.append(panel(
        [[paragraph(performance)]],
        widths=[CONTENT_WIDTH],
        background="#EFFAF7",
    ))

    # ==============================================
    # PAGE 2 — DATA QUALITY & INCIDENT ANALYSIS
    # ==============================================

    story.append(PageBreak())

    report_header("Data Quality & Incident Analysis")

    kpi_cards(story, [
        ("Incident Records", number(total)),
        ("Recovered", number(recovered)),
        ("Quarantined", number(quarantined)),
        (
            "Recovery Rate",
            f"{recovery_rate:.1f}%"
            if recovery_rate is not None else "N/A",
        ),
    ])

    section(story, "Incident Type Distribution")

    story.append(
        incident_donut(incident_types.most_common(6))
    )

    section(story, "Data Quality Overview")

    if quality is not None:
        story.append(quality_bar(quality))
    else:
        story.append(paragraph(
            "Quality metrics are unavailable "
            "for the latest ingest batch."
        ))

    quality_rows = [
        [
            rich("<b>Metric</b>", "PA_Table"),
            rich("<b>Latest Result</b>", "PA_Table"),
        ],
        [
            paragraph("Usable rows", "PA_Table"),
            paragraph(number(usable), "PA_Table"),
        ],
        [
            paragraph("Quarantined rows", "PA_Table"),
            paragraph(number(quarantined), "PA_Table"),
        ],
        [
            paragraph("Usable data share", "PA_Table"),
            paragraph(
                f"{quality:.1f}%"
                if quality is not None else "N/A",
                "PA_Table",
            ),
        ],
        [
            paragraph("Healthy stages", "PA_Table"),
            paragraph(
                f"{healthy}/{len(pipelines)}",
                "PA_Table",
            ),
        ],
    ]

    quality_table = Table(
        quality_rows,
        colWidths=[260, 252],
        repeatRows=1,
    )

    quality_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), PALE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [WHITE, colors.HexColor("#FAF9FF")]),
        ("LINEBELOW", (0, 0), (-1, -1),
         0.4, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
    ]))

    story.append(quality_table)

    section(story, "Recent Incident Highlights")

    if highlights:
        for incident in highlights[:4]:
            name = labelize(incident.get("error_type"))
            status = labelize(incident.get("status"))
            outcome = str(incident.get("outcome") or "")

            story.append(KeepTogether([
                rich(
                    f"<b>{safe(name)}</b>  |  {safe(status)}"
                ),
                paragraph(
                    outcome[:190] or
                    "No additional outcome details recorded.",
                    "PA_Small",
                ),
            ]))
    else:
        story.append(paragraph(
            "No recent incidents were recorded."
        ))

    # ==============================================
    # PAGE 3 — BUSINESS INSIGHTS
    # ==============================================

    story.append(PageBreak())

    report_header("Business Insights & Recommendations")

    section(story, "Business Impact")

    reliable_data = (
        f"The latest ingest run produced "
        f"{number(usable)} usable records."
    )

    if quarantined:
        reliable_data += (
            f" {number(quarantined)} records "
            "were quarantined for quality control."
        )

    business_cards = [
        (
            "RELIABLE DATA",
            reliable_data,
        ),
        (
            "AUTOMATED OPERATIONS",
            f"{number(recovered)} recorded incidents "
            "were automatically recovered.",
        ),
        (
            "PIPELINE VISIBILITY",
            f"{healthy} of {len(pipelines)} stages "
            "are currently healthy.",
        ),
    ]

    for title, description in business_cards:
        story.append(panel(
            [[
                rich(f"<b>{safe(title)}</b>"),
                paragraph(description),
            ]],
            widths=[170, 342],
            background="#F5F1FF",
        ))

        story.append(Spacer(1, 8))

    section(story, "Key Insights")

    insights = []

    if incident_types:
        issue, count = incident_types.most_common(1)[0]

        insights.append(
            f"{issue} was the most frequently recorded "
            f"issue, with {count:,} incidents."
        )

    if quality is not None:
        insights.append(
            f"The latest ingest batch delivered "
            f"{quality:.1f}% usable data."
        )

    if recovery_rate is not None:
        insights.append(
            f"The recorded auto-recovery rate "
            f"for the last seven days was "
            f"{recovery_rate:.1f}%."
        )

    if failed:
        insights.append(
            f"{failed:,} recovery outcomes were "
            "not verified as successful."
        )

    if not insights:
        insights.append(
            "Additional operational history is needed "
            "to generate meaningful insights."
        )

    for insight in insights:
        story.append(paragraph("• " + insight))

    section(story, "Recommendations & Next Steps")

    recommendations = [
        (
            "Review the most frequent incident categories "
            "to identify recurring upstream data issues."
        ),
        (
            "Monitor quarantined records alongside "
            "usable-row volume to assess data coverage."
        ),
        (
            "Continue automated recovery and validation, "
            "with detailed evidence retained in Audit Log."
        ),
        (
            "Use weekly reporting to compare operational "
            "performance across reporting periods."
        ),
    ]

    for recommendation in recommendations:
        story.append(paragraph("• " + recommendation))

    story.append(Spacer(1, 16))

    story.append(panel(
        [[paragraph(
            "REPORTING NOTE: All numerical metrics are "
            "derived from recorded database values. "
            "This report does not estimate uptime, "
            "recovery duration, or missing historical data.",
            "PA_Small",
        )]],
        widths=[CONTENT_WIDTH],
        background="#F4F0FF",
    ))

    # ----------------------------------------------
    # PAGE DECORATION
    # ----------------------------------------------

    def decorate(canvas, document):
        canvas.saveState()

        width, height = document.pagesize

        canvas.setFillColor(PURPLE)
        canvas.rect(
            0, height - 8,
            width, 8,
            fill=1,
            stroke=0,
        )

        canvas.setStrokeColor(BORDER)
        canvas.line(
            50, 42,
            width - 50, 42,
        )

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(GREY)

        canvas.drawString(
            50, 29,
            "PIPELINE AUTOPILOT | Executive Operations",
        )

        canvas.drawRightString(
            width - 50, 29,
            f"Page {document.page}",
        )

        canvas.restoreState()

    doc.build(
        story,
        onFirstPage=decorate,
        onLaterPages=decorate,
    )

    return buffer.getvalue()
