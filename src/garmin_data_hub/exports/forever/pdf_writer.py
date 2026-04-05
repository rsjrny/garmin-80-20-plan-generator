from pathlib import Path
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import LETTER

def _styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="FT_Title", fontSize=20, leading=24, alignment=1, spaceAfter=14))
    styles.add(ParagraphStyle(name="FT_Header", fontSize=15, leading=18, spaceBefore=16, spaceAfter=8, bold=True))
    styles.add(ParagraphStyle(name="FT_Body", fontSize=11, leading=14, spaceAfter=8))
    styles.add(ParagraphStyle(name="FT_Italic", fontSize=11, leading=14, spaceAfter=8, italic=True))
    return styles

def write_playbook_pdf(path: Path, brand: str = "Forever Training System"):
    styles = _styles()
    doc = SimpleDocTemplate(str(path), pagesize=LETTER, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    story.append(Paragraph(f"THE {brand.upper()}", styles["FT_Title"]))
    story.append(Paragraph("A Repeatable, Data-Driven Endurance Training Playbook", styles["FT_Italic"]))
    story.append(Spacer(1, 20))
    story.append(Paragraph("Designed for aging endurance athletes who value longevity, durability, and clarity over hype.", styles["FT_Body"]))
    story.append(PageBreak())

    story.append(Paragraph("Executive Summary", styles["FT_Header"]))
    story.append(Paragraph("This playbook documents a repeatable process for transforming activity data into a sustainable training system.", styles["FT_Body"]))
    story.append(PageBreak())

    story.append(Paragraph("Core Principles", styles["FT_Header"]))
    for p in [
        "Train for decades, not dates.",
        "Physiology is discovered from data, not formulas.",
        "Heart rate governs pace, not the reverse.",
        "Consistency beats intensity.",
        "Complexity is the enemy of durability.",
        "One planning system, one execution device.",
        "Fueling and hydration are performance-critical.",
        "A system adapts; a plan expires.",
    ]:
        story.append(Paragraph(f"• {p}", styles["FT_Body"]))
    story.append(PageBreak())

    story.append(Paragraph("Repeatable Process", styles["FT_Header"]))
    steps = [
        "Step 1 — Data Analysis (anchors: HRmax, LTHR, durability, risks)",
        "Step 2 — Philosophy (non-negotiables)",
        "Step 3 — Macro Plan (phases, long-run logic, cutbacks, taper)",
        "Step 4 — Workout Library (minimal reusable set)",
        "Step 5 — Tool Integration (Intervals plans, Garmin executes)",
        "Step 6 — Calendar (table format)",
        "Step 7 — Metrics (weekly rollups)",
        "Step 8 — Nutrition (sweat-driven sodium + carbs)",
        "Step 9 — Consolidation (single workbook as source of truth)",
    ]
    for s in steps:
        story.append(Paragraph(f"• {s}", styles["FT_Body"]))

    doc.build(story)

def write_public_edition_pdf(path: Path, brand: str = "Forever Training System"):
    styles = _styles()
    doc = SimpleDocTemplate(str(path), pagesize=LETTER, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    story.append(Paragraph(brand, styles["FT_Title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "A simple, data-driven framework for endurance athletes who value longevity, consistency, and health over hype.",
        styles["FT_Body"]
    ))
    story.append(Paragraph(
        "Core ideas: discover physiology from data, govern effort with heart rate, keep workouts minimal and repeatable, track weekly trends, and fuel deliberately.",
        styles["FT_Body"]
    ))
    doc.build(story)

def write_russ_edition_pdf(path: Path, athlete_name: str, age: int, event_name: str, event_date: str, brand: str = "Forever Training System"):
    styles = _styles()
    doc = SimpleDocTemplate(str(path), pagesize=LETTER, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    story.append(Paragraph(f"{brand} — {athlete_name} Edition", styles["FT_Title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        f"This edition documents a real application of the system for {athlete_name} (age {age}) preparing for {event_name} on {event_date}.",
        styles["FT_Body"]
    ))
    story.append(Paragraph(
        "It prioritizes durability, independence, and low-friction execution: Intervals.icu for planning, Garmin for workouts, and a single Excel workbook for truth.",
        styles["FT_Body"]
    ))
    doc.build(story)
