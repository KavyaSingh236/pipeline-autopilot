"""Readable on-demand PDF reports from live pipeline and audit data."""
import io
import json
import os
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether

import agents
import db


def _p(value):
    return escape(str(value or ''))


async def build_instant_pdf(client=None):
    pool = await db.get_pool()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)
    async with pool.acquire() as conn:
        pipelines = [dict(r) for r in await conn.fetch('SELECT id, name, status, dag_id FROM public.pipelines ORDER BY name')]
        runs = [dict(r) for r in await conn.fetch('''SELECT dag_id, status, started_at, rows_processed, rows_quarantined
            FROM public.pipeline_runs ORDER BY started_at DESC LIMIT 30''')]
        incidents = [dict(r) for r in await conn.fetch('''SELECT error_type, status, pipeline_id, created_at, root_cause, outcome
            FROM public.audit_log WHERE created_at >= $1 ORDER BY created_at DESC LIMIT 100''', start)]
    healthy = sum(p['status'] == 'healthy' for p in pipelines)
    latest = runs[0] if runs else None
    recovered = sum(r['status'] == 'auto_fixed' for r in incidents)
    failed = sum(r['status'] == 'recovery_failed' for r in incidents)
    types = {}
    for row in incidents:
        name = (row.get('error_type') or 'unknown').replace('_', ' ')
        types[name] = types.get(name, 0) + 1
    common = sorted(types.items(), key=lambda item: -item[1])[:4]
    facts = {'healthy_stages': healthy, 'total_stages': len(pipelines), 'latest_run': {
        'rows_processed': latest.get('rows_processed') if latest else None,
        'rows_quarantined': latest.get('rows_quarantined') if latest else None,
        'status': latest.get('status') if latest else None},
        'recent_auto_fixed': recovered, 'recent_recovery_failed': failed,
        'top_incident_types': common, 'window': 'last 7 days (up to 100 incident records)'}
    ai_summary = ''
    if client is not None:
        result = await agents._chat(client,
            'You write a clear, factual business summary of ETL operations. Return JSON with one field: summary. '
            'Write 2-4 short sentences using ONLY the supplied facts. Do not assert all incidents are resolved, '
            'or invent data, uptime, validation, or recovery times. Avoid technical jargon.', json.dumps(facts))
        if isinstance(result, dict):
            ai_summary = str(result.get('summary', '')).strip()[:1400]
    if not ai_summary:
        ai_summary = (f'{healthy} of {len(pipelines)} monitored pipeline stages are currently healthy. '
            f'The last seven days of available audit records include {recovered} auto-recovered incidents and '
            f'{failed} failed recovery attempts. Historical failures do not necessarily represent current unresolved issues.')

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=(8.5*inch, 11*inch), rightMargin=48, leftMargin=48,
                            topMargin=48, bottomMargin=45, title='Pipeline Autopilot | Instant Report')
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='PTitle', parent=styles['Title'], fontName='Helvetica-Bold', fontSize=23,
                              leading=29, textColor=colors.HexColor('#302367'), spaceAfter=7))
    styles.add(ParagraphStyle(name='PSection', parent=styles['Heading2'], fontName='Helvetica-Bold', fontSize=13,
                              leading=18, textColor=colors.HexColor('#624bc4'), spaceBefore=19, spaceAfter=8))
    styles.add(ParagraphStyle(name='PBody', parent=styles['BodyText'], fontName='Helvetica', fontSize=10,
                              leading=16, textColor=colors.HexColor('#45445f'), spaceAfter=8))
    styles.add(ParagraphStyle(name='PSub', parent=styles['BodyText'], fontSize=9, textColor=colors.HexColor('#8b83a3'), spaceAfter=17))
    story = [Paragraph('Pipeline Autopilot', styles['PTitle']),
             Paragraph('Instant Pipeline Health &amp; Recovery Report', styles['PSection']),
             Paragraph(f'Generated {now:%d %B %Y, %H:%M} UTC | Based on live pipeline records', styles['PSub']),
             Paragraph('Executive Summary', styles['PSection']), Paragraph(_p(ai_summary), styles['PBody'])]
    def section(title, paragraphs):
        story.append(Paragraph(title, styles['PSection']))
        for paragraph in paragraphs: story.append(Paragraph(_p(paragraph), styles['PBody']))
    section('Current Pipeline Health', [f'{healthy} of {len(pipelines)} pipeline stages report healthy status.' if pipelines else 'No pipeline health records were available.'] +
        [f"{p['name']}: {p['status']}" for p in pipelines])
    if latest:
        section('Latest Recorded Run', [f"Status: {latest.get('status', 'unknown')}. Rows processed: {latest.get('rows_processed', 'not available')}. "
                 f"Rows quarantined: {latest.get('rows_quarantined', 'not available')}. "
                 'Quarantined records may be excluded from published results.'])
    else: section('Latest Recorded Run', ['No run data was available.'])
    section('Incidents & Recovery (Last 7 Days)', [
        f'{len(incidents)} incident records included (maximum 100). {recovered} marked auto-fixed; {failed} marked recovery-failed.',
        'These are recorded incident outcomes, not a count of currently unresolved problems.'] +
        ([f"Most common incident types: {', '.join(f'{name} ({count})' for name,count in common)}."] if common else ['No recent incidents recorded.']))
    if incidents:
        lines=[]
        for incident in incidents[:5]:
            title=(incident.get('error_type') or 'Incident').replace('_',' ')
            state=(incident.get('status') or 'unknown').replace('_',' ')
            detail=incident.get('outcome') or incident.get('root_cause') or 'No further explanation recorded.'
            lines.append(f'{title} — {state}. {str(detail)[:280]}')
        section('Recent Incident Highlights', lines)
    section('Recommendations & Next Steps', [
        'Review recurring error types and any recovery-failed incidents in the Audit Log to confirm whether later runs resolved them.',
        'Monitor quarantined rows and the usable data share; do not interpret a successful run as proof that all original source rows were recovered.',
        'Keep automated recovery verification and scheduled reporting enabled as appropriate for your environment.'])
    story.append(Spacer(1,14))
    story.append(Paragraph('Prepared automatically from Pipeline Autopilot records. AI narrative is optional; recorded metrics are authoritative.', styles['PSub']))
    doc.build(story)
    return buffer.getvalue()
