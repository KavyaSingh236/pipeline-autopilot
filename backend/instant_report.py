"""Executive-quality instant PDF based exclusively on recorded pipeline facts."""
import io
from collections import Counter
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.graphics.shapes import Drawing, Rect, String, Wedge, Circle, Line, PolyLine
import db

NAVY=colors.HexColor('#19184e'); PURPLE=colors.HexColor('#8054f7'); GREEN=colors.HexColor('#0fbf96'); BLUE=colors.HexColor('#4e8af7'); PALE=colors.HexColor('#f4f0ff'); GREY=colors.HexColor('#74718e'); ORANGE=colors.HexColor('#f2a34a')
def P(x): return escape(str(x if x is not None else '—'))

def panel(table, widths=None, bg='#f6f3ff', pad=12):
    t=Table(table,colWidths=widths,hAlign='LEFT');t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),colors.HexColor(bg)),('BOX',(0,0),(-1,-1),.7,colors.HexColor('#e2d9ff')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),pad),('RIGHTPADDING',(0,0),(-1,-1),pad),('TOPPADDING',(0,0),(-1,-1),12),('BOTTOMPADDING',(0,0),(-1,-1),12)]));return t

def bars(items, width=475, height=160):
    d=Drawing(width,height); maxv=max((v for _,v in items),default=1) or 1
    for i,(name,n) in enumerate(items[:6]):
        y=height-24-i*25
        d.add(String(5,y+4,str(name).title()[:22],fontSize=9,fillColor=NAVY))
        d.add(Rect(165,y, max(2,240*n/maxv),13,fillColor=[PURPLE,BLUE,GREEN,ORANGE][i%4],strokeColor=None))
        d.add(String(418,y+2,str(n),fontSize=10,fillColor=NAVY))
    return d

def trend(days):
    """Two-series line chart using real daily aggregate values."""
    d=Drawing(485,190);maxv=max([max(x[1],x[2]) for x in days] or [1]) or 1
    for i in range(5):
        y=38+i*29;d.add(Line(37,y,470,y,strokeColor=colors.HexColor('#e6e2f4'),strokeWidth=.7));d.add(String(5,y-3,str(round(maxv*i/4)),fontSize=8,fillColor=GREY))
    if days:
        xs=[40+i*425/max(1,len(days)-1) for i in range(len(days))]
        for index,col in [(1,PURPLE),(2,GREEN)]:
            coords=[]
            for i,day in enumerate(days):
                x=xs[i];y=38+116*day[index]/maxv;coords.extend((x,y));d.add(Circle(x,y,3,fillColor=col,strokeColor=colors.white,strokeWidth=.6))
            if len(days)>1:d.add(PolyLine(coords,strokeColor=col,strokeWidth=2.5,fillColor=None))
        for i,(day,_,_) in enumerate(days):d.add(String(xs[i]-15,20,day,fontSize=8,fillColor=GREY))
    d.add(Line(152,179,166,179,strokeColor=PURPLE,strokeWidth=3));d.add(String(170,175,'Detected',fontSize=9,fillColor=NAVY));d.add(Line(260,179,274,179,strokeColor=GREEN,strokeWidth=3));d.add(String(278,175,'Auto-recovered',fontSize=9,fillColor=NAVY))
    return d

def donut(items, width=485, height=175):
    d=Drawing(width,height);total=sum(v for _,v in items) or 1
    palette=[PURPLE,BLUE,GREEN,ORANGE,colors.HexColor('#b6a1f9'),colors.HexColor('#e76a92')]
    angle=90
    for i,(name,n) in enumerate(items[:6]):
        sweep=360*n/total
        if sweep>0:d.add(Wedge(93,88,65,angle-sweep,angle,fillColor=palette[i%len(palette)],strokeColor=colors.white,strokeWidth=1))
        angle-=sweep
    d.add(Circle(93,88,35,fillColor=colors.white,strokeColor=None));d.add(String(75,92,str(sum(v for _,v in items)),fontName='Helvetica-Bold',fontSize=17,fillColor=NAVY));d.add(String(72,78,'incidents',fontSize=9,fillColor=GREY))
    for i,(name,n) in enumerate(items[:6]):
        y=145-i*24;d.add(Circle(202,y,5,fillColor=palette[i%len(palette)],strokeColor=None));d.add(String(215,y-3,f'{str(name).title()[:24]}  ·  {n:,}',fontSize=9,fillColor=NAVY))
    return d

async def build_instant_pdf(client=None):
    now=datetime.now(timezone.utc);start=now-timedelta(days=7);pool=await db.get_pool()
    async with pool.acquire() as conn:
        pipelines=[dict(r) for r in await conn.fetch('SELECT id,name,status,dag_id FROM public.pipelines ORDER BY name')]
        runs=[dict(r) for r in await conn.fetch('SELECT dag_id,status,started_at,rows_processed,rows_quarantined FROM public.pipeline_runs ORDER BY started_at DESC LIMIT 30')]
        # Aggregation avoids the old 100-row sample masquerading as a complete week.
        groups=[dict(r) for r in await conn.fetch('SELECT error_type,status,COUNT(*)::int AS total FROM public.audit_log WHERE created_at >= $1 GROUP BY error_type,status',start)]
        daily=[dict(r) for r in await conn.fetch("SELECT date_trunc('day',created_at) AS day, COUNT(*)::int AS incidents, COUNT(*) FILTER (WHERE status='auto_fixed')::int AS recovered FROM public.audit_log WHERE created_at >= $1 GROUP BY 1 ORDER BY 1",start)]
        highlights=[dict(r) for r in await conn.fetch('SELECT error_type,status,outcome,created_at FROM public.audit_log WHERE created_at >= $1 ORDER BY created_at DESC LIMIT 4',start)]
    healthy=sum(p['status']=='healthy' for p in pipelines);latest=runs[0] if runs else {}
    usable=latest.get('rows_processed');q=latest.get('rows_quarantined');share=(100*usable/(usable+q)) if isinstance(usable,int) and isinstance(q,int) and usable+q>0 else None
    total=sum(r['total'] for r in groups);recovered=sum(r['total'] for r in groups if r['status']=='auto_fixed');failed=sum(r['total'] for r in groups if r['status']=='recovery_failed')
    types=Counter();[types.update({str(r['error_type'] or 'unknown').replace('_',' '):r['total']}) for r in groups]
    bydate={r['day'].date():r for r in daily};days=[]
    for i in range(6,-1,-1):
        day=(now-timedelta(days=i)).date();v=bydate.get(day,{});days.append((day.strftime('%b %d'),v.get('incidents',0),v.get('recovered',0)))
    buf=io.BytesIO();doc=SimpleDocTemplate(buf,pagesize=(8.5*inch,11*inch),leftMargin=49,rightMargin=49,topMargin=48,bottomMargin=48,title='Pipeline Autopilot | Executive Health Report')
    st=getSampleStyleSheet();st.add(ParagraphStyle(name='BrandX',fontName='Helvetica-Bold',fontSize=15,leading=20,textColor=NAVY));st.add(ParagraphStyle(name='TitleX',fontName='Helvetica-Bold',fontSize=22,leading=27,textColor=NAVY,spaceAfter=9));st.add(ParagraphStyle(name='HeadX',fontName='Helvetica-Bold',fontSize=13,leading=18,textColor=NAVY,spaceBefore=16,spaceAfter=9));st.add(ParagraphStyle(name='BodyX',fontName='Helvetica',fontSize=9.5,leading=15,textColor=NAVY,spaceAfter=7));st.add(ParagraphStyle(name='SmallX',fontName='Helvetica',fontSize=8,leading=12,textColor=GREY,spaceAfter=6));st.add(ParagraphStyle(name='KpiX',fontName='Helvetica-Bold',fontSize=20,leading=26,textColor=NAVY));
    para=lambda x,style='BodyX':Paragraph(P(x),st[style]);story=[]
    def head(title):story.append(para(title,'HeadX'))
    def kpis(data):
        cells=[]
        for label,value in data:cells.append(Paragraph(f'<font color="#7769a2" size="8">{P(label)}</font><br/><font color="#19184e" size="20"><b>{P(value)}</b></font>',st['BodyX']))
        story.append(panel([cells],[128]*4,bg='#eeeaff',pad=10));story.append(Spacer(1,9))
    def header(section):
        story.append(para('✦  PIPELINE AUTOPILOT','BrandX'));story.append(para('Autonomous ETL Monitoring & Recovery','SmallX'));story.append(Spacer(1,9));story.append(para(section,'TitleX'));story.append(para(f'Generated {now:%d %b %Y, %H:%M} UTC  |  Based on live recorded data','SmallX'))
    header('Instant Pipeline Health & Recovery Report')
    head('Executive Summary')
    summary=f'{healthy} of {len(pipelines)} monitored stages are currently healthy. In the past seven days, {total:,} incident records were observed, including {recovered:,} verified auto-recovery outcomes and {failed:,} failed recovery outcomes.'
    story.append(panel([[para(summary)]],[512],bg='#eee7ff',pad=18));story.append(Spacer(1,12))
    kpis([('HEALTHY STAGES',f'{healthy}/{len(pipelines)}'),('LATEST USABLE ROWS',usable if usable is not None else 'N/A'),('USABLE DATA SHARE',f'{share:.1f}%' if share is not None else 'N/A'),('AUTO-RECOVERIES',f'{recovered:,}')])
    head('Pipeline Health & Run Overview')
    stage_cells=[]
    for p in pipelines[:3]:stage_cells.append(Paragraph(f'<font color="#8054f7"><b>{P(p["name"])}</b></font><br/><font color="#167f67">{P(p["status"]).title()}</font>',st['BodyX']))
    if stage_cells:story.append(panel([stage_cells],[512/len(stage_cells)]*len(stage_cells),bg='#f0fff8'))
    head('Incident & Recovery Trend | Last 7 Days');story.append(trend(days))
    story.append(para('Purple = incidents recorded; green = auto-recovered outcomes. These are recorded events, not a claim of zero outstanding operational risk.','SmallX'))
    story.append(PageBreak());header('Data Quality & Operational Insights')
    kpis([('LATEST RUN',P(latest.get('status','N/A')).title()),('ROWS QUARANTINED',q if q is not None else 'N/A'),('INCIDENT RECORDS',f'{total:,}'),('RECOVERY FAILURES',f'{failed:,}')])
    head('Incident Type Distribution | Last 7 Days');story.append(donut(types.most_common(6)))
    head('Data Quality Analysis')
    story.append(panel([[para(f'Usable data share: {share:.1f}%' if share is not None else 'Usable data share unavailable'),para(f'Quarantined in latest run: {q}' if q is not None else 'Quarantine count unavailable')]],widths=[256,256],bg='#eefaf6'))
    head('Business Impact & Operational Value')
    impact=(f'The latest run delivered {usable:,} usable rows for downstream analytics.' if isinstance(usable,int) else 'Latest usable-row volume is unavailable.')
    if q:impact+=f' {q:,} rows were quarantined and may not appear in downstream reports.'
    story.append(panel([[para('RELIABLE DATA\n'+impact),para('OPERATIONAL EFFICIENCY\nAutomated recovery reduces routine manual work.'),para('CONTINUOUS MONITORING\nPipeline status and validation evidence remain available in the Audit Log.')]],widths=[171,171,170],bg='#f1edff',pad=9))
    head('Key Insights')
    story.append(para(f'• {recovered:,} of {total:,} incident records were marked auto-recovered during the reporting window.'))
    if types:story.append(para(f'• Most frequent issue: {types.most_common(1)[0][0]} ({types.most_common(1)[0][1]:,} recorded incidents).'))
    if failed:story.append(para(f'• {failed:,} recovery attempts were not verified; these should remain visible in technical audit evidence.'))
    head('Recommendations & Next Steps')
    for line in ['Review the most frequent recurring issue category to reduce preventable disruptions.','Track quarantined-row volume alongside successful pipeline runs to assess data coverage.','Continue automated validation and review full evidence in the Audit Log.']:story.append(para('• '+line))
    head('Recent Incident Highlights')
    for x in highlights[:3]:story.append(para(f"{str(x.get('error_type') or 'Incident').replace('_',' ').title()} — {str(x.get('status') or 'unknown').replace('_',' ')}. {str(x.get('outcome') or '')[:160]}"))
    story.append(Spacer(1,10));story.append(para('Metrics reflect actual database aggregates for the preceding seven days. No estimated uptime, resolution duration, or fabricated historical values are included.','SmallX'))
    def decorate(canvas,doc):
        canvas.saveState();w,h=doc.pagesize;canvas.setFillColor(PURPLE);canvas.rect(0,h-9,w,9,fill=1,stroke=0);canvas.setStrokeColor(colors.HexColor('#e9e0ff'));canvas.line(49,42,w-49,42);canvas.setFont('Helvetica',8);canvas.setFillColor(GREY);canvas.drawString(49,30,'PIPELINE AUTOPILOT  |  Executive Operations');canvas.drawRightString(w-49,30,f'Page {doc.page}');canvas.restoreState()
    doc.build(story,onFirstPage=decorate,onLaterPages=decorate);return buf.getvalue()
