import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, ArrowRight, Database, GitBranch, ShieldCheck, RefreshCw, Info, Layers3 } from 'lucide-react';
import { getPipelines, getLineage, getFlowPreview } from '@/lib/api';

const layers = [
  {key:'source', title:'Source', subtitle:'Google Trends · BigQuery', description:'The public BigQuery dataset supplies trending search terms. The source is read by the ingestion job; it is not a local database table.', icon:Database},
  {key:'bronze', title:'Bronze', subtitle:'Raw incoming records', description:'The ingestion job loads source records into bronze.raw_trends before downstream cleaning.', icon:Database},
  {key:'silver', title:'Silver', subtitle:'Cleaned and standardized', description:'Transformation prepares consistent search terms, regions and ranks in silver.trends_clean.', icon:GitBranch},
  {key:'gold_terms', title:'Gold · Terms', subtitle:'Trending terms', description:'The gold.trending_terms table provides analysis-ready trend results.', icon:Layers3},
  {key:'gold_regions', title:'Gold · Regions', subtitle:'Regional leaders', description:'The gold.region_leaders table supports regional comparison and reporting.', icon:ShieldCheck},
];
const nodeId = {bronze:'b_trends', silver:'s_trends', gold_terms:'g_terms', gold_regions:'g_region'};
const format = n => typeof n === 'number' ? n.toLocaleString() : '—';
export default function DataFlowExplorer({lastEvent}) {
  const [selected,setSelected] = useState('bronze');
  const [pipelines,setPipelines] = useState([]);
  const [lineage,setLineage] = useState(null);
  const [preview,setPreview] = useState(null);
  const [error,setError] = useState('');
  const [loading,setLoading] = useState(false);
  const loadFlow = useCallback(async () => {
    try {
      const p = await getPipelines();
      setPipelines(Array.isArray(p)?p:[]);
      const id = (p||[]).find(x=>String(x.dag_id||x.id).includes('ingest'))?.id || p?.[0]?.id;
      if (id) setLineage(await getLineage(id));
      else setLineage(null);
      setError('');
    } catch { setError('Live pipeline data is temporarily unavailable.'); }
  },[]);
  useEffect(()=>{loadFlow();const timer=setInterval(loadFlow,45000);return()=>clearInterval(timer);},[loadFlow]);
  useEffect(()=>{if(lastEvent)loadFlow()},[lastEvent,loadFlow]);
  useEffect(()=>{
    if(selected==='source'||selected==='gold_regions'){setPreview(null);return;}
    let active=true;
    setLoading(true);setPreview(null);
    getFlowPreview(selected).then(x=>{if(active){setPreview(x);setError('');}}).catch(()=>{if(active)setError('Preview temporarily unavailable; lineage counts may still be visible.');}).finally(()=>{if(active)setLoading(false);});
    return ()=>{active=false};
  },[selected,lastEvent]);
  const stage = layers.find(x=>x.key===selected);
  const node = useMemo(()=>lineage?.nodes?.find(n=>n.id===nodeId[selected]),[lineage,selected]);
  const columns = preview?.rows?.length ? Object.keys(preview.rows[0]).slice(0,8) : [];
  const healthy = pipelines.length>0 && pipelines.every(x=>x.status==='healthy');
  return <div className="pa-dashboard pa-explorer">
    <Link className="pa-back-link" to="/"><ArrowLeft size={16}/> Back to Dashboard</Link>
    <div className="pa-heading"><div><span className="pa-eyebrow">LIVE DATA LINEAGE</span><h1>Data Flow Explorer <span>✦</span></h1><p>Follow your Google Trends data from its source through each analytics layer.</p></div><button type="button" className="pa-refresh" onClick={loadFlow}><RefreshCw size={15}/> Refresh</button></div>
    {error&&<div className="pa-warning" role="status">{error}</div>}
    <section className="pa-panel"><div className="pa-panel-heading"><h2><GitBranch size={22}/> Source → Bronze → Silver → Gold</h2><span className={'pa-health-pill '+(healthy?'ok':'attention')}>{healthy?'✓ All stages healthy':'Live monitoring'}</span></div>
      <div className="pa-flow-steps" aria-label="Data processing stages">
        {layers.map((x,i)=><div className="pa-flow-step-wrap" key={x.key}><button type="button" onClick={()=>setSelected(x.key)} aria-pressed={selected===x.key} className={'pa-flow-step '+(selected===x.key?'selected':'')}><span className="pa-flow-circle"><x.icon size={25}/></span><strong>{x.title}</strong><small>{x.subtitle}</small>{nodeId[x.key]&&<em>{format(lineage?.nodes?.find(n=>n.id===nodeId[x.key])?.rows)} rows in table</em>}</button>{i<layers.length-1&&<span className="pa-flow-connector" aria-hidden="true">→</span>}</div>)}
      </div>
      <p className="pa-muted pa-flow-note"><Info size={14}/> Gold has two output tables. Counts represent current table contents, not the latest batch size. Select any stage for details.</p>
    </section>
    <section className="pa-panel pa-flow-detail"><div className="pa-panel-heading"><h2><stage.icon size={21}/> {stage.title} · What happens here?</h2><span className="pa-muted">Read-only live exploration</span></div><p>{stage.description}</p>
      <div className="pa-flow-stat"><div><small>Storage / source</small><strong>{preview?.table|| (selected==='source'?'Google Trends (BigQuery)':stage.subtitle)}</strong></div><div><small>Rows currently stored</small><strong>{selected==='source'?'Not stored locally':format(preview?.row_count ?? node?.rows)}</strong></div></div>
      {!['source','gold_regions'].includes(selected) && <>
      <div className="pa-panel-heading"><h2><Database size={19}/> Sample records</h2><span className="pa-muted">Up to 5 real rows · no edits</span></div>
      {loading?<div className="pa-empty">Loading a live data preview…</div>:columns.length?<div className="pa-preview-scroll"><table className="pa-preview-table"><thead><tr>{columns.map(c=><th key={c}>{c.replaceAll('_',' ')}</th>)}</tr></thead><tbody>{preview.rows.map((r,i)=><tr key={i}>{columns.map(c=><td key={c} title={String(r[c]??'')}>{r[c]===null?'—':typeof r[c]==='object'?JSON.stringify(r[c]):String(r[c])}</td>)}</tr>)}</tbody></table></div>:<div className="pa-empty">No preview records are currently available for this layer.</div>}
      </>}
    </section>
    <p className="pa-muted pa-explorer-footer">Data previews are read-only. Detailed failures, diagnoses, recovery actions and evidence remain in <Link to="/audit">Audit Log <ArrowRight size={13}/></Link>.</p>
  </div>;
}
