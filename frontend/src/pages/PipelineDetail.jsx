import { useEffect, useState, useCallback } from "react";
import { useParams, Link } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowLeft, Clock, CheckCircle2, XCircle, Loader } from "lucide-react";
import FailureDetail from "@/components/FailureDetail";
import LineageFlow from "@/components/LineageFlow";
import { StatusBadge, StatusDot, fmtTime, fmtNum } from "@/components/status";
import { getPipeline, getFailures, getLineage } from "@/lib/api";

export default function PipelineDetail({ lastEvent }) {
  const { id } = useParams();
  const [pipeline, setPipeline] = useState(null);
  const [failures, setFailures] = useState([]);
  const [lineage, setLineage] = useState(null);

  const load = useCallback(async () => {
    try {
      const [p, f, l] = await Promise.all([getPipeline(id), getFailures(id), getLineage(id)]);
      setPipeline(p);
      setFailures(f);
      setLineage(l);
    } catch (e) {}
  }, [id]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (lastEvent?.payload?.pipeline_id === id) load();
  }, [lastEvent, id, load]);

  if (!pipeline) return <div className="text-white/40 font-mono text-sm">Loading pipeline…</div>;

  return (
    <div>
      <Link to="/" data-testid="back-link" className="inline-flex items-center gap-2 text-white/40 hover:text-white text-[10px] tracking-[0.2em] uppercase mb-6 transition-colors">
        <ArrowLeft size={13} /> Control Tower
      </Link>

      <div className="flex items-center gap-3 mb-2">
        <StatusDot status={pipeline.status} size={12} />
        <span className="text-[10px] tracking-[0.2em] uppercase text-white/40">{pipeline.dag_id}</span>
      </div>
      <div className="flex items-center justify-between flex-wrap gap-4">
        <h1 className="font-display text-3xl sm:text-4xl tracking-tighter text-white">{pipeline.name}</h1>
        <StatusBadge status={pipeline.status} testId="detail-status-badge" />
      </div>
      <p className="text-sm text-white/50 mt-2 max-w-2xl">{pipeline.description}</p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-8">
        {/* Left: failure + timeline */}
        <div className="space-y-6">
          {failures.length > 0 ? (
            failures.map((f) => (
              <FailureDetail key={f.id} pipelineId={id} failure={f} onResolved={load} />
            ))
          ) : (
            <div className="border border-[#00FF66]/30 bg-[#0A0A0A] p-6" data-testid="no-failure">
              <div className="flex items-center gap-2 text-[#00FF66]">
                <CheckCircle2 size={16} />
                <span className="text-[10px] tracking-[0.2em] uppercase">All Clear</span>
              </div>
              <p className="text-sm text-white/60 mt-2">No open failures. Pipeline is operating within data-quality thresholds.</p>
            </div>
          )}

          <div className="border border-white/10 bg-[#0A0A0A]">
            <div className="px-5 py-3 border-b border-white/10 flex items-center gap-2 text-white/40 text-[10px] tracking-[0.2em] uppercase">
              <Clock size={13} /> Recent Runs
            </div>
            <div className="divide-y divide-white/5" data-testid="run-timeline">
              {pipeline.runs?.length ? pipeline.runs.map((r) => (
                <RunRow key={r.id} run={r} />
              )) : (
                <div className="px-5 py-6 text-white/40 text-xs">No runs recorded yet.</div>
              )}
            </div>
          </div>
        </div>

        {/* Right: lineage */}
        <div>
          <div className="text-[10px] tracking-[0.2em] uppercase text-white/40 mb-3">Data Lineage · Bronze → Silver → Gold</div>
          <LineageFlow lineage={lineage} />
        </div>
      </div>
    </div>
  );
}

function RunRow({ run }) {
  const map = {
    success: { icon: CheckCircle2, color: "#00FF66" },
    failed: { icon: XCircle, color: "#FF0055" },
    running: { icon: Loader, color: "#00E5FF" },
  };
  const { icon: Icon, color } = map[run.status] || map.success;
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="px-5 py-3 flex items-center justify-between gap-4 hover:bg-white/5 transition-colors"
    >
      <div className="flex items-center gap-3 min-w-0">
        <Icon size={15} style={{ color }} />
        <div className="min-w-0">
          <div className="font-mono text-xs text-white truncate">{run.run_id}</div>
          <div className="font-mono text-[10px] text-white/40">{fmtTime(run.finished_at || run.started_at)}</div>
        </div>
      </div>
      <div className="flex items-center gap-5 text-right shrink-0">
        <div>
          <div className="text-[9px] tracking-[0.15em] uppercase text-white/40">Rows</div>
          <div className="font-mono text-xs text-white">{fmtNum(run.rows_processed)}</div>
        </div>
        <div>
          <div className="text-[9px] tracking-[0.15em] uppercase text-white/40">Quar.</div>
          <div className="font-mono text-xs" style={{ color: run.rows_quarantined ? "#FFCC00" : "#ffffff99" }}>
            {fmtNum(run.rows_quarantined)}
          </div>
        </div>
        <span className="font-mono text-[10px] tracking-[0.1em] uppercase px-2 py-0.5 border" style={{ color, borderColor: color, background: `${color}1a` }}>
          {run.status}
        </span>
      </div>
    </motion.div>
  );
}
