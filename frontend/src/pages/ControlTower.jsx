import { useEffect, useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import { AlertTriangle, ArrowRight } from "lucide-react";
import PipelineCard from "@/components/PipelineCard";
import { getPipelines } from "@/lib/api";

export default function ControlTower({ lastEvent }) {
  const [pipelines, setPipelines] = useState([]);
  const [now, setNow] = useState(new Date());

  const load = useCallback(async () => {
    try {
      setPipelines(await getPipelines());
    } catch (e) {}
  }, []);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => { if (lastEvent) load(); }, [lastEvent, load]);

  const needApproval = pipelines.filter((p) => p.needs_approval);
  const healthy = pipelines.filter((p) => p.status === "healthy").length;

  return (
    <div>
      {needApproval.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          className="tracing-beam border border-transparent bg-[#0A0A0A] px-6 py-4 mb-8 flex items-center justify-between flex-wrap gap-4"
          data-testid="approval-banner"
          style={{ boxShadow: "0 0 40px rgba(255,0,85,0.12)" }}
        >
          <div className="flex items-center gap-3 text-[#FF0055]">
            <AlertTriangle size={18} />
            <span className="font-display text-sm tracking-[0.1em] uppercase">
              {needApproval.length} pipeline{needApproval.length > 1 ? "s" : ""} awaiting human approval
            </span>
          </div>
          <div className="flex items-center gap-3 flex-wrap">
            {needApproval.map((p) => (
              <Link
                key={p.id}
                to={`/pipeline/${p.id}`}
                data-testid={`banner-link-${p.id}`}
                className="inline-flex items-center gap-2 px-3 py-1.5 border border-[#FF0055] text-[#FF0055] text-[10px] tracking-[0.15em] uppercase hover:bg-[#FF0055]/10 transition-colors"
              >
                {p.name} <ArrowRight size={12} />
              </Link>
            ))}
          </div>
        </motion.div>
      )}

      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-10">
        <div>
          <div className="text-[10px] tracking-[0.3em] uppercase text-white/40 mb-3">Mission Control</div>
          <h1 className="font-display text-4xl sm:text-5xl tracking-tighter uppercase font-light leading-none">
            Control Tower
          </h1>
          <p className="text-sm text-white/50 mt-3 max-w-lg">
            Real-time posture across the self-healing ETL. Failures are classified, remediations
            proposed, and healing applied on operator approval.
          </p>
        </div>
        <div className="border border-white/10 bg-[#0A0A0A] px-6 py-4">
          <div className="text-[9px] tracking-[0.2em] uppercase text-white/40">System Time · UTC</div>
          <div className="font-mono text-2xl text-[#00E5FF] mt-1 tabular-nums">
            {now.toISOString().slice(11, 19)}
          </div>
          <div className="flex gap-4 mt-2">
            <Stat label="Healthy" value={`${healthy}/${pipelines.length}`} color="#00FF66" />
            <Stat label="Alerts" value={needApproval.length} color={needApproval.length ? "#FF0055" : "#00FF66"} />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" data-testid="pipeline-grid">
        {pipelines.map((p, i) => (
          <PipelineCard key={p.id} pipeline={p} index={i} />
        ))}
      </div>
    </div>
  );
}

function Stat({ label, value, color }) {
  return (
    <div>
      <div className="text-[9px] tracking-[0.2em] uppercase text-white/40">{label}</div>
      <div className="font-mono text-sm mt-0.5" style={{ color }}>{value}</div>
    </div>
  );
}
