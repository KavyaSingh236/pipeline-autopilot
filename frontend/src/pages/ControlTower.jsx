import { useEffect, useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import { AlertTriangle, ArrowRight, CheckCircle2 } from "lucide-react";
import PipelineCard from "@/components/PipelineCard";
import { getPipelines, getAudit } from "@/lib/api";

export default function ControlTower({ lastEvent }) {
  const [pipelines, setPipelines] = useState([]);
  const [recent, setRecent] = useState([]);
  const [now, setNow] = useState(new Date());

  const load = useCallback(async () => {
    try {
      const [p, a] = await Promise.all([getPipelines(), getAudit({})]);
      setPipelines(p);
      setRecent(a.filter(r => ["auto_fixed", "recovery_failed"].includes(r.status)).slice(0, 5));
    } catch (e) {}
  }, []);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => { if (lastEvent) load(); }, [lastEvent, load]);

  const recovered = recent.filter((r) => r.status === "auto_fixed").length;
  const healthy = pipelines.filter((p) => p.status === "healthy").length;

  return (
    <div>
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-10">
        <div>
          <div className="text-[10px] tracking-[0.3em] uppercase text-white/40 mb-3">Mission Control</div>
          <h1 className="font-display text-4xl sm:text-5xl tracking-tighter uppercase font-light leading-none">
            Control Tower
          </h1>
          <p className="text-sm text-white/50 mt-3 max-w-lg">
            Real-time posture across the self-healing ETL. Failures are classified, recovery actions
            applied autonomously, revalidated, and logged without human approval.
          </p>
        </div>
        <div className="border border-white/10 bg-[#0A0A0A] px-6 py-4">
          <div className="text-[9px] tracking-[0.2em] uppercase text-white/40">System Time · UTC</div>
          <div className="font-mono text-2xl text-[#00E5FF] mt-1 tabular-nums">
            {now.toISOString().slice(11, 19)}
          </div>
          <div className="flex gap-4 mt-2">
            <Stat label="Healthy" value={`${healthy}/${pipelines.length}`} color="#00FF66" />
            <Stat label="Recent Auto-Recovered" value={recovered} color="#00FF66" />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" data-testid="pipeline-grid">
        {pipelines.map((p, i) => (
          <PipelineCard key={p.id} pipeline={p} index={i} />
        ))}
      </div>
      <section className="mt-10 border border-white/10 bg-[#0A0A0A]" data-testid="recent-resolutions">
        <div className="flex items-center justify-between border-b border-white/10 px-6 py-5 gap-4">
          <div><h2 className="text-lg font-display uppercase tracking-wider">Recent Resolutions</h2>
            <p className="text-xs text-white/40 mt-1">Latest five detected failures and their outcomes</p></div>
          <Link to="/audit" className="text-[#00E5FF] text-xs font-mono uppercase flex items-center gap-2 hover:underline">View all <ArrowRight size={14}/></Link>
        </div>
        {recent.length === 0 ? <p className="p-6 text-sm text-white/40">No incidents recorded yet.</p> : recent.map(r => (
          <Link to={`/audit?incident=${encodeURIComponent(r.id)}`} key={r.id} className="flex flex-wrap items-center gap-3 justify-between px-6 py-4 border-b border-white/5 hover:bg-white/5 transition-colors">
            <div className="flex items-start gap-3 min-w-0">
              {r.status === "recovery_failed" ? <AlertTriangle size={17} className="text-[#FF0055] shrink-0"/> : <CheckCircle2 size={17} className="text-[#00E5FF] shrink-0"/>}
              <div className="min-w-0"><div className="text-sm text-white/90 font-medium">{(r.error_type || "Unknown error").replaceAll("_", " ")}</div>
                <div className="text-xs text-white/50 mt-1 break-words">{r.outcome || r.explanation || r.proposed_fix || r.description || "Awaiting resolution"}</div>
                <div className="text-[10px] text-white/30 mt-1 font-mono">{r.pipeline_id} · {r.created_at ? new Date(r.created_at).toLocaleString() : ""}</div></div>
            </div><span className={`text-[10px] font-mono tracking-wider uppercase border px-2 py-1 ${r.status === "recovery_failed" ? "text-[#FF0055] border-[#FF0055]/50" : "text-[#00E5FF] border-[#00E5FF]/50"}`}>{r.status.replaceAll("_", " ")}</span>
          </Link>
        ))}
      </section>
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
