import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowUpRight, Database, ShieldCheck, GitBranch, Layers } from "lucide-react";
import { StatusDot, StatusBadge, fmtTime, fmtNum, statusOf } from "@/components/status";

const ICONS = { ingest_dag: Database, validate_dag: ShieldCheck, transform_dag: GitBranch };

export default function PipelineCard({ pipeline, index }) {
  const Icon = ICONS[pipeline.dag_id] || Layers;
  const s = statusOf(pipeline.status);
  const last = pipeline.last_run;
  const needsApproval = pipeline.needs_approval;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.06, duration: 0.4 }}
    >
      <Link to={`/pipeline/${pipeline.id}`} data-testid={`pipeline-card-${pipeline.id}`}>
        <div
          className={`group relative bg-[#0A0A0A] border p-6 h-full transition-[transform,border-color] duration-300 hover:-translate-y-0.5 ${
            needsApproval ? "tracing-beam border-transparent" : "border-white/10 hover:border-white/30"
          }`}
          style={needsApproval ? { boxShadow: "0 0 40px rgba(255,0,85,0.12)" } : {}}
        >
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className="border border-white/10 p-2" style={{ color: s.color }}>
                <Icon size={18} />
              </div>
              <div className="flex items-center gap-2">
                <StatusDot status={pipeline.status} />
                <span className="text-[10px] tracking-[0.2em] uppercase text-white/40">
                  {pipeline.dag_id}
                </span>
              </div>
            </div>
            <ArrowUpRight size={16} className="text-white/30 group-hover:text-[#00E5FF] transition-colors" />
          </div>

          <h3 className="font-display text-xl mt-5 tracking-tight text-white">{pipeline.name}</h3>
          <p className="text-xs text-white/50 mt-2 leading-relaxed line-clamp-2">{pipeline.description}</p>

          <div className="mt-5">
            <StatusBadge status={pipeline.status} testId={`status-badge-${pipeline.id}`} />
          </div>

          <div className="grid grid-cols-2 gap-px mt-6 bg-white/10 border border-white/10">
            <Metric label="Rows Processed" value={fmtNum(last?.rows_processed)} />
            <Metric label="Quarantined" value={fmtNum(last?.rows_quarantined)} />
            <Metric label="Last Run" value={fmtTime(last?.finished_at)} />
            <Metric label="Total Runs" value={fmtNum(pipeline.total_runs)} />
          </div>
        </div>
      </Link>
    </motion.div>
  );
}

function Metric({ label, value }) {
  return (
    <div className="bg-[#0A0A0A] p-3">
      <div className="text-[9px] tracking-[0.2em] uppercase text-white/40">{label}</div>
      <div className="font-mono text-sm text-white mt-1 truncate">{value}</div>
    </div>
  );
}
