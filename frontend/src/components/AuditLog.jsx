import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { fmtTime } from "@/components/status";

const OUTCOME_COLOR = {
  approved: "#00FF66",
  auto_fixed: "#00E5FF",
  manually_fixed: "#B388FF",
  rejected: "#FF0055",
  pending_approval: "#FFCC00",
};

const PIPE_LABEL = {
  trends_ingest: "Ingest",
  trends_validate: "Validate",
  trends_transform: "Transform",
};

export default function AuditLog({ rows }) {
  const [params, setParams] = useSearchParams();
  const [expanded, setExpanded] = useState(params.get("incident"));
  useEffect(() => { if (params.get("incident")) setExpanded(params.get("incident")); }, [params]);
  if (!rows?.length) {
    return (
      <div className="border border-white/10 bg-[#0A0A0A] p-12 text-center text-white/40 text-sm" data-testid="audit-empty">
        No audit records match the current filters.
      </div>
    );
  }
  return (
    <div className="border border-white/10 bg-[#0A0A0A] overflow-x-auto" data-testid="audit-table">
      <table className="w-full border-collapse text-left">
        <thead>
          <tr className="text-[9px] tracking-[0.2em] uppercase text-white/40">
            {["Timestamp", "Pipeline", "Error", "Fix Applied", "Approved By", "Outcome"].map((h) => (
              <th key={h} className="px-4 py-3 border-b border-white/10 font-normal whitespace-nowrap">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (<ReactRow key={r.id} r={r} expanded={expanded === String(r.id)} toggle={() => { const next = expanded === String(r.id) ? null : String(r.id); setExpanded(next); if (next) setParams({incident: next}); else setParams({}); }} />))}
        </tbody>
      </table>
    </div>
  );
}

function ReactRow({r,expanded,toggle}) {
 return <>
            <tr className="hover:bg-white/5 transition-colors" data-testid={`audit-row-${r.id}`}>
              <td className="px-4 py-3 border-b border-white/5 font-mono text-xs text-white/70 whitespace-nowrap">
                <button onClick={toggle} aria-expanded={expanded} className="text-[#00E5FF] hover:underline focus:outline-none focus:underline">{fmtTime(r.created_at)} {expanded ? "▴" : "▾"}</button>
              </td>
              <td className="px-4 py-3 border-b border-white/5 font-mono text-xs text-white/90 whitespace-nowrap">
                {PIPE_LABEL[r.pipeline_id] || r.pipeline_id}
              </td>
              <td className="px-4 py-3 border-b border-white/5">
                <span className="font-mono text-[10px] px-2 py-0.5 border border-white/20 text-white/70">
                  {r.error_type}
                </span>
              </td>
              <td className="px-4 py-3 border-b border-white/5 font-mono text-xs text-white/60 max-w-[280px]">
                {r.proposed_fix}
                {r.outcome && r.status === "manually_fixed" && (
                  <div className="text-[10px] text-white/40 mt-1 whitespace-normal">{r.outcome}</div>
                )}
                {r.root_cause && (
                  <div className="text-[10px] text-white/40 mt-1 whitespace-normal">
                    AI root cause: {r.root_cause}{r.model ? ` · ${r.model}` : ""}
                  </div>
                )}
              </td>
              <td className="px-4 py-3 border-b border-white/5 font-mono text-xs text-white/70 whitespace-nowrap">
                {r.approved_by || "—"}
              </td>
              <td className="px-4 py-3 border-b border-white/5 whitespace-nowrap">
                <span
                  className="font-mono text-[10px] tracking-[0.1em] uppercase px-2 py-1 border"
                  style={{
                    color: OUTCOME_COLOR[r.status] || "#fff",
                    borderColor: OUTCOME_COLOR[r.status] || "#fff",
                    background: `${OUTCOME_COLOR[r.status] || "#fff"}1a`,
                  }}
                >
                  {r.status}
                </span>
              </td>
            </tr>
            {expanded && <tr><td colSpan={6} className="px-6 py-5 bg-[#10151B] border-b border-[#00E5FF]/30">
              <div className="grid md:grid-cols-2 gap-5 text-xs font-mono break-words">
                {[["Error detected", r.description], ["Root cause", r.root_cause], ["Diagnosis source", r.model || "Not recorded"], ["Recommended fix", r.proposed_fix], ["Executed action", r.action], ["Result / human decision", r.outcome], ["Operator", r.approved_by], ["Validation evidence / error log", r.error_log], ["Downstream impact", r.downstream_impact], ["Risk if approved", r.risk_if_approved], ["Risk if rejected", r.risk_if_rejected], ["Affected rows", r.rows_affected]].filter(([,v]) => v !== null && v !== undefined && v !== "").map(([label,value]) => (
                  <div key={label}><div className="uppercase tracking-widest text-[10px] text-[#00E5FF]/70 mb-2">{label}</div><div className="text-white/75 whitespace-pre-wrap">{String(value)}</div></div>
                ))}
              </div>
            </td></tr>}
 </>;
}
