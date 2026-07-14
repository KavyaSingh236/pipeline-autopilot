import { fmtTime } from "@/components/status";

const OUTCOME_COLOR = {
  approved: "#00FF66",
  rejected: "#FF0055",
  pending_approval: "#FFCC00",
};

const PIPE_LABEL = {
  olist_ingest: "Ingest",
  olist_validate: "Validate",
  olist_transform: "Transform",
};

export default function AuditLog({ rows }) {
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
          {rows.map((r) => (
            <tr key={r.id} className="hover:bg-white/5 transition-colors" data-testid={`audit-row-${r.id}`}>
              <td className="px-4 py-3 border-b border-white/5 font-mono text-xs text-white/70 whitespace-nowrap">
                {fmtTime(r.created_at)}
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
          ))}
        </tbody>
      </table>
    </div>
  );
}
