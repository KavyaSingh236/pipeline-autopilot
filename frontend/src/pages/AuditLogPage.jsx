import { useEffect, useState, useCallback } from "react";
import AuditLog from "@/components/AuditLog";
import { getAudit } from "@/lib/api";

const PIPELINES = [
  { id: "", label: "All Pipelines" },
  { id: "olist_ingest", label: "Ingest" },
  { id: "olist_validate", label: "Validate" },
  { id: "olist_transform", label: "Transform" },
];
const STATUSES = [
  { id: "", label: "All Statuses" },
  { id: "pending_approval", label: "Pending" },
  { id: "approved", label: "Approved" },
  { id: "rejected", label: "Rejected" },
];

export default function AuditLogPage({ lastEvent }) {
  const [rows, setRows] = useState([]);
  const [pipeline, setPipeline] = useState("");
  const [status, setStatus] = useState("");
  const [since, setSince] = useState("");

  const load = useCallback(async () => {
    const params = {};
    if (pipeline) params.pipeline_id = pipeline;
    if (status) params.status = status;
    if (since) params.since = new Date(since).toISOString();
    try {
      setRows(await getAudit(params));
    } catch (e) {}
  }, [pipeline, status, since]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (lastEvent) load(); }, [lastEvent, load]);

  return (
    <div>
      <div className="text-[10px] tracking-[0.3em] uppercase text-white/40 mb-3">Compliance Trail</div>
      <h1 className="font-display text-4xl sm:text-5xl tracking-tighter uppercase font-light leading-none mb-8">
        Audit Log
      </h1>

      <div className="flex flex-wrap gap-3 mb-6 border border-white/10 bg-[#0A0A0A] p-4">
        <Select label="Pipeline" value={pipeline} onChange={setPipeline} options={PIPELINES} testId="filter-pipeline" />
        <Select label="Status" value={status} onChange={setStatus} options={STATUSES} testId="filter-status" />
        <div className="flex flex-col gap-1">
          <label className="text-[9px] tracking-[0.2em] uppercase text-white/40">Since</label>
          <input
            type="date"
            data-testid="filter-since"
            value={since}
            onChange={(e) => setSince(e.target.value)}
            className="bg-[#111111] border border-white/10 text-white font-mono text-xs px-3 py-2 focus:outline-none focus:border-[#00E5FF]/50"
          />
        </div>
        <div className="flex items-end">
          <button
            data-testid="clear-filters"
            onClick={() => { setPipeline(""); setStatus(""); setSince(""); }}
            className="px-4 py-2 border border-white/20 text-white/60 font-mono text-[10px] tracking-[0.15em] uppercase hover:border-white/50 hover:text-white transition-colors"
          >
            Clear
          </button>
        </div>
        <div className="flex items-end ml-auto">
          <span className="font-mono text-xs text-white/40" data-testid="audit-count">{rows.length} records</span>
        </div>
      </div>

      <AuditLog rows={rows} />
    </div>
  );
}

function Select({ label, value, onChange, options, testId }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-[9px] tracking-[0.2em] uppercase text-white/40">{label}</label>
      <select
        data-testid={testId}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-[#111111] border border-white/10 text-white font-mono text-xs px-3 py-2 focus:outline-none focus:border-[#00E5FF]/50"
      >
        {options.map((o) => (
          <option key={o.id} value={o.id}>{o.label}</option>
        ))}
      </select>
    </div>
  );
}
