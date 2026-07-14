import { useState } from "react";
import { motion } from "framer-motion";
import { toast } from "sonner";
import { AlertTriangle, Check, X, Wrench, Zap, ShieldAlert } from "lucide-react";
import { approveFix, rejectFix } from "@/lib/api";
import { fmtTime } from "@/components/status";

export default function FailureDetail({ pipelineId, failure, onResolved }) {
  const [busy, setBusy] = useState(false);

  const handleApprove = async () => {
    setBusy(true);
    try {
      await approveFix(pipelineId, { audit_id: failure.id, approved_by: "operator" });
      toast.success("Fix approved · pipeline healed and rerun");
      onResolved?.();
    } catch (e) {
      toast.error("Approval failed");
    } finally {
      setBusy(false);
    }
  };

  const handleReject = async () => {
    setBusy(true);
    try {
      await rejectFix(pipelineId, { audit_id: failure.id, rejected_by: "operator", reason: "Manual review" });
      toast("Fix rejected · escalated to on-call", { icon: "⚠" });
      onResolved?.();
    } catch (e) {
      toast.error("Rejection failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="tracing-beam border border-transparent bg-[#0A0A0A] p-6"
      data-testid="failure-detail"
      style={{ boxShadow: "0 0 40px rgba(255,0,85,0.10)" }}
    >
      <div className="flex items-center gap-2 text-[#FF0055]">
        <AlertTriangle size={16} />
        <span className="text-[10px] tracking-[0.2em] uppercase">Human Approval Required</span>
      </div>

      <h3 className="font-display text-2xl mt-4 tracking-tight text-white">
        {failure.description}
      </h3>

      <div className="mt-2 flex items-center gap-3 flex-wrap">
        <span
          className="font-mono text-[10px] tracking-[0.15em] uppercase px-2.5 py-1 border border-[#FF0055] text-[#FF0055]"
          style={{ background: "rgba(255,0,85,0.1)" }}
        >
          {failure.error_type}
        </span>
        <span className="text-xs text-white/40">detected {fmtTime(failure.created_at)}</span>
        <span
          className="font-mono text-[10px] tracking-[0.15em] uppercase px-2.5 py-1 border"
          style={
            failure.auto_fixable
              ? { color: "#00E5FF", borderColor: "#00E5FF", background: "rgba(0,229,255,0.1)" }
              : { color: "#FFCC00", borderColor: "#FFCC00", background: "rgba(255,204,0,0.1)" }
          }
        >
          {failure.auto_fixable ? (
            <span className="inline-flex items-center gap-1"><Zap size={11} /> auto-fixable</span>
          ) : (
            <span className="inline-flex items-center gap-1"><ShieldAlert size={11} /> manual</span>
          )}
        </span>
      </div>

      <div className="mt-6 border border-white/10 bg-[#111111] p-4">
        <div className="flex items-center gap-2 text-white/40 text-[10px] tracking-[0.2em] uppercase">
          <Wrench size={13} /> Proposed Fix
        </div>
        <p className="font-mono text-sm text-white mt-2 leading-relaxed">{failure.proposed_fix}</p>
      </div>

      <div className="mt-6 flex gap-3">
        <button
          data-testid="approve-fix-button"
          disabled={busy}
          onClick={handleApprove}
          className="inline-flex items-center gap-2 px-5 py-2.5 font-mono text-xs tracking-[0.15em] uppercase bg-[#00FF66] text-black hover:bg-white transition-colors disabled:opacity-40"
        >
          <Check size={15} /> Approve Fix
        </button>
        <button
          data-testid="reject-fix-button"
          disabled={busy}
          onClick={handleReject}
          className="inline-flex items-center gap-2 px-5 py-2.5 font-mono text-xs tracking-[0.15em] uppercase border border-[#FF0055] text-[#FF0055] hover:bg-[#FF0055]/10 transition-colors disabled:opacity-40"
        >
          <X size={15} /> Reject
        </button>
      </div>
    </motion.div>
  );
}
