import { useState } from "react";
import { motion } from "framer-motion";
import { toast } from "sonner";
import {
  AlertTriangle,
  Check,
  X,
  Wrench,
  Zap,
  ShieldAlert,
} from "lucide-react";
import {
  approveFix,
  rejectFix,
  manualFix,
} from "@/lib/api";
import { fmtTime } from "@/components/status";

export default function FailureDetail({
  pipelineId,
  failure,
  onResolved,
}) {
  const [busy, setBusy] = useState(false);
  const [choice, setChoice] = useState("");
  const [instruction, setInstruction] = useState("");

  const rejected = failure.status === "rejected";

  let alternatives = [];

  try {
    alternatives = JSON.parse(
      failure.alternatives || "[]"
    );
  } catch (e) {
    alternatives = [];
  }

  alternatives = alternatives.slice(0, 3);

  const handleApprove = async () => {
    setBusy(true);

    try {
      await approveFix(
        pipelineId,
        {
          audit_id: failure.id,
          approved_by: "operator",
        }
      );

      toast.success(
        "Fix approved · pipeline healed and rerun"
      );

      onResolved?.();
    } catch (error) {
      toast.error(
        error?.response?.data?.detail ||
          "Approval failed"
      );
    } finally {
      setBusy(false);
    }
  };

  const handleReject = async () => {
    setBusy(true);

    try {
      await rejectFix(
        pipelineId,
        {
          audit_id: failure.id,
          rejected_by: "operator",
          reason: "Manual review",
        }
      );

      toast(
        "Fix rejected · manual remediation required",
        { icon: "⚠" }
      );

      onResolved?.();
    } catch (error) {
      toast.error(
        error?.response?.data?.detail ||
          "Rejection failed"
      );
    } finally {
      setBusy(false);
    }
  };

  const handleManual = async () => {
    if (!choice && !instruction.trim()) {
      toast.error(
        "Choose an option or enter an instruction"
      );
      return;
    }

    setBusy(true);

    try {
      await manualFix(
        pipelineId,
        {
          audit_id: failure.id,
          fixed_by: "operator",
          action: instruction.trim()
            ? undefined
            : choice || undefined,
          instruction: instruction.trim()
            ? instruction.trim()
            : undefined,
        }
      );

      toast.success(
        "Manual fix applied · pipeline healed"
      );

      onResolved?.();
    } catch (error) {
      toast.error(
        error?.response?.data?.detail ||
          "Manual fix failed"
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <motion.div
      initial={{
        opacity: 0,
        y: 10,
      }}
      animate={{
        opacity: 1,
        y: 0,
      }}
      className="tracing-beam border border-transparent bg-[#0A0A0A] p-6"
      data-testid="failure-detail"
      style={{
        boxShadow:
          "0 0 40px rgba(255,0,85,0.10)",
      }}
    >
      <div className="flex items-center gap-2 text-[#FF0055]">
        <AlertTriangle size={16} />

        <span className="text-[10px] tracking-[0.2em] uppercase">
          {rejected
            ? "Rejected · Manual Fix Required"
            : "Human Approval Required"}
        </span>
      </div>

      <h3 className="font-display text-2xl mt-4 tracking-tight text-white">
        {failure.description}
      </h3>

      <div className="mt-2 flex items-center gap-3 flex-wrap">
        <span className="font-mono text-[10px] tracking-[0.15em] uppercase px-2.5 py-1 border border-[#FF0055] text-[#FF0055] bg-[rgba(255,0,85,0.1)]">
          {failure.error_type}
        </span>

        <span className="text-xs text-white/40">
          detected {fmtTime(failure.created_at)}
        </span>

        <span
          className="font-mono text-[10px] tracking-[0.15em] uppercase px-2.5 py-1 border"
          style={
            failure.auto_fixable
              ? {
                  color: "#00E5FF",
                  borderColor: "#00E5FF",
                  background:
                    "rgba(0,229,255,0.1)",
                }
              : {
                  color: "#FFCC00",
                  borderColor: "#FFCC00",
                  background:
                    "rgba(255,204,0,0.1)",
                }
          }
        >
          {failure.auto_fixable ? (
            <span className="inline-flex items-center gap-1">
              <Zap size={11} />
              auto-fixable
            </span>
          ) : (
            <span className="inline-flex items-center gap-1">
              <ShieldAlert size={11} />
              human approval
            </span>
          )}
        </span>
      </div>

      <div className="mt-6 border border-white/10 bg-[#111111] p-4">
        <div className="flex items-center gap-2 text-white/40 text-[10px] tracking-[0.2em] uppercase">
          <Wrench size={13} />
          Proposed Fix
        </div>

        <p className="font-mono text-sm text-white mt-2 leading-relaxed">
          {failure.proposed_fix}
        </p>
      </div>

      <div className="mt-4 border border-white/10 bg-[#111111] p-4">
        <div className="text-white/40 text-[10px] tracking-[0.2em] uppercase">
          AI Diagnosis
          {failure.model
            ? ` · ${failure.model}`
            : ""}
        </div>

        {failure.root_cause && (
          <p className="font-mono text-sm text-white mt-2">
            {failure.root_cause}
          </p>
        )}

        {failure.explanation && (
          <p className="font-mono text-xs text-white/60 mt-2">
            {failure.explanation}
          </p>
        )}

        {failure.error_log && (
          <pre className="font-mono text-[11px] text-[#FF0055]/80 mt-3 whitespace-pre-wrap">
            {failure.error_log}
          </pre>
        )}
      </div>

      {failure.recommendation && (
        <div className="mt-4 border border-[#00E5FF]/30 bg-[#00E5FF]/5 p-4">
          <div className="flex items-center gap-3">
            <span className="text-[#00E5FF] text-[10px] tracking-[0.2em] uppercase">
              AI Recommendation
            </span>

            <span
              className="font-mono text-[10px] tracking-[0.15em] uppercase px-2 py-0.5 border"
              style={
                failure.recommendation ===
                "approve"
                  ? {
                      color: "#00FF66",
                      borderColor: "#00FF66",
                    }
                  : {
                      color: "#FF0055",
                      borderColor: "#FF0055",
                    }
              }
            >
              {failure.recommendation}
            </span>
          </div>

          {failure.downstream_impact && (
            <p className="font-mono text-xs text-white/70 mt-3">
              <span className="text-white/40">
                Downstream impact ·{" "}
              </span>
              {failure.downstream_impact}
            </p>
          )}

          {failure.risk_if_approved && (
            <p className="font-mono text-xs text-white/70 mt-2">
              <span className="text-[#00FF66]/70">
                Risk if approved ·{" "}
              </span>
              {failure.risk_if_approved}
            </p>
          )}

          {failure.risk_if_rejected && (
            <p className="font-mono text-xs text-white/70 mt-2">
              <span className="text-[#FF0055]/70">
                Risk if rejected ·{" "}
              </span>
              {failure.risk_if_rejected}
            </p>
          )}
        </div>
      )}

      {rejected ? (
        <div
          className="mt-6 border border-white/10 bg-[#111111] p-4"
          data-testid="manual-fix-panel"
        >
          <div className="text-white/40 text-[10px] tracking-[0.2em] uppercase">
            AI-ranked manual remediation
          </div>

          <div className="mt-3 space-y-2">
            {alternatives.map(
              (alternative, index) => (
                <label
                  key={alternative.action}
                  className={`flex items-start gap-3 p-3 border cursor-pointer transition-colors ${
                    choice ===
                    alternative.action
                      ? "border-[#00E5FF]/60 bg-[#00E5FF]/5"
                      : "border-white/5 hover:border-white/20"
                  }`}
                >
                  <input
                    type="radio"
                    name={`manual-fix-${failure.id}`}
                    className="mt-1"
                    checked={
                      choice ===
                      alternative.action
                    }
                    onChange={() => {
                      setChoice(
                        alternative.action
                      );
                      setInstruction("");
                    }}
                  />

                  <span className="font-mono text-xs text-white/80">
                    <span className="text-[#00E5FF] mr-2">
                      #{index + 1}
                    </span>

                    {alternative.label}

                    <span className="block text-white/40 mt-1">
                      {alternative.why}
                    </span>
                  </span>
                </label>
              )
            )}
          </div>

          <div className="text-white/40 text-[10px] tracking-[0.2em] uppercase mt-5">
            Or instruct the AI
          </div>

          <textarea
            data-testid="manual-fix-instruction"
            value={instruction}
            onChange={(event) => {
              setInstruction(
                event.target.value
              );
              setChoice("");
            }}
            placeholder="e.g. reload the last good batch"
            className="w-full mt-2 bg-[#0A0A0A] border border-white/10 text-white font-mono text-xs p-3 focus:outline-none focus:border-[#00E5FF]/50"
            rows={3}
          />

          <button
            data-testid="apply-manual-fix-button"
            disabled={
              busy ||
              (!choice &&
                !instruction.trim())
            }
            onClick={handleManual}
            className="mt-3 inline-flex items-center gap-2 px-5 py-2.5 font-mono text-xs tracking-[0.15em] uppercase bg-[#00E5FF] text-black hover:bg-white transition-colors disabled:opacity-40"
          >
            <Wrench size={15} />
            Apply Fix
          </button>
        </div>
      ) : (
        <div className="mt-6 flex gap-3">
          <button
            data-testid="approve-fix-button"
            disabled={busy}
            onClick={handleApprove}
            className="inline-flex items-center gap-2 px-5 py-2.5 font-mono text-xs tracking-[0.15em] uppercase bg-[#00FF66] text-black hover:bg-white transition-colors disabled:opacity-40"
          >
            <Check size={15} />
            Approve Fix
          </button>

          <button
            data-testid="reject-fix-button"
            disabled={busy}
            onClick={handleReject}
            className="inline-flex items-center gap-2 px-5 py-2.5 font-mono text-xs tracking-[0.15em] uppercase border border-[#FF0055] text-[#FF0055] hover:bg-[#FF0055]/10 transition-colors disabled:opacity-40"
          >
            <X size={15} />
            Reject
          </button>
        </div>
      )}
    </motion.div>
  );
}
