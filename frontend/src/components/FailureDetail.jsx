import { CheckCircle2, AlertTriangle } from "lucide-react";

export default function FailureDetail({ failure }) {
  const recovered = failure.status === "auto_fixed";
  return (
    <section className="border border-[#FFCC00]/30 bg-[#0A0A0A] p-6" data-testid="autonomous-incident">
      <div className="flex items-center gap-3 text-[#FFCC00] mb-4">
        {recovered ? <CheckCircle2 size={19}/> : <AlertTriangle size={19}/>}
        <h2 className="font-display uppercase tracking-wider">Autonomous Recovery Activity</h2>
      </div>
      <p className="text-sm text-white/90">{(failure.error_type || "incident").replaceAll("_", " ")}</p>
      <p className="text-xs text-white/50 mt-2">Status: {(failure.status || "unknown").replaceAll("_", " ")}</p>
      <p className="text-xs text-white/60 mt-3">{failure.outcome || failure.explanation || failure.description}</p>
      <p className="text-xs text-white/50 mt-3">Action: {failure.action || "Not executed"}</p>
      <p className="text-xs text-white/40 mt-3">No human approval is required. Recovery outcomes are verified and recorded.</p>
    </section>
  );
}
