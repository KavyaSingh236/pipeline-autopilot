export const STATUS = {
  healthy: { label: "HEALTHY", color: "#00FF66", dot: "#00FF66" },
  warning: { label: "NEEDS APPROVAL", color: "#FFCC00", dot: "#FFCC00" },
  critical: { label: "CRITICAL", color: "#FF0055", dot: "#FF0055" },
  running: { label: "RUNNING", color: "#00E5FF", dot: "#00E5FF" },
};

export const statusOf = (s) => STATUS[s] || STATUS.healthy;

export function StatusDot({ status, size = 10 }) {
  const c = statusOf(status).dot;
  return (
    <span className="relative inline-flex" style={{ width: size, height: size }}>
      <span
        className="pulse-dot absolute inset-0 rounded-full"
        style={{ background: c, boxShadow: `0 0 12px ${c}` }}
      />
    </span>
  );
}

export function StatusBadge({ status, testId }) {
  const s = statusOf(status);
  return (
    <span
      data-testid={testId}
      className="font-mono text-[10px] tracking-[0.2em] uppercase px-2.5 py-1 border"
      style={{ color: s.color, borderColor: s.color, background: `${s.color}1a` }}
    >
      {s.label}
    </span>
  );
}

export const fmtTime = (t) => {
  if (!t) return "—";
  const d = new Date(t);
  return d.toLocaleString("en-GB", {
    day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
};

export const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));
