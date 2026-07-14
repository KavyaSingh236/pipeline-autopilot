import { motion } from "framer-motion";

const LAYER_COLOR = { source: "#8A8A8A", bronze: "#CD7F32", silver: "#C0C0C0", gold: "#FFD700" };
const HEALTH_COLOR = { healthy: "#00FF66", warning: "#FFCC00", critical: "#FF0055" };

export default function LineageFlow({ lineage }) {
  if (!lineage) return null;
  const { nodes, edges } = lineage;
  const W = 1000;
  const H = 300;
  const nodeW = 160;
  const nodeH = 54;
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
  const center = (n) => ({ x: n.x + nodeW / 2, y: n.y + nodeH / 2 });

  return (
    <div className="border border-white/10 bg-[#0A0A0A] p-4 overflow-x-auto" data-testid="lineage-flow">
      <div className="relative" style={{ width: W, height: H, minWidth: W }}>
        <svg className="absolute inset-0" width={W} height={H}>
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" fill="rgba(0,229,255,0.6)" />
            </marker>
          </defs>
          {edges.map((e, i) => {
            const a = center(byId[e.source]);
            const b = center(byId[e.target]);
            const midX = (a.x + b.x) / 2;
            return (
              <motion.path
                key={i}
                initial={{ pathLength: 0, opacity: 0 }}
                animate={{ pathLength: 1, opacity: 1 }}
                transition={{ delay: 0.2 + i * 0.08, duration: 0.6 }}
                d={`M ${a.x + nodeW / 2} ${a.y} C ${midX} ${a.y}, ${midX} ${b.y}, ${b.x - nodeW / 2} ${b.y}`}
                stroke="rgba(0,229,255,0.5)"
                strokeWidth="1.5"
                fill="none"
                markerEnd="url(#arrow)"
              />
            );
          })}
        </svg>
        {nodes.map((n, i) => {
          const lc = LAYER_COLOR[n.layer] || "#888";
          const hc = HEALTH_COLOR[n.health] || "#00FF66";
          return (
            <motion.div
              key={n.id}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: i * 0.05 }}
              className="absolute bg-[#111111] border px-3 py-2"
              style={{ left: n.x, top: n.y, width: nodeW, height: nodeH, borderColor: `${lc}66` }}
              data-testid={`lineage-node-${n.id}`}
            >
              <div className="flex items-center justify-between">
                <span className="text-[9px] tracking-[0.2em] uppercase" style={{ color: lc }}>
                  {n.layer}
                </span>
                {n.layer !== "source" && (
                  <span className="w-1.5 h-1.5 rounded-full" style={{ background: hc, boxShadow: `0 0 8px ${hc}` }} />
                )}
              </div>
              <div className="font-mono text-[11px] text-white truncate mt-0.5">{n.label}</div>
              {n.layer !== "source" && (
                <div className="font-mono text-[9px] text-white/40">{Number(n.rows).toLocaleString()} rows</div>
              )}
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
