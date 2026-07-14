import { Link, useLocation } from "react-router-dom";
import { Radar, ScrollText, Activity, BellOff, Bell } from "lucide-react";
import { useState, useEffect } from "react";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;

export default function Layout({ children, connected }) {
  const { pathname } = useLocation();
  const [alertsOn, setAlertsOn] = useState(false);
  const [toggling, setToggling] = useState(false);

  useEffect(() => {
    fetch(`${BACKEND_URL}/api/alerts/status`)
      .then(r => r.json())
      .then(d => setAlertsOn(d.enabled))
      .catch(() => {});
  }, []);

  const handleToggle = async () => {
    setToggling(true);
    try {
      const res = await fetch(`${BACKEND_URL}/api/alerts/toggle`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !alertsOn }),
      });
      const data = await res.json();
      setAlertsOn(data.enabled);
    } catch (e) {}
    setToggling(false);
  };

  const nav = [
    { to: "/", label: "Control Tower", icon: Radar },
    { to: "/audit", label: "Audit Log", icon: ScrollText },
  ];

  return (
    <div className="grain relative min-h-screen text-white">
      <header className="sticky top-0 z-50 backdrop-blur-2xl bg-black/60 border-b border-white/10">
        <div className="max-w-[1400px] mx-auto px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-3" data-testid="brand-home">
            <div className="border border-[#00E5FF]/40 p-1.5 text-[#00E5FF]">
              <Activity size={18} />
            </div>
            <div>
              <div className="font-display text-sm tracking-[0.15em] uppercase leading-none">
                Pipeline Autopilot
              </div>
              <div className="text-[9px] tracking-[0.3em] uppercase text-white/40 mt-0.5">
                Self-Healing ETL · Control Tower
              </div>
            </div>
          </Link>

          <div className="flex items-center gap-6">
            <nav className="flex items-center gap-1">
              {nav.map((n) => {
                const active = pathname === n.to;
                const Icon = n.icon;
                return (
                  <Link
                    key={n.to}
                    to={n.to}
                    data-testid={`nav-${n.label.toLowerCase().replace(/\s/g, "-")}`}
                    className={`inline-flex items-center gap-2 px-3 py-2 text-[10px] tracking-[0.2em] uppercase transition-colors ${
                      active ? "text-[#00E5FF] border border-[#00E5FF]/40" : "text-white/50 hover:text-white border border-transparent"
                    }`}
                  >
                    <Icon size={13} /> {n.label}
                  </Link>
                );
              })}
            </nav>

            {/* Email alert toggle */}
            <button
              onClick={handleToggle}
              disabled={toggling}
              title={alertsOn ? "Email alerts ON — click to turn off" : "Email alerts OFF — click to turn on"}
              className={`inline-flex items-center gap-2 px-3 py-2 text-[10px] tracking-[0.2em] uppercase border transition-colors ${
                alertsOn
                  ? "border-[#00FF66] text-[#00FF66] bg-[#00FF66]/10"
                  : "border-white/20 text-white/40 hover:border-white/40 hover:text-white/60"
              }`}
            >
              {alertsOn ? <Bell size={13} /> : <BellOff size={13} />}
              {alertsOn ? "Alerts On" : "Alerts Off"}
            </button>

            <div className="flex items-center gap-2" data-testid="ws-status">
              <span
                className={`w-2 h-2 rounded-full ${connected ? "pulse-dot" : ""}`}
                style={{ background: connected ? "#00FF66" : "#FF0055", boxShadow: `0 0 8px ${connected ? "#00FF66" : "#FF0055"}` }}
              />
              <span className="text-[9px] tracking-[0.2em] uppercase text-white/40">
                {connected ? "Live" : "Offline"}
              </span>
            </div>
          </div>
        </div>
      </header>
      <main className="relative z-10 max-w-[1400px] mx-auto px-6 py-10">{children}</main>
    </div>
  );
}