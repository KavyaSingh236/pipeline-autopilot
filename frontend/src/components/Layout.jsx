import { Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, FileBarChart2, ScrollText, Activity, DatabaseZap } from 'lucide-react';

export default function Layout({ children, connected }) {
  const { pathname } = useLocation();
  const nav = [{to:'/',label:'Dashboard',Icon:LayoutDashboard},{to:'/reports',label:'Reports',Icon:FileBarChart2},{to:'/audit',label:'Audit Log',Icon:ScrollText}];
  return <div className="pa-shell">
    <aside className="pa-sidebar">
      <Link to="/" className="pa-brand"><div className="pa-brand-icon"><Activity size={27}/></div><div><strong>Pipeline<br/>Autopilot <span>✦</span></strong><small>Autonomous ETL<br/>Monitoring & Recovery</small></div></Link>
      <nav className="pa-nav">{nav.map(({to,label,Icon})=><Link key={to} to={to} className={(pathname===to?'active ':'')+'pa-nav-item'}><Icon size={20}/>{label}</Link>)}</nav>
      <div className="pa-side-art"><DatabaseZap size={76} strokeWidth={1.1}/><div>Smarter Pipelines.<br/>Healthier Data. ♡</div></div>
    </aside>
    <div className="pa-main"><header className="pa-topbar"><div className="pa-top-title">Pipeline Autopilot <span>✦</span><small>Autonomous ETL Monitoring & Recovery</small></div><div className="pa-top-right"><span className={'pa-live '+(connected?'online':'offline')}><span className="pa-dot"/>{connected?'Live':'Connecting'}</span><span className="pa-updated">{new Date().toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric'})}</span></div></header><main className="pa-content">{children}</main></div>
  </div>;
}
