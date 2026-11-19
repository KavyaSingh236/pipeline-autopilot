import "@/App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Toaster } from "sonner";
import Layout from "@/components/Layout";
import ControlTower from "@/pages/ControlTower";
import Reports from "@/pages/Reports";
import DataFlowExplorer from "@/pages/DataFlowExplorer";
import AuditLogPage from "@/pages/AuditLogPage";
import { useLiveStatus } from "@/lib/useLiveStatus";

function App() {
  const { connected, lastEvent } = useLiveStatus();

  return (
    <div className="App">
      <BrowserRouter>
        <Layout connected={connected}>
          <Routes>
            <Route path="/" element={<ControlTower lastEvent={lastEvent} />} />
            <Route path="/reports" element={<Reports />} />
            <Route path="/data-flow" element={<DataFlowExplorer lastEvent={lastEvent} />} />
            <Route path="/audit" element={<AuditLogPage lastEvent={lastEvent} />} />
          </Routes>
        </Layout>
      </BrowserRouter>
      <Toaster
        theme="light"
        position="bottom-right"
        toastOptions={{
          style: {
            background: "#fff",
            border: "1px solid rgba(255,255,255,0.1)",
            borderRadius: 0,
            fontFamily: "'IBM Plex Mono', monospace",
            color: "#20204c",
          },
        }}
      />
    </div>
  );
}

export default App;
