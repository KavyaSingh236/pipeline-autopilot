import "@/App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Toaster } from "sonner";
import Layout from "@/components/Layout";
import ControlTower from "@/pages/ControlTower";
import PipelineDetail from "@/pages/PipelineDetail";
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
            <Route path="/pipeline/:id" element={<PipelineDetail lastEvent={lastEvent} />} />
            <Route path="/audit" element={<AuditLogPage lastEvent={lastEvent} />} />
          </Routes>
        </Layout>
      </BrowserRouter>
      <Toaster
        theme="dark"
        position="bottom-right"
        toastOptions={{
          style: {
            background: "#0A0A0A",
            border: "1px solid rgba(255,255,255,0.1)",
            borderRadius: 0,
            fontFamily: "'IBM Plex Mono', monospace",
            color: "#fff",
          },
        }}
      />
    </div>
  );
}

export default App;
