import { useEffect, useRef, useState, useCallback } from "react";
import { wsUrl } from "./api";

/** Subscribes to the /api/ws/pipelines stream. Returns last event + connection state. */
export function useLiveStatus(onEvent) {
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState(null);
  const wsRef = useRef(null);
  const cbRef = useRef(onEvent);
  cbRef.current = onEvent;

  const connect = useCallback(() => {
    let ws;
    try {
      ws = new WebSocket(wsUrl());
    } catch (e) {
      return;
    }
    wsRef.current = ws;
    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      setTimeout(() => {
        if (wsRef.current === ws) connect();
      }, 3000);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (msg) => {
      try {
        const data = JSON.parse(msg.data);
        setLastEvent(data);
        if (cbRef.current) cbRef.current(data);
      } catch (_) {}
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (wsRef.current) {
        const ws = wsRef.current;
        wsRef.current = null;
        ws.close();
      }
    };
  }, [connect]);

  return { connected, lastEvent };
}
