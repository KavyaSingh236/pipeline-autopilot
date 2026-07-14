import axios from "axios";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
export const API = `${BACKEND_URL}/api`;

export const wsUrl = () => {
  const base = BACKEND_URL.replace(/^http/, "ws");
  return `${base}/api/ws/pipelines`;
};

export const api = axios.create({ baseURL: API });

export const getPipelines = () => api.get("/pipelines").then((r) => r.data);
export const getPipeline = (id) => api.get(`/pipelines/${id}`).then((r) => r.data);
export const getFailures = (id) => api.get(`/pipelines/${id}/failures`).then((r) => r.data);
export const getLineage = (id) => api.get(`/pipelines/${id}/lineage`).then((r) => r.data);
export const approveFix = (id, body) => api.post(`/pipelines/${id}/approve`, body).then((r) => r.data);
export const rejectFix = (id, body) => api.post(`/pipelines/${id}/reject`, body).then((r) => r.data);
export const getAudit = (params) => api.get("/audit", { params }).then((r) => r.data);
