import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://localhost:8000",
  headers: { "Content-Type": "application/json" },
});

export default api;

// --- Types ---

export type InspectionStatus = "prepared" | "ongoing" | "completed";
export type NCLevel = "conforme" | "observation" | "nc_mineure" | "nc_majeure";

export interface Constat {
  id: string;
  inspection_id: string;
  checklist_point_id: string | null;
  raw_text: string;
  reformulated_text: string;
  classification: NCLevel;
  norm_reference: string | null;
  norm_excerpt: string | null;
  suggested_evidence: string | null;
  suggested_action: string | null;
  rag_chunks: RagChunk[] | null;
  photo_path: string | null;
  created_at: string;
}

export interface RagChunk {
  section: string;
  excerpt: string;
  score: number;
}

export interface Inspection {
  id: string;
  client_name: string;
  site_name: string;
  auditor_name: string;
  referential: string;
  scope: string;
  status: InspectionStatus;
  started_at: string | null;
  created_at: string;
  checklist_json: Checklist | null;
  constats: Constat[];
}

export interface Checklist {
  referential: string;
  scope_summary: string;
  sections: ChecklistSection[];
}

export interface ChecklistSection {
  id: string;
  title: string;
  points: ChecklistPoint[];
}

export interface ChecklistPoint {
  id: string;
  question: string;
  expected_evidence: string;
  norm_reference: string;
}

export interface DashboardKpis {
  audits_today_count: number;
  audits_month_count: number;
  avg_delay_days: number;
  pending_recurrences_count: number;
}

export interface AuditToday {
  id: string;
  scheduled_at: string;
  client_name: string;
  location: string;
  scope: string;
  status: InspectionStatus;
  is_next: boolean;
}

// --- API functions ---

export const getInspection = (id: string) =>
  api.get<Inspection>(`/api/inspections/${id}`).then((r) => r.data);

export const listInspections = () =>
  api.get<Inspection[]>("/api/inspections").then((r) => r.data);

export const createInspection = (body: Partial<Inspection>) =>
  api.post<Inspection>("/api/inspections", body).then((r) => r.data);

export const updateInspection = (id: string, body: Partial<Inspection>) =>
  api.patch<Inspection>(`/api/inspections/${id}`, body).then((r) => r.data);

export const generateChecklist = (id: string, referential?: string) => {
  const params = referential ? { referential } : {};
  return api.post<{ checklist_json: Checklist; generation_duration_seconds: number }>(
    `/api/inspections/${id}/checklist`,
    {},
    { params }
  ).then((r) => r.data);
};

export const createConstat = (
  inspectionId: string,
  body: { raw_text: string; checklist_point_id?: string; photo_id?: string }
) => api.post<Constat>(`/api/inspections/${inspectionId}/constats`, body).then((r) => r.data);

export const uploadPhoto = (file: File) => {
  const form = new FormData();
  form.append("file", file);
  return api
    .post<{ id: string; path: string; kind: string }>("/api/uploads", form, {
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};

export const generateReport = (id: string) =>
  api
    .post<{ report_structure: object; generation_duration_seconds: number; docx_url: string }>(
      `/api/inspections/${id}/report`
    )
    .then((r) => r.data);

export const getDashboardKpis = () =>
  api.get<DashboardKpis>("/api/dashboard/kpis").then((r) => r.data);

export const getAuditsToday = () =>
  api.get<AuditToday[]>("/api/dashboard/audits_today").then((r) => r.data);

export const resetDemo = () =>
  api.post<{ reset_at: string; inspection_id: string }>("/api/dev/reset-demo").then((r) => r.data);

export const replayConstat = (index: number) =>
  api.post<{ constat: { id: string; classification: string } }>(`/api/dev/replay/${index}`).then((r) => r.data);
