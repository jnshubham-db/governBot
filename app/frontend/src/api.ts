const API_BASE = '';

export type AppConfig = { catalog: string; schema: string; warehouse_http_path: string };

function headers(): Record<string, string> {
  const catalog = localStorage.getItem('governbot_catalog') || '';
  const schema = localStorage.getItem('governbot_schema') || '';
  const path = localStorage.getItem('governbot_warehouse_http_path') || '';
  const h: Record<string, string> = { 'Content-Type': 'application/json' };
  if (catalog) h['X-Catalog'] = catalog;
  if (schema) h['X-Schema'] = schema;
  if (path) h['X-Warehouse-HTTP-Path'] = path;
  return h;
}

export async function getConfig(): Promise<AppConfig> {
  const r = await fetch(`${API_BASE}/api/config`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export type SummaryResponse = {
  total: number;
  pending: number;
  completed: number;
  by_type: { name: string; count: number }[];
  by_object_type: { name: string; count: number }[];
  latest: Record<string, unknown>[];
};

export type SummaryTrendResponse = {
  trend: { period: string; generated: number; failed: number; completed: number }[];
};

export async function getSummary(hours: number): Promise<SummaryResponse> {
  const r = await fetch(`${API_BASE}/api/summary?hours=${hours}`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getSummaryTrend(hours: number): Promise<SummaryTrendResponse> {
  const r = await fetch(`${API_BASE}/api/summary/trend?hours=${hours}`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export type ActionRow = Record<string, unknown>;

export async function getActions(filters?: {
  workspace?: string;
  violation_type?: string;
  remediation_action?: string;
}): Promise<{ rows: ActionRow[] }> {
  const params = new URLSearchParams();
  if (filters?.workspace && filters.workspace !== '(all)') params.set('workspace', filters.workspace);
  if (filters?.violation_type && filters.violation_type !== '(all)') params.set('violation_type', filters.violation_type);
  if (filters?.remediation_action && filters.remediation_action !== '(all)') params.set('remediation_action', filters.remediation_action);
  const r = await fetch(`${API_BASE}/api/actions?${params}`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function approveViolation(violationId: string, note?: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/actions/approve`, {
    method: 'POST',
    headers: { ...headers(), 'X-Violation-Id': violationId },
    body: JSON.stringify({ note: note || '' }),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function rejectViolation(violationId: string, reason: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/actions/reject`, {
    method: 'POST',
    headers: { ...headers(), 'X-Violation-Id': violationId },
    body: JSON.stringify({ reason }),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function addNoteViolation(violationId: string, note: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/actions/note`, {
    method: 'POST',
    headers: { ...headers(), 'X-Violation-Id': violationId },
    body: JSON.stringify({ note }),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function getWorkspaces(): Promise<{ rows: Record<string, unknown>[] }> {
  const r = await fetch(`${API_BASE}/api/configs/workspaces`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function createWorkspace(body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/workspaces`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function updateWorkspace(workspaceId: string, body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/workspaces/${encodeURIComponent(workspaceId)}`, {
    method: 'PUT',
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function deleteWorkspace(workspaceId: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/workspaces/${encodeURIComponent(workspaceId)}`, {
    method: 'DELETE',
    headers: headers(),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function getIdentities(): Promise<{ rows: Record<string, unknown>[] }> {
  const r = await fetch(`${API_BASE}/api/configs/identities`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function createIdentity(body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/identities`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function updateIdentity(identityName: string, identityType: string, body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/identities/${encodeURIComponent(identityName)}`, {
    method: 'PUT',
    headers: { ...headers(), 'X-Identity-Type': identityType },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function deleteIdentity(identityName: string, identityType: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/identities/${encodeURIComponent(identityName)}`, {
    method: 'DELETE',
    headers: { ...headers(), 'X-Identity-Type': identityType },
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function getFilters(): Promise<{ rows: Record<string, unknown>[] }> {
  const r = await fetch(`${API_BASE}/api/configs/filters`, { headers: headers() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function createFilter(body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/filters`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function updateFilter(filterId: string, body: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/filters/${encodeURIComponent(filterId)}`, {
    method: 'PUT',
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

export async function deleteFilter(filterId: string): Promise<void> {
  const r = await fetch(`${API_BASE}/api/configs/filters/${encodeURIComponent(filterId)}`, {
    method: 'DELETE',
    headers: headers(),
  });
  if (!r.ok) throw new Error(await r.text());
}
