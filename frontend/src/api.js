// 后端 API 封装
const BASE = ''

async function request(path, options = {}) {
  const resp = await fetch(BASE + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!resp.ok) {
    let detail = resp.statusText
    try {
      const body = await resp.json()
      detail = body.detail || JSON.stringify(body)
    } catch {
      /* ignore */
    }
    throw new Error(detail)
  }
  return resp.json()
}

export const api = {
  health: () => request('/api/health'),
  config: () => request('/api/config'),
  plan: (dates, kind) =>
    request('/api/runs/plan', {
      method: 'POST',
      body: JSON.stringify({ dates, kind }),
    }),
  confirm: (runId) =>
    request(`/api/runs/${runId}/confirm`, { method: 'POST' }),
  runs: () => request('/api/runs'),
  run: (runId) => request(`/api/runs/${runId}`),
  partitions: () => request('/api/partitions'),
  preview: (task, dt) => request(`/api/preview/${encodeURIComponent(task)}/${dt}`),
  faults: () => request('/api/faults'),
  setFault: (task_name, enabled, kind = 'fail') =>
    request('/api/faults', {
      method: 'POST',
      body: JSON.stringify({ task_name, enabled, kind }),
    }),
  scanSources: () => request('/api/sources/scan'),
}
