// 运行记录列表
import React from 'react'
import { runStatusMeta } from '../stateMeta.js'

export default function RunHistory({ runs, activeId, onSelect }) {
  return (
    <div className="card">
      <h3>运行记录</h3>
      {runs.length === 0 && <p className="muted">暂无运行。</p>}
      <div className="run-list">
        {runs.map((r) => {
          const m = runStatusMeta(r.status)
          return (
            <button
              key={r.id}
              className={`run-item ${r.id === activeId ? 'active' : ''}`}
              onClick={() => onSelect(r.id)}
            >
              <span className="run-id">#{r.id}</span>
              <span className="tag" style={{ color: m.color, background: m.bg }}>{m.label}</span>
              <span className="muted small">{r.kind}</span>
              <span className="muted small">{r.requested_dates.join(',')}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
