// 执行计划/运行详情：任务 × 日期 的状态网格，点击节点查看原因与分区数据
import React from 'react'
import { stateMeta, runStatusMeta } from '../stateMeta.js'

function StateCell({ node, onClick }) {
  const meta = stateMeta(node.state)
  return (
    <td
      className="state-cell"
      style={{ color: meta.color, background: meta.bg, cursor: 'pointer' }}
      title={node.reason || meta.label}
      onClick={() => onClick(node)}
    >
      {meta.label}
    </td>
  )
}

export default function PlanGrid({ run, nodes, stats, onConfirm, onRefresh, onCellClick }) {
  if (!run) {
    return (
      <div className="card">
        <h3>执行计划</h3>
        <p className="muted">在左侧选择日期生成计划，确认前不会执行任何任务。</p>
      </div>
    )
  }

  const taskNames = [...new Set(nodes.map((n) => n.task_name))].sort()
  const dates = [...new Set(nodes.map((n) => n.dt))].sort()
  const byKey = Object.fromEntries(nodes.map((n) => [`${n.task_name}|${n.dt}`, n]))
  const status = runStatusMeta(run.status)
  const counts = nodes.reduce((acc, n) => {
    acc[n.state] = (acc[n.state] || 0) + 1
    return acc
  }, {})

  return (
    <div className="card">
      <div className="card-head">
        <h3>
          运行 #{run.id} <span className="tag" style={{ color: status.color, background: status.bg }}>
            {status.label}
          </span>
          <span className="muted small">
            {' '}{run.kind} · 请求日期 {run.requested_dates.join(', ')} · 前向边界 {run.horizon_end}
          </span>
        </h3>
        <div className="head-actions">
          {run.status === 'running' && (
            <button onClick={onRefresh}>刷新</button>
          )}
          {(run.status === 'planned' || run.status === 'failed' || run.status === 'partial') && (
            <button className="primary" onClick={() => onConfirm(run.id)}>
              {run.status === 'planned' ? '② 确认并运行' : '重试未完成节点'}
            </button>
          )}
        </div>
      </div>

      {run.error && <p className="error">{run.error}</p>}

      <div className="legend">
        {Object.entries(counts).map(([s, c]) => {
          const m = stateMeta(s)
          return (
            <span key={s} className="legend-item" style={{ color: m.color, background: m.bg }}>
              {m.label} × {c}
            </span>
          )
        })}
        {stats && stats.max_concurrent > 0 && (
          <span className="legend-item accent">峰值并发 {stats.max_concurrent}</span>
        )}
      </div>

      <div className="table-wrap">
        <table className="grid">
          <thead>
            <tr>
              <th>任务 ＼ 日期</th>
              {dates.map((d) => <th key={d}>{d}</th>)}
            </tr>
          </thead>
          <tbody>
            {taskNames.map((t) => (
              <tr key={t}>
                <th className="row-head">{t}</th>
                {dates.map((d) => {
                  const node = byKey[`${t}|${d}`]
                  return node
                    ? <StateCell key={d} node={node} onClick={onCellClick} />
                    : <td key={d} className="muted dim">—</td>
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="muted small">点击彩色单元格查看阻塞/失败原因或分区数据。</p>
    </div>
  )
}
