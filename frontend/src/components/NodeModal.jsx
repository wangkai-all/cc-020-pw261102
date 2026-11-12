// 节点详情 / 分区数据预览弹窗
import React, { useEffect, useState } from 'react'
import { api } from '../api.js'
import { stateMeta } from '../stateMeta.js'

export default function NodeModal({ node, onClose }) {
  const [preview, setPreview] = useState(null)
  const [err, setErr] = useState('')

  useEffect(() => {
    let alive = true
    setPreview(null); setErr('')
    if (!node) return
    api.preview(node.task_name, node.dt)
      .then((d) => alive && setPreview(d))
      .catch((e) => alive && setErr(e.message))
    return () => { alive = false }
  }, [node])

  if (!node) return null
  const m = stateMeta(node.state)

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <h3>{node.task_name} @ {node.dt}</h3>
          <button onClick={onClose}>关闭</button>
        </div>
        <p>
          <span className="tag" style={{ color: m.color, background: m.bg }}>{m.label}</span>
          <span className="muted small"> 尝试次数 {node.attempts}</span>
        </p>
        {node.reason && <p className="reason">{node.reason}</p>}
        {node.input_signature && (
          <p className="muted tiny">
            输入签名 {node.input_signature.slice(0, 16)}… · SQL 版本 {node.sql_version?.slice(0, 12)}
          </p>
        )}
        <h4>分区数据（正式表）</h4>
        {err && <p className="error">{err}</p>}
        {!preview && <p className="muted">加载中…</p>}
        {preview && !preview.available && (
          <p className="muted">该分区当前无可用数据（不会以空表代替）。</p>
        )}
        {preview && preview.available && (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>{preview.columns.map((c) => <th key={c}>{c}</th>)}</tr>
              </thead>
              <tbody>
                {preview.rows.map((r, i) => (
                  <tr key={i}>{r.map((v, j) => <td key={j}>{String(v)}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
