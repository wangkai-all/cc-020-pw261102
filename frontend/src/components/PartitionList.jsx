// 已发布分区与源分区登记：每个任务分区的签名、发布运行与时间
import React from 'react'

export default function PartitionList({ data, onPreview }) {
  const { published = [], sources = [] } = data || {}
  return (
    <div className="card">
      <h3>日期分区</h3>
      <h4>已发布结果分区（失败时保留上一版）</h4>
      {published.length === 0 && <p className="muted">暂无已发布分区。</p>}
      <div className="chip-wrap">
        {published.map((p) => (
          <button
            key={`${p.task_name}-${p.dt}`}
            className="partition-chip"
            onClick={() => onPreview(p.task_name, p.dt)}
            title={`run #${p.run_id} · 输入 ${p.input_signature.slice(0, 10)} · 输出 ${p.output_signature.slice(0, 10)}`}
          >
            <strong>{p.task_name}</strong>
            <span className="muted small">{p.dt}</span>
            <span className="muted tiny">via run #{p.run_id}</span>
          </button>
        ))}
      </div>
      <h4>源分区登记（签名变化即触发下游重算）</h4>
      <div className="chip-wrap">
        {sources.map((s) => (
          <button
            key={`${s.source_name}-${s.dt}`}
            className="partition-chip source"
            onClick={() => onPreview(s.source_name, s.dt)}
            title={`签名 ${s.signature.slice(0, 12)}`}
          >
            <strong>{s.source_name}</strong>
            <span className="muted small">{s.dt}</span>
            <span className="muted tiny">sig {s.signature.slice(0, 8)}</span>
          </button>
        ))}
      </div>
    </div>
  )
}
