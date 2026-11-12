// 任务依赖图：纵向分层（源 -> 任务），边标注日期偏移，环会以红色高亮（后端已拦截，前端兜底渲染）
import React from 'react'

const LAYER_COLORS = ['#0ea5e9', '#22c55e', '#f59e0b', '#8b5cf6', '#ec4899']

export default function TaskGraph({ config }) {
  if (!config || !config.valid) {
    return (
      <div className="card">
        <h3>任务图</h3>
        <p className="error">配置非法：{config?.error || '未知错误'}</p>
      </div>
    )
  }

  // 拓扑分层
  const depth = {}
  const tasks = config.tasks
  const visit = (name, stack = []) => {
    if (depth[name] !== undefined) return depth[name]
    if (stack.includes(name)) return -1 // 环
    const deps = tasks.find((t) => t.name === name)?.inputs
      .filter((i) => i.kind === 'task').map((i) => i.name) || []
    if (deps.length === 0) {
      depth[name] = 1
      return 1
    }
    const d = 1 + Math.max(...deps.map((p) => visit(p, [...stack, name])))
    depth[name] = d
    return d
  }
  tasks.forEach((t) => visit(t.name))

  const layers = {}
  tasks.forEach((t) => {
    const d = depth[t.name]
    ;(layers[d] = layers[d] || []).push(t)
  })
  const sourceNames = config.sources.map((s) => s.name)

  // 生成边（源/任务 -> 消费任务）
  const edges = []
  tasks.forEach((t) => {
    t.inputs.forEach((inp) => {
      edges.push({
        from: inp.name,
        to: t.name,
        offset: inp.offset,
        kind: inp.kind,
      })
    })
  })

  const nodeColor = (name) => {
    if (sourceNames.includes(name)) return LAYER_COLORS[0]
    return LAYER_COLORS[(depth[name] ?? 1) % LAYER_COLORS.length]
  }

  return (
    <div className="card">
      <h3>任务图（边标注上游日期偏移，单位：天）</h3>
      <div className="graph">
        <div className="graph-layer">
          <div className="layer-title">数据源</div>
          <div className="layer-nodes">
            {config.sources.map((s) => (
              <div key={s.name} className="graph-node source" style={{ borderColor: nodeColor(s.name) }}>
                <strong>{s.name}</strong>
                <span className="muted">{s.table}</span>
              </div>
            ))}
          </div>
        </div>
        {Object.keys(layers).sort((a, b) => a - b).map((d) => (
          <div className="graph-layer" key={d}>
            <div className="layer-title">第 {d} 层</div>
            <div className="layer-nodes">
              {layers[d].map((t) => (
                <div key={t.name} className="graph-node" style={{ borderColor: nodeColor(t.name) }}>
                  <strong>{t.name}</strong>
                  <span className="muted">{t.sql_file}</span>
                  <span className="muted">sql@{t.sql_version.slice(0, 8)}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <div className="edge-list">
        {edges.map((e, i) => (
          <span key={i} className="edge-chip">
            {e.from}
            <em> offset {e.offset >= 0 ? `+${e.offset}` : e.offset} </em>
            → {e.to}
          </span>
        ))}
      </div>
    </div>
  )
}
