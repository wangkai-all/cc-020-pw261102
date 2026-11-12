// 计划构建表单：选择日期与运行类型，先预览执行计划，确认后才运行
import React, { useState } from 'react'

export default function PlanBuilder({ onPlan, busy }) {
  const [datesText, setDatesText] = useState('2026-10-27, 2026-10-28, 2026-10-29')
  const [kind, setKind] = useState('initial')
  const [error, setError] = useState('')

  const submit = async () => {
    setError('')
    const dates = datesText
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
    if (dates.length === 0) {
      setError('请至少填写一个日期')
      return
    }
    const bad = dates.filter((d) => !/^\d{4}-\d{2}-\d{2}$/.test(d))
    if (bad.length) {
      setError(`日期格式错误: ${bad.join(', ')}（需要 YYYY-MM-DD）`)
      return
    }
    try {
      await onPlan(dates, kind)
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div className="card">
      <h3>构建执行计划</h3>
      <div className="form-row">
        <label>日期（逗号分隔）</label>
        <input
          value={datesText}
          onChange={(e) => setDatesText(e.target.value)}
          placeholder="2026-10-30"
        />
      </div>
      <div className="form-row">
        <label>运行类型</label>
        <select value={kind} onChange={(e) => setKind(e.target.value)}>
          <option value="initial">初始加工 initial</option>
          <option value="backfill">点式补算 backfill</option>
        </select>
      </div>
      {error && <p className="error">{error}</p>}
      <button className="primary" onClick={submit} disabled={busy}>
        {busy ? '生成中…' : '① 预览执行计划'}
      </button>
    </div>
  )
}
