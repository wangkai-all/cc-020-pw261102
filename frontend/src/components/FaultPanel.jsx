// 故障注入面板：强制某任务失败（fail）或增加执行延迟（delay，用于演示崩溃恢复）
import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

export default function FaultPanel({ tasks }) {
  const [faults, setFaults] = useState({})
  const [msg, setMsg] = useState('')

  const load = async () => setFaults(await api.faults())
  useEffect(() => { load() }, [])

  const toggle = async (task, kind, enabled) => {
    await api.setFault(task, enabled, kind)
    setMsg(`${task} ${kind} 开关已${enabled ? '开启' : '关闭'}`)
    await load()
  }

  return (
    <div className="card">
      <h3>故障注入（演练失败 / 阻塞 / 重试）</h3>
      <table className="fault-table">
        <thead>
          <tr><th>任务</th><th>强制失败 fail</th><th>执行延迟 delay</th></tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.name}>
              <td>{t.name}</td>
              <td>
                <input
                  type="checkbox"
                  checked={!!faults[`fail:${t.name}`]}
                  onChange={(e) => toggle(t.name, 'fail', e.target.checked)}
                />
              </td>
              <td>
                <input
                  type="checkbox"
                  checked={!!faults[`delay:${t.name}`]}
                  onChange={(e) => toggle(t.name, 'delay', e.target.checked)}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {msg && <p className="muted small">{msg}</p>}
    </div>
  )
}
