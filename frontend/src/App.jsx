import React, { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api.js'
import TaskGraph from './components/TaskGraph.jsx'
import PlanBuilder from './components/PlanBuilder.jsx'
import PlanGrid from './components/PlanGrid.jsx'
import RunHistory from './components/RunHistory.jsx'
import PartitionList from './components/PartitionList.jsx'
import FaultPanel from './components/FaultPanel.jsx'
import NodeModal from './components/NodeModal.jsx'

export default function App() {
  const [health, setHealth] = useState(null)
  const [config, setConfig] = useState(null)
  const [runs, setRuns] = useState([])
  const [activeRunId, setActiveRunId] = useState(null)
  const [runDetail, setRunDetail] = useState(null)
  const [partitions, setPartitions] = useState({ published: [], sources: [] })
  const [modalNode, setModalNode] = useState(null)
  const [toast, setToast] = useState('')
  const timerRef = useRef(null)

  const notify = (msg) => {
    setToast(msg)
    setTimeout(() => setToast(''), 3500)
  }

  const refreshRuns = useCallback(async () => {
    const list = await api.runs()
    setRuns(list)
    return list
  }, [])

  const refreshDetail = useCallback(async (id) => {
    const detail = await api.run(id)
    setRunDetail(detail)
    return detail
  }, [])

  const refreshPartitions = useCallback(async () => {
    setPartitions(await api.partitions())
  }, [])

  // 初始化
  useEffect(() => {
    (async () => {
      setHealth(await api.health())
      setConfig(await api.config())
      await refreshPartitions()
      const list = await refreshRuns()
      if (list.length) {
        setActiveRunId(list[0].id)
        await refreshDetail(list[0].id)
      }
    })()
  }, [refreshRuns, refreshDetail, refreshPartitions])

  // 运行中的自动轮询
  useEffect(() => {
    if (!runDetail || runDetail.run.status !== 'running') {
      clearInterval(timerRef.current)
      return
    }
    timerRef.current = setInterval(async () => {
      const d = await refreshDetail(runDetail.run.id)
      if (d.run.status !== 'running') {
        clearInterval(timerRef.current)
        await refreshRuns()
        await refreshPartitions()
      }
    }, 800)
    return () => clearInterval(timerRef.current)
  }, [runDetail, refreshDetail, refreshRuns, refreshPartitions])

  const handlePlan = async (dates, kind) => {
    const result = await api.plan(dates, kind)
    await refreshRuns()
    setActiveRunId(result.run.id)
    await refreshDetail(result.run.id)
    notify(`计划 #${result.run.id} 已生成，共 ${result.plan.length} 个节点，请确认后运行`)
  }

  const handleConfirm = async (runId) => {
    await api.confirm(runId)
    notify(`运行 #${runId} 已启动`)
    await refreshRuns()
    await refreshDetail(runId)
  }

  const handleSelectRun = async (id) => {
    setActiveRunId(id)
    await refreshDetail(id)
  }

  const handleCellClick = (node) => setModalNode(node)
  const handlePreview = async (task, dt) => {
    // 用预览构造一个最小 node 打开弹窗
    setModalNode({ task_name: task, dt, state: 'succeeded', reason: '', attempts: 0 })
  }

  const scanSources = async () => {
    await api.scanSources()
    await refreshPartitions()
    notify('源分区已重新扫描并登记签名')
  }

  const configInvalid = health && !health.config_valid

  return (
    <div className="app">
      <header className="topbar">
        <h1>点式补数 SQL 加工编排</h1>
        <div className="topbar-right">
          <button onClick={scanSources}>扫描源分区签名</button>
          <span className={`health ${health?.config_valid ? 'ok' : 'bad'}`}>
            {health ? (health.config_valid ? '配置正常' : '配置异常') : '连接中…'}
          </span>
        </div>
      </header>

      {configInvalid && (
        <div className="banner error-banner">
          配置非法：{health.config_error}。请修正配置文件后重启服务。
        </div>
      )}

      <div className="layout">
        <div className="col side">
          <PlanBuilder onPlan={handlePlan} busy={false} />
          {config?.valid && <FaultPanel tasks={config.tasks} />}
          <RunHistory runs={runs} activeId={activeRunId} onSelect={handleSelectRun} />
        </div>
        <div className="col main">
          <TaskGraph config={config} />
          <PlanGrid
            run={runDetail?.run}
            nodes={runDetail?.nodes || []}
            stats={runDetail?.stats}
            onConfirm={handleConfirm}
            onRefresh={() => activeRunId && refreshDetail(activeRunId)}
            onCellClick={handleCellClick}
          />
          <PartitionList data={partitions} onPreview={handlePreview} />
        </div>
      </div>

      <NodeModal node={modalNode} onClose={() => setModalNode(null)} />
      {toast && <div className="toast">{toast}</div>}
    </div>
  )
}
