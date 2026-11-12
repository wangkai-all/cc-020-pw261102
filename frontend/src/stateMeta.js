// 节点/运行状态的统一配色与中文标签
export const STATE_META = {
  planned: { label: '已计划', color: '#64748b', bg: '#f1f5f9' },
  ready: { label: '就绪', color: '#0369a1', bg: '#e0f2fe' },
  running: { label: '运行中', color: '#a16207', bg: '#fef9c3' },
  succeeded: { label: '成功', color: '#15803d', bg: '#dcfce7' },
  reused: { label: '复用', color: '#6d28d9', bg: '#ede9fe' },
  failed: { label: '失败', color: '#b91c1c', bg: '#fee2e2' },
  blocked: { label: '阻塞', color: '#9a3412', bg: '#ffedd5' },
  skipped: { label: '跳过', color: '#475569', bg: '#f1f5f9' },
}

export const RUN_STATUS_META = {
  planned: { label: '待确认', color: '#0369a1', bg: '#e0f2fe' },
  running: { label: '运行中', color: '#a16207', bg: '#fef9c3' },
  succeeded: { label: '成功', color: '#15803d', bg: '#dcfce7' },
  partial: { label: '部分完成', color: '#c2410c', bg: '#ffedd5' },
  failed: { label: '失败', color: '#b91c1c', bg: '#fee2e2' },
}

export function stateMeta(state) {
  return STATE_META[state] || { label: state, color: '#334155', bg: '#f1f5f9' }
}

export function runStatusMeta(status) {
  return RUN_STATUS_META[status] || { label: status, color: '#334155', bg: '#f1f5f9' }
}
