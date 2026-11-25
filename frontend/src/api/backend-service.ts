import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type RecoveryManifestResponse = {
  ok: boolean
  total?: number
  records?: Record<string, unknown>[]
  events?: Record<string, unknown>[]
}

export async function getRecoveryManifest(): Promise<RecoveryManifestResponse> {
  const response = await request('/api/backfill_job/recovery/manifest')
  if (!response.ok) throw new Error('恢复清单读取失败')
  return await response.json()
}

export type RecoveryActionResult = {
  ok: boolean
  message?: string
  reason?: string
  [key: string]: unknown
}

export async function recoverBackfill(jobId: string): Promise<RecoveryActionResult> {
  const response = await request('/api/backfill_job/recovery/recover', {
    method: 'POST',
    body: JSON.stringify({ job_id: jobId }),
  })
  return await response.json()
}

export async function revokeRecoveryWindow(jobId: string, windowId: string): Promise<RecoveryActionResult> {
  const response = await request('/api/backfill_job/recovery/revoke', {
    method: 'POST',
    body: JSON.stringify({ job_id: jobId, window_id: windowId }),
  })
  return await response.json()
}
