<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <section
      v-if="isBackfillModule"
      class="recovery-panel"
      data-testid="recovery-manifest"
    >
      <div class="recovery-head">
        <div>
          <h3>回填窗口恢复清单</h3>
          <p class="recovery-hint">
            每个窗口钉住快照版本，记录实际写入范围、尚未确认区间、交付属主和优先级。
          </p>
        </div>
        <button class="btn ghost" data-testid="refresh-recovery" type="button" @click="loadRecovery">
          刷新恢复清单
        </button>
      </div>
      <table class="data-table recovery-table">
        <thead>
          <tr>
            <th>作业</th>
            <th>窗口</th>
            <th>状态</th>
            <th>快照版本</th>
            <th>实际写入范围</th>
            <th>尚未确认区间</th>
            <th>属主/优先级</th>
            <th>恢复操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in recoveryRecords"
            :key="String(item.id)"
            :data-testid="`recovery-row-${item.window_id}`"
            :data-state="String(item.state)"
          >
            <td>{{ item.job_id ?? '—' }}</td>
            <td>{{ item.window_id ?? '—' }}</td>
            <td>{{ item.state ?? item.status ?? '—' }}</td>
            <td>{{ item.快照版本 ?? item.snapshot_version ?? '—' }}</td>
            <td>{{ formatRange(item.actual_written_range) }}</td>
            <td>{{ formatRange(item.unacknowledged_ranges) }}</td>
            <td>{{ item.owner_job_id ?? item.job_id ?? '—' }} / {{ item.priority ?? '—' }}</td>
            <td class="row-actions">
              <button
                v-if="item.state === '待恢复' || item.state === '待执行'"
                class="link"
                type="button"
                :disabled="recoveryBusy"
                data-testid="recovery-resume"
                @click="recoverJob(item.job_id)"
              >
                续做
              </button>
              <button
                v-if="item.state !== '已撤销' && item.state !== '已写入'"
                class="link"
                type="button"
                :disabled="recoveryBusy"
                data-testid="recovery-revoke"
                @click="revokeWindow(item.job_id, item.id)"
              >
                撤销
              </button>
              <span v-if="item.state === '已写入'" class="recovery-done">已交付</span>
              <span v-else-if="item.state === '已撤销'" class="recovery-revoked">已撤销</span>
            </td>
          </tr>
          <tr v-if="!recoveryRecords.length">
            <td colspan="8" class="empty-state">暂无窗口恢复记录，执行按窗口回填后自动回写。</td>
          </tr>
        </tbody>
      </table>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  createEntry,
  downloadEntries,
  getRecoveryManifest,
  listEntries,
  moduleMeta,
  recoverBackfill,
  revokeRecoveryWindow,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const recoveryRecords = ref<Record<string, any>[]>([])
const recoveryBusy = ref(false)

const isBackfillModule = computed(() => props.moduleKey === 'backfill_job')
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function formatRange(value: unknown): string {
  if (!value) return '—'
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value.map(formatRange).filter(Boolean).join('；') || '—'
  if (typeof value === 'object') {
    const range = value as { start?: unknown; end?: unknown }
    return `${range.start ?? ''} ~ ${range.end ?? ''}`
  }
  return String(value)
}

async function loadRecovery() {
  if (!isBackfillModule.value) return
  try {
    const payload = await getRecoveryManifest()
    recoveryRecords.value = Array.isArray(payload.records) ? payload.records : []
  } catch (error) {
    // 普通列表仍应可用，恢复面板只记录页脚错误。
    errorMessage.value = error instanceof Error ? error.message : '恢复清单读取失败'
  }
}

async function recoverJob(jobId: unknown) {
  if (!jobId || recoveryBusy.value) return
  recoveryBusy.value = true
  errorMessage.value = ''
  try {
    const result = await recoverBackfill(String(jobId))
    if (!result.ok) throw new Error(result.message || result.reason || '恢复失败')
    await Promise.all([loadRecovery(), reload()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  } finally {
    recoveryBusy.value = false
  }
}

async function revokeWindow(jobId: unknown, windowId: unknown) {
  if (!jobId || !windowId || recoveryBusy.value) return
  recoveryBusy.value = true
  errorMessage.value = ''
  try {
    const result = await revokeRecoveryWindow(String(jobId), String(windowId))
    if (!result.ok) throw new Error(result.message || result.reason || '撤销失败')
    await Promise.all([loadRecovery(), reload()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '撤销失败'
  } finally {
    recoveryBusy.value = false
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
    await loadRecovery()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

watch(() => props.moduleKey, async () => {
  recoveryRecords.value = []
  await reload()
  await loadRecovery()
})

onMounted(async () => {
  await reload()
  await loadRecovery()
})
</script>

<style scoped>
.recovery-panel {
  margin: 16px 0;
  padding: 16px;
  border: 1px solid var(--border-color, #e5e7eb);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.72);
}

.recovery-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 12px;
}

.recovery-head h3 {
  margin: 0 0 4px;
  font-size: 16px;
}

.recovery-hint {
  margin: 0;
  color: #6b7280;
  font-size: 13px;
}

.recovery-table {
  margin-top: 8px;
}

.recovery-done {
  color: #047857;
}

.recovery-revoked {
  color: #b91c1c;
}
</style>
