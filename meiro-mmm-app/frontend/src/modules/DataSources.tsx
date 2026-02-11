import { useState, useRef, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import {
  connectMeiroCDP,
  disconnectMeiroCDP,
  getMeiroCDPStatus,
  getMeiroConfig,
  testMeiroConnection,
  saveMeiroCDP,
  getMeiroMapping,
  saveMeiroMapping,
  getMeiroPullConfig,
  saveMeiroPullConfig,
  meiroPull,
  meiroRotateWebhookSecret,
  meiroDryRun,
  type MeiroConfig,
  type MeiroMapping,
} from '../connectors/meiroConnector'

interface DataSourcesProps {
  onJourneysImported: () => void
}

type SystemState = 'empty' | 'loading' | 'data_loaded' | 'stale' | 'partial' | 'error'
type ImportMethod = 'sample' | 'upload' | 'meiro' | null
type PreviewTab = 'preview' | 'validation' | 'mapping'
type MeiroTab = 'connection' | 'pull' | 'push' | 'mapping'

const card = {
  backgroundColor: t.color.surface,
  borderRadius: t.radius.lg,
  border: `1px solid ${t.color.border}`,
  padding: t.space.xl,
  boxShadow: t.shadowSm,
  marginBottom: t.space.lg,
} as const

const ADMIN_PLATFORMS = [
  { id: 'google', label: 'Google Ads', clientIdLabel: 'Client ID', clientSecretLabel: 'Client Secret', hasAppId: false },
  { id: 'meta', label: 'Meta Ads', clientIdLabel: 'App ID', clientSecretLabel: 'App Secret', hasAppId: true },
  { id: 'linkedin', label: 'LinkedIn Ads', clientIdLabel: 'Client ID', clientSecretLabel: 'Client Secret', hasAppId: false },
] as const

const JSON_FORMAT_V1 = `[
  {
    "customer_id": "c001",
    "touchpoints": [
      {"channel": "google_ads", "campaign": "Brand", "timestamp": "2024-01-02"},
      {"channel": "email", "campaign": "Welcome", "timestamp": "2024-01-05"},
      {"channel": "direct", "timestamp": "2024-01-07"}
    ],
    "conversion_value": 120.0,
    "converted": true
  }
]`

const JSON_FORMAT_V2 = `{
  "schema_version": "2.0",
  "defaults": { "timezone": "UTC", "currency": "EUR" },
  "journeys": [
    {
      "journey_id": "j_001",
      "customer": { "id": "c001", "type": "profile_id" },
      "touchpoints": [
        {
          "id": "tp_1",
          "ts": "2024-01-02T10:12:00Z",
          "channel": "google_ads",
          "source": { "platform": "google_ads", "account_id": "123", "campaign_id": "987" },
          "campaign": { "name": "Brand", "id": "987" },
          "utm": { "source": "google", "medium": "cpc", "campaign": "brand" },
          "cost": { "amount": 1.23, "currency": "EUR" }
        }
      ],
      "conversions": [
        { "id": "cv_1", "ts": "2024-01-07T09:00:00Z", "name": "purchase", "value": 120.0, "currency": "EUR" }
      ]
    }
  ]
}`

const IMPORT_METHODS = [
  {
    id: 'sample' as const,
    title: 'Sample Data',
    icon: '📦',
    color: t.color.success,
    what: '30 sample journeys with 6 channels (google_ads, meta_ads, linkedin_ads, email, whatsapp, direct)',
    prerequisites: 'None',
    impact: 'Replaces existing data. Ideal for trying the platform or demos.',
  },
  {
    id: 'upload' as const,
    title: 'Upload JSON',
    icon: '📄',
    color: t.color.accent,
    what: 'JSON file with customer journeys array (touchpoints, conversion_value)',
    prerequisites: 'File matching expected schema',
    impact: 'Replaces existing data. Use for one-off imports or batch uploads.',
  },
  {
    id: 'meiro' as const,
    title: 'Import from Meiro',
    icon: '🔗',
    color: '#7c3aed',
    what: 'Parse CDP profile data into attribution journeys',
    prerequisites: 'Meiro CDP connected, profiles pushed via webhook or fetch',
    impact: 'Replaces existing data. Configurable field mapping applied.',
  },
] as const

function formatRelativeTime(iso: string | null | undefined): string {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    const now = new Date()
    const diffMs = now.getTime() - d.getTime()
    const diffMins = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMs / 3600000)
    const diffDays = Math.floor(diffMs / 86400000)
    if (diffMins < 1) return 'Just now'
    if (diffMins < 60) return `${diffMins}m ago`
    if (diffHours < 24) return `${diffHours}h ago`
    if (diffDays < 7) return `${diffDays}d ago`
    return d.toLocaleDateString()
  } catch {
    return iso
  }
}

function formatFreshness(hours: number | null | undefined): string {
  if (hours == null) return '—'
  if (hours < 1) return '< 1h'
  if (hours < 24) return `${Math.round(hours)}h`
  if (hours < 168) return `${Math.round(hours / 24)}d`
  return `${Math.round(hours / 168)}w`
}

function systemStateLabel(s: SystemState): string {
  const map: Record<SystemState, string> = {
    empty: 'Empty',
    loading: 'Loading',
    data_loaded: 'Data Loaded',
    stale: 'Stale',
    partial: 'Partial',
    error: 'Error',
  }
  return map[s] || s
}

export default function DataSources({ onJourneysImported }: DataSourcesProps) {
  const fileRef = useRef<HTMLInputElement>(null)
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [adminOpen, setAdminOpen] = useState(false)
  const [importLogOpen, setImportLogOpen] = useState(false)
  const [selectedImport, setSelectedImport] = useState<ImportMethod>(null)
  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewTab, setPreviewTab] = useState<PreviewTab>('preview')
  const [meiroTab, setMeiroTab] = useState<MeiroTab>('connection')
  const [adminCreds, setAdminCreds] = useState<Record<string, { client_id?: string; client_secret?: string; app_id?: string; app_secret?: string }>>({
    google: {}, meta: {}, linkedin: {},
  })

  const configStatusQuery = useQuery({
    queryKey: ['admin-datasource-config'],
    queryFn: async () => {
      const res = await fetch('/api/admin/datasource-config')
      if (!res.ok) return { google: { configured: false }, meta: { configured: false }, linkedin: { configured: false } }
      return res.json()
    },
  })

  const connectionsQuery = useQuery({
    queryKey: ['datasources-connections'],
    queryFn: async () => {
      const res = await fetch('/api/datasources/connections')
      if (!res.ok) return { connections: [] }
      return res.json()
    },
  })

  const meiroConfigQuery = useQuery({
    queryKey: ['meiro-config'],
    queryFn: getMeiroConfig,
    refetchInterval: 30000,
  })

  const meiroProfilesQuery = useQuery({
    queryKey: ['meiro-profiles'],
    queryFn: async () => {
      const res = await fetch('/api/connectors/meiro/profiles')
      if (!res.ok) return { stored_count: 0, webhook_url: '' }
      return res.json()
    },
  })

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false, system_state: 'empty' as SystemState }
      return res.json()
    },
  })

  const [importLogFilters, setImportLogFilters] = useState<{ status?: string; source?: string; since?: string; until?: string }>({})
  const [importLogDrawerRunId, setImportLogDrawerRunId] = useState<string | null>(null)
  const [confirmModal, setConfirmModal] = useState<{ source: string; action: (note?: string) => void; precheck?: Record<string, unknown> } | null>(null)
  const [importNote, setImportNote] = useState('')

  const runWithPrecheck = async (source: string, action: (note?: string) => void) => {
    try {
      const res = await fetch(`/api/attribution/import-precheck?source=${encodeURIComponent(source)}`)
      const precheck = res.ok ? await res.json() : null
      if (precheck?.would_overwrite) {
        setConfirmModal({ source, action, precheck })
      } else {
        action()
      }
    } catch {
      action()
    }
  }

  const importLogQuery = useQuery({
    queryKey: ['import-log', importLogFilters],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (importLogFilters.status) params.set('status', importLogFilters.status)
      if (importLogFilters.source) params.set('source', importLogFilters.source)
      if (importLogFilters.since) params.set('since', importLogFilters.since)
      if (importLogFilters.until) params.set('until', importLogFilters.until)
      const res = await fetch(`/api/attribution/import-log?${params}`)
      if (!res.ok) return { runs: [] }
      return res.json()
    },
    enabled: importLogOpen,
  })

  const importRunDetailQuery = useQuery({
    queryKey: ['import-run-detail', importLogDrawerRunId],
    queryFn: async () => {
      const res = await fetch(`/api/attribution/import-log/${importLogDrawerRunId}`)
      if (!res.ok) throw new Error('Failed to load run')
      return res.json()
    },
    enabled: importLogOpen && !!importLogDrawerRunId,
  })

  const previewQuery = useQuery({
    queryKey: ['journeys-preview', previewTab],
    queryFn: async () => {
      if (previewTab === 'preview') {
        const res = await fetch('/api/attribution/journeys/preview?limit=20')
        if (!res.ok) return { rows: [], columns: [], total: 0 }
        return res.json()
      }
      if (previewTab === 'validation') {
        const res = await fetch('/api/attribution/journeys/validation')
        if (!res.ok) return { error_count: 0, warn_count: 0, validation_items: [], import_summary: {}, total: 0 }
        return res.json()
      }
      const res = await fetch('/api/attribution/field-mapping')
      if (!res.ok) return {}
      return res.json()
    },
    enabled: previewOpen,
  })

  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerValidIndex, setDrawerValidIndex] = useState<number | null>(null)
  const rowDetailsQuery = useQuery({
    queryKey: ['row-details', drawerValidIndex],
    queryFn: async () => {
      const res = await fetch(`/api/attribution/journeys/row-details?valid_index=${drawerValidIndex}`)
      if (!res.ok) throw new Error('Failed to load row details')
      return res.json()
    },
    enabled: drawerOpen && drawerValidIndex != null,
  })

  const invalidateJourneys = () => {
    journeysQuery.refetch()
    importLogQuery.refetch()
    previewQuery.refetch()
  }

  const uploadMutation = useMutation({
    mutationFn: async ({ file, import_note }: { file: File; import_note?: string }) => {
      const formData = new FormData()
      formData.append('file', file)
      if (import_note) formData.append('import_note', import_note)
      const res = await fetch('/api/attribution/journeys/upload', { method: 'POST', body: formData })
      if (!res.ok) {
        const data = await res.json().catch(() => ({ detail: 'Upload failed' }))
        throw new Error(data.detail || 'Upload failed')
      }
      return res.json()
    },
    onSuccess: () => {
      onJourneysImported()
      invalidateJourneys()
      importLogQuery.refetch()
      setSelectedImport(null)
    },
  })

  const loadSampleMutation = useMutation({
    mutationFn: async (opts?: { import_note?: string }) => {
      const res = await fetch('/api/attribution/journeys/load-sample', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(opts || {}),
      })
      if (!res.ok) throw new Error('Failed')
      return res.json()
    },
    onSuccess: () => {
      onJourneysImported()
      invalidateJourneys()
      importLogQuery.refetch()
      setSelectedImport(null)
    },
  })

  const importCdpMutation = useMutation({
    mutationFn: async (opts?: { import_note?: string }) => {
      const res = await fetch('/api/attribution/journeys/from-cdp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(opts || {}),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({ detail: 'Import failed' }))
        throw new Error(data.detail || 'Import failed')
      }
      return res.json()
    },
    onSuccess: () => {
      onJourneysImported()
      invalidateJourneys()
      importLogQuery.refetch()
      setSelectedImport(null)
    },
  })

  const testMeiroMutation = useMutation({
    mutationFn: async (opts?: { save_on_success?: boolean }) =>
      testMeiroConnection({
        api_base_url: meiroUrl || undefined,
        api_key: meiroKey || undefined,
        save_on_success: opts?.save_on_success,
      }),
    onSuccess: (_data, opts) => {
      meiroConfigQuery.refetch()
      if (opts?.save_on_success) {
        connectionsQuery.refetch()
        setMeiroUrl('')
        setMeiroKey('')
      }
    },
  })

  const connectMeiroMutation = useMutation({
    mutationFn: async () => connectMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey }),
    onSuccess: () => {
      connectionsQuery.refetch()
      meiroProfilesQuery.refetch()
      meiroConfigQuery.refetch()
      setMeiroUrl('')
      setMeiroKey('')
    },
  })

  const saveMeiroMutation = useMutation({
    mutationFn: async () => saveMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey }),
    onSuccess: () => {
      connectionsQuery.refetch()
      meiroConfigQuery.refetch()
      setMeiroUrl('')
      setMeiroKey('')
    },
  })

  const disconnectMeiroMutation = useMutation({
    mutationFn: disconnectMeiroCDP,
    onSuccess: () => {
      connectionsQuery.refetch()
      meiroConfigQuery.refetch()
      setMeiroUrl('')
      setMeiroKey('')
    },
  })

  const saveConfigMutation = useMutation({
    mutationFn: async (payload: { platform: string; client_id?: string; client_secret?: string; app_id?: string; app_secret?: string }) => {
      const res = await fetch('/api/admin/datasource-config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        throw new Error(data.detail || 'Failed to save')
      }
      return res.json()
    },
    onSuccess: () => {
      configStatusQuery.refetch()
      connectionsQuery.refetch()
    },
  })

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const success = params.get('success')
    const error = params.get('error')
    const msg = params.get('message')
    if (success || error) {
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [])

  const summary = journeysQuery.data
  const loaded = summary?.loaded ?? false
  const systemState = (summary?.system_state ?? 'empty') as SystemState
  const isLoading = loadSampleMutation.isPending || uploadMutation.isPending || importCdpMutation.isPending
  const connections = connectionsQuery.data?.connections ?? []

  const statusBg = systemState === 'error' ? t.color.dangerMuted
    : systemState === 'stale' || systemState === 'partial' ? t.color.warningMuted
    : loaded ? t.color.successMuted : t.color.borderLight
  const statusBorder = systemState === 'error' ? t.color.danger
    : systemState === 'stale' || systemState === 'partial' ? t.color.warning
    : loaded ? t.color.success : t.color.border

  return (
    <div style={{ maxWidth: 960 }}>
      <h2 style={{ margin: '0 0 8px', fontSize: t.font.size2xl, fontWeight: t.font.weightBold, color: t.color.text }}>Data Sources</h2>
      <p style={{ margin: '0 0 24px', fontSize: t.font.sizeBase, color: t.color.textSecondary }}>
        Import conversion path data for attribution. Choose Sample Data, Upload JSON, or Import from Meiro.
      </p>

      {/* Status strip */}
      <div style={{
        ...card,
        backgroundColor: statusBg,
        borderColor: statusBorder,
        padding: `${t.space.md}px ${t.space.xl}px`,
      }}>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.lg, alignItems: 'center', marginBottom: t.space.md }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Journeys:</span>
            <strong style={{ fontSize: t.font.sizeBase, color: t.color.text }}>{loaded ? summary?.count ?? 0 : '—'}</strong>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Converted:</span>
            <strong style={{ fontSize: t.font.sizeBase, color: t.color.text }}>{loaded ? summary?.converted ?? 0 : '—'}</strong>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Channels:</span>
            <span style={{ fontSize: t.font.sizeBase, color: t.color.text }}>
              {loaded && summary?.channels?.length ? summary.channels.join(', ') : '—'}
            </span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Last import:</span>
            <span style={{ fontSize: t.font.sizeBase, color: t.color.text }}>
              {formatRelativeTime(summary?.last_import_at)} {summary?.last_import_source ? `(${summary.last_import_source})` : ''}
            </span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Freshness:</span>
            <span style={{ fontSize: t.font.sizeBase, color: t.color.text }}>
              {formatFreshness(summary?.data_freshness_hours)}
            </span>
          </div>
          <div style={{
            padding: '4px 10px',
            borderRadius: 6,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightSemibold,
            backgroundColor: statusBorder,
            color: t.color.surface,
          }}>
            {isLoading ? 'Loading…' : systemStateLabel(systemState)}
          </div>
        </div>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => setSelectedImport(selectedImport === 'sample' ? null : 'sample')}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightSemibold,
              backgroundColor: t.color.accent,
              color: 'white',
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            New Import
          </button>
          <button
            type="button"
            onClick={() => setImportLogOpen(!importLogOpen)}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightSemibold,
              backgroundColor: t.color.surface,
              color: t.color.text,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Import Log
          </button>
        </div>
        {importLogOpen && (
          <ImportLogPanel
            runs={importLogQuery.data?.runs ?? []}
            filters={importLogFilters}
            onFiltersChange={setImportLogFilters}
            onRunClick={(id) => setImportLogDrawerRunId(id)}
            drawerRunId={importLogDrawerRunId}
            drawerData={importRunDetailQuery.data}
            onDrawerClose={() => setImportLogDrawerRunId(null)}
            onRerun={(runId) => {
              fetch(`/api/attribution/import-log/${runId}/rerun`, { method: 'POST' })
                .then((res) => { if (res.ok) { journeysQuery.refetch(); importLogQuery.refetch(); invalidateJourneys(); onJourneysImported() } })
                .catch(() => {})
            }}
            formatRelativeTime={formatRelativeTime}
          />
        )}
        {confirmModal && (
          <ImportConfirmModal
            precheck={confirmModal.precheck}
            source={confirmModal.source}
            importNote={importNote}
            onImportNoteChange={setImportNote}
            onConfirm={() => { confirmModal.action(importNote); setConfirmModal(null); setImportNote('') }}
            onCancel={() => { setConfirmModal(null); setImportNote('') }}
            tokens={t}
          />
        )}
      </div>

      {/* Hidden file input for journey JSON upload – always mounted so ref is stable */}
      <input
        ref={fileRef}
        type="file"
        accept=".json"
        style={{ display: 'none' }}
        onChange={(e) => {
          const f = e.target.files?.[0]
          if (f) uploadMutation.mutate({ file: f })
          e.target.value = ''
        }}
        data-testid="journey-json-upload"
      />

      {/* Import methods – cards with progressive disclosure */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: t.space.lg, marginBottom: t.space.xl }}>
        {IMPORT_METHODS.map(m => (
          <div
            key={m.id}
            onClick={() => setSelectedImport(selectedImport === m.id ? null : m.id)}
            style={{
              ...card,
              cursor: 'pointer',
              borderColor: selectedImport === m.id ? m.color : t.color.border,
              borderWidth: selectedImport === m.id ? 2 : 1,
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginBottom: t.space.sm }}>
              <span style={{ fontSize: '20px' }}>{m.icon}</span>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{m.title}</h3>
            </div>
            <p style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}><strong>Imports:</strong> {m.what}</p>
            <p style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}><strong>Prerequisites:</strong> {m.prerequisites}</p>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}><strong>Impact:</strong> {m.impact}</p>
            {selectedImport === m.id && (
              <div style={{ marginTop: t.space.lg, paddingTop: t.space.lg, borderTop: `1px solid ${t.color.border}` }} onClick={e => e.stopPropagation()}>
                {m.id === 'sample' && (
                  <button
                    type="button"
                    onClick={() => runWithPrecheck('sample', (note) => loadSampleMutation.mutate({ import_note: note }))}
                    disabled={loadSampleMutation.isPending}
                    style={{ padding: '10px 20px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.success, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer', width: '100%' }}
                  >
                    {loadSampleMutation.isPending ? 'Loading…' : 'Load Sample Data'}
                  </button>
                )}
                {m.id === 'upload' && (
                  <>
                    <button
                      type="button"
                      onClick={() => fileRef.current?.click()}
                      disabled={uploadMutation.isPending}
                      style={{ padding: '10px 20px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer', width: '100%' }}
                    >
                      {uploadMutation.isPending ? 'Uploading…' : 'Upload JSON File'}
                    </button>
                    {uploadMutation.isError && <p style={{ color: t.color.danger, fontSize: t.font.sizeSm, marginTop: 8 }}>{(uploadMutation.error as Error).message}</p>}
                  </>
                )}
                {m.id === 'meiro' && (
                  <>
                    <button
                      onClick={(e) => { e.stopPropagation(); runWithPrecheck('meiro_webhook', (note) => importCdpMutation.mutate({ import_note: note })) }}
                      disabled={importCdpMutation.isPending}
                      style={{ padding: '10px 20px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: '#7c3aed', color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer', width: '100%' }}
                    >
                      {importCdpMutation.isPending ? 'Importing…' : 'Import from CDP'}
                    </button>
                    {importCdpMutation.isError && <p style={{ color: t.color.danger, fontSize: t.font.sizeSm, marginTop: 8 }}>{(importCdpMutation.error as Error).message}</p>}
                  </>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Connections – unified section */}
      <div style={card}>
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>Connections</h3>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Meiro CDP provides journeys; ad platforms enrich spend data. Ad platforms are optional.
        </p>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, marginBottom: t.space.xl }}>
          {connections.map((c: { id: string; label: string; status: string; role: string }) => {
            const pillStyle: Record<string, { bg: string; color: string }> = {
              connected: { bg: t.color.successMuted, color: t.color.success },
              not_connected: { bg: t.color.borderLight, color: t.color.textMuted },
              needs_attention: { bg: t.color.warningMuted, color: t.color.warning },
              error: { bg: t.color.dangerMuted, color: t.color.danger },
            }
            const s = pillStyle[c.status] || pillStyle.not_connected
            const label = c.status === 'connected' ? 'Connected' : c.status === 'needs_attention' ? 'Needs attention' : c.status === 'error' ? 'Error' : 'Not connected'
            return (
              <div key={c.id} style={{
                display: 'flex',
                alignItems: 'center',
                gap: t.space.sm,
                padding: `${t.space.sm}px ${t.space.lg}px`,
                borderRadius: t.radius.md,
                backgroundColor: s.bg,
                border: `1px solid ${s.color}`,
              }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{c.label}</span>
                <span style={{ fontSize: t.font.sizeXs, color: s.color, fontWeight: t.font.weightSemibold }}>{label}</span>
                {c.id === 'meiro' && (
                  <MeiroConnectionPill
                    config={meiroConfigQuery.data}
                    lastTestAt={meiroConfigQuery.data?.last_test_at}
                  />
                )}
                {['meta', 'google', 'linkedin'].includes(c.id) && c.status !== 'connected' && (
                  <a href={`/api/auth/${c.id}`} style={{ padding: '4px 12px', fontSize: t.font.sizeXs, backgroundColor: t.color.accent, color: 'white', textDecoration: 'none', borderRadius: t.radius.sm }}>
                    Connect
                  </a>
                )}
              </div>
            )
          })}
        </div>

        {/* Meiro Integration – full panel when Meiro card selected or Meiro in connections */}
        {(selectedImport === 'meiro' || connections.some((c: { id: string }) => c.id === 'meiro')) && (
          <MeiroIntegrationPanel
            config={meiroConfigQuery.data}
            profilesData={meiroProfilesQuery.data}
            meiroUrl={meiroUrl}
            meiroKey={meiroKey}
            setMeiroUrl={setMeiroUrl}
            setMeiroKey={setMeiroKey}
            meiroTab={meiroTab}
            setMeiroTab={setMeiroTab}
            testMeiroMutation={testMeiroMutation}
            connectMeiroMutation={connectMeiroMutation}
            saveMeiroMutation={saveMeiroMutation}
            disconnectMeiroMutation={disconnectMeiroMutation}
            importCdpMutation={importCdpMutation}
            runWithPrecheck={runWithPrecheck}
            onImportSuccess={() => {
              onJourneysImported()
              invalidateJourneys()
              setSelectedImport(null)
            }}
            refetchConfig={() => meiroConfigQuery.refetch()}
            refetchProfiles={() => meiroProfilesQuery.refetch()}
          />
        )}

        {/* Administration – OAuth credentials (collapsed) */}
        <div style={{ marginTop: t.space.xl }}>
          <button
            type="button"
            onClick={() => setAdminOpen(!adminOpen)}
            style={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: 0, border: 'none', background: 'none', cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, textAlign: 'left' }}
          >
            <span>Administration – OAuth credentials for ad platforms</span>
            <span>{adminOpen ? '▼' : '▶'}</span>
          </button>
          {adminOpen && (
            <div style={{ marginTop: t.space.lg, paddingTop: t.space.lg, borderTop: `1px solid ${t.color.border}` }}>
              {ADMIN_PLATFORMS.map(p => {
                const status = configStatusQuery.data?.[p.id]
                const configured = status?.configured ?? false
                const creds = adminCreds[p.id] || {}
                const idKey = p.hasAppId ? 'app_id' : 'client_id'
                const secretKey = p.hasAppId ? 'app_secret' : 'client_secret'
                const saving = saveConfigMutation.isPending && saveConfigMutation.variables?.platform === p.id
                return (
                  <div key={p.id} style={{ marginBottom: t.space.lg, padding: t.space.lg, backgroundColor: t.color.borderLight, borderRadius: t.radius.md }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                      <strong style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{p.label}</strong>
                      <span style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: configured ? t.color.success : t.color.danger }}>{configured ? 'Configured' : 'Not configured'}</span>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: 12, alignItems: 'end' }}>
                      <div>
                        <label style={{ display: 'block', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: 4 }}>{p.clientIdLabel}</label>
                        <input type="text" placeholder={configured ? '••••••••' : 'Enter ' + p.clientIdLabel} value={creds[idKey as keyof typeof creds] ?? ''}
                          onChange={e => setAdminCreds(prev => ({ ...prev, [p.id]: { ...prev[p.id], [idKey]: e.target.value } }))}
                          style={{ width: '100%', padding: '8px 10px', fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, boxSizing: 'border-box' }} />
                      </div>
                      <div>
                        <label style={{ display: 'block', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: 4 }}>{p.clientSecretLabel}</label>
                        <input type="password" placeholder={configured ? '••••••••' : 'Enter ' + p.clientSecretLabel} value={creds[secretKey as keyof typeof creds] ?? ''}
                          onChange={e => setAdminCreds(prev => ({ ...prev, [p.id]: { ...prev[p.id], [secretKey]: e.target.value } }))}
                          style={{ width: '100%', padding: '8px 10px', fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, boxSizing: 'border-box' }} />
                      </div>
                      <button type="button" disabled={saving} onClick={() => {
                        const payload: Record<string, string | undefined> = { platform: p.id }
                        if (p.hasAppId) { payload.app_id = creds.app_id?.trim(); payload.app_secret = creds.app_secret?.trim() }
                        else { payload.client_id = creds.client_id?.trim(); payload.client_secret = creds.client_secret?.trim() }
                        saveConfigMutation.mutate(payload as any)
                      }} style={{ padding: '8px 20px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: saving ? t.color.border : t.color.textSecondary, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: saving ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap' }}>
                        {saving ? 'Saving…' : 'Save'}
                      </button>
                    </div>
                    {saveConfigMutation.isError && saveConfigMutation.variables?.platform === p.id && (
                      <p style={{ color: t.color.danger, fontSize: t.font.sizeXs, marginTop: 8 }}>{(saveConfigMutation.error as Error).message}</p>
                    )}
                  </div>
                )
              })}
              <p style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Env vars (GOOGLE_CLIENT_ID, etc.) override these.</p>
            </div>
          )}
        </div>
      </div>

      {/* Data Preview & Validation – collapsible */}
      <div style={card}>
        <button
          type="button"
          onClick={() => setPreviewOpen(!previewOpen)}
          style={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: 0, border: 'none', background: 'none', cursor: 'pointer', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text, textAlign: 'left' }}
        >
          <span>Data Preview & Validation</span>
          <span>{previewOpen ? '▼' : '▶'}</span>
        </button>
        {previewOpen && (
          <div style={{ marginTop: t.space.lg }}>
            <div style={{ display: 'flex', gap: t.space.sm, marginBottom: t.space.lg }}>
              {(['preview', 'validation', 'mapping'] as const).map(tab => (
                <button key={tab} onClick={() => setPreviewTab(tab)} style={{
                  padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold,
                  backgroundColor: previewTab === tab ? t.color.accent : t.color.borderLight,
                  color: previewTab === tab ? 'white' : t.color.text,
                  border: 'none', borderRadius: t.radius.sm, cursor: 'pointer',
                }}>
                  {tab === 'preview' ? 'Preview' : tab === 'validation' ? 'Validation' : 'Field Mapping'}
                </button>
              ))}
            </div>
            {previewTab === 'preview' && (
              <div style={{ overflow: 'auto', maxHeight: 400 }}>
                {previewQuery.data?.rows?.length ? (
                  <>
                    <table style={{ width: '100%', fontSize: t.font.sizeSm, borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>
                          {['customer_id', 'touchpoints_count', 'first_ts', 'last_ts', 'converted', 'conversion_value', 'channels_list'].map(col => (
                            <th key={col} style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>{col.replace(/_/g, ' ')}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewQuery.data.rows.map((row: Record<string, unknown>, i: number) => (
                          <tr
                            key={i}
                            onClick={() => { setDrawerValidIndex(row.validIndex as number); setDrawerOpen(true) }}
                            style={{ cursor: 'pointer', padding: 8 }}
                          >
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{String(row.customer_id ?? '—')}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{String(row.touchpoints_count ?? '—')}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{(row.first_ts as string)?.slice(0, 10) ?? '—'}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{(row.last_ts as string)?.slice(0, 10) ?? '—'}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{String(row.converted ?? '—')}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{row.conversion_value != null ? String(row.conversion_value) : '—'}</td>
                            <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{(Array.isArray(row.channels_list) ? row.channels_list.join(', ') : row.channels_list) ?? '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {drawerOpen && drawerValidIndex != null && (
                      <>
                        <div onClick={() => setDrawerOpen(false)} style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.3)', zIndex: 999 }} />
                        <div style={{
                          position: 'fixed', top: 0, right: 0, bottom: 0, width: 420, backgroundColor: t.color.surface, boxShadow: '-4px 0 12px rgba(0,0,0,0.1)',
                          zIndex: 1000, overflow: 'auto', padding: t.space.xl,
                        }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg }}>
                          <strong style={{ fontSize: t.font.sizeLg }}>Journey details</strong>
                          <button onClick={() => setDrawerOpen(false)} style={{ padding: '4px 12px', fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}>Close</button>
                        </div>
                        {rowDetailsQuery.data ? (
                          <div style={{ fontSize: t.font.sizeSm }}>
                            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeBase }}>Original JSON</h4>
                            <pre style={{ backgroundColor: t.color.borderLight, padding: 12, borderRadius: t.radius.sm, overflow: 'auto', maxHeight: 200 }}>{JSON.stringify(rowDetailsQuery.data.original, null, 2)}</pre>
                            <h4 style={{ margin: '16px 0 8px', fontSize: t.font.sizeBase }}>Normalized JSON</h4>
                            <pre style={{ backgroundColor: t.color.borderLight, padding: 12, borderRadius: t.radius.sm, overflow: 'auto', maxHeight: 200 }}>{JSON.stringify(rowDetailsQuery.data.normalized, null, 2)}</pre>
                          </div>
                        ) : rowDetailsQuery.isLoading ? (
                          <p style={{ color: t.color.textMuted }}>Loading…</p>
                        ) : (
                          <p style={{ color: t.color.danger }}>Failed to load details</p>
                        )}
                        </div>
                      </>
                    )}
                  </>
                ) : (
                  <p style={{ color: t.color.textMuted }}>No data. Import journeys first.</p>
                )}
                {previewQuery.data?.total != null && previewQuery.data.total > 20 && (
                  <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>Showing first 20 of {previewQuery.data.total} journeys.</p>
                )}
              </div>
            )}
            {previewTab === 'validation' && (
              <div>
                {previewQuery.data && (
                  <>
                    <div style={{ display: 'flex', gap: t.space.lg, flexWrap: 'wrap', marginBottom: t.space.lg }}>
                      <a
                        href="/api/attribution/journeys/validation-report"
                        download="validation-report.json"
                        style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer', textDecoration: 'none', display: 'inline-block' }}
                      >
                        Download validation report
                      </a>
                    </div>
                    {(!previewQuery.data.import_summary || Object.keys(previewQuery.data.import_summary).length === 0) ? (
                      <p style={{ color: t.color.textMuted }}>No import yet. Upload or load sample data first.</p>
                    ) : (
                    <>
                    <div style={{ display: 'flex', gap: t.space.xl, flexWrap: 'wrap', marginBottom: t.space.lg }}>
                      <div><span style={{ color: t.color.textSecondary }}>Total:</span> <strong>{(previewQuery.data.import_summary as { total?: number })?.total ?? 0}</strong></div>
                      <div><span style={{ color: t.color.textSecondary }}>Valid:</span> <strong style={{ color: t.color.success }}>{(previewQuery.data.import_summary as { valid?: number })?.valid ?? 0}</strong></div>
                      <div><span style={{ color: t.color.textSecondary }}>Invalid:</span> <strong style={{ color: t.color.danger }}>{(previewQuery.data.import_summary as { invalid?: number })?.invalid ?? 0}</strong></div>
                      <div><span style={{ color: t.color.textSecondary }}>Converted:</span> <strong>{(previewQuery.data.import_summary as { converted?: number })?.converted ?? 0}</strong></div>
                      <div><span style={{ color: t.color.textSecondary }}>Channels:</span> <span>{(previewQuery.data.import_summary as { channels_detected?: string[] })?.channels_detected?.join(', ') ?? '—'}</span></div>
                    </div>
                    <div style={{ display: 'flex', gap: t.space.xl, marginBottom: t.space.lg }}>
                      <div><span style={{ color: t.color.textSecondary }}>Errors:</span> <strong style={{ color: (previewQuery.data as { error_count: number }).error_count ? t.color.danger : t.color.text }}>{(previewQuery.data as { error_count: number }).error_count}</strong></div>
                      <div><span style={{ color: t.color.textSecondary }}>Warnings:</span> <strong style={{ color: (previewQuery.data as { warn_count: number }).warn_count ? t.color.warning : t.color.text }}>{(previewQuery.data as { warn_count: number }).warn_count}</strong></div>
                    </div>
                    {(previewQuery.data as { validation_items?: Array<{ journeyIndex: number; fieldPath: string; severity: string; message: string; suggestedFix: string }> }).validation_items?.length > 0 && (
                      <div style={{ marginTop: t.space.lg }}>
                        <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeBase }}>Validation items</h4>
                        <div style={{ fontSize: t.font.sizeSm, maxHeight: 300, overflow: 'auto' }}>
                          {(previewQuery.data as { validation_items: Array<{ journeyIndex: number; fieldPath: string; severity: string; message: string; suggestedFix: string }> }).validation_items.map((item, i) => (
                            <div key={i} style={{ padding: 8, marginBottom: 4, backgroundColor: item.severity === 'error' ? t.color.dangerMuted : t.color.warningMuted, borderRadius: t.radius.sm }}>
                              <span style={{ fontWeight: t.font.weightSemibold, color: item.severity === 'error' ? t.color.danger : t.color.warning }}>Journey {item.journeyIndex}</span>
                              {item.fieldPath && <span style={{ color: t.color.textSecondary }}> → {item.fieldPath}</span>}
                              <p style={{ margin: '4px 0 0', color: t.color.text }}>{item.message}</p>
                              {item.suggestedFix && <p style={{ margin: '2px 0 0', fontSize: t.font.sizeXs, color: t.color.textMuted }}>Fix: {item.suggestedFix}</p>}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    </>
                    )}
                  </>
                )}
              </div>
            )}
            {previewTab === 'mapping' && (
              <div>
                {previewQuery.data && typeof previewQuery.data === 'object' && !Array.isArray(previewQuery.data) && 'touchpoint_attr' in previewQuery.data && (
                  <div style={{ fontSize: t.font.sizeSm }}>
                    <p style={{ margin: '0 0 12px', color: t.color.textSecondary }}>CDP field mapping used when importing from Meiro:</p>
                    <table style={{ width: '100%', maxWidth: 400 }}>
                      <tbody>
                        {Object.entries(previewQuery.data as Record<string, string>).map(([k, v]) => (
                          <tr key={k}><td style={{ padding: '4px 0', color: t.color.textMuted }}>{k.replace(/_/g, ' ')}</td><td style={{ padding: '4px 0', fontFamily: 'monospace' }}>{v}</td></tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Expected JSON Format */}
      <div style={{ ...card, backgroundColor: t.color.accentMuted + '20', borderColor: t.color.accent }}>
        <h3 style={{ margin: '0 0 12px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>Expected JSON format</h3>
        <p style={{ margin: '0 0 12px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Both formats are supported. v2 is recommended for production (multiple conversions, IDs, spend matching, Meiro mapping).
        </p>

        <div style={{ marginBottom: t.space.xl }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Legacy (v1)</span>
            <button
              type="button"
              onClick={() => navigator.clipboard.writeText(JSON_FORMAT_V1)}
              style={{ padding: '4px 10px', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}
            >
              Copy
            </button>
          </div>
          <pre style={{ backgroundColor: t.color.surface, padding: 16, borderRadius: t.radius.md, fontSize: t.font.sizeXs, overflow: 'auto', border: `1px solid ${t.color.border}`, color: t.color.text, maxHeight: 160 }}>
            {JSON_FORMAT_V1}
          </pre>
        </div>

        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.accent }}>Recommended (v2)</span>
            <button
              type="button"
              onClick={() => navigator.clipboard.writeText(JSON_FORMAT_V2)}
              style={{ padding: '4px 10px', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}
            >
              Copy
            </button>
          </div>
          <pre style={{ backgroundColor: t.color.surface, padding: 16, borderRadius: t.radius.md, fontSize: t.font.sizeXs, overflow: 'auto', border: `1px solid ${t.color.border}`, color: t.color.text, maxHeight: 260 }}>
            {JSON_FORMAT_V2}
          </pre>
        </div>
      </div>
    </div>
  )
}

function MeiroConnectionPill({ config, lastTestAt }: { config?: MeiroConfig | null; lastTestAt?: string | null }) {
  const connected = config?.connected ?? false
  return (
    <span style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
      <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
        {lastTestAt ? `Tested ${formatRelativeTime(lastTestAt)}` : '—'}
      </span>
    </span>
  )
}

function MeiroIntegrationPanel({
  config,
  profilesData,
  meiroUrl,
  meiroKey,
  setMeiroUrl,
  setMeiroKey,
  meiroTab,
  setMeiroTab,
  testMeiroMutation,
  connectMeiroMutation,
  saveMeiroMutation,
  disconnectMeiroMutation,
  importCdpMutation,
  runWithPrecheck,
  onImportSuccess,
  refetchConfig,
  refetchProfiles,
}: {
  config?: MeiroConfig | null
  profilesData?: { webhook_url?: string; stored_count?: number; webhook_last_received_at?: string | null; webhook_received_count?: number }
  meiroUrl: string
  meiroKey: string
  setMeiroUrl: (v: string) => void
  setMeiroKey: (v: string) => void
  meiroTab: MeiroTab
  setMeiroTab: (t: MeiroTab) => void
  testMeiroMutation: { mutate: (opts?: { save_on_success?: boolean }) => void; isPending: boolean; error: Error | null }
  connectMeiroMutation: { mutate: () => void; isPending: boolean; error: Error | null }
  saveMeiroMutation: { mutate: () => void; isPending: boolean; error: Error | null }
  disconnectMeiroMutation: { mutate: () => void; isPending: boolean }
  importCdpMutation: { mutate: () => void; isPending: boolean; error: Error | null }
  runWithPrecheck: (source: string, action: () => void) => void
  onImportSuccess: () => void
  refetchConfig: () => void
  refetchProfiles: () => void
}) {
  const connected = config?.connected ?? false
  const [dryRunResult, setDryRunResult] = useState<{ count: number; preview: Array<{ id: string; touchpoints: number; value: number }>; warnings: string[] } | null>(null)
  const [secretCopied, setSecretCopied] = useState(false)

  const mappingQuery = useQuery({
    queryKey: ['meiro-mapping'],
    queryFn: getMeiroMapping,
    enabled: connected && meiroTab === 'mapping',
  })
  const pullConfigQuery = useQuery({
    queryKey: ['meiro-pull-config'],
    queryFn: getMeiroPullConfig,
    enabled: connected && meiroTab === 'pull',
  })
  const [mapping, setMapping] = useState<MeiroMapping>({})
  const [pullConfig, setPullConfig] = useState<Record<string, unknown>>({})
  useEffect(() => {
    if (mappingQuery.data?.mapping) setMapping(mappingQuery.data.mapping)
  }, [mappingQuery.data])
  useEffect(() => {
    if (pullConfigQuery.data) setPullConfig(pullConfigQuery.data)
  }, [pullConfigQuery.data])

  const saveMappingMutation = useMutation({
    mutationFn: saveMeiroMapping,
    onSuccess: () => mappingQuery.refetch(),
  })
  const savePullConfigMutation = useMutation({
    mutationFn: saveMeiroPullConfig,
    onSuccess: () => pullConfigQuery.refetch(),
  })
  const dryRunMutation = useMutation({
    mutationFn: () => meiroDryRun(100),
    onSuccess: (data) => setDryRunResult({ count: data.count, preview: data.preview, warnings: data.warnings }),
  })
  const pullMutation = useMutation({
    mutationFn: () => meiroPull(),
    onSuccess: () => {
      onImportSuccess()
      refetchProfiles()
    },
  })
  const rotateSecretMutation = useMutation({
    mutationFn: meiroRotateWebhookSecret,
    onSuccess: (data) => {
      navigator.clipboard.writeText(data.secret)
      setSecretCopied(true)
      setTimeout(() => setSecretCopied(false), 2000)
      refetchConfig()
    },
  })

  const presets = (mappingQuery.data?.presets ?? {}) as Record<string, MeiroMapping & { name?: string }>

  return (
    <div style={{ marginTop: t.space.xl, paddingTop: t.space.xl, borderTop: `1px solid ${t.color.border}` }}>
      <h4 style={{ margin: '0 0 12px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro Integration</h4>
      <div style={{ display: 'flex', gap: t.space.sm, marginBottom: t.space.lg, flexWrap: 'wrap' }}>
        {(['connection', 'pull', 'push', 'mapping'] as const).map(tab => (
          <button key={tab} onClick={() => setMeiroTab(tab)} style={{
            padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold,
            backgroundColor: meiroTab === tab ? '#7c3aed' : t.color.borderLight,
            color: meiroTab === tab ? 'white' : t.color.text,
            border: 'none', borderRadius: t.radius.sm, cursor: 'pointer',
          }}>
            {tab === 'connection' ? 'Connection' : tab === 'pull' ? 'Pull (API)' : tab === 'push' ? 'Push (Webhook)' : 'Field Mapping'}
          </button>
        ))}
      </div>

      {meiroTab === 'connection' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
            <span style={{
              padding: '4px 10px', borderRadius: 6, fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold,
              backgroundColor: connected ? t.color.successMuted : t.color.borderLight,
              color: connected ? t.color.success : t.color.textMuted,
            }}>
              {connected ? 'Connected' : 'Not connected'}
            </span>
            {config?.last_test_at && (
              <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Last test: {formatRelativeTime(config.last_test_at)}
              </span>
            )}
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: 12, alignItems: 'end', maxWidth: 560 }}>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: 4 }}>API Base URL</label>
              <input type="text" value={connected && config?.has_key ? (config.api_base_url ?? '') : meiroUrl}
                onChange={e => setMeiroUrl(e.target.value)}
                readOnly={connected && config?.has_key}
                placeholder="https://your-cdp.meiro.io"
                style={{ width: '100%', padding: '8px 10px', fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, boxSizing: 'border-box' }} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: 4 }}>API Key</label>
              <input type="password" value={meiroKey} onChange={e => setMeiroKey(e.target.value)}
                placeholder={connected && config?.has_key ? '••••••••' : 'API key'}
                readOnly={connected && config?.has_key}
                style={{ width: '100%', padding: '8px 10px', fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, boxSizing: 'border-box' }} />
              {connected && config?.has_key && (
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Key is stored; never displayed after save</span>
              )}
            </div>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button onClick={() => testMeiroMutation.mutate()} disabled={testMeiroMutation.isPending}
              style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
              {testMeiroMutation.isPending ? 'Testing…' : 'Test connection'}
            </button>
            {!connected ? (
              <>
                <button onClick={() => connectMeiroMutation.mutate()} disabled={!meiroUrl || !meiroKey || connectMeiroMutation.isPending}
                  style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: '#7c3aed', color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
                  {connectMeiroMutation.isPending ? 'Connecting…' : 'Connect'}
                </button>
                <button onClick={() => saveMeiroMutation.mutate()} disabled={!meiroUrl || !meiroKey || saveMeiroMutation.isPending}
                  style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, color: t.color.text, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}>
                  Save
                </button>
              </>
            ) : (
              <button onClick={() => disconnectMeiroMutation.mutate()}
                style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.danger, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
                Disconnect
              </button>
            )}
          </div>
          {(testMeiroMutation.error || connectMeiroMutation.error || saveMeiroMutation.error) && (
            <span style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>
              {(testMeiroMutation.error || connectMeiroMutation.error || saveMeiroMutation.error)?.message}
            </span>
          )}
        </div>
      )}

      {meiroTab === 'pull' && connected && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg }}>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Fetch events from Meiro API and build journeys. Configure lookback, session gap, and conversion selector below.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 12 }}>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, marginBottom: 4 }}>Lookback (days)</label>
              <input type="number" value={(pullConfig.lookback_days as number) ?? 30} onChange={e => setPullConfig(p => ({ ...p, lookback_days: +e.target.value }))}
                style={{ width: '100%', padding: 8, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, marginBottom: 4 }}>Session gap (min)</label>
              <input type="number" value={(pullConfig.session_gap_minutes as number) ?? 30} onChange={e => setPullConfig(p => ({ ...p, session_gap_minutes: +e.target.value }))}
                style={{ width: '100%', padding: 8, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, marginBottom: 4 }}>Conversion</label>
              <input type="text" value={(pullConfig.conversion_selector as string) ?? 'purchase'} onChange={e => setPullConfig(p => ({ ...p, conversion_selector: e.target.value }))}
                style={{ width: '100%', padding: 8, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }} />
            </div>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button onClick={() => savePullConfigMutation.mutate(pullConfig)} disabled={savePullConfigMutation.isPending}
              style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, color: t.color.text, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}>
              Save config
            </button>
            <button onClick={() => pullMutation.mutate()} disabled={pullMutation.isPending}
              style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: '#7c3aed', color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
              {pullMutation.isPending ? 'Pulling…' : 'Pull journeys'}
            </button>
          </div>
        </div>
      )}

      {meiroTab === 'push' && connected && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg }}>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Configure a webhook in Meiro to POST profiles to the URL below. Header X-Meiro-Webhook-Secret required if secret is set.
          </p>
          {profilesData?.webhook_url && (
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeXs, marginBottom: 4 }}>Webhook URL</label>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <code style={{ flex: 1, padding: 8, backgroundColor: t.color.borderLight, borderRadius: t.radius.sm, fontSize: t.font.sizeSm, wordBreak: 'break-all' }}>
                  {profilesData.webhook_url}
                </code>
                <button onClick={() => navigator.clipboard.writeText(profilesData.webhook_url ?? '')}
                  style={{ padding: '8px 16px', fontSize: t.font.sizeSm, backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
                  Copy URL
                </button>
              </div>
            </div>
          )}
          <div style={{ display: 'flex', gap: t.space.lg, flexWrap: 'wrap' }}>
            <div><span style={{ color: t.color.textSecondary }}>Last received:</span> <strong>{formatRelativeTime(profilesData?.webhook_last_received_at)}</strong></div>
            <div><span style={{ color: t.color.textSecondary }}>Received count:</span> <strong>{profilesData?.webhook_received_count ?? 0}</strong></div>
          </div>
          <button onClick={() => rotateSecretMutation.mutate()} disabled={rotateSecretMutation.isPending}
            style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, color: t.color.text, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer', width: 'fit-content' }}>
            {rotateSecretMutation.isPending ? 'Rotating…' : secretCopied ? 'Secret copied!' : 'Rotate secret'}
          </button>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>
            Stored profiles: <strong>{profilesData?.stored_count ?? 0}</strong>. Use Import from CDP below to load into attribution.
          </p>
        </div>
      )}

      {meiroTab === 'mapping' && connected && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg }}>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Map Meiro fields to Journey v2. Apply a preset or configure manually.
          </p>
          {Object.keys(presets).length > 0 && (
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              {Object.entries(presets).map(([key, p]) => (
                <button key={key} onClick={() => setMapping(m => ({ ...m, ...p }))}
                  style={{ padding: '8px 16px', fontSize: t.font.sizeSm, backgroundColor: t.color.borderLight, color: t.color.text, border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
                  {p.name ?? key}
                </button>
              ))}
            </div>
          )}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, maxWidth: 480 }}>
            {(['touchpoint_attr', 'value_attr', 'id_attr', 'channel_field', 'timestamp_field'] as const).map(f => (
              <div key={f}>
                <label style={{ display: 'block', fontSize: t.font.sizeXs, marginBottom: 4 }}>{f.replace(/_/g, ' ')}</label>
                <input type="text" value={mapping[f] ?? ''} onChange={e => setMapping(m => ({ ...m, [f]: e.target.value }))}
                  style={{ width: '100%', padding: 8, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }} />
              </div>
            ))}
          </div>
          <button onClick={() => saveMappingMutation.mutate(mapping)} disabled={saveMappingMutation.isPending}
            style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer', width: 'fit-content' }}>
            Save mapping
          </button>
        </div>
      )}

      {/* Import from CDP – always visible when connected */}
      {connected && (
        <div style={{ marginTop: t.space.xl, paddingTop: t.space.lg, borderTop: `1px solid ${t.color.border}` }}>
          <h5 style={{ margin: '0 0 8px', fontSize: t.font.sizeBase, color: t.color.text }}>Import journeys</h5>
          <p style={{ margin: '0 0 12px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Parse stored CDP profiles (from webhook or fetch) into attribution journeys. Run dry run first to preview.
          </p>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button onClick={() => dryRunMutation.mutate()} disabled={dryRunMutation.isPending}
              style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: t.color.surface, color: t.color.text, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}>
              {dryRunMutation.isPending ? 'Running…' : 'Dry run (100 profiles)'}
            </button>
            <button onClick={() => runWithPrecheck('meiro_webhook', (note) => importCdpMutation.mutate({ import_note: note }))} disabled={importCdpMutation.isPending}
              style={{ padding: '8px 16px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, backgroundColor: '#7c3aed', color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
              {importCdpMutation.isPending ? 'Importing…' : 'Import from CDP'}
            </button>
          </div>
          {dryRunResult && (
            <div style={{ marginTop: t.space.md, padding: t.space.md, backgroundColor: t.color.borderLight, borderRadius: t.radius.sm, fontSize: t.font.sizeSm }}>
              <p style={{ margin: '0 0 8px' }}><strong>{dryRunResult.count}</strong> journeys in preview.</p>
              {dryRunResult.warnings.length > 0 && (
                <p style={{ margin: '0 0 8px', color: t.color.warning }}>
                  Warnings: {dryRunResult.warnings.join('; ')}
                </p>
              )}
              {dryRunResult.preview.length > 0 && (
                <table style={{ width: '100%', fontSize: t.font.sizeXs, borderCollapse: 'collapse' }}>
                  <thead><tr><th style={{ textAlign: 'left', padding: 4 }}>ID</th><th style={{ textAlign: 'left', padding: 4 }}>Touchpoints</th><th style={{ textAlign: 'left', padding: 4 }}>Value</th></tr></thead>
                  <tbody>
                    {dryRunResult.preview.slice(0, 10).map((r, i) => (
                      <tr key={i}><td style={{ padding: 4 }}>{r.id}</td><td style={{ padding: 4 }}>{r.touchpoints}</td><td style={{ padding: 4 }}>{r.value}</td></tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
          {importCdpMutation.error && <p style={{ color: t.color.danger, fontSize: t.font.sizeSm, marginTop: 8 }}>{(importCdpMutation.error as Error).message}</p>}
        </div>
      )}
    </div>
  )
}

function ImportLogPanel({
  runs,
  filters,
  onFiltersChange,
  onRunClick,
  drawerRunId,
  drawerData,
  onDrawerClose,
  onRerun,
  formatRelativeTime,
}: {
  runs: Array<Record<string, unknown>>
  filters: { status?: string; source?: string; since?: string; until?: string }
  onFiltersChange: (f: { status?: string; source?: string; since?: string; until?: string }) => void
  onRunClick: (id: string) => void
  drawerRunId: string | null
  drawerData?: Record<string, unknown> | null
  onDrawerClose: () => void
  onRerun: (runId: string) => void
  formatRelativeTime: (iso: string | null | undefined) => string
}) {
  const sourceLabel = (s: string) => ({ sample: 'Sample', upload: 'Upload', meiro_webhook: 'Meiro Webhook', meiro_pull: 'Meiro Pull', meiro: 'Meiro' }[s] || s)
  return (
    <div style={{ marginTop: t.space.lg, paddingTop: t.space.lg, borderTop: `1px solid ${t.color.border}` }}>
      <h4 style={{ margin: '0 0 12px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>Import Log</h4>
      <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', marginBottom: t.space.md }}>
        <select value={filters.status ?? ''} onChange={e => onFiltersChange({ ...filters, status: e.target.value || undefined })} style={{ padding: 6, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
          <option value="">All status</option>
          <option value="success">Success</option>
          <option value="error">Error</option>
        </select>
        <select value={filters.source ?? ''} onChange={e => onFiltersChange({ ...filters, source: e.target.value || undefined })} style={{ padding: 6, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
          <option value="">All sources</option>
          <option value="sample">Sample</option>
          <option value="upload">Upload</option>
          <option value="meiro_webhook">Meiro Webhook</option>
          <option value="meiro_pull">Meiro Pull</option>
          <option value="meiro">Meiro</option>
        </select>
      </div>
      {runs.length ? (
        <div style={{ overflow: 'auto', maxHeight: 320 }}>
          <table style={{ width: '100%', fontSize: t.font.sizeSm, borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Time</th>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Source</th>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Status</th>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Total</th>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Converted</th>
                <th style={{ textAlign: 'left', padding: 8, borderBottom: `2px solid ${t.color.border}` }}>Channels</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r: Record<string, unknown>) => (
                <tr key={String(r.id)} onClick={() => onRunClick(String(r.id))} style={{ cursor: 'pointer', backgroundColor: drawerRunId === r.id ? t.color.borderLight : undefined }}>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{formatRelativeTime((r.at ?? r.finished_at ?? r.started_at) as string)}</td>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{sourceLabel(String(r.source ?? ''))}</td>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <span style={{ color: r.status === 'success' ? t.color.success : t.color.danger }}>{String(r.status)}</span>
                  </td>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{String(r.total ?? r.count ?? 0)}</td>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{String(r.converted ?? 0)}</td>
                  <td style={{ padding: 8, borderBottom: `1px solid ${t.color.borderLight}` }}>{(r.channels_detected as string[] ?? []).slice(0, 5).join(', ')}{(r.channels_detected as string[] ?? []).length > 5 ? '…' : ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p style={{ margin: 0, color: t.color.textMuted }}>No imports yet.</p>
      )}
      {drawerRunId && (
        <>
          <div onClick={onDrawerClose} style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.3)', zIndex: 999 }} />
          <div style={{ position: 'fixed', top: 0, right: 0, bottom: 0, width: 440, backgroundColor: t.color.surface, boxShadow: '-4px 0 12px rgba(0,0,0,0.1)', zIndex: 1000, overflow: 'auto', padding: t.space.xl }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg }}>
              <strong>Run details</strong>
              <button onClick={onDrawerClose} style={{ padding: '4px 12px', border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, cursor: 'pointer' }}>Close</button>
            </div>
            {drawerData ? (
              <div style={{ fontSize: t.font.sizeSm }}>
                <p><strong>ID:</strong> {String(drawerData.id)}</p>
                <p><strong>Source:</strong> {sourceLabel(String(drawerData.source ?? ''))}</p>
                <p><strong>Status:</strong> {String(drawerData.status)}</p>
                <p><strong>Valid:</strong> {String(drawerData.valid ?? 0)} / {String(drawerData.total ?? 0)}</p>
                <p><strong>Converted:</strong> {String(drawerData.converted ?? 0)}</p>
                <p><strong>Channels:</strong> {(drawerData.channels_detected as string[] ?? []).join(', ')}</p>
                {(drawerData.validation_summary as Record<string, unknown>)?.top_errors?.length > 0 && (
                  <div style={{ marginTop: t.space.md }}>
                    <strong>Top errors</strong>
                    {(drawerData.validation_summary as { top_errors?: Array<{ message: string }> }).top_errors?.slice(0, 5).map((e, i) => (
                      <p key={i} style={{ margin: '4px 0', color: t.color.danger, fontSize: t.font.sizeXs }}>{e.message}</p>
                    ))}
                  </div>
                )}
                {(drawerData.validation_summary as Record<string, unknown>)?.top_warnings?.length > 0 && (
                  <div style={{ marginTop: t.space.md }}>
                    <strong>Top warnings</strong>
                    {(drawerData.validation_summary as { top_warnings?: Array<{ message: string }> }).top_warnings?.slice(0, 5).map((w, i) => (
                      <p key={i} style={{ margin: '4px 0', color: t.color.warning, fontSize: t.font.sizeXs }}>{w.message}</p>
                    ))}
                  </div>
                )}
                {drawerData.config_snapshot && Object.keys(drawerData.config_snapshot as object).length > 0 && (
                  <div style={{ marginTop: t.space.md }}>
                    <strong>Config snapshot</strong>
                    <pre style={{ backgroundColor: t.color.borderLight, padding: 8, borderRadius: t.radius.sm, fontSize: t.font.sizeXs }}>{JSON.stringify(drawerData.config_snapshot, null, 2)}</pre>
                  </div>
                )}
                {(drawerData.preview_rows as unknown[])?.length > 0 && (
                  <div style={{ marginTop: t.space.md }}>
                    <strong>Preview</strong>
                    <table style={{ width: '100%', fontSize: t.font.sizeXs }}>
                      {(drawerData.preview_rows as Array<Record<string, unknown>>).slice(0, 10).map((row, i) => (
                        <tr key={i}><td>{String(row.customer_id ?? row.id ?? '?')}</td><td>{String(row.touchpoints ?? 0)} tps</td><td>{String(row.value ?? row.converted ?? '')}</td></tr>
                      ))}
                    </table>
                  </div>
                )}
                {['sample', 'meiro_webhook', 'meiro_pull', 'meiro'].includes(String(drawerData.source)) && (
                  <button onClick={() => onRerun(String(drawerData.id))} style={{ marginTop: t.space.lg, padding: '8px 16px', backgroundColor: t.color.accent, color: 'white', border: 'none', borderRadius: t.radius.sm, cursor: 'pointer' }}>
                    Re-run with same settings
                  </button>
                )}
              </div>
            ) : (
              <p>Loading…</p>
            )}
          </div>
        </>
      )}
    </div>
  )
}

function ImportConfirmModal({
  precheck,
  source,
  importNote,
  onImportNoteChange,
  onConfirm,
  onCancel,
  tokens,
}: {
  precheck?: Record<string, unknown>
  source: string
  importNote: string
  onImportNoteChange: (v: string) => void
  onConfirm: () => void
  onCancel: () => void
  tokens: typeof t
}) {
  const w = precheck as { would_overwrite?: boolean; current_count?: number; current_converted?: number; current_channels?: string[]; incoming_count?: number; incoming_converted?: number; incoming_channels?: string[]; warning?: string } | undefined
  return (
    <>
      <div onClick={onCancel} style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.4)', zIndex: 999 }} />
      <div style={{ position: 'fixed', left: '50%', top: '50%', transform: 'translate(-50%, -50%)', width: 420, maxWidth: '90vw', backgroundColor: tokens.color.surface, borderRadius: tokens.radius.lg, boxShadow: '0 8px 32px rgba(0,0,0,0.2)', zIndex: 1000, padding: tokens.space.xl }}>
        <h4 style={{ margin: '0 0 12px' }}>Confirm overwrite</h4>
        <p style={{ margin: '0 0 16px', fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary }}>
          This import will replace existing journey data.
        </p>
        {w?.would_overwrite && (
          <div style={{ marginBottom: tokens.space.lg, fontSize: tokens.font.sizeSm }}>
            <p><strong>Current:</strong> {w.current_count ?? 0} journeys, {w.current_converted ?? 0} converted, {(w.current_channels ?? []).join(', ') || '—'}</p>
            <p><strong>Incoming:</strong> {w.incoming_count ?? 0} journeys, {w.incoming_converted ?? 0} converted</p>
            {w.warning && <p style={{ color: tokens.color.warning, marginTop: 8 }}>{w.warning}</p>}
          </div>
        )}
        <label style={{ display: 'block', marginBottom: 8, fontSize: tokens.font.sizeSm }}>Import note (optional)</label>
        <input type="text" value={importNote} onChange={e => onImportNoteChange(e.target.value)} placeholder="e.g. Q1 refresh" style={{ width: '100%', padding: 8, marginBottom: tokens.space.lg, border: `1px solid ${tokens.color.border}`, borderRadius: tokens.radius.sm, boxSizing: 'border-box' }} />
        <div style={{ display: 'flex', gap: tokens.space.sm, justifyContent: 'flex-end' }}>
          <button onClick={onCancel} style={{ padding: '8px 16px', border: `1px solid ${tokens.color.border}`, borderRadius: tokens.radius.sm, cursor: 'pointer' }}>Cancel</button>
          <button onClick={onConfirm} style={{ padding: '8px 16px', backgroundColor: tokens.color.accent, color: 'white', border: 'none', borderRadius: tokens.radius.sm, cursor: 'pointer' }}>Confirm import</button>
        </div>
      </div>
    </>
  )
}
