import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

// --- Types (aligned with backend) ---
interface ExpenseEntry {
  id: string
  channel: string
  cost_type: string
  amount: number
  currency: string
  reporting_currency?: string
  converted_amount?: number | null
  fx_rate?: number | null
  fx_date?: string | null
  period?: string | null
  service_period_start?: string | null
  service_period_end?: string | null
  entry_date?: string | null
  invoice_ref?: string | null
  external_link?: string | null
  notes?: string | null
  source_type: string
  source_name?: string | null
  status: string
  created_at?: string | null
  updated_at?: string | null
  deleted_at?: string | null
  actor_type?: string
  change_note?: string | null
}

interface ExpenseSummary {
  by_channel: Record<string, number>
  total: number
  channels: string[]
  reporting_currency?: string
  imported_share_pct?: number
  manual_total?: number
  imported_total?: number
  unknown_total?: number
}

interface AuditEvent {
  expense_id: string
  timestamp: string
  event_type: string
  actor_type: string
  note?: string | null
}

interface ImportSourceHealth {
  source: string
  status: string
  last_success_at: string | null
  last_attempt_at: string | null
  records_imported: number | null
  period_start: string | null
  period_end: string | null
  last_error: string | null
  action_hint: string | null
  syncing: boolean
}

interface ImportHealthResponse {
  sources: ImportSourceHealth[]
  overall_freshness: string
}

interface ReconciliationRow {
  source: string
  platform_total: number
  app_normalized_total: number
  delta: number
  delta_pct: number
  status: string
}

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
} as const

const CHANNEL_OPTIONS = [
  'google_ads', 'meta_ads', 'linkedin_ads', 'email', 'whatsapp',
  'tiktok_ads', 'twitter_ads', 'sms', 'direct_mail', 'other',
]

const COST_TYPES = [
  'Media Spend',
  'Platform Fees',
  'Agency Fees',
  'Creative/Production',
  'Tools/Software',
  'Other',
]

const CURRENCIES = ['USD', 'EUR', 'CZK', 'GBP']

const COLORS: Record<string, string> = {
  google_ads: '#4285F4', meta_ads: '#1877F2', linkedin_ads: '#0A66C2',
  email: '#28a745', whatsapp: '#25D366', tiktok_ads: '#000000',
  twitter_ads: '#1DA1F2', sms: '#fd7e14', direct_mail: '#6c757d', other: '#adb5bd',
}

function buildExpensesQueryParams(filters: {
  includeDeleted?: boolean
  channel?: string
  cost_type?: string
  source_type?: string
  currency?: string
  status?: string
  service_period_start?: string
  service_period_end?: string
  search?: string
}) {
  const p = new URLSearchParams()
  if (filters.includeDeleted) p.set('include_deleted', 'true')
  if (filters.channel) p.set('channel', filters.channel)
  if (filters.cost_type) p.set('cost_type', filters.cost_type)
  if (filters.source_type) p.set('source_type', filters.source_type)
  if (filters.currency) p.set('currency', filters.currency)
  if (filters.status) p.set('status', filters.status)
  if (filters.service_period_start) p.set('service_period_start', filters.service_period_start)
  if (filters.service_period_end) p.set('service_period_end', filters.service_period_end)
  if (filters.search) p.set('search', filters.search)
  return p.toString()
}

export default function ExpenseManager() {
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState<'expenses' | 'imports'>('expenses')

  // Filters for expenses list
  const [includeDeleted, setIncludeDeleted] = useState(false)
  const [filterChannel, setFilterChannel] = useState('')
  const [filterCostType, setFilterCostType] = useState('')
  const [filterSourceType, setFilterSourceType] = useState('')
  const [filterCurrency, setFilterCurrency] = useState('')
  const [filterStatus, setFilterStatus] = useState('')
  const [servicePeriodStart, setServicePeriodStart] = useState('')
  const [servicePeriodEnd, setServicePeriodEnd] = useState('')
  const [searchText, setSearchText] = useState('')

  // Add form state (extended)
  const [newChannel, setNewChannel] = useState('google_ads')
  const [newCostType, setNewCostType] = useState('Media Spend')
  const [newAmount, setNewAmount] = useState('')
  const [newCurrency, setNewCurrency] = useState('USD')
  const [newServicePeriodStart, setNewServicePeriodStart] = useState('')
  const [newServicePeriodEnd, setNewServicePeriodEnd] = useState('')
  const [newInvoiceRef, setNewInvoiceRef] = useState('')
  const [newExternalLink, setNewExternalLink] = useState('')
  const [newNotes, setNewNotes] = useState('')

  // Details drawer
  const [selectedExpenseId, setSelectedExpenseId] = useState<string | null>(null)
  const [auditEvents, setAuditEvents] = useState<AuditEvent[]>([])

  const queryParams = useMemo(() => buildExpensesQueryParams({
    includeDeleted,
    channel: filterChannel || undefined,
    cost_type: filterCostType || undefined,
    source_type: filterSourceType || undefined,
    currency: filterCurrency || undefined,
    status: filterStatus || undefined,
    service_period_start: servicePeriodStart || undefined,
    service_period_end: servicePeriodEnd || undefined,
    search: searchText || undefined,
  }), [includeDeleted, filterChannel, filterCostType, filterSourceType, filterCurrency, filterStatus, servicePeriodStart, servicePeriodEnd, searchText])

  const summaryParams = useMemo(() => {
    const p = new URLSearchParams()
    if (includeDeleted) p.set('include_deleted', 'true')
    if (servicePeriodStart) p.set('service_period_start', servicePeriodStart)
    if (servicePeriodEnd) p.set('service_period_end', servicePeriodEnd)
    return p.toString()
  }, [includeDeleted, servicePeriodStart, servicePeriodEnd])

  const expensesQuery = useQuery<ExpenseEntry[]>({
    queryKey: ['expenses', queryParams],
    queryFn: async () => {
      const res = await fetch(`/api/expenses?${queryParams}`)
      if (!res.ok) throw new Error('Failed to fetch expenses')
      return res.json()
    },
  })

  const summaryQuery = useQuery<ExpenseSummary>({
    queryKey: ['expenses-summary', summaryParams],
    queryFn: async () => {
      const res = await fetch(`/api/expenses/summary?${summaryParams}`)
      if (!res.ok) throw new Error('Failed to fetch summary')
      return res.json()
    },
  })

  const importHealthQuery = useQuery<ImportHealthResponse>({
    queryKey: ['imports-health'],
    queryFn: async () => {
      const res = await fetch('/api/imports/health')
      if (!res.ok) throw new Error('Failed to fetch import health')
      return res.json()
    },
    refetchInterval: activeTab === 'imports' ? 10000 : false,
  })

  const reconciliationParams = useMemo(() => {
    const p = new URLSearchParams()
    if (servicePeriodStart) p.set('service_period_start', servicePeriodStart)
    if (servicePeriodEnd) p.set('service_period_end', servicePeriodEnd)
    return p.toString()
  }, [servicePeriodStart, servicePeriodEnd])

  const reconciliationQuery = useQuery<{ rows: ReconciliationRow[]; period_start: string; period_end: string }>({
    queryKey: ['imports-reconciliation', reconciliationParams],
    queryFn: async () => {
      const res = await fetch(`/api/imports/reconciliation?${reconciliationParams}`)
      if (!res.ok) throw new Error('Failed to fetch reconciliation')
      return res.json()
    },
  })
  const hasReconciliationWarning = (reconciliationQuery.data?.rows ?? []).some(
    r => r.status === 'Warning' || r.status === 'Critical'
  )

  const [drilldownSource, setDrilldownSource] = useState<string | null>(null)
  const drilldownParams = useMemo(() => {
    const p = new URLSearchParams()
    if (drilldownSource) p.set('source', drilldownSource)
    if (servicePeriodStart) p.set('service_period_start', servicePeriodStart)
    if (servicePeriodEnd) p.set('service_period_end', servicePeriodEnd)
    return p.toString()
  }, [drilldownSource, servicePeriodStart, servicePeriodEnd])

  const drilldownQuery = useQuery<{ source: string; missing_days: string[]; missing_campaigns: string[] }>({
    queryKey: ['imports-drilldown', drilldownParams],
    queryFn: async () => {
      const res = await fetch(`/api/imports/reconciliation/drilldown?${drilldownParams}`)
      if (!res.ok) throw new Error('Failed to fetch drilldown')
      return res.json()
    },
    enabled: activeTab === 'imports' && !!drilldownSource,
  })

  const addMutation = useMutation({
    mutationFn: async (entry: Partial<ExpenseEntry>) => {
      const payload = {
        channel: entry.channel,
        cost_type: entry.cost_type ?? 'Media Spend',
        amount: Number(entry.amount),
        currency: entry.currency ?? 'USD',
        reporting_currency: entry.reporting_currency ?? 'USD',
        service_period_start: entry.service_period_start || undefined,
        service_period_end: entry.service_period_end || undefined,
        invoice_ref: entry.invoice_ref || undefined,
        external_link: entry.external_link || undefined,
        notes: entry.notes || undefined,
        source_type: 'manual',
        source_name: null,
        status: 'active',
        actor_type: 'manual',
      }
      const res = await fetch('/api/expenses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!res.ok) throw new Error('Failed to add expense')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
      setNewAmount('')
      setNewNotes('')
      setNewInvoiceRef('')
      setNewExternalLink('')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/expenses/${id}?change_note=Soft+delete`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Failed to delete')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
      setSelectedExpenseId(null)
    },
  })

  const restoreMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/expenses/${id}/restore`, { method: 'POST' })
      if (!res.ok) throw new Error('Failed to restore')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
      setSelectedExpenseId(null)
    },
  })

  const syncMutation = useMutation({
    mutationFn: async (source: string) => {
      const res = await fetch(`/api/imports/sync/${source}`, { method: 'POST' })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Sync failed')
      }
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['imports-health'] })
      queryClient.invalidateQueries({ queryKey: ['imports-reconciliation'] })
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
    },
  })

  const handleAdd = () => {
    const amount = parseFloat(newAmount)
    if (isNaN(amount) || amount <= 0) return
    const start = newServicePeriodStart || undefined
    const end = newServicePeriodEnd || undefined
    addMutation.mutate({
      channel: newChannel,
      cost_type: newCostType,
      amount,
      currency: newCurrency,
      reporting_currency: 'USD',
      service_period_start: start,
      service_period_end: end,
      invoice_ref: newInvoiceRef || undefined,
      external_link: newExternalLink || undefined,
      notes: newNotes,
    })
  }

  const openDrawer = async (id: string) => {
    setSelectedExpenseId(id)
    try {
      const res = await fetch(`/api/expenses/${id}/audit`)
      if (res.ok) setAuditEvents(await res.json())
      else setAuditEvents([])
    } catch {
      setAuditEvents([])
    }
  }

  const summary = summaryQuery.data
  const expenses = expensesQuery.data || []
  const reportingCurrency = summary?.reporting_currency || 'USD'
  const chartData = summary ? Object.entries(summary.by_channel).map(([channel, amount]) => ({
    channel, amount, fill: COLORS[channel] || '#6c757d',
  })).sort((a, b) => b.amount - a.amount) : []

  const health = importHealthQuery.data
  const overallFreshness = health?.overall_freshness || 'unknown'
  const hasImportProblems = overallFreshness === 'Stale' || overallFreshness === 'Broken'
  const selectedExpense = selectedExpenseId ? expenses.find(e => e.id === selectedExpenseId) : null

  // Converted amount preview for add form (simple 1:1 for same currency, else placeholder)
  const reportingPreview = useMemo(() => {
    const amt = parseFloat(newAmount)
    if (isNaN(amt) || amt <= 0) return null
    if (newCurrency === 'USD') return amt
    return null // Could integrate FX later; for now show "—" or same
  }, [newAmount, newCurrency])

  return (
    <div>
      <h2 style={{ margin: '0 0 8px', fontSize: '22px', fontWeight: '700', color: '#212529' }}>Expense Management</h2>
      <p style={{ margin: '0 0 16px', fontSize: '14px', color: '#6c757d' }}>
        Track marketing spend across channels with full ledger attributes, service periods, and import health.
      </p>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        <button
          onClick={() => setActiveTab('expenses')}
          style={{
            padding: '8px 16px',
            fontWeight: 600,
            borderRadius: 8,
            border: activeTab === 'expenses' ? '2px solid #0d6efd' : '1px solid #dee2e6',
            background: activeTab === 'expenses' ? '#e7f1ff' : '#fff',
            color: activeTab === 'expenses' ? '#0d6efd' : '#495057',
            cursor: 'pointer',
          }}
        >
          Expenses
        </button>
        <button
          onClick={() => setActiveTab('imports')}
          style={{
            padding: '8px 16px',
            fontWeight: 600,
            borderRadius: 8,
            border: activeTab === 'imports' ? '2px solid #0d6efd' : '1px solid #dee2e6',
            background: activeTab === 'imports' ? '#e7f1ff' : '#fff',
            color: activeTab === 'imports' ? '#0d6efd' : '#495057',
            cursor: 'pointer',
          }}
        >
          Imports
        </button>
      </div>

      {activeTab === 'expenses' && (
        <>
          {/* Alert banner when imports are stale/broken */}
          {hasImportProblems && (
            <div
              style={{
                marginBottom: 16,
                padding: 12,
                borderRadius: 8,
                background: overallFreshness === 'Broken' ? '#f8d7da' : '#fff3cd',
                border: `1px solid ${overallFreshness === 'Broken' ? '#f5c2c7' : '#ffecb5'}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                flexWrap: 'wrap',
                gap: 8,
              }}
            >
              <span style={{ fontWeight: 600, color: '#856404' }}>
                Import status: {overallFreshness}. Some expense data may be outdated or missing.
              </span>
              <button
                onClick={() => setActiveTab('imports')}
                style={{
                  padding: '6px 12px',
                  fontSize: 13,
                  fontWeight: 600,
                  background: '#0d6efd',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 6,
                  cursor: 'pointer',
                }}
              >
                View Imports
              </button>
            </div>
          )}

          {/* Summary tiles */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 24 }}>
            <div style={{ ...card, textAlign: 'center' }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase', letterSpacing: '0.5px', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
                Total Spend ({reportingCurrency})
                {hasReconciliationWarning && (
                  <span title="Reconciliation has warnings or critical deltas" style={{ color: '#856404', fontSize: 16 }}>⚠️</span>
                )}
              </div>
              <div style={{ fontSize: '28px', fontWeight: '700', color: '#dc3545', marginTop: 8 }}>
                {reportingCurrency === 'USD' && '$'}
                {(summary?.total ?? 0).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
                {reportingCurrency !== 'USD' && ` ${reportingCurrency}`}
              </div>
              <div style={{ fontSize: '12px', color: '#6c757d', marginTop: 4 }}>
                Across {summary?.channels?.length ?? 0} channels
              </div>
            </div>
            <div style={{ ...card, textAlign: 'center' }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase' }}>
                Imported share
              </div>
              <div style={{ fontSize: '28px', fontWeight: '700', color: '#0d6efd', marginTop: 8 }}>
                {(summary?.imported_share_pct ?? 0).toFixed(1)}%
              </div>
              <div style={{ fontSize: '12px', color: '#6c757d', marginTop: 4 }}>
                of spend from imports
              </div>
            </div>
            <div style={{ ...card, textAlign: 'center' }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase' }}>
                Freshness
              </div>
              <div style={{ marginTop: 8 }}>
                <span
                  style={{
                    padding: '4px 10px',
                    borderRadius: 6,
                    fontSize: 14,
                    fontWeight: 600,
                    backgroundColor:
                      overallFreshness === 'Healthy' ? '#d1e7dd' : overallFreshness === 'Broken' ? '#f8d7da' : '#fff3cd',
                    color: overallFreshness === 'Healthy' ? '#0f5132' : overallFreshness === 'Broken' ? '#842029' : '#856404',
                  }}
                >
                  {overallFreshness}
                </span>
              </div>
            </div>
            <div style={{ ...card, textAlign: 'center' }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase' }}>
                Unmapped / Unknown
              </div>
              <div style={{ fontSize: '28px', fontWeight: '700', color: '#6c757d', marginTop: 8 }}>
                {(summary?.unknown_total ?? 0).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
              </div>
              <div style={{ fontSize: '12px', color: '#6c757d', marginTop: 4 }}>
                {reportingCurrency}
              </div>
            </div>
          </div>

          {/* Chart */}
          <div style={{ ...card, marginBottom: 24 }}>
            <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>
              Spend by Channel ({reportingCurrency})
            </h3>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={chartData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis type="number" tickFormatter={(v) => `${reportingCurrency === 'USD' ? '$' : ''}${v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v}`} />
                <YAxis type="category" dataKey="channel" width={110} tick={{ fontSize: 12 }} />
                <Tooltip formatter={(value: number) => `${reportingCurrency === 'USD' ? '$' : ''}${value.toLocaleString()}`} />
                <Bar dataKey="amount" fill="#dc3545" name="Spend" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Add Expense Form (extended) */}
          <div style={{ ...card, marginBottom: 24 }}>
            <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Add Expense</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 12, alignItems: 'end', maxWidth: 1000 }}>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Channel</label>
                <select
                  value={newChannel}
                  onChange={(e) => setNewChannel(e.target.value)}
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6 }}
                >
                  {CHANNEL_OPTIONS.map(ch => (
                    <option key={ch} value={ch}>{ch.replace(/_/g, ' ')}</option>
                  ))}
                </select>
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Cost Type</label>
                <select
                  value={newCostType}
                  onChange={(e) => setNewCostType(e.target.value)}
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6 }}
                >
                  {COST_TYPES.map(ct => (
                    <option key={ct} value={ct}>{ct}</option>
                  ))}
                </select>
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Amount</label>
                <input
                  type="number"
                  value={newAmount}
                  onChange={(e) => setNewAmount(e.target.value)}
                  placeholder="0.00"
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Currency</label>
                <select
                  value={newCurrency}
                  onChange={(e) => setNewCurrency(e.target.value)}
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6 }}
                >
                  {CURRENCIES.map(c => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Service period start</label>
                <input
                  type="date"
                  value={newServicePeriodStart}
                  onChange={(e) => setNewServicePeriodStart(e.target.value)}
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Service period end</label>
                <input
                  type="date"
                  value={newServicePeriodEnd}
                  onChange={(e) => setNewServicePeriodEnd(e.target.value)}
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Invoice/PO ref</label>
                <input
                  type="text"
                  value={newInvoiceRef}
                  onChange={(e) => setNewInvoiceRef(e.target.value)}
                  placeholder="Optional"
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>External link</label>
                <input
                  type="url"
                  value={newExternalLink}
                  onChange={(e) => setNewExternalLink(e.target.value)}
                  placeholder="Optional URL"
                  style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div style={{ gridColumn: '1 / -1' }}>
                <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Notes</label>
                <input
                  type="text"
                  value={newNotes}
                  onChange={(e) => setNewNotes(e.target.value)}
                  placeholder="Optional notes"
                  style={{ width: '100%', maxWidth: 400, padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                {reportingPreview != null && (
                  <span style={{ fontSize: 13, color: '#6c757d' }}>
                    ≈ {reportingCurrency} {reportingPreview.toLocaleString()}
                  </span>
                )}
                <button
                  onClick={handleAdd}
                  disabled={addMutation.isPending || !newAmount}
                  style={{
                    padding: '10px 24px', fontSize: '14px', fontWeight: '600',
                    backgroundColor: (!newAmount || addMutation.isPending) ? '#ccc' : '#28a745',
                    color: 'white', border: 'none', borderRadius: 6,
                    cursor: (!newAmount || addMutation.isPending) ? 'not-allowed' : 'pointer',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {addMutation.isPending ? 'Adding...' : 'Add'}
                </button>
              </div>
            </div>
          </div>

          {/* Filters + Table */}
          <div style={card}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, alignItems: 'center', marginBottom: 16 }}>
              <span style={{ fontWeight: 600, color: '#495057' }}>Filters</span>
              <input
                type="date"
                value={servicePeriodStart}
                onChange={(e) => setServicePeriodStart(e.target.value)}
                placeholder="Period start"
                style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}
              />
              <input
                type="date"
                value={servicePeriodEnd}
                onChange={(e) => setServicePeriodEnd(e.target.value)}
                placeholder="Period end"
                style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}
              />
              <select value={filterChannel} onChange={(e) => setFilterChannel(e.target.value)} style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}>
                <option value="">All channels</option>
                {CHANNEL_OPTIONS.map(ch => <option key={ch} value={ch}>{ch}</option>)}
              </select>
              <select value={filterCostType} onChange={(e) => setFilterCostType(e.target.value)} style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}>
                <option value="">All cost types</option>
                {COST_TYPES.map(ct => <option key={ct} value={ct}>{ct}</option>)}
              </select>
              <select value={filterSourceType} onChange={(e) => setFilterSourceType(e.target.value)} style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}>
                <option value="">All sources</option>
                <option value="import">Imported</option>
                <option value="manual">Manual</option>
              </select>
              <select value={filterCurrency} onChange={(e) => setFilterCurrency(e.target.value)} style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}>
                <option value="">All currencies</option>
                {CURRENCIES.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              <select value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)} style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6 }}>
                <option value="">All statuses</option>
                <option value="active">Active</option>
                <option value="deleted">Deleted</option>
              </select>
              <input
                type="text"
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                placeholder="Search notes, invoice, link"
                style={{ padding: '8px', fontSize: 13, border: '1px solid #dee2e6', borderRadius: 6, minWidth: 180 }}
              />
              <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
                <input type="checkbox" checked={includeDeleted} onChange={(e) => setIncludeDeleted(e.target.checked)} />
                <span style={{ fontSize: 13 }}>Show deleted</span>
              </label>
            </div>

            <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>All Expenses</h3>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
                <thead>
                  <tr style={{ backgroundColor: '#f8f9fa' }}>
                    {['Channel', 'Cost Type', 'Amount (orig)', 'Amount (' + reportingCurrency + ')', 'Service Period', 'Source', 'Status', 'Notes', 'Actions'].map(h => (
                      <th key={h} style={{ padding: '12px 16px', textAlign: h === 'Actions' ? 'center' : 'left', fontWeight: '600', color: '#495057', borderBottom: '2px solid #e9ecef' }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {expenses.map((exp, idx) => (
                    <tr
                      key={exp.id}
                      onClick={() => openDrawer(exp.id)}
                      style={{
                        borderBottom: '1px solid #f1f3f5',
                        backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa',
                        cursor: 'pointer',
                      }}
                    >
                      <td style={{ padding: '10px 16px' }}>
                        <span style={{
                          display: 'inline-block', padding: '2px 10px', borderRadius: 4,
                          fontSize: '12px', fontWeight: '600',
                          backgroundColor: `${COLORS[exp.channel] || '#6c757d'}20`,
                          color: COLORS[exp.channel] || '#6c757d',
                        }}>
                          {exp.channel.replace(/_/g, ' ')}
                        </span>
                      </td>
                      <td style={{ padding: '10px 16px' }}>{exp.cost_type ?? '-'}</td>
                      <td style={{ padding: '10px 16px' }}>{exp.amount.toLocaleString()} {exp.currency}</td>
                      <td style={{ padding: '10px 16px', fontWeight: 600, color: '#dc3545' }}>
                        {(exp.converted_amount ?? exp.amount).toLocaleString()} {exp.reporting_currency || reportingCurrency}
                      </td>
                      <td style={{ padding: '10px 16px', color: '#6c757d' }}>
                        {exp.service_period_start && exp.service_period_end
                          ? `${exp.service_period_start} – ${exp.service_period_end}`
                          : exp.period || '-'}
                      </td>
                      <td style={{ padding: '10px 16px', fontSize: 13 }}>
                        {exp.source_type === 'import' ? `Imported (${exp.source_name || '-'})` : 'Manual'}
                      </td>
                      <td style={{ padding: '10px 16px' }}>
                        <span style={{
                          padding: '2px 8px', borderRadius: 4, fontSize: 12, fontWeight: 600,
                          background: exp.status === 'deleted' ? '#f8d7da' : '#d1e7dd',
                          color: exp.status === 'deleted' ? '#842029' : '#0f5132',
                        }}>
                          {exp.status}
                        </span>
                      </td>
                      <td style={{ padding: '10px 16px', color: '#6c757d', fontSize: 13, maxWidth: 120, overflow: 'hidden', textOverflow: 'ellipsis' }}>{exp.notes || '-'}</td>
                      <td style={{ padding: '10px 16px', textAlign: 'center' }} onClick={e => e.stopPropagation()}>
                        {exp.status === 'deleted' ? (
                          <button
                            onClick={() => restoreMutation.mutate(exp.id)}
                            style={{ padding: '4px 12px', fontSize: '12px', fontWeight: '600', background: '#0d6efd', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer' }}
                          >
                            Restore
                          </button>
                        ) : (
                          <button
                            onClick={() => window.confirm('Soft delete this expense?') && deleteMutation.mutate(exp.id)}
                            style={{ padding: '4px 12px', fontSize: '12px', fontWeight: '600', background: '#fff', color: '#dc3545', border: '1px solid #dc3545', borderRadius: 4, cursor: 'pointer' }}
                          >
                            Delete
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                  {expenses.length === 0 && (
                    <tr>
                      <td colSpan={9} style={{ padding: 24, textAlign: 'center', color: '#6c757d' }}>
                        No expenses match the filters. Add one above or run an import.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Details drawer */}
          {selectedExpense && (
            <div
              style={{
                position: 'fixed',
                top: 0,
                right: 0,
                width: 420,
                maxWidth: '100%',
                height: '100%',
                background: '#fff',
                boxShadow: '-4px 0 12px rgba(0,0,0,0.1)',
                zIndex: 1000,
                overflow: 'auto',
                padding: 24,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                <h3 style={{ margin: 0, fontSize: 18, fontWeight: 600 }}>Expense details</h3>
                <button onClick={() => setSelectedExpenseId(null)} style={{ padding: '6px 12px', border: '1px solid #dee2e6', borderRadius: 6, background: '#fff', cursor: 'pointer' }}>Close</button>
              </div>
              <dl style={{ margin: 0, fontSize: 14 }}>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Channel</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.channel}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Cost type</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.cost_type}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Amount (original)</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.amount} {selectedExpense.currency}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Amount (reporting)</dt>
                <dd style={{ marginLeft: 0 }}>{(selectedExpense.converted_amount ?? selectedExpense.amount).toLocaleString()} {selectedExpense.reporting_currency || reportingCurrency}</dd>
                {(selectedExpense.fx_rate != null || selectedExpense.fx_date) && (
                  <>
                    <dt style={{ color: '#6c757d', marginTop: 8 }}>FX</dt>
                    <dd style={{ marginLeft: 0 }}>Rate {selectedExpense.fx_rate ?? '—'} @ {selectedExpense.fx_date ?? '—'}</dd>
                  </>
                )}
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Service period</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.service_period_start && selectedExpense.service_period_end ? `${selectedExpense.service_period_start} – ${selectedExpense.service_period_end}` : selectedExpense.period || '—'}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Source</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.source_type === 'import' ? `Imported (${selectedExpense.source_name || '-'})` : 'Manual'}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Invoice/PO ref</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.invoice_ref || '—'}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>External link</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.external_link ? <a href={selectedExpense.external_link} target="_blank" rel="noopener noreferrer">{selectedExpense.external_link}</a> : '—'}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Created / Updated</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.created_at ?? '—'} / {selectedExpense.updated_at ?? '—'}</dd>
                <dt style={{ color: '#6c757d', marginTop: 8 }}>Notes</dt>
                <dd style={{ marginLeft: 0 }}>{selectedExpense.notes || '—'}</dd>
              </dl>
              <h4 style={{ marginTop: 24, marginBottom: 8, fontSize: 14, fontWeight: 600 }}>Audit trail</h4>
              <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                {auditEvents.map((ev, i) => (
                  <li key={i} style={{ padding: '8px 0', borderBottom: '1px solid #f1f3f5', fontSize: 13 }}>
                    <strong>{ev.event_type}</strong> @ {ev.timestamp} ({ev.actor_type}) {ev.note ? `— ${ev.note}` : ''}
                  </li>
                ))}
                {auditEvents.length === 0 && <li style={{ color: '#6c757d' }}>No events</li>}
              </ul>
            </div>
          )}
        </>
      )}

      {activeTab === 'imports' && (
        <>
          <div style={{ ...card, marginBottom: 24 }}>
            <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Import Health</h3>
            <div style={{ display: 'grid', gap: 12 }}>
              {(importHealthQuery.data?.sources ?? []).map(s => (
                <div
                  key={s.source}
                  style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    alignItems: 'center',
                    gap: 12,
                    padding: 12,
                    border: '1px solid #e9ecef',
                    borderRadius: 8,
                    background: '#f8f9fa',
                  }}
                >
                  <span style={{ fontWeight: 600, minWidth: 120 }}>{s.source.replace(/_/g, ' ')}</span>
                  <span
                    style={{
                      padding: '2px 10px',
                      borderRadius: 6,
                      fontSize: 12,
                      fontWeight: 600,
                      backgroundColor: s.status === 'Healthy' ? '#d1e7dd' : s.status === 'Broken' ? '#f8d7da' : '#fff3cd',
                      color: s.status === 'Healthy' ? '#0f5132' : s.status === 'Broken' ? '#842029' : '#856404',
                    }}
                  >
                    {s.status}
                  </span>
                  <span style={{ fontSize: 13, color: '#6c757d' }}>
                    Last sync: {s.last_success_at ? new Date(s.last_success_at).toLocaleString() : 'Never'}
                  </span>
                  <span style={{ fontSize: 13, color: '#6c757d' }}>
                    Records: {s.records_imported ?? '—'}
                  </span>
                  {s.last_error && <span style={{ fontSize: 12, color: '#dc3545' }}>{s.last_error}</span>}
                  {s.action_hint && <span style={{ fontSize: 12, color: '#856404' }}>{s.action_hint}</span>}
                  <button
                    onClick={() => syncMutation.mutate(s.source)}
                    disabled={s.syncing || (syncMutation.isPending && syncMutation.variables === s.source)}
                    style={{
                      padding: '6px 14px',
                      fontSize: 13,
                      fontWeight: 600,
                      background: (s.syncing || (syncMutation.isPending && syncMutation.variables === s.source)) ? '#ccc' : '#0d6efd',
                      color: '#fff',
                      border: 'none',
                      borderRadius: 6,
                      cursor: (s.syncing || (syncMutation.isPending && syncMutation.variables === s.source)) ? 'not-allowed' : 'pointer',
                    }}
                  >
                    {s.syncing || (syncMutation.isPending && syncMutation.variables === s.source) ? 'Syncing...' : 'Sync now'}
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div style={{ ...card, marginBottom: 24 }}>
            <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Reconciliation</h3>
            <p style={{ margin: '0 0 12px', fontSize: 13, color: '#6c757d' }}>
              Service period: {servicePeriodStart || 'any'} – {servicePeriodEnd || 'any'}
            </p>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
                <thead>
                  <tr style={{ backgroundColor: '#f8f9fa' }}>
                    <th style={{ padding: '10px 16px', textAlign: 'left', fontWeight: 600 }}>Source</th>
                    <th style={{ padding: '10px 16px', textAlign: 'right', fontWeight: 600 }}>Platform total</th>
                    <th style={{ padding: '10px 16px', textAlign: 'right', fontWeight: 600 }}>App normalized</th>
                    <th style={{ padding: '10px 16px', textAlign: 'right', fontWeight: 600 }}>Delta</th>
                    <th style={{ padding: '10px 16px', textAlign: 'center', fontWeight: 600 }}>Status</th>
                    <th style={{ padding: '10px 16px', fontWeight: 600 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {(reconciliationQuery.data?.rows ?? []).map(row => (
                    <tr key={row.source} style={{ borderBottom: '1px solid #f1f3f5' }}>
                      <td style={{ padding: '10px 16px' }}>{row.source.replace(/_/g, ' ')}</td>
                      <td style={{ padding: '10px 16px', textAlign: 'right' }}>{row.platform_total.toLocaleString()}</td>
                      <td style={{ padding: '10px 16px', textAlign: 'right' }}>{row.app_normalized_total.toLocaleString()}</td>
                      <td style={{ padding: '10px 16px', textAlign: 'right' }}>{row.delta.toLocaleString()} ({row.delta_pct.toFixed(1)}%)</td>
                      <td style={{ padding: '10px 16px', textAlign: 'center' }}>
                        <span
                          style={{
                            padding: '2px 8px',
                            borderRadius: 4,
                            fontSize: 12,
                            fontWeight: 600,
                            backgroundColor: row.status === 'OK' ? '#d1e7dd' : row.status === 'Warning' ? '#fff3cd' : '#f8d7da',
                            color: row.status === 'OK' ? '#0f5132' : row.status === 'Warning' ? '#856404' : '#842029',
                          }}
                        >
                          {row.status}
                        </span>
                      </td>
                      <td style={{ padding: '10px 16px' }}>
                        <button
                          type="button"
                          onClick={() => setDrilldownSource(drilldownSource === row.source ? null : row.source)}
                          style={{ padding: '4px 10px', fontSize: 12, background: '#e9ecef', border: 'none', borderRadius: 4, cursor: 'pointer' }}
                        >
                          {drilldownSource === row.source ? 'Hide details' : 'View details'}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p style={{ marginTop: 12, fontSize: 12, color: '#6c757d' }}>
              Delta thresholds: OK ≤1%, Warning ≤5%, Critical &gt;5%.
            </p>
            {drilldownSource && drilldownQuery.data && (
              <div style={{ marginTop: 16, padding: 12, background: '#f8f9fa', borderRadius: 8, border: '1px solid #e9ecef' }}>
                <h4 style={{ margin: '0 0 8px', fontSize: 14 }}>Drilldown: {drilldownSource.replace(/_/g, ' ')}</h4>
                <p style={{ margin: 0, fontSize: 13, color: '#6c757d' }}>
                  <strong>Sample days in range:</strong> {drilldownQuery.data.missing_days?.length ? drilldownQuery.data.missing_days.join(', ') : '—'}
                </p>
                <p style={{ margin: '8px 0 0', fontSize: 13, color: '#6c757d' }}>
                  <strong>Campaigns:</strong> {drilldownQuery.data.missing_campaigns?.length ? drilldownQuery.data.missing_campaigns.join(', ') : '—'}
                </p>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
