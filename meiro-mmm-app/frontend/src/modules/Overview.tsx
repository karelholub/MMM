import { useMemo } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { ResponsiveContainer, PieChart, Pie, Cell } from 'recharts'
import { tokens } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'

type PageKey =
  | 'overview'
  | 'dashboard'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'

interface OverviewProps {
  lastPage: PageKey | null
  onNavigate: (page: PageKey) => void
  onConnectDataSources: () => void
}

interface ChannelSummary {
  channel: string
  spend: number
  attributed_value: number
  attributed_conversions: number
  attributed_share: number
  roas: number
}

interface PerformanceResponse {
  model: string
  channels: ChannelSummary[]
  total_spend: number
  total_attributed_value: number
  total_conversions: number
}

interface ExplainabilityConfidence {
  score: number
  label: string
}

interface ExplainabilitySummaryLite {
  data_health: {
    confidence?: ExplainabilityConfidence | null
    notes: string[]
  }
}

interface DQAlert {
  id: number
  triggered_at: string
  metric_value: number
  baseline_value?: number | null
  status: string
  message: string
  note?: string | null
  rule?: {
    name?: string | null
    metric_key?: string | null
    source?: string | null
    severity?: string | null
  } | null
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const SEVERITY_COLORS: Record<string, string> = {
  info: '#0ea5e9',
  warn: '#f59e0b',
  critical: '#dc2626',
}

function formatCurrency(val: number): string {
  if (!Number.isFinite(val)) return '—'
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`
  if (val >= 1_000) return `$${(val / 1_000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

export default function Overview({ lastPage, onNavigate, onConnectDataSources }: OverviewProps) {
  const t = tokens
  const {
    attributionModel,
    selectedConfigId,
    journeysSummary,
    journeysLoaded,
    isRunningAttribution,
    isLoadingSampleJourneys,
    runAllAttribution,
    loadSampleJourneys,
  } = useWorkspaceContext()

  const perfQuery = useQuery<PerformanceResponse>({
    queryKey: ['channel-performance', attributionModel, selectedConfigId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({ model: attributionModel })
      if (selectedConfigId) params.append('config_id', selectedConfigId)
      const res = await fetch(`/api/attribution/performance?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to fetch performance')
      return res.json()
    },
    enabled: journeysLoaded,
    refetchInterval: false,
  })

  const explainabilityQuery = useQuery<ExplainabilitySummaryLite>({
    queryKey: ['explainability-lite-overview', attributionModel, selectedConfigId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({
        scope: 'channel',
        model: attributionModel,
      })
      if (selectedConfigId) params.append('config_id', selectedConfigId)
      const res = await fetch(`/api/explainability/summary?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load data confidence')
      return res.json()
    },
    enabled: journeysLoaded,
    refetchInterval: false,
  })

  const dqAlertsQuery = useQuery<DQAlert[]>({
    queryKey: ['overview-dq-alerts'],
    queryFn: async () => {
      const params = new URLSearchParams({ limit: '3', status: 'open' })
      const res = await fetch(`/api/data-quality/alerts?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load DQ alerts')
      return res.json()
    },
    enabled: true,
    refetchInterval: false,
  })

  const dqRunMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/data-quality/run', { method: 'POST' })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Failed to run data quality checks')
      }
      return res.json()
    },
  })

  const totalSpend = perfQuery.data?.total_spend ?? 0
  const totalValue = perfQuery.data?.total_attributed_value ?? journeysSummary?.total_value ?? 0
  const totalConversions = perfQuery.data?.total_conversions ?? journeysSummary?.converted ?? 0
  const roas = totalSpend > 0 ? totalValue / totalSpend : 0

  const confidence = explainabilityQuery.data?.data_health?.confidence ?? null

  const channels = perfQuery.data?.channels ?? []
  const topChannels = useMemo(
    () => [...channels].sort((a, b) => b.attributed_value - a.attributed_value).slice(0, 3),
    [channels],
  )
  const donutChannels = useMemo(
    () => [...channels].sort((a, b) => b.attributed_value - a.attributed_value).slice(0, 5),
    [channels],
  )

  const lastPageLabel =
    lastPage && lastPage !== 'overview'
      ? (() => {
          switch (lastPage) {
            case 'dashboard':
              return 'Channel performance'
            case 'campaigns':
              return 'Campaign performance'
            case 'comparison':
              return 'Attribution models'
            case 'paths':
              return 'Conversion paths'
            case 'path_archetypes':
              return 'Path archetypes'
            case 'mmm':
              return 'MMM (advanced)'
            case 'dq':
              return 'Data quality'
            case 'datasources':
              return 'Data sources'
            case 'expenses':
              return 'Expenses'
            case 'incrementality':
              return 'Incrementality'
            case 'settings':
              return 'Settings'
            default:
              return null
          }
        })()
      : null

  // First‑run / empty state
  if (!journeysLoaded) {
    return (
      <div
        style={{
          maxWidth: 720,
          margin: '0 auto',
          background: t.color.surface,
          borderRadius: t.radius.lg,
          border: `1px solid ${t.color.border}`,
          boxShadow: t.shadow,
          padding: t.space.xxl,
        }}
      >
        <h1
          style={{
            margin: 0,
            fontSize: t.font.size2xl,
            fontWeight: t.font.weightBold,
            color: t.color.text,
            textAlign: 'center',
            letterSpacing: '-0.02em',
          }}
        >
          Welcome to the Meiro measurement workspace
        </h1>
        <p
          style={{
            margin: `${t.space.sm}px 0 ${t.space.lg}px`,
            fontSize: t.font.sizeSm,
            color: t.color.textSecondary,
            textAlign: 'center',
          }}
        >
          This workspace combines attribution, incrementality, MMM, and data quality into a single view.
        </p>
        <ol
          style={{
            fontSize: t.font.sizeSm,
            color: t.color.text,
            margin: `0 0 ${t.space.xl}px`,
            paddingLeft: 22,
            lineHeight: 1.6,
          }}
        >
          <li>Load conversion path data (sample, upload, or connect a data source).</li>
          <li>Run attribution models, then explore Channel and Campaign performance.</li>
          <li>Use Journeys, Incrementality, and MMM for deeper planning and optimization.</li>
        </ol>
        <div
          style={{
            display: 'flex',
            justifyContent: 'center',
            gap: t.space.md,
            flexWrap: 'wrap',
          }}
        >
          <button
            type="button"
            onClick={() => loadSampleJourneys()}
            disabled={isLoadingSampleJourneys}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              backgroundColor: t.color.success,
              color: '#ffffff',
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: isLoadingSampleJourneys ? 'wait' : 'pointer',
            }}
          >
            {isLoadingSampleJourneys ? 'Loading sample…' : 'Load sample data'}
          </button>
          <button
            type="button"
            onClick={onConnectDataSources}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              backgroundColor: '#fd7e14',
              color: '#ffffff',
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Connect data sources
          </button>
        </div>
      </div>
    )
  }

  return (
    <div>
      {/* Page header */}
      <div
        style={{
          marginBottom: t.space.lg,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          gap: t.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color: t.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Overview
          </h1>
          <p
            style={{
              margin: `${t.space.xs}px 0 0`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Is measurement healthy and where should you look next?
          </p>
        </div>
        <div
          style={{
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
            textAlign: 'right',
          }}
        >
          Attribution model:{' '}
          <strong style={{ color: t.color.text }}>
            {MODEL_LABELS[attributionModel] || attributionModel}
          </strong>
        </div>
      </div>

      {/* Top summary row */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <SummaryCard label="Total spend" value={formatCurrency(totalSpend)} />
        <SummaryCard label="Attributed revenue" value={formatCurrency(totalValue)} />
        <SummaryCard
          label="Attributed conversions"
          value={totalConversions.toLocaleString()}
        />
        <SummaryCard
          label="ROAS"
          value={totalSpend > 0 ? `${roas.toFixed(2)}×` : 'N/A'}
        />
        <div
          style={{
            background: t.color.surface,
            borderRadius: t.radius.md,
            border: `1px solid ${t.color.borderLight}`,
            padding: `${t.space.lg}px ${t.space.xl}px`,
            boxShadow: t.shadowSm,
            display: 'flex',
            flexDirection: 'column',
            gap: 6,
          }}
        >
          <div
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textMuted,
              textTransform: 'uppercase',
              letterSpacing: '0.04em',
            }}
          >
            Data confidence
          </div>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
            }}
          >
            <span
              style={{
                padding: '2px 8px',
                borderRadius: 999,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                backgroundColor: confidence
                  ? confidence.label === 'High'
                    ? t.color.successMuted
                    : confidence.label === 'Medium'
                      ? t.color.warningMuted
                      : t.color.dangerMuted
                  : t.color.bg,
                color: confidence
                  ? confidence.label === 'High'
                    ? t.color.success
                    : confidence.label === 'Medium'
                      ? t.color.warning
                      : t.color.danger
                  : t.color.textMuted,
              }}
            >
              {confidence ? confidence.label : 'N/A'}
            </span>
            {confidence && (
              <span
                style={{
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                {Math.round(confidence.score)}
              </span>
            )}
          </div>
          <p
            style={{
              margin: 0,
              fontSize: t.font.sizeXs,
              color: t.color.textSecondary,
            }}
          >
            Based on latest data quality checks and attribution consistency.
          </p>
        </div>
      </div>

      {/* Middle row: What changed + Alerts + Quick links */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'minmax(0, 2fr) minmax(0, 1.5fr)',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        {/* What changed / top drivers */}
        <div
          style={{
            background: t.color.surface,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.borderLight}`,
            padding: t.space.lg,
            boxShadow: t.shadowSm,
          }}
        >
          <h2
            style={{
              margin: '0 0 4px',
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            What should I look at?
          </h2>
          <p
            style={{
              margin: `0 0 ${t.space.sm}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Top drivers by attributed revenue in the current period.
          </p>
          {topChannels.length === 0 ? (
            <p
              style={{
                margin: 0,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              Run attribution models to see top contributing channels.
            </p>
          ) : (
            <ul
              style={{
                listStyle: 'none',
                margin: 0,
                padding: 0,
                fontSize: t.font.sizeSm,
                color: t.color.text,
              }}
            >
              {topChannels.map((ch) => (
                <li
                  key={ch.channel}
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    padding: `${t.space.xs}px 0`,
                    borderBottom: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <span>{ch.channel}</span>
                  <span
                    style={{
                      display: 'inline-flex',
                      gap: 8,
                      fontVariantNumeric: 'tabular-nums',
                    }}
                  >
                    <span>{formatCurrency(ch.attributed_value)}</span>
                    <span style={{ color: t.color.textSecondary }}>
                      {(ch.attributed_share * 100).toFixed(1)}% share
                    </span>
                    <span style={{ color: ch.roas >= 1 ? t.color.success : t.color.danger }}>
                      {ch.roas.toFixed(2)}× ROAS
                    </span>
                  </span>
                </li>
              ))}
            </ul>
          )}
          <button
            type="button"
            onClick={() => onNavigate('dashboard')}
            style={{
              marginTop: t.space.md,
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.accent,
              background: 'transparent',
              border: `1px solid ${t.color.accent}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Open Channel performance
          </button>
        </div>

        {/* Alerts & Quick links (stacked) */}
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: t.space.md,
          }}
        >
          {/* Alerts & risks */}
          <div
            style={{
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.borderLight}`,
              padding: t.space.lg,
              boxShadow: t.shadowSm,
            }}
          >
            <h2
              style={{
                margin: '0 0 4px',
                fontSize: t.font.sizeMd,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Alerts &amp; risks
            </h2>
            <p
              style={{
                margin: `0 0 ${t.space.sm}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              Data quality issues that may impact measurement accuracy.
            </p>
            {dqAlertsQuery.isLoading ? (
              <p
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                Loading alerts…
              </p>
            ) : dqAlertsQuery.data && dqAlertsQuery.data.length > 0 ? (
              <ul
                style={{
                  listStyle: 'none',
                  margin: 0,
                  padding: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.text,
                }}
              >
                {dqAlertsQuery.data.slice(0, 3).map((a) => {
                  const sev = (a.rule?.severity ?? 'warn').toLowerCase()
                  const sevColor = SEVERITY_COLORS[sev] ?? t.color.warning
                  return (
                    <li
                      key={a.id}
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        gap: t.space.sm,
                        padding: `${t.space.xs}px 0`,
                        borderBottom: `1px solid ${t.color.borderLight}`,
                      }}
                    >
                      <div>
                        <div
                          style={{
                            fontWeight: t.font.weightMedium,
                          }}
                        >
                          {a.rule?.name || a.message}
                        </div>
                        <div
                          style={{
                            fontSize: t.font.sizeXs,
                            color: t.color.textSecondary,
                          }}
                        >
                          {a.rule?.source || '—'} · {a.rule?.metric_key || 'metric'} · value{' '}
                          {a.metric_value.toFixed(2)}
                          {a.baseline_value != null && ` vs ${a.baseline_value.toFixed(2)}`}
                        </div>
                      </div>
                      <span
                        style={{
                          padding: '2px 8px',
                          borderRadius: 999,
                          fontSize: t.font.sizeXs,
                          fontWeight: t.font.weightMedium,
                          backgroundColor: 'rgba(15,23,42,0.04)',
                          color: sevColor,
                        }}
                      >
                        {sev}
                      </span>
                    </li>
                  )
                })}
              </ul>
            ) : (
              <p
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                All checks passing. No open alerts.
              </p>
            )}
            <button
              type="button"
              onClick={() => onNavigate('dq')}
              style={{
                marginTop: t.space.sm,
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                color: t.color.accent,
                background: 'transparent',
                border: 'none',
                cursor: 'pointer',
              }}
            >
              Open Data quality
            </button>
          </div>

          {/* Quick links / next actions */}
          <div
            style={{
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.borderLight}`,
              padding: t.space.lg,
              boxShadow: t.shadowSm,
            }}
          >
            <h2
              style={{
                margin: '0 0 4px',
                fontSize: t.font.sizeMd,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Quick links
            </h2>
            <p
              style={{
                margin: `0 0 ${t.space.sm}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              Jump back into the most common workflows.
            </p>
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                gap: 6,
                fontSize: t.font.sizeSm,
              }}
            >
              {lastPageLabel && (
                <QuickLinkButton onClick={() => onNavigate(lastPage!)} emphasis>
                  Continue: {lastPageLabel}
                </QuickLinkButton>
              )}
              <QuickLinkButton
                onClick={() => dqRunMutation.mutate()}
                disabled={dqRunMutation.isPending}
              >
                {dqRunMutation.isPending ? 'Running data quality checks…' : 'Run data quality checks'}
              </QuickLinkButton>
              <QuickLinkButton
                onClick={() => runAllAttribution()}
                disabled={isRunningAttribution}
              >
                {isRunningAttribution ? 'Re-running attribution models…' : 'Re-run attribution models'}
              </QuickLinkButton>
              <QuickLinkButton onClick={() => onNavigate('incrementality')}>
                Create incrementality experiment
              </QuickLinkButton>
              <QuickLinkButton onClick={() => onNavigate('mmm')}>
                Build MMM dataset (platform data)
              </QuickLinkButton>
            </div>
          </div>
        </div>
      </div>

      {/* Bottom row: attribution share visual */}
      {donutChannels.length > 0 && (
        <div
          style={{
            background: t.color.surface,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.borderLight}`,
            padding: t.space.xl,
            boxShadow: t.shadowSm,
          }}
        >
          <h2
            style={{
              margin: '0 0 4px',
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Attribution share (top channels)
          </h2>
          <p
            style={{
              margin: `0 0 ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Share of attributed revenue by channel, top 5 channels.
          </p>
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'minmax(0, 1.4fr) minmax(0, 1fr)',
              gap: t.space.lg,
            }}
          >
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie
                  data={donutChannels}
                  dataKey="attributed_share"
                  nameKey="channel"
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={90}
                  paddingAngle={1}
                >
                  {donutChannels.map((_, i) => (
                    <Cell key={i} fill={t.color.chart[i % t.color.chart.length]} />
                  ))}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
            <ul
              style={{
                listStyle: 'none',
                margin: 0,
                padding: 0,
                fontSize: t.font.sizeSm,
                color: t.color.text,
              }}
            >
              {donutChannels.map((ch, idx) => (
                <li
                  key={ch.channel}
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    padding: `${t.space.xs}px 0`,
                    borderBottom: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <span
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 8,
                    }}
                  >
                    <span
                      style={{
                        width: 10,
                        height: 10,
                        borderRadius: 999,
                        backgroundColor: t.color.chart[idx % t.color.chart.length],
                      }}
                    />
                    {ch.channel}
                  </span>
                  <span
                    style={{
                      display: 'inline-flex',
                      gap: 8,
                      fontVariantNumeric: 'tabular-nums',
                      color: t.color.textSecondary,
                    }}
                  >
                    <span>{(ch.attributed_share * 100).toFixed(1)}%</span>
                    <span>{formatCurrency(ch.attributed_value)}</span>
                  </span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}
    </div>
  )
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  const t = tokens
  return (
    <div
      style={{
        background: t.color.surface,
        borderRadius: t.radius.md,
        border: `1px solid ${t.color.borderLight}`,
        padding: `${t.space.lg}px ${t.space.xl}px`,
        boxShadow: t.shadowSm,
      }}
    >
      <div
        style={{
          fontSize: t.font.sizeXs,
          fontWeight: t.font.weightMedium,
          color: t.color.textMuted,
          textTransform: 'uppercase',
          letterSpacing: '0.04em',
        }}
      >
        {label}
      </div>
      <div
        style={{
          marginTop: t.space.xs,
          fontSize: t.font.sizeXl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {value}
      </div>
    </div>
  )
}

function QuickLinkButton(props: {
  children: React.ReactNode
  onClick: () => void
  disabled?: boolean
  emphasis?: boolean
}) {
  const t = tokens
  const { children, onClick, disabled, emphasis } = props
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: `${t.space.xs}px ${t.space.sm}px`,
        fontSize: t.font.sizeSm,
        borderRadius: t.radius.sm,
        border: 'none',
        backgroundColor: emphasis ? t.color.accentMuted : 'transparent',
        color: emphasis ? t.color.accent : t.color.textSecondary,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.7 : 1,
      }}
    >
      <span>{children}</span>
    </button>
  )
}

