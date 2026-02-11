import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

interface PathCluster {
  id: number
  name: string
  size: number
  share: number
  avg_length: number
  avg_time_to_conversion_days: number | null
  top_channels: string[]
  top_paths: { path: string; count: number; share: number }[]
  representative_path?: string
  cluster_label?: number
}

interface ArchetypesResponse {
  clusters: PathCluster[]
  total_converted: number
  diagnostics?: {
    algorithm?: string
    k_mode?: string
    k_selected?: number
    silhouette_cosine?: number | null
    n_unique_paths?: number
  }
}

export default function PathArchetypes() {
  const [kMode, setKMode] = useState<'auto' | 'fixed'>('auto')
  const [kFixed, setKFixed] = useState(6)
  const [selectedId, setSelectedId] = useState<number | null>(null)

  const query = useQuery<ArchetypesResponse>({
    queryKey: ['path-archetypes', kMode, kFixed],
    queryFn: async () => {
      const params = new URLSearchParams()
      params.set('k_mode', kMode)
      if (kMode === 'fixed') params.set('k', String(kFixed))
      const res = await fetch(`/api/paths/archetypes?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load path archetypes')
      return res.json()
    },
  })

  const data = query.data
  const clusters = data?.clusters ?? []

  const selected = useMemo(() => {
    if (!clusters.length) return null
    const id = selectedId ?? clusters[0].id
    return clusters.find((c) => c.id === id) ?? clusters[0]
  }, [clusters, selectedId])

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.xl,
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
            Path archetypes
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            The most common conversion paths, grouped as archetypes. Use these to understand typical journeys and where to
            focus optimization.
          </p>
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: t.space.sm,
            flexWrap: 'wrap',
            justifyContent: 'flex-end',
          }}
        >
          <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Clustering
            <select
              value={kMode}
              onChange={(e) => setKMode(e.target.value as 'auto' | 'fixed')}
              style={{
                marginLeft: 8,
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                background: '#ffffff',
              }}
            >
              <option value="auto">Auto</option>
              <option value="fixed">Fixed K</option>
            </select>
          </label>
          {kMode === 'fixed' && (
            <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              K
              <input
                type="number"
                min={2}
                max={20}
                value={kFixed}
                onChange={(e) => setKFixed(Math.max(2, Math.min(20, parseInt(e.target.value || '6', 10))))}
                style={{
                  width: 72,
                  marginLeft: 8,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                }}
              />
            </label>
          )}
        </div>
      </div>

      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        {query.isLoading && (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading path archetypes…</p>
        )}
        {query.isError && (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(query.error as Error).message || 'Failed to load path archetypes.'}
          </p>
        )}
        {data && data.clusters.length === 0 && !query.isLoading && !query.isError && (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No converted journeys to build archetypes from yet. Load journeys and run attribution first.
          </p>
        )}

        {data && clusters.length > 0 && (
          <>
            {data.diagnostics && (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.md }}>
                {data.diagnostics.algorithm ? `Algorithm: ${data.diagnostics.algorithm}` : 'Algorithm: —'}
                {data.diagnostics.k_selected != null ? ` • K=${data.diagnostics.k_selected}` : ''}
                {data.diagnostics.silhouette_cosine != null ? ` • silhouette=${data.diagnostics.silhouette_cosine.toFixed(3)}` : ''}
                {data.diagnostics.n_unique_paths != null ? ` • unique paths=${data.diagnostics.n_unique_paths.toLocaleString()}` : ''}
              </div>
            )}
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(260px, 1.2fr) minmax(0, 2fr)',
                gap: t.space.lg,
                alignItems: 'start',
              }}
            >
              {/* Cluster list */}
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.lg,
                  overflow: 'hidden',
                }}
              >
                <div style={{ padding: t.space.md, background: t.color.bg, borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    {data.total_converted.toLocaleString()} converted journeys • {clusters.length} archetypes
                  </div>
                </div>
                <div style={{ maxHeight: 520, overflowY: 'auto' }}>
                  {clusters.map((c) => {
                    const isSel = selected?.id === c.id
                    return (
                      <button
                        key={c.id}
                        type="button"
                        onClick={() => setSelectedId(c.id)}
                        style={{
                          width: '100%',
                          textAlign: 'left',
                          padding: `${t.space.sm}px ${t.space.md}px`,
                          border: 'none',
                          borderBottom: `1px solid ${t.color.borderLight}`,
                          background: isSel ? t.color.accentMuted : t.color.surface,
                          cursor: 'pointer',
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                          #{c.id} • {c.name}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          {c.size.toLocaleString()} • {(c.share * 100).toFixed(1)}% • {c.avg_length.toFixed(1)} steps
                        </div>
                      </button>
                    )
                  })}
                </div>
              </div>

              {/* Cluster detail */}
              {selected && (
                <div
                  style={{
                    background: t.color.surface,
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.lg,
                    padding: t.space.xl,
                    boxShadow: t.shadowSm,
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
                    <div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        Archetype #{selected.id} • {selected.name}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: 4 }}>
                        {selected.size.toLocaleString()} journeys • {(selected.share * 100).toFixed(1)}% of converted •{' '}
                        {selected.avg_length.toFixed(1)} steps
                        {selected.avg_time_to_conversion_days != null
                          ? ` • ${selected.avg_time_to_conversion_days.toFixed(1)} days to convert`
                          : ''}
                      </div>
                    </div>
                    {selected.top_channels?.length > 0 && (
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
                        {selected.top_channels.slice(0, 5).map((ch) => (
                          <span
                            key={ch}
                            style={{
                              padding: '2px 8px',
                              borderRadius: t.radius.full,
                              background: t.color.bg,
                              border: `1px solid ${t.color.borderLight}`,
                              fontSize: t.font.sizeXs,
                              color: t.color.textSecondary,
                              fontWeight: t.font.weightMedium,
                            }}
                          >
                            {ch}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>

                  <div style={{ marginTop: t.space.lg }}>
                    <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
                      Representative path
                    </div>
                    <div
                      style={{
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                        background: t.color.bg,
                        borderRadius: t.radius.sm,
                        padding: t.space.sm,
                        border: `1px solid ${t.color.borderLight}`,
                      }}
                    >
                      {selected.representative_path ?? selected.top_paths?.[0]?.path ?? '—'}
                    </div>
                  </div>

                  <div style={{ marginTop: t.space.xl }}>
                    <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                      Top paths in this archetype
                    </div>
                    <div style={{ overflowX: 'auto' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
                        <thead>
                          <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                            <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Path</th>
                            <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Count</th>
                            <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Share</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(selected.top_paths ?? []).map((p) => (
                            <tr key={p.path} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                              <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.text }}>{p.path}</td>
                              <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                                {p.count.toLocaleString()}
                              </td>
                              <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: t.color.accent }}>
                                {(p.share * 100).toFixed(1)}%
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

