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
}

interface ArchetypesResponse {
  clusters: PathCluster[]
  total_converted: number
}

export default function PathArchetypes() {
  const query = useQuery<ArchetypesResponse>({
    queryKey: ['path-archetypes'],
    queryFn: async () => {
      const res = await fetch('/api/paths/archetypes')
      if (!res.ok) throw new Error('Failed to load path archetypes')
      return res.json()
    },
  })

  const data = query.data

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

        {data && data.clusters.length > 0 && (
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
              gap: t.space.lg,
            }}
          >
            {data.clusters.map((c) => (
              <div
                key={c.id}
                style={{
                  background: t.color.surface,
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.lg,
                  padding: t.space.lg,
                  boxShadow: t.shadowSm,
                }}
              >
                <div
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                    marginBottom: t.space.sm,
                  }}
                >
                  #{c.id} • {c.name}
                </div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                  {c.size.toLocaleString()} journeys • {(c.share * 100).toFixed(1)}% of converted •{' '}
                  {c.avg_length.toFixed(1)} steps
                  {c.avg_time_to_conversion_days != null &&
                    ` • ${c.avg_time_to_conversion_days.toFixed(1)} days to convert`}
                </div>
                {c.top_channels.length > 0 && (
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                    Top channels: {c.top_channels.join(', ')}
                  </div>
                )}
                <div>
                  <div
                    style={{
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: t.space.xs,
                    }}
                  >
                    Representative path
                  </div>
                  <div
                    style={{
                      fontSize: t.font.sizeXs,
                      color: t.color.text,
                      background: t.color.bg,
                      borderRadius: t.radius.sm,
                      padding: t.space.sm,
                    }}
                  >
                    {c.top_paths[0]?.path}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

