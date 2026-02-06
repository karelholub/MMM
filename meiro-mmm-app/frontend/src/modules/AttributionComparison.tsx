import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

interface AttributionComparisonProps {
  selectedModel: string
  onSelectModel: (model: string) => void
}

interface ModelResult {
  model: string
  channel_credit: Record<string, number>
  total_conversions: number
  total_value: number
  channels: { channel: string; attributed_value: number; attributed_share: number; attributed_conversions: number }[]
  error?: string
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const MODEL_COLORS: Record<string, string> = {
  last_touch: '#dc3545',
  first_touch: '#fd7e14',
  linear: '#007bff',
  time_decay: '#28a745',
  position_based: '#6f42c1',
  markov: '#17a2b8',
}

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
} as const

export default function AttributionComparison({ selectedModel, onSelectModel }: AttributionComparisonProps) {
  const resultsQuery = useQuery<Record<string, ModelResult>>({
    queryKey: ['attribution-results'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/results')
      if (!res.ok) throw new Error('Failed to fetch results')
      return res.json()
    },
    refetchInterval: 3000,
  })

  const results = resultsQuery.data || {}
  const models = Object.keys(results).filter(k => !results[k].error)

  if (models.length === 0) {
    return (
      <div style={{ ...card, textAlign: 'center', padding: 40 }}>
        <p style={{ color: '#6c757d', fontSize: '16px' }}>
          No attribution results yet. Run models first by clicking "Re-run All Models" in the header.
        </p>
      </div>
    )
  }

  // Build comparison data: one row per channel, columns for each model
  const allChannels = new Set<string>()
  for (const model of models) {
    const r = results[model]
    if (r.channels) {
      for (const ch of r.channels) allChannels.add(ch.channel)
    }
  }

  const comparisonData = Array.from(allChannels).map(channel => {
    const row: Record<string, any> = { channel }
    for (const model of models) {
      const r = results[model]
      const ch = r.channels?.find(c => c.channel === channel)
      row[model] = ch ? ch.attributed_value : 0
      row[`${model}_share`] = ch ? ch.attributed_share : 0
    }
    return row
  })

  // Selected model detail
  const selectedResult = results[selectedModel]

  return (
    <div>
      <h2 style={{ margin: '0 0 8px', fontSize: '22px', fontWeight: '700', color: '#212529' }}>Attribution Model Comparison</h2>
      <p style={{ margin: '0 0 24px', fontSize: '14px', color: '#6c757d' }}>
        Compare how different attribution models distribute credit across channels.
      </p>

      {/* Comparison Bar Chart */}
      <div style={{ ...card, marginBottom: 24 }}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>
          Attributed Revenue by Model & Channel
        </h3>
        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={comparisonData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="channel" tick={{ fontSize: 13 }} />
            <YAxis tickFormatter={(v) => `$${v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v}`} />
            <Tooltip formatter={(value: number) => `$${value.toFixed(2)}`} />
            <Legend />
            {models.map(model => (
              <Bar
                key={model}
                dataKey={model}
                name={MODEL_LABELS[model] || model}
                fill={MODEL_COLORS[model] || '#6c757d'}
                radius={[4, 4, 0, 0]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Model Cards Grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16, marginBottom: 24 }}>
        {models.map(model => {
          const r = results[model]
          const isSelected = model === selectedModel
          return (
            <div
              key={model}
              onClick={() => onSelectModel(model)}
              style={{
                ...card,
                cursor: 'pointer',
                border: isSelected ? `2px solid ${MODEL_COLORS[model]}` : '1px solid #e9ecef',
                transition: 'all 0.2s',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                <h4 style={{ margin: 0, fontSize: '15px', fontWeight: '700', color: MODEL_COLORS[model] }}>
                  {MODEL_LABELS[model] || model}
                </h4>
                {isSelected && (
                  <span style={{ fontSize: '11px', fontWeight: '700', color: 'white', backgroundColor: MODEL_COLORS[model], padding: '2px 8px', borderRadius: 10 }}>
                    ACTIVE
                  </span>
                )}
              </div>
              <div style={{ fontSize: '13px', color: '#6c757d' }}>
                <div style={{ marginBottom: 4 }}>Conversions: <strong>{r.total_conversions}</strong></div>
                <div style={{ marginBottom: 8 }}>Total Value: <strong>${r.total_value.toFixed(2)}</strong></div>
              </div>
              {r.channels && (
                <div style={{ fontSize: '12px' }}>
                  {r.channels.slice(0, 3).map(ch => (
                    <div key={ch.channel} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid #f1f3f5' }}>
                      <span style={{ color: '#495057' }}>{ch.channel}</span>
                      <span style={{ fontWeight: '600', color: MODEL_COLORS[model] }}>
                        {(ch.attributed_share * 100).toFixed(1)}%
                      </span>
                    </div>
                  ))}
                  {r.channels.length > 3 && (
                    <div style={{ color: '#adb5bd', fontSize: '11px', marginTop: 4 }}>
                      +{r.channels.length - 3} more channels
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Detailed Comparison Table */}
      <div style={card}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>
          Attribution Share Comparison (%)
        </h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f9fa' }}>
                <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: '600', color: '#495057', borderBottom: '2px solid #e9ecef' }}>
                  Channel
                </th>
                {models.map(model => (
                  <th
                    key={model}
                    style={{
                      padding: '12px 16px',
                      textAlign: 'center',
                      fontWeight: '600',
                      color: MODEL_COLORS[model],
                      borderBottom: `2px solid ${MODEL_COLORS[model]}`,
                      cursor: 'pointer',
                    }}
                    onClick={() => onSelectModel(model)}
                  >
                    {MODEL_LABELS[model] || model}
                    {model === selectedModel && ' *'}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {comparisonData.map((row, idx) => (
                <tr key={row.channel} style={{ borderBottom: '1px solid #f1f3f5', backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                  <td style={{ padding: '10px 16px', fontWeight: '600', color: '#495057' }}>{row.channel}</td>
                  {models.map(model => {
                    const share = (row[`${model}_share`] || 0) * 100
                    const maxShare = Math.max(...models.map(m => (row[`${m}_share`] || 0) * 100))
                    const isMax = share > 0 && share === maxShare
                    return (
                      <td key={model} style={{
                        padding: '10px 16px',
                        textAlign: 'center',
                        fontWeight: isMax ? '700' : '400',
                        color: isMax ? MODEL_COLORS[model] : '#495057',
                        backgroundColor: share > 20 ? `${MODEL_COLORS[model]}10` : 'transparent',
                      }}>
                        {share.toFixed(1)}%
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
