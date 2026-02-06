import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface PathAnalysis {
  total_journeys: number
  avg_path_length: number
  avg_time_to_conversion_days: number | null
  common_paths: { path: string; count: number; share: number }[]
  channel_frequency: Record<string, number>
  path_length_distribution: { min: number; max: number; median: number }
}

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
} as const

export default function ConversionPaths() {
  const pathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/paths')
      if (!res.ok) throw new Error('Failed to fetch path analysis')
      return res.json()
    },
  })

  const data = pathsQuery.data

  if (pathsQuery.isLoading) {
    return (
      <div style={{ ...card, textAlign: 'center', padding: 60 }}>
        <p style={{ color: '#6c757d' }}>Analyzing conversion paths...</p>
      </div>
    )
  }

  if (!data || data.total_journeys === 0) {
    return (
      <div style={{ ...card, textAlign: 'center', padding: 40 }}>
        <p style={{ color: '#6c757d' }}>No conversion path data available.</p>
      </div>
    )
  }

  const freqData = Object.entries(data.channel_frequency)
    .sort((a, b) => b[1] - a[1])
    .map(([channel, count]) => ({ channel, count }))

  return (
    <div>
      <h2 style={{ margin: '0 0 8px', fontSize: '22px', fontWeight: '700', color: '#212529' }}>Conversion Path Analysis</h2>
      <p style={{ margin: '0 0 24px', fontSize: '14px', color: '#6c757d' }}>
        Understand how customers interact with channels before converting.
      </p>

      {/* Summary KPIs */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 24 }}>
        {[
          { label: 'Total Journeys', value: data.total_journeys.toString(), color: '#007bff' },
          { label: 'Avg Path Length', value: `${data.avg_path_length} touchpoints`, color: '#6f42c1' },
          { label: 'Avg Time to Convert', value: data.avg_time_to_conversion_days !== null ? `${data.avg_time_to_conversion_days} days` : 'N/A', color: '#17a2b8' },
          { label: 'Path Length Range', value: `${data.path_length_distribution.min} - ${data.path_length_distribution.max}`, color: '#28a745' },
        ].map(kpi => (
          <div key={kpi.label} style={{ ...card, textAlign: 'center', padding: 20 }}>
            <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              {kpi.label}
            </div>
            <div style={{ fontSize: '24px', fontWeight: '700', color: kpi.color, marginTop: 8 }}>
              {kpi.value}
            </div>
          </div>
        ))}
      </div>

      {/* Channel Frequency */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <div style={card}>
          <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Channel Frequency in Paths</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={freqData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="channel" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="count" fill="#17a2b8" name="Appearances" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Channel Frequency Table */}
        <div style={card}>
          <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Channel Touchpoint Stats</h3>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f9fa' }}>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>Channel</th>
                <th style={{ padding: '10px 12px', textAlign: 'right', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>Touchpoints</th>
                <th style={{ padding: '10px 12px', textAlign: 'right', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>% of Total</th>
              </tr>
            </thead>
            <tbody>
              {freqData.map((row, idx) => {
                const total = freqData.reduce((s, r) => s + r.count, 0)
                return (
                  <tr key={row.channel} style={{ backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                    <td style={{ padding: '8px 12px', fontWeight: '600', color: '#495057' }}>{row.channel}</td>
                    <td style={{ padding: '8px 12px', textAlign: 'right' }}>{row.count}</td>
                    <td style={{ padding: '8px 12px', textAlign: 'right', color: '#6f42c1', fontWeight: '600' }}>
                      {(row.count / total * 100).toFixed(1)}%
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Common Paths */}
      <div style={card}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Most Common Conversion Paths</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f9fa' }}>
                <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: '600', borderBottom: '2px solid #e9ecef', width: 40 }}>#</th>
                <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>Path</th>
                <th style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>Count</th>
                <th style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', borderBottom: '2px solid #e9ecef' }}>Share</th>
              </tr>
            </thead>
            <tbody>
              {data.common_paths.slice(0, 15).map((p, idx) => (
                <tr key={idx} style={{ borderBottom: '1px solid #f1f3f5', backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                  <td style={{ padding: '10px 16px', color: '#6c757d' }}>{idx + 1}</td>
                  <td style={{ padding: '10px 16px' }}>
                    {p.path.split(' > ').map((step, i, arr) => (
                      <span key={i}>
                        <span style={{
                          display: 'inline-block',
                          padding: '2px 8px',
                          backgroundColor: '#e7f3ff',
                          color: '#007bff',
                          borderRadius: 4,
                          fontSize: '12px',
                          fontWeight: '600',
                        }}>
                          {step}
                        </span>
                        {i < arr.length - 1 && (
                          <span style={{ margin: '0 4px', color: '#adb5bd' }}>&rarr;</span>
                        )}
                      </span>
                    ))}
                  </td>
                  <td style={{ padding: '10px 16px', textAlign: 'right', fontWeight: '600' }}>{p.count}</td>
                  <td style={{ padding: '10px 16px', textAlign: 'right', color: '#6f42c1', fontWeight: '600' }}>
                    {(p.share * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
