import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts'

interface ChannelPerformanceProps {
  model: string
  channels: string[]
  modelsReady: boolean
}

interface ChannelData {
  channel: string
  spend: number
  attributed_value: number
  attributed_conversions: number
  attributed_share: number
  roi: number
  roas: number
  cpa: number
}

interface PerformanceResponse {
  model: string
  channels: ChannelData[]
  total_spend: number
  total_attributed_value: number
  total_conversions: number
}

const COLORS = ['#007bff', '#28a745', '#fd7e14', '#dc3545', '#6f42c1', '#17a2b8', '#ffc107', '#6c757d']

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
} as const

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

export default function ChannelPerformance({ model, modelsReady }: ChannelPerformanceProps) {
  const perfQuery = useQuery<PerformanceResponse>({
    queryKey: ['channel-performance', model],
    queryFn: async () => {
      const res = await fetch(`/api/attribution/performance?model=${model}`)
      if (!res.ok) throw new Error('Failed to fetch performance')
      return res.json()
    },
    enabled: modelsReady,
    refetchInterval: false,
  })

  const data = perfQuery.data
  const loading = perfQuery.isLoading || !modelsReady

  if (loading) {
    return (
      <div style={{ ...card, textAlign: 'center', padding: 60 }}>
        <p style={{ fontSize: '16px', color: '#6c757d' }}>Running attribution models...</p>
      </div>
    )
  }

  if (!data || !data.channels?.length) {
    return (
      <div style={{ ...card, textAlign: 'center', padding: 40 }}>
        <p style={{ color: '#6c757d' }}>No performance data available. Make sure journeys and expenses are loaded.</p>
      </div>
    )
  }

  const totalROI = data.total_spend > 0 ? (data.total_attributed_value - data.total_spend) / data.total_spend : 0
  const totalROAS = data.total_spend > 0 ? data.total_attributed_value / data.total_spend : 0
  const avgCPA = data.total_conversions > 0 ? data.total_spend / data.total_conversions : 0

  const MODEL_LABELS: Record<string, string> = {
    last_touch: 'Last Touch', first_touch: 'First Touch', linear: 'Linear',
    time_decay: 'Time Decay', position_based: 'Position Based', markov: 'Data-Driven (Markov)',
  }

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '22px', fontWeight: '700', color: '#212529' }}>Channel Performance Overview</h2>
          <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#6c757d' }}>
            Attribution model: <strong style={{ color: '#6f42c1' }}>{MODEL_LABELS[model] || model}</strong>
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 16, marginBottom: 24 }}>
        {[
          { label: 'Total Spend', value: formatCurrency(data.total_spend), color: '#dc3545' },
          { label: 'Attributed Revenue', value: formatCurrency(data.total_attributed_value), color: '#28a745' },
          { label: 'Conversions', value: data.total_conversions.toString(), color: '#007bff' },
          { label: 'Overall ROAS', value: `${totalROAS.toFixed(2)}x`, color: '#6f42c1' },
          { label: 'Avg CPA', value: formatCurrency(avgCPA), color: '#fd7e14' },
        ].map(kpi => (
          <div key={kpi.label} style={{ ...card, textAlign: 'center', padding: 20 }}>
            <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              {kpi.label}
            </div>
            <div style={{ fontSize: '28px', fontWeight: '700', color: kpi.color, marginTop: 8 }}>
              {kpi.value}
            </div>
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16, marginBottom: 24 }}>
        {/* Bar chart: spend vs attributed value */}
        <div style={card}>
          <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Spend vs. Attributed Revenue by Channel</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.channels} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis type="number" tickFormatter={(v) => formatCurrency(v)} />
              <YAxis type="category" dataKey="channel" width={110} tick={{ fontSize: 13 }} />
              <Tooltip formatter={(value: number) => formatCurrency(value)} />
              <Legend />
              <Bar dataKey="spend" fill="#dc3545" name="Spend" radius={[0, 4, 4, 0]} />
              <Bar dataKey="attributed_value" fill="#28a745" name="Attributed Revenue" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Pie chart: attribution share */}
        <div style={card}>
          <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Revenue Attribution Share</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={data.channels}
                dataKey="attributed_share"
                nameKey="channel"
                cx="50%" cy="50%"
                innerRadius={60} outerRadius={100}
                paddingAngle={2}
                label={({ channel, attributed_share }) => `${channel} ${(attributed_share * 100).toFixed(0)}%`}
              >
                {data.channels.map((_entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ROI Bar Chart */}
      <div style={{ ...card, marginBottom: 24 }}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>ROI & ROAS by Channel</h3>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={data.channels}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="channel" tick={{ fontSize: 13 }} />
            <YAxis tick={{ fontSize: 13 }} />
            <Tooltip formatter={(value: number) => value.toFixed(2)} />
            <Legend />
            <Bar dataKey="roas" fill="#6f42c1" name="ROAS" radius={[4, 4, 0, 0]} />
            <Bar dataKey="roi" fill="#17a2b8" name="ROI" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Detail Table */}
      <div style={card}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Channel Detail</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f9fa' }}>
                {['Channel', 'Spend', 'Attributed Revenue', 'Conversions', 'Share', 'ROI', 'ROAS', 'CPA'].map(h => (
                  <th key={h} style={{ padding: '12px 16px', textAlign: h === 'Channel' ? 'left' : 'right', fontWeight: '600', color: '#495057', borderBottom: '2px solid #e9ecef' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.channels.map((ch, idx) => (
                <tr key={ch.channel} style={{ borderBottom: '1px solid #f1f3f5', backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                  <td style={{ padding: '12px 16px', fontWeight: '600', color: COLORS[idx % COLORS.length] }}>{ch.channel}</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right' }}>{formatCurrency(ch.spend)}</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', color: '#28a745' }}>{formatCurrency(ch.attributed_value)}</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right' }}>{ch.attributed_conversions.toFixed(1)}</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right' }}>{(ch.attributed_share * 100).toFixed(1)}%</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', color: ch.roi >= 0 ? '#28a745' : '#dc3545' }}>
                    {(ch.roi * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', color: '#6f42c1' }}>{ch.roas.toFixed(2)}x</td>
                  <td style={{ padding: '12px 16px', textAlign: 'right' }}>{ch.cpa > 0 ? formatCurrency(ch.cpa) : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
