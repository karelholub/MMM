import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  BarChart, Bar, ResponsiveContainer
} from 'recharts'

interface MMMDashboardProps {
  runId: string
  datasetId: string
}

interface KPI {
  channel: string
  roi: number
}

interface Contrib {
  channel: string
  mean_share: number
}

export default function MMMDashboard({ runId, datasetId }: MMMDashboardProps) {
  const [dataset, setDataset] = useState<any[]>([])

  const { data: run } = useQuery({
    queryKey: ['run', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}`)
      if (!res.ok) throw new Error('Failed to fetch run')
      return res.json()
    }
  })

  const { data: contrib } = useQuery({
    queryKey: ['contrib', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}/contrib`)
      if (!res.ok) throw new Error('Failed to fetch contributions')
      return res.json()
    }
  })

  const { data: roi } = useQuery({
    queryKey: ['roi', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}/roi`)
      if (!res.ok) throw new Error('Failed to fetch ROI')
      return res.json()
    }
  })

  useEffect(() => {
    if (datasetId) {
      fetch(`/api/datasets/${datasetId}?preview_only=false`)
        .then(res => res.json())
        .then(d => {
          setDataset(d.preview_rows || [])
        })
        .catch(err => console.error('Failed to fetch dataset:', err))
    }
  }, [datasetId])

  // Calculate summary metrics
  const kpiTotal = dataset.reduce((sum, row) => {
    const kpiValue = run?.config?.kpi ? row[run.config.kpi] : 0
    return sum + (kpiValue || 0)
  }, 0)

  const totalSpend = dataset.reduce((sum, row) => {
    let rowSpend = 0
    const channels = run?.config?.spend_channels || []
    channels.forEach((ch: string) => {
      rowSpend += row[ch] || 0
    })
    return sum + rowSpend
  }, 0)

  const weightedROI = roi && roi.length > 0 
    ? roi.reduce((sum: number, r: KPI) => sum + r.roi, 0) / roi.length
    : 0

  const uplift = run?.uplift || 0

  // Get KPI name from mode
  const getKpiDisplayName = () => {
    const mode = run?.kpi_mode || run?.config?.kpi_mode || 'conversions'
    const mapping: Record<string, string> = {
      conversions: 'Conversions',
      aov: 'Average Order Value (AOV)',
      profit: 'Profitability'
    }
    return mapping[mode] || 'MMM'
  }

  return (
    <div style={{ marginTop: 32, padding: 24, backgroundColor: '#f9fafb' }}>
      <h1 style={{ fontSize: '28px', fontWeight: 'bold', marginBottom: 32, color: '#333' }}>
        {getKpiDisplayName()} Dashboard
      </h1>

      {/* KPI Summary Cards */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
        gap: 16, 
        marginBottom: 32 
      }}>
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h3 style={{ fontSize: '14px', color: '#666', marginBottom: 8 }}>Total KPI</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>
            {kpiTotal.toLocaleString()}
          </p>
        </div>
        
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h3 style={{ fontSize: '14px', color: '#666', marginBottom: 8 }}>Model Fit (R²)</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>
            {run?.r2 ? run.r2.toFixed(3) : '—'}
          </p>
        </div>
        
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h3 style={{ fontSize: '14px', color: '#666', marginBottom: 8 }}>Optimal Uplift</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold', color: uplift > 0 ? '#28a745' : '#6c757d' }}>
            {uplift ? `+${(uplift * 100).toFixed(1)}%` : '—'}
          </p>
        </div>
        
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h3 style={{ fontSize: '14px', color: '#666', marginBottom: 8 }}>Channels Modeled</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>
            {run?.config?.spend_channels?.length || 0}
          </p>
        </div>
      </div>

      {/* Chart Container */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginBottom: 24 }}>
        {/* Spend vs KPI Trend */}
        {dataset.length > 0 && (
          <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
            <h2 style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: 16 }}>Spend vs KPI Over Time</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dataset}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                <YAxis yAxisId="left" orientation="left" stroke="#8884d8" tick={{ fontSize: 12 }} />
                <YAxis yAxisId="right" orientation="right" stroke="#82ca9d" tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                {run?.config?.spend_channels?.slice(0, 2).map((channel: string, idx: number) => (
                  <Line
                    key={channel}
                    yAxisId="left"
                    type="monotone"
                    dataKey={channel}
                    stroke={['#8884d8', '#82ca9d'][idx] || '#8884d8'}
                    name={`${channel} Spend`}
                  />
                ))}
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey={run?.config?.kpi || 'sales'}
                  stroke="#ff7300"
                  strokeWidth={2}
                  name="KPI"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Total Spend Over Time */}
        {dataset.length > 0 && (
          <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
            <h2 style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: 16 }}>Total Spend Trend</h2>
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={dataset.map((row: any) => {
                const channels = run?.config?.spend_channels || []
                const totalSpend = channels.reduce((sum: number, ch: string) => sum + (row[ch] || 0), 0)
                return { ...row, total_spend: totalSpend }
              })}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                <Area type="monotone" dataKey="total_spend" stroke="#8884d8" fill="#8884d8" fillOpacity={0.3} name="Total Spend" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* ROI by Channel */}
      {roi && roi.length > 0 && (
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)', marginBottom: 24 }}>
          <h2 style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: 16 }}>ROI by Channel</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={roi}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis dataKey="channel" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              <Bar dataKey="roi" fill="#82ca9d" name="ROI" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Channel Contribution */}
      {contrib && contrib.length > 0 && (
        <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)', marginBottom: 24 }}>
          <h2 style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: 16 }}>Channel Contribution</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={contrib}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis dataKey="channel" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} />
              <Legend />
              <Bar dataKey="mean_share" fill="#8884d8" name="Contribution Share" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Summary Stats */}
      <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h2 style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: 16 }}>Summary Statistics</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16 }}>
          <div>
            <p style={{ fontSize: '14px', color: '#666', marginBottom: 4 }}>Total Spend</p>
            <p style={{ fontSize: '20px', fontWeight: 'bold' }}>
              ${totalSpend.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            </p>
          </div>
          <div>
            <p style={{ fontSize: '14px', color: '#666', marginBottom: 4 }}>Avg ROI per Channel</p>
            <p style={{ fontSize: '20px', fontWeight: 'bold', color: weightedROI > 1 ? '#28a745' : '#dc3545' }}>
              {weightedROI.toFixed(2)}x
            </p>
          </div>
          <div>
            <p style={{ fontSize: '14px', color: '#666', marginBottom: 4 }}>Total Data Points</p>
            <p style={{ fontSize: '20px', fontWeight: 'bold' }}>
              {dataset.length} weeks
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

