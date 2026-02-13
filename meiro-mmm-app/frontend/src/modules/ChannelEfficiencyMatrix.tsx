import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { apiGetJson } from '../lib/apiClient'

interface CompareData {
  channel: string
  roi: {
    conversions?: number
    aov?: number
    profit?: number
  }
  contrib: {
    conversions?: number
    aov?: number
    profit?: number
  }
}

export default function ChannelEfficiencyMatrix() {
  const [metricType, setMetricType] = useState<"roi" | "contrib">("roi")
  const { data, isLoading } = useQuery<CompareData[]>({
    queryKey: ["compare"],
    queryFn: async () => {
      try {
        return await apiGetJson<CompareData[]>('/api/models/compare', { fallbackMessage: 'Failed to fetch comparison data' })
      } catch (error) {
        console.error("Failed to fetch comparison data:", error)
        return []
      }
    }
  })

  const kpis = ["conversions", "aov", "profit"]

  const colorScale = (value: number | undefined) => {
    if (value === null || value === undefined) return "#f8f9fa"
    if (metricType === "roi") {
      if (value >= 2) return "#c8e6c9"  // Light green
      if (value >= 1) return "#e8f5e9"  // Very light green
      if (value >= 0.5) return "#fff3cd"  // Light yellow
      return "#ffcdd2"  // Light red
    } else {
      // For contribution, use shades of blue
      if (value >= 0.3) return "#e3f2fd"  // Light blue
      if (value >= 0.2) return "#e8f5e9"  // Very light green
      if (value >= 0.1) return "#fff3cd"  // Light yellow
      return "#f8f9fa"  // Gray
    }
  }

  const getDisplayValue = (val: number | undefined, type: "roi" | "contrib") => {
    if (val === null || val === undefined) return "—"
    if (type === "roi") {
      return val.toFixed(2)
    } else {
      return (val * 100).toFixed(1) + "%"
    }
  }

  const getKpiLabel = (kpi: string) => {
    const labels: Record<string, string> = {
      conversions: "Conversions",
      aov: "AOV",
      profit: "Profitability"
    }
    return labels[kpi] || kpi
  }

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto', padding: 32 }}>
      <header style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom: 32, paddingBottom: 20, borderBottom: '2px solid #e9ecef'}}>
        <h1 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', margin: 0 }}>Channel Efficiency Matrix</h1>
        <div style={{ display: 'flex', gap: 12 }}>
          <button onClick={() => window.location.reload()} style={{ 
            padding: '10px 20px', 
            fontSize: '14px', 
            fontWeight: '600',
            backgroundColor: '#007bff', 
            color: 'white', 
            border: 'none', 
            borderRadius: '6px', 
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(0,123,255,0.3)',
            transition: 'all 0.2s'
          }}
          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#0056b3'}
          onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#007bff'}>
            Back to Home
          </button>
        </div>
      </header>

      <p style={{ fontSize: '15px', color: '#6c757d', marginBottom: 32 }}>
        Compare ROI and contribution for each marketing channel across MMM models (Conversions, AOV, Profitability).
      </p>

      <div style={{ 
        marginBottom: 24,
        padding: 16,
        backgroundColor: '#fff',
        borderRadius: '8px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
        border: '1px solid #e9ecef'
      }}>
        <label style={{ marginRight: 12, fontWeight: '600', color: '#495057' }}>View:</label>
        <select
          value={metricType}
          onChange={(e) => setMetricType(e.target.value as "roi" | "contrib")}
          style={{
            padding: '8px 16px',
            fontSize: '14px',
            border: '2px solid #007bff',
            borderRadius: '6px',
            cursor: 'pointer',
            fontWeight: '500'
          }}
        >
          <option value="roi">ROI</option>
          <option value="contrib">Contribution Share</option>
        </select>
      </div>

      {isLoading ? (
        <div style={{ textAlign: 'center', padding: 60 }}>
          <div style={{ 
            width: '48px', 
            height: '48px', 
            border: '4px solid #e3f2fd', 
            borderTopColor: '#007bff', 
            borderRadius: '50%', 
            animation: 'spin 1s linear infinite',
            margin: '0 auto 20px'
          }}></div>
          <p style={{ fontSize: '16px', color: '#666' }}>Loading comparison data...</p>
        </div>
      ) : !data || !Array.isArray(data) || data.length === 0 ? (
        <div style={{ 
          padding: 40, 
          textAlign: 'center', 
          backgroundColor: '#fff',
          borderRadius: '8px',
          border: '1px solid #e9ecef'
        }}>
          <p style={{ fontSize: '16px', color: '#6c757d', margin: 0 }}>
            No model runs available for comparison. Run at least one model to see results.
          </p>
        </div>
      ) : (
        <div style={{ 
          overflowX: 'auto',
          backgroundColor: '#fff',
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          border: '1px solid #e9ecef'
        }}>
          <table style={{ 
            width: '100%', 
            borderCollapse: 'collapse',
            fontSize: '14px'
          }}>
            <thead style={{ backgroundColor: '#f8f9fa' }}>
              <tr>
                <th style={{ 
                  padding: '16px', 
                  textAlign: 'left',
                  fontWeight: '600',
                  color: '#495057',
                  borderBottom: '2px solid #e9ecef'
                }}>
                  Channel
                </th>
                {kpis.map((kpi) => (
                  <th 
                    key={kpi} 
                    style={{ 
                      padding: '16px', 
                      textAlign: 'center',
                      fontWeight: '600',
                      color: '#495057',
                      borderBottom: '2px solid #e9ecef',
                      fontSize: '13px'
                    }}
                  >
                    {getKpiLabel(kpi)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {Array.isArray(data) && data.map((row, idx) => (
                <tr 
                  key={row.channel} 
                  style={{ 
                    borderBottom: idx === data.length - 1 ? 'none' : '1px solid #e9ecef',
                    backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa'
                  }}
                >
                  <td style={{ 
                    padding: '16px',
                    fontWeight: '600',
                    color: '#495057'
                  }}>
                    {row.channel}
                  </td>
                  {kpis.map((kpi) => {
                    const val = row[metricType]?.[kpi as keyof typeof row.roi]
                    return (
                      <td 
                        key={kpi} 
                        style={{ 
                          padding: '16px',
                          textAlign: 'center',
                          backgroundColor: colorScale(val),
                          fontWeight: '500',
                          color: '#212529'
                        }}
                      >
                        {getDisplayValue(val, metricType)}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Legend */}
      <div style={{ marginTop: 24, padding: 16, backgroundColor: '#f8f9fa', borderRadius: '8px' }}>
        <p style={{ margin: '0 0 12px 0', fontWeight: '600', color: '#495057', fontSize: '14px' }}>
          Color Legend:
        </p>
        <div style={{ fontSize: '13px', color: '#6c757d' }}>
          {metricType === "roi" ? (
            <>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#c8e6c9', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                High (≥2.0)
              </span>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#e8f5e9', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Medium (1.0-2.0)
              </span>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#fff3cd', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Low (0.5-1.0)
              </span>
              <span style={{ display: 'inline-block' }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#ffcdd2', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Very Low (&lt;0.5)
              </span>
            </>
          ) : (
            <>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#e3f2fd', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                High (≥30%)
              </span>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#e8f5e9', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Medium (20-30%)
              </span>
              <span style={{ display: 'inline-block', marginRight: 16 }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#fff3cd', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Low (10-20%)
              </span>
              <span style={{ display: 'inline-block' }}>
                <span style={{ 
                  display: 'inline-block',
                  width: 16, 
                  height: 16, 
                  backgroundColor: '#f8f9fa', 
                  marginRight: 4,
                  borderRadius: '2px'
                }}></span>
                Very Low (&lt;10%)
              </span>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
