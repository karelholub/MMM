import { useEffect, useMemo, useState } from 'react'

interface ChannelRow {
  channel: string
  spend: number
  roi: number
  mroas: number
  elasticity: number
}

interface CampaignRow {
  channel: string
  campaign: string
  mean_spend: number
  roi: number
  mroas: number
  elasticity: number
}

export default function CampaignDrilldown({ runId }: { runId: string }) {
  const [channels, setChannels] = useState<ChannelRow[]>([])
  const [campaigns, setCampaigns] = useState<CampaignRow[]>([])
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [multipliers, setMultipliers] = useState<Record<string, number>>({})
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let mounted = true
    const load = async () => {
      setLoading(true)
      try {
        const [ch, ca] = await Promise.all([
          fetch(`/api/models/${runId}/summary/channel`).then(r => r.json()),
          fetch(`/api/models/${runId}/summary/campaign`).then(r => r.json()),
        ])
        if (!mounted) return
        setChannels(Array.isArray(ch) ? ch : [])
        setCampaigns(Array.isArray(ca) ? ca : [])
        // init multipliers to 1.0 per campaign key `${channel}|${campaign}`
        const init: Record<string, number> = {}
        ;(Array.isArray(ca) ? ca : []).forEach((row: any) => {
          const key = `${row.channel}|${row.campaign}`
          init[key] = 1.0
        })
        setMultipliers(init)
      } finally {
        if (mounted) setLoading(false)
      }
    }
    load()
    return () => { mounted = false }
  }, [runId])

  const campaignsByChannel = useMemo(() => {
    const map: Record<string, CampaignRow[]> = {}
    for (const row of campaigns) {
      if (!map[row.channel]) map[row.channel] = []
      map[row.channel].push(row)
    }
    return map
  }, [campaigns])

  const handleToggle = (channel: string) => {
    setExpanded(prev => ({ ...prev, [channel]: !prev[channel] }))
  }

  const handleMultiplier = (channel: string, campaign: string, val: number) => {
    const key = `${channel}|${campaign}`
    setMultipliers(prev => ({ ...prev, [key]: val }))
  }

  // Simple predicted uplift calculator using roi * spend * multiplier
  const predicted = useMemo(() => {
    let baseline = 0
    let scenario = 0
    for (const row of campaigns) {
      const key = `${row.channel}|${row.campaign}`
      const m = multipliers[key] ?? 1.0
      const base = row.mean_spend * (row.roi || 0)
      baseline += base
      scenario += base * m
    }
    const upliftPct = baseline > 0 ? ((scenario - baseline) / baseline) * 100 : 0
    return { baseline, scenario, upliftPct }
  }, [campaigns, multipliers])

  const handleReset = () => {
    const reset: Record<string, number> = {}
    Object.keys(multipliers).forEach(k => reset[k] = 1.0)
    setMultipliers(reset)
  }

  return (
    <div style={{ marginTop: 32 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <h2 style={{ margin: 0 }}>Campaign Drill-down</h2>
        <div style={{ display: 'flex', gap: 8 }}>
          <a
            href={`/api/models/${runId}/export.csv`}
            style={{ padding: '10px 16px', background: '#6c757d', color: '#fff', borderRadius: 6, textDecoration: 'none' }}
          >
            Export CSV
          </a>
          <button onClick={handleReset} style={{ padding: '10px 16px', background: '#007bff', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
            Reset Multipliers
          </button>
        </div>
      </div>

      <div style={{ marginBottom: 16, background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <div>Predicted KPI (baseline): <strong>{predicted.baseline.toFixed(0)}</strong></div>
          <div>Scenario KPI: <strong style={{ color: predicted.upliftPct >= 0 ? '#28a745' : '#dc3545' }}>{predicted.scenario.toFixed(0)}</strong></div>
          <div>Uplift: <strong style={{ color: predicted.upliftPct >= 0 ? '#28a745' : '#dc3545' }}>{predicted.upliftPct >= 0 ? '+' : ''}{predicted.upliftPct.toFixed(1)}%</strong></div>
        </div>
      </div>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 40 }}>Loading campaign data…</div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 12 }}>
          {channels.map((ch) => (
            <div key={ch.channel} style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <div style={{ fontWeight: 700 }}>{ch.channel}</div>
                  <div style={{ fontSize: 12, color: '#6c757d' }}>Spend: {ch.spend.toFixed(0)} · ROI: {ch.roi.toFixed(2)} · mROAS: {ch.mroas.toFixed(2)} · Elasticity: {ch.elasticity.toFixed(2)}</div>
                </div>
                <button onClick={() => handleToggle(ch.channel)} style={{ padding: '6px 12px', background: '#f8f9fa', border: '1px solid #e9ecef', borderRadius: 6, cursor: 'pointer' }}>
                  {expanded[ch.channel] ? 'Hide Campaigns' : 'Show Campaigns'}
                </button>
              </div>

              {expanded[ch.channel] && (
                <div style={{ marginTop: 12 }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
                    <thead>
                      <tr style={{ background: '#f8f9fa' }}>
                        <th style={{ textAlign: 'left', padding: 8, borderBottom: '1px solid #e9ecef' }}>Campaign</th>
                        <th style={{ textAlign: 'right', padding: 8, borderBottom: '1px solid #e9ecef' }}>Spend</th>
                        <th style={{ textAlign: 'right', padding: 8, borderBottom: '1px solid #e9ecef' }}>ROI</th>
                        <th style={{ textAlign: 'right', padding: 8, borderBottom: '1px solid #e9ecef' }}>mROAS</th>
                        <th style={{ textAlign: 'right', padding: 8, borderBottom: '1px solid #e9ecef' }}>Elasticity</th>
                        <th style={{ textAlign: 'center', padding: 8, borderBottom: '1px solid #e9ecef' }}>Multiplier</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(campaignsByChannel[ch.channel] || []).map(row => {
                        const key = `${row.channel}|${row.campaign}`
                        const m = multipliers[key] ?? 1.0
                        return (
                          <tr key={key}>
                            <td style={{ padding: 8, borderBottom: '1px solid #f1f3f5' }}>{row.campaign}</td>
                            <td style={{ padding: 8, textAlign: 'right', borderBottom: '1px solid #f1f3f5' }}>{row.mean_spend.toFixed(0)}</td>
                            <td style={{ padding: 8, textAlign: 'right', borderBottom: '1px solid #f1f3f5' }}>{row.roi.toFixed(2)}</td>
                            <td style={{ padding: 8, textAlign: 'right', borderBottom: '1px solid #f1f3f5' }}>{row.mroas.toFixed(2)}</td>
                            <td style={{ padding: 8, textAlign: 'right', borderBottom: '1px solid #f1f3f5' }}>{row.elasticity.toFixed(2)}</td>
                            <td style={{ padding: 8, textAlign: 'center', borderBottom: '1px solid #f1f3f5' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <input type="range" min={0} max={2} step={0.05} value={m}
                                  onChange={(e) => handleMultiplier(row.channel, row.campaign, parseFloat(e.target.value))}
                                  style={{ width: 160 }} />
                                <div style={{ width: 54, textAlign: 'right' }}>{m.toFixed(2)}x</div>
                              </div>
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}


