import { useState, useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts'

interface BudgetOptimizerProps {
  roiData: { channel: string; roi: number }[]
  contribData: { channel: string; mean_share: number }[]
  baselineKPI?: number
  runId?: string
}

interface BudgetScenario {
  [channel: string]: number
}

interface ChannelConstraint {
  min: number
  max: number
  locked: boolean
}

interface WhatIfResult {
  baseline: {
    total_kpi: number
    per_channel: Record<string, number>
  }
  scenario: {
    total_kpi: number
    per_channel: Record<string, number>
    multipliers: Record<string, number>
  }
  lift: {
    absolute: number
    percent: number
  }
}

const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c7c', '#8dd1e1', '#d084d0']

export default function BudgetOptimizer({ roiData, contribData, baselineKPI = 100, runId }: BudgetOptimizerProps) {
  const [multipliers, setMultipliers] = useState<BudgetScenario>(
    roiData.reduce((acc, { channel }) => {
      acc[channel] = 1.0
      return acc
    }, {} as BudgetScenario)
  )
  const [constraints, setConstraints] = useState<Record<string, ChannelConstraint>>(
    roiData.reduce((acc, { channel }) => {
      acc[channel] = { min: 0.5, max: 2.0, locked: false }
      return acc
    }, {} as Record<string, ChannelConstraint>)
  )
  const [optimalMix, setOptimalMix] = useState<BudgetScenario | null>(null)
  const [optimalUplift, setOptimalUplift] = useState<number | null>(null)
  const [optimizationMessage, setOptimizationMessage] = useState<string | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)
  const [totalBudget, setTotalBudget] = useState(1.0)
  const [whatIfResult, setWhatIfResult] = useState<WhatIfResult | null>(null)
  const [isWhatIfLoading, setIsWhatIfLoading] = useState(false)

  // Calculate baseline (when all multipliers = 1.0)
  // Contributions represent the share of impact each channel drives
  const baseline = useMemo(() => {
    // Baseline = sum of all contributions at multiplier 1.0
    // This should be normalized to 100
    let total = 0
    for (const contrib of contribData) {
      total += contrib.mean_share || 0
    }
    // Scale to 100 for display (contributions sum to ~1.0)
    return total > 0 ? 100 : 0
  }, [contribData])

  // Calculate current predicted KPI with current multipliers
  const predictedKPI = useMemo(() => {
    const contribMap = contribData.reduce((acc, { channel, mean_share }) => {
      acc[channel] = mean_share
      return acc
    }, {} as Record<string, number>)

    // Predicted KPI = Σ(contrib[ch] * multiplier[ch]) * 100
    let total = 0
    for (const channel of contribData.map(c => c.channel)) {
      const contrib = contribMap[channel] || 0
      const multiplier = multipliers[channel] || 1.0
      total += contrib * multiplier
    }
    // Scale by 100 to match baseline units
    return total > 0 ? total * 100 : 0
  }, [contribData, multipliers])

  const upliftPercent = ((predictedKPI - baseline) / baseline) * 100

  const handleSliderChange = (channel: string, value: string) => {
    setMultipliers(prev => ({ ...prev, [channel]: parseFloat(value) }))
  }

  const handleReset = () => {
    setMultipliers(
      roiData.reduce((acc, { channel }) => {
        acc[channel] = 1.0
        return acc
      }, {} as BudgetScenario)
    )
    setOptimalMix(null)
    setOptimalUplift(null)
    setOptimizationMessage(null)
    setTotalBudget(1.0)
    setWhatIfResult(null)
  }

  const updateConstraint = (channel: string, patch: Partial<ChannelConstraint>) => {
    setConstraints(prev => ({
      ...prev,
      [channel]: { ...(prev[channel] || { min: 0.5, max: 2.0, locked: false }), ...patch }
    }))
  }

  const handleSuggestOptimal = async () => {
    if (!runId) {
      alert('Run ID not available')
      return
    }

    setIsOptimizing(true)
    try {
      const channel_constraints: Record<string, any> = {}
      Object.entries(constraints).forEach(([ch, c]) => {
        channel_constraints[ch] = {
          min: c.min,
          max: c.max,
          locked: c.locked,
        }
      })

      const res = await fetch(`/api/models/${runId}/optimize/auto`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          total_budget: totalBudget,
          min_spend: 0.5,
          max_spend: 2.0,
          channel_constraints,
        }),
      })

      if (!res.ok) throw new Error('Optimization failed')

      const data = await res.json()
      setOptimalMix(data.optimal_mix)
      setOptimalUplift(data.uplift)
      setOptimizationMessage(data.message || 'Optimization complete')
    } catch (error) {
      console.error('Optimization error:', error)
      alert('Failed to get optimal mix. Please try again.')
    } finally {
      setIsOptimizing(false)
    }
  }

  const handleRunWhatIfBackend = async () => {
    if (!runId) {
      alert('Run ID not available')
      return
    }

    setIsWhatIfLoading(true)
    setWhatIfResult(null)
    try {
      const res = await fetch(`/api/models/${runId}/what_if`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(multipliers),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        const d = data?.detail
        const msg = typeof d === 'string' ? d : Array.isArray(d) ? (d[0]?.msg || JSON.stringify(d)) : (d?.msg || 'What-if simulation failed')
        throw new Error(msg)
      }
      setWhatIfResult(data as WhatIfResult)
    } catch (error) {
      console.error('What-if error:', error)
      alert(error instanceof Error ? error.message : 'Failed to run backend what-if simulation. Please try again.')
    } finally {
      setIsWhatIfLoading(false)
    }
  }

  const handleApplyOptimal = () => {
    if (optimalMix) {
      setMultipliers(optimalMix)
    }
  }

  // Prepare data for visualization
  const chartData = roiData.map(({ channel }, idx) => ({
    channel,
    multiplier: multipliers[channel],
    color: COLORS[idx % COLORS.length]
  }))

  return (
    <div style={{ marginTop: 32, backgroundColor: '#fafafa', padding: 24, borderRadius: 8 }}>
      <h2 style={{ marginTop: 0, marginBottom: 24 }}>Budget Optimizer</h2>
      <p style={{ fontSize: '14px', color: '#666', marginBottom: 24 }}>
        Adjust channel spend levels to see predicted KPI impact. 1.0 = current spend, 2.0 = double budget. The optimizer
        works with relative multipliers around your current mix.
      </p>

      {/* Global optimization settings */}
      <div style={{ 
        display: 'flex', 
        gap: 16, 
        marginBottom: 24, 
        padding: 16, 
        borderRadius: 8, 
        backgroundColor: 'white',
        border: '1px solid #e0e0e0'
      }}>
        <div style={{ flex: 1 }}>
          <label style={{ display: 'block', fontSize: 13, fontWeight: 600, marginBottom: 4 }}>
            Average budget multiplier (all channels)
          </label>
          <input
            type="number"
            min={0.5}
            max={2}
            step={0.1}
            value={totalBudget}
            onChange={(e) => setTotalBudget(parseFloat(e.target.value) || 1)}
            style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #ced4da', fontSize: 13 }}
          />
          <p style={{ fontSize: 12, color: '#6c757d', marginTop: 4 }}>
            1.0 keeps the overall budget at baseline; values &gt; 1.0 increase total spend, &lt; 1.0 decrease it.
          </p>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        {/* Left: Sliders & per-channel constraints */}
        <div>
          <h3 style={{ marginTop: 0, marginBottom: 16 }}>Spend Multipliers</h3>
          {roiData.map(({ channel }, idx) => {
            const multiplier = multipliers[channel] || 1.0
            const spendChange = ((multiplier - 1) * 100).toFixed(0)
            const spendChangeStr = spendChange === '0' ? '0%' : spendChange.startsWith('-') ? `${spendChange}%` : `+${spendChange}%`
            
            const constraint = constraints[channel] || { min: 0.5, max: 2.0, locked: false }
            return (
              <div key={channel} style={{ marginBottom: 24 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <label style={{ fontWeight: 'bold', color: COLORS[idx % COLORS.length] }}>
                    {channel}
                  </label>
                  <span style={{ fontSize: '14px', fontWeight: 'bold' }}>
                    {multiplier.toFixed(2)}x {spendChangeStr}
                  </span>
                </div>
                <input
                  type="range"
                  min="0"
                  max="2"
                  step="0.05"
                  value={multiplier}
                  onChange={(e) => handleSliderChange(channel, e.target.value)}
                  style={{ width: '100%', accentColor: COLORS[idx % COLORS.length] }}
                />
                <div style={{ display: 'flex', gap: 8, marginTop: 8, alignItems: 'center', fontSize: 12 }}>
                  <label>
                    Min&nbsp;
                    <input
                      type="number"
                      step={0.1}
                      value={constraint.min}
                      onChange={(e) => updateConstraint(channel, { min: parseFloat(e.target.value) || 0 })}
                      style={{ width: 60, padding: 4, borderRadius: 4, border: '1px solid #ced4da' }}
                    />
                  </label>
                  <label>
                    Max&nbsp;
                    <input
                      type="number"
                      step={0.1}
                      value={constraint.max}
                      onChange={(e) => updateConstraint(channel, { max: parseFloat(e.target.value) || 0 })}
                      style={{ width: 60, padding: 4, borderRadius: 4, border: '1px solid #ced4da' }}
                    />
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <input
                      type="checkbox"
                      checked={constraint.locked}
                      onChange={(e) => updateConstraint(channel, { locked: e.target.checked })}
                    />
                    Lock
                  </label>
                </div>
              </div>
            )
          })}
        </div>

        {/* Right: Summary & Visualization */}
        <div>
          <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8, marginBottom: 16 }}>
            <h3 style={{ marginTop: 0, marginBottom: 16 }}>Predicted Impact</h3>
            <div style={{ marginBottom: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span>Predicted KPI:</span>
                <span style={{ fontWeight: 'bold', color: upliftPercent >= 0 ? '#28a745' : '#dc3545' }}>
                  {predictedKPI.toFixed(0)}
                </span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span>Baseline KPI:</span>
                <span style={{ fontWeight: 'bold' }}>{baseline.toFixed(0)}</span>
              </div>
              <hr style={{ margin: '12px 0', borderColor: '#ddd' }} />
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span><strong>Uplift:</strong></span>
                <span style={{ 
                  fontWeight: 'bold', 
                  fontSize: '18px',
                  color: upliftPercent >= 0 ? '#28a745' : '#dc3545'
                }}>
                  {upliftPercent >= 0 ? '+' : ''}{upliftPercent.toFixed(1)}%
                </span>
              </div>
            </div>
          </div>

          {/* Bar Chart */}
          <div style={{ backgroundColor: 'white', padding: 20, borderRadius: 8 }}>
            <h4 style={{ marginTop: 0, marginBottom: 16 }}>Budget Allocation</h4>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="channel" fontSize={12} />
                <YAxis domain={[0, 2]} label={{ value: 'Multiplier', angle: -90, position: 'insideLeft' }} />
                <Tooltip formatter={(value) => `${parseFloat(value as string).toFixed(2)}x`} />
                <Bar dataKey="multiplier" radius={4}>
                  {chartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Suggested Optimal Mix */}
      {optimalUplift !== null && optimalMix && (
        <div style={{ 
          marginTop: 24, 
          backgroundColor: 'white', 
          padding: 20, 
          borderRadius: 8, 
          border: `2px solid ${optimalUplift > 0.5 ? '#28a745' : optimalUplift === 0 ? '#6c757d' : '#ff9800'}` 
        }}>
          <h3 style={{ 
            marginTop: 0, 
            fontSize: '20px',
            fontWeight: '600',
            color: optimalUplift > 0.5 ? '#28a745' : optimalUplift === 0 ? '#6c757d' : '#ff9800' 
          }}>
            Optimal Mix Suggestion
          </h3>
          
          {/* Status Message */}
          {optimizationMessage && (
            <div style={{ 
              marginBottom: 16, 
              padding: 12, 
              borderRadius: 4,
              backgroundColor: optimalUplift > 0.5 ? '#d4edda' : optimalUplift === 0 ? '#f8f9fa' : '#fff3cd',
              color: optimalUplift > 0.5 ? '#155724' : optimalUplift === 0 ? '#6c757d' : '#856404',
              fontSize: '14px',
              borderLeft: `4px solid ${optimalUplift > 0.5 ? '#28a745' : optimalUplift === 0 ? '#6c757d' : '#ff9800'}`
            }}>
              {optimizationMessage}
            </div>
          )}
          
          <div style={{ marginBottom: 16 }}>
            <p style={{ fontSize: '18px', fontWeight: 'bold', color: optimalUplift >= 0 ? '#28a745' : '#dc3545' }}>
              Suggested Uplift: {optimalUplift >= 0 ? '+' : ''}{optimalUplift.toFixed(1)}%
            </p>
            <div style={{ marginTop: 12, fontSize: '14px' }}>
              <strong>Suggested Allocation:</strong>
              <ul style={{ listStyle: 'none', padding: 0, marginTop: 8 }}>
                {Object.entries(optimalMix).map(([channel, multiplier]) => {
                  const currentValue = multipliers[channel] || 1.0
                  const isChange = Math.abs(multiplier - currentValue) > 0.01
                  return (
                    <li key={channel} style={{ padding: '4px 0', color: isChange ? (optimalUplift > 0.5 ? '#28a745' : '#ff9800') : '#666' }}>
                      <strong>{channel}:</strong> {multiplier.toFixed(2)}x {isChange && (multiplier > currentValue ? ' ↑' : ' ↓')}
                    </li>
                  )
                })}
              </ul>
            </div>
          </div>
          <button
            onClick={handleApplyOptimal}
            style={{
              padding: '10px 24px',
              fontSize: '14px',
              backgroundColor: optimalUplift > 0.5 ? '#28a745' : '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              marginRight: 8
            }}
          >
            Apply Suggested Mix
          </button>
        </div>
      )}

      {/* Action Buttons */}
      <div style={{ marginTop: 24, display: 'flex', flexDirection: 'column', gap: 16 }}>
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', flexWrap: 'wrap' }}>
          <button
            onClick={handleSuggestOptimal}
            disabled={isOptimizing || !runId}
            style={{
              padding: '10px 24px',
              fontSize: '14px',
              backgroundColor: (!runId || isOptimizing) ? '#6c757d' : '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: (!runId || isOptimizing) ? 'not-allowed' : 'pointer'
            }}
          >
            {isOptimizing ? 'Optimizing...' : 'Suggest Optimal Mix'}
          </button>
          <button
            onClick={handleRunWhatIfBackend}
            disabled={isWhatIfLoading || !runId}
            style={{
              padding: '10px 24px',
              fontSize: '14px',
              backgroundColor: (!runId || isWhatIfLoading) ? '#6c757d' : '#17a2b8',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: (!runId || isWhatIfLoading) ? 'not-allowed' : 'pointer'
            }}
          >
            {isWhatIfLoading ? 'Running…' : 'Run backend what-if'}
          </button>
          <button
            onClick={handleReset}
            style={{
              padding: '10px 24px',
              fontSize: '14px',
              backgroundColor: '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Reset to Baseline
          </button>
        </div>

        {whatIfResult && (
          <div
            style={{
              marginTop: 8,
              backgroundColor: 'white',
              padding: 16,
              borderRadius: 8,
              border: '1px solid #e0e0e0'
            }}
          >
            <h3 style={{ marginTop: 0, marginBottom: 8, fontSize: 16 }}>Backend What-if Summary</h3>
            <p style={{ fontSize: 13, color: '#6c757d', marginBottom: 12 }}>
              Uses the fitted MMM model (ROI × contribution) to recompute total KPI and channel contributions for the current slider multipliers.
            </p>
            <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
              <div style={{ minWidth: 180 }}>
                <p style={{ margin: 0, fontSize: 13 }}>Baseline KPI</p>
                <p style={{ margin: 0, fontSize: 18, fontWeight: 600 }}>
                  {whatIfResult.baseline.total_kpi.toFixed(2)}
                </p>
              </div>
              <div style={{ minWidth: 180 }}>
                <p style={{ margin: 0, fontSize: 13 }}>Scenario KPI</p>
                <p
                  style={{
                    margin: 0,
                    fontSize: 18,
                    fontWeight: 600,
                    color: whatIfResult.lift.percent >= 0 ? '#28a745' : '#dc3545'
                  }}
                >
                  {whatIfResult.scenario.total_kpi.toFixed(2)}
                </p>
              </div>
              <div style={{ minWidth: 180 }}>
                <p style={{ margin: 0, fontSize: 13 }}>Lift</p>
                <p
                  style={{
                    margin: 0,
                    fontSize: 18,
                    fontWeight: 600,
                    color: whatIfResult.lift.percent >= 0 ? '#28a745' : '#dc3545'
                  }}
                >
                  {whatIfResult.lift.percent >= 0 ? '+' : ''}
                  {whatIfResult.lift.percent.toFixed(1)}%
                </p>
              </div>
            </div>

            <div style={{ marginTop: 16, overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: 6, borderBottom: '1px solid #dee2e6' }}>Channel</th>
                    <th style={{ textAlign: 'right', padding: 6, borderBottom: '1px solid #dee2e6' }}>Baseline</th>
                    <th style={{ textAlign: 'right', padding: 6, borderBottom: '1px solid #dee2e6' }}>Scenario</th>
                    <th style={{ textAlign: 'right', padding: 6, borderBottom: '1px solid #dee2e6' }}>Delta</th>
                    <th style={{ textAlign: 'right', padding: 6, borderBottom: '1px solid #dee2e6' }}>Multiplier</th>
                  </tr>
                </thead>
                <tbody>
                  {roiData.map(({ channel }) => {
                    const base = whatIfResult.baseline.per_channel[channel] ?? 0
                    const scen = whatIfResult.scenario.per_channel[channel] ?? 0
                    const mult = whatIfResult.scenario.multipliers[channel] ?? 1
                    const delta = scen - base
                    return (
                      <tr key={channel}>
                        <td style={{ padding: 6, borderBottom: '1px solid #f1f3f5' }}>{channel}</td>
                        <td style={{ padding: 6, borderBottom: '1px solid #f1f3f5', textAlign: 'right' }}>
                          {base.toFixed(3)}
                        </td>
                        <td style={{ padding: 6, borderBottom: '1px solid #f1f3f5', textAlign: 'right' }}>
                          {scen.toFixed(3)}
                        </td>
                        <td
                          style={{
                            padding: 6,
                            borderBottom: '1px solid #f1f3f5',
                            textAlign: 'right',
                            color: delta >= 0 ? '#28a745' : '#dc3545'
                          }}
                        >
                          {delta >= 0 ? '+' : ''}
                          {delta.toFixed(3)}
                        </td>
                        <td style={{ padding: 6, borderBottom: '1px solid #f1f3f5', textAlign: 'right' }}>
                          {mult.toFixed(2)}x
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
