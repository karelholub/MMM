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

const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c7c', '#8dd1e1', '#d084d0']

export default function BudgetOptimizer({ roiData, contribData, baselineKPI = 100, runId }: BudgetOptimizerProps) {
  const [multipliers, setMultipliers] = useState<BudgetScenario>(
    roiData.reduce((acc, { channel }) => {
      acc[channel] = 1.0
      return acc
    }, {} as BudgetScenario)
  )
  const [optimalMix, setOptimalMix] = useState<BudgetScenario | null>(null)
  const [optimalUplift, setOptimalUplift] = useState<number | null>(null)
  const [optimizationMessage, setOptimizationMessage] = useState<string | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)

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
  }

  const handleSuggestOptimal = async () => {
    if (!runId) {
      alert('Run ID not available')
      return
    }

    setIsOptimizing(true)
    try {
      const res = await fetch(`/api/models/${runId}/optimize/auto`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ total_budget: 1.0 }),
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
        Adjust channel spend levels to see predicted KPI impact. 1.0 = current spend, 2.0 = double budget.
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        {/* Left: Sliders */}
        <div>
          <h3 style={{ marginTop: 0, marginBottom: 16 }}>Spend Multipliers</h3>
          {roiData.map(({ channel }, idx) => {
            const multiplier = multipliers[channel] || 1.0
            const spendChange = ((multiplier - 1) * 100).toFixed(0)
            const spendChangeStr = spendChange === '0' ? '0%' : spendChange.startsWith('-') ? `${spendChange}%` : `+${spendChange}%`
            
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
      <div style={{ marginTop: 24, textAlign: 'center', display: 'flex', gap: 8, justifyContent: 'center' }}>
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
    </div>
  )
}
