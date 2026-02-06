import { useState } from 'react'

interface ModelConfig {
  dataset_id: string
  frequency: string
  kpi_mode: string
  kpi: string
  spend_channels: string[]
  covariates: string[]
  priors: {
    adstock: { alpha_mean: number; alpha_sd: number }
    saturation: { lam_mean: number; lam_sd: number }
  }
  mcmc: {
    draws: number
    tune: number
    chains: number
    target_accept: number
  }
}

interface ModelConfiguratorProps {
  datasetId: string
  kpi: string
  spendChannels: string[]
  covariates?: string[]
  onRunModel: (config: ModelConfig) => void
}

export default function ModelConfigurator({ 
  datasetId, 
  kpi, 
  spendChannels, 
  covariates = [], 
  onRunModel 
}: ModelConfiguratorProps) {
  const [kpiMode, setKpiMode] = useState('conversions')
  const [alphaMean, setAlphaMean] = useState(0.5)
  const [alphaSd, setAlphaSd] = useState(0.2)
  const [lamMean, setLamMean] = useState(0.001)
  const [lamSd, setLamSd] = useState(0.0005)
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [draws, setDraws] = useState(1000)
  const [tune, setTune] = useState(1000)
  const [chains, setChains] = useState(4)
  const [targetAccept, setTargetAccept] = useState(0.9)

  const handleRunModel = () => {
    // Map kpi_mode to actual column name
    const kpiMapping: Record<string, string> = {
      'conversions': 'conversions',
      'aov': 'aov',
      'profit': 'profit'
    }
    const actualKpi = kpiMapping[kpiMode] || kpi
    
    const config: ModelConfig = {
      dataset_id: datasetId,
      frequency: 'W',
      kpi_mode: kpiMode,
      kpi: actualKpi,
      spend_channels: spendChannels,
      covariates,
      priors: {
        adstock: {
          alpha_mean: alphaMean,
          alpha_sd: alphaSd
        },
        saturation: {
          lam_mean: lamMean,
          lam_sd: lamSd
        }
      },
      mcmc: {
        draws,
        tune,
        chains,
        target_accept: targetAccept
      }
    }
    onRunModel(config)
  }

  return (
    <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
      <h2>Configure Model</h2>
      
      {/* Dataset Summary */}
      <div style={{ backgroundColor: '#f5f5f5', padding: 16, borderRadius: 4, marginBottom: 24 }}>
        <h3 style={{ marginTop: 0 }}>Dataset Summary</h3>
        <p><strong>Dataset ID:</strong> {datasetId}</p>
        <p><strong>KPI:</strong> {kpi}</p>
        <p><strong>Spend Channels:</strong> {spendChannels.join(', ')}</p>
        {covariates.length > 0 && (
          <p><strong>Covariates:</strong> {covariates.join(', ')}</p>
        )}
      </div>

      {/* KPI Selection */}
      <div style={{ backgroundColor: '#f8f9fa', padding: 20, borderRadius: 8, marginBottom: 24, border: '2px solid #1976d2' }}>
        <h3 style={{ marginTop: 0, color: '#1976d2', fontSize: '18px', fontWeight: '600' }}>Select Model Type</h3>
        <label style={{ display: 'block', marginBottom: 12, fontWeight: '600', fontSize: '14px', color: '#333' }}>
          Choose which KPI to optimize:
        </label>
        <select
          value={kpiMode}
          onChange={(e) => setKpiMode(e.target.value)}
          style={{
            width: '100%',
            padding: '12px',
            fontSize: '15px',
            border: '2px solid #1976d2',
            borderRadius: '6px',
            backgroundColor: 'white',
            cursor: 'pointer',
            fontWeight: '500'
          }}
        >
          <option value="conversions">Conversions (Volume) - Optimize for conversion count</option>
          <option value="aov">Average Order Value (AOV) - Optimize for revenue per order</option>
          <option value="profit">Profitability - Optimize for profit (revenue - costs)</option>
        </select>
        <p style={{ fontSize: '13px', color: '#666', marginTop: '8px' }}>
          {kpiMode === 'conversions' && 'Models will optimize for maximum conversion volume'}
          {kpiMode === 'aov' && 'Models will optimize for higher average order values'}
          {kpiMode === 'profit' && 'Models will optimize for maximum profitability'}
        </p>
      </div>

      {/* Model Behavior Section */}
      <div style={{ marginBottom: 24 }}>
        <h3>Model Behavior</h3>
        
        {/* Adstock Parameters */}
        <div style={{ marginBottom: 16 }}>
          <label style={{ display: 'block', marginBottom: 8, fontWeight: 'bold' }}>
            Adstock Decay (α): {alphaMean.toFixed(2)}
          </label>
          <input 
            type="range"
            min="0.1"
            max="0.9"
            step="0.05"
            value={alphaMean}
            onChange={(e) => setAlphaMean(parseFloat(e.target.value))}
            style={{ width: '100%' }}
          />
          <p style={{ fontSize: '12px', color: '#666', margin: '4px 0' }}>
            Controls how long the ad effect lasts after the campaign ends. Higher values = longer effect.
          </p>
          
          <label style={{ display: 'block', marginTop: 12, fontSize: '14px' }}>
            Decay Uncertainty (SD): {alphaSd.toFixed(2)}
          </label>
          <input 
            type="range"
            min="0.05"
            max="0.5"
            step="0.05"
            value={alphaSd}
            onChange={(e) => setAlphaSd(parseFloat(e.target.value))}
            style={{ width: '100%' }}
          />
        </div>

        {/* Saturation Parameters */}
        <div style={{ marginBottom: 16 }}>
          <label style={{ display: 'block', marginBottom: 8, fontWeight: 'bold' }}>
            Saturation Lambda (λ): {lamMean.toFixed(4)}
          </label>
          <input 
            type="range"
            min="0.0001"
            max="0.01"
            step="0.0005"
            value={lamMean}
            onChange={(e) => setLamMean(parseFloat(e.target.value))}
            style={{ width: '100%' }}
          />
          <p style={{ fontSize: '12px', color: '#666', margin: '4px 0' }}>
            Controls how returns diminish with higher spend. Higher values = more saturation.
          </p>
          
          <label style={{ display: 'block', marginTop: 12, fontSize: '14px' }}>
            Saturation Uncertainty (SD): {lamSd.toFixed(4)}
          </label>
          <input 
            type="range"
            min="0.0001"
            max="0.002"
            step="0.0001"
            value={lamSd}
            onChange={(e) => setLamSd(parseFloat(e.target.value))}
            style={{ width: '100%' }}
          />
        </div>
      </div>

      {/* Advanced MCMC Section */}
      <div style={{ marginBottom: 24 }}>
        <button
          onClick={() => setShowAdvanced(!showAdvanced)}
          style={{
            padding: '8px 16px',
            backgroundColor: showAdvanced ? '#0066cc' : '#666',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            marginBottom: showAdvanced ? 16 : 0
          }}
        >
          {showAdvanced ? '▼' : '▶'} Advanced MCMC Settings
        </button>

        {showAdvanced && (
          <div style={{ backgroundColor: '#f9f9f9', padding: 16, borderRadius: 4 }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
              <div>
                <label style={{ display: 'block', marginBottom: 8, fontSize: '14px' }}>
                  Draws: 
                  <input
                    type="number"
                    value={draws}
                    onChange={(e) => setDraws(parseInt(e.target.value))}
                    style={{ marginLeft: 8, padding: '4px 8px', width: '80px' }}
                  />
                </label>
              </div>
              <div>
                <label style={{ display: 'block', marginBottom: 8, fontSize: '14px' }}>
                  Tune: 
                  <input
                    type="number"
                    value={tune}
                    onChange={(e) => setTune(parseInt(e.target.value))}
                    style={{ marginLeft: 8, padding: '4px 8px', width: '80px' }}
                  />
                </label>
              </div>
              <div>
                <label style={{ display: 'block', marginBottom: 8, fontSize: '14px' }}>
                  Chains: 
                  <input
                    type="number"
                    value={chains}
                    onChange={(e) => setChains(parseInt(e.target.value))}
                    style={{ marginLeft: 8, padding: '4px 8px', width: '80px' }}
                  />
                </label>
              </div>
              <div>
                <label style={{ display: 'block', marginBottom: 8, fontSize: '14px' }}>
                  Target Accept: 
                  <input
                    type="number"
                    min="0.5"
                    max="1"
                    step="0.05"
                    value={targetAccept}
                    onChange={(e) => setTargetAccept(parseFloat(e.target.value))}
                    style={{ marginLeft: 8, padding: '4px 8px', width: '80px' }}
                  />
                </label>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Run Model Button */}
      <button
        onClick={handleRunModel}
        style={{
          padding: '12px 24px',
          fontSize: '16px',
          backgroundColor: '#28a745',
          color: 'white',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer',
          width: '100%'
        }}
      >
        Run Model
      </button>
    </div>
  )
}
