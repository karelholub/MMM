import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import DatasetUploader from './DatasetUploader'
import ModelConfigurator from './ModelConfigurator'
import BudgetOptimizer from './BudgetOptimizer'
import MMMDashboard from './MMMDashboard'
import ChannelEfficiencyMatrix from './ChannelEfficiencyMatrix'
import DataSources from './DataSources'
import CampaignDrilldown from './CampaignDrilldown'

// Add CSS for spinner animation
const styles = `
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`

interface ColumnMapping {
  kpi: string
  spend_channels: string[]
  covariates: string[]
}

interface ModelConfig {
  dataset_id: string
  frequency: string
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

async function getRun(runId: string) {
  const res = await fetch(`/api/models/${runId}`);
  return res.json();
}

async function createRun(payload: ModelConfig) {
  const res = await fetch(`/api/models`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export default function App() {
  const [step, setStep] = useState<'upload' | 'configure' | 'results' | 'compare' | 'datasources'>('results')
  const [datasetConfig, setDatasetConfig] = useState<{ dataset_id: string, columns: ColumnMapping } | null>(null)
  const [runId, setRunId] = useState<string>('');
  
  const mutation = useMutation({
    mutationFn: createRun,
    onSuccess: (data) => {
      setRunId(data.run_id)
      setStep('results')
    },
  });

  const { data: run } = useQuery({
    queryKey: ['run', runId],
    queryFn: () => getRun(runId),
    enabled: !!runId && step === 'results',
    refetchInterval: (data) => (data?.status === 'finished' ? false : 1000),
  });

  const handleMappingComplete = (mapping: { dataset_id: string, columns: ColumnMapping }) => {
    setDatasetConfig(mapping)
    // Immediately run model via API with sensible defaults (no manual configure step)
    const cfg: ModelConfig = {
      dataset_id: mapping.dataset_id,
      frequency: 'W',
      kpi: mapping.columns.kpi,
      spend_channels: mapping.columns.spend_channels,
      covariates: mapping.columns.covariates,
      priors: {
        adstock: { alpha_mean: 0.5, alpha_sd: 0.2 },
        saturation: { lam_mean: 0.001, lam_sd: 0.0005 }
      },
      mcmc: { draws: 1000, tune: 1000, chains: 4, target_accept: 0.9 }
    }
    mutation.mutate(cfg)
    setStep('results')
  }

  const handleRunModel = (config: ModelConfig) => {
    mutation.mutate(config)
  }

  if (step === 'upload') {
    return <DatasetUploader onMappingComplete={handleMappingComplete} />
  }

  // Configure step removed from flow; model runs via API immediately after mapping

  if (step === 'datasources') {
    return <DataSources />
  }

  if (step === 'compare') {
    return <ChannelEfficiencyMatrix />
  }

  return (
    <div style={{maxWidth: 1400, margin: '0 auto', padding: 32}}>
      <style>{styles}</style>
      <header style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom: 32, paddingBottom: 20, borderBottom: '2px solid #e9ecef'}}>
        <h1 style={{ fontSize: '32px', fontWeight: '700', color: '#212529', margin: 0 }}>Meiro MMM</h1>
        <div style={{ display: 'flex', gap: 12 }}>
          <button onClick={() => setStep('results')} style={{ 
            padding: '10px 20px', 
            fontSize: '14px', 
            fontWeight: '600',
            backgroundColor: '#343a40', 
            color: 'white', 
            border: 'none', 
            borderRadius: '6px', 
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(52,58,64,0.3)',
            transition: 'all 0.2s'
          }}
          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#23272b'}
          onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#343a40'}>
            Dashboard
          </button>
          <button onClick={() => setStep('datasources')} style={{ 
            padding: '10px 20px', 
            fontSize: '14px', 
            fontWeight: '600',
            backgroundColor: '#17a2b8', 
            color: 'white', 
            border: 'none', 
            borderRadius: '6px', 
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(23,162,184,0.3)',
            transition: 'all 0.2s'
          }}
          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#117a8b'}
          onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#17a2b8'}>
            Data Sources
          </button>
          <button onClick={() => setStep('upload')} style={{ 
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
            Upload Data
          </button>
          <button onClick={() => setStep('compare')} style={{ 
            padding: '10px 20px', 
            fontSize: '14px', 
            fontWeight: '600',
            backgroundColor: '#28a745', 
            color: 'white', 
            border: 'none', 
            borderRadius: '6px', 
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(40,167,69,0.3)',
            transition: 'all 0.2s'
          }}
          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#218838'}
          onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#28a745'}>
            Compare Models
          </button>
        </div>
      </header>
      
      {runId && (
        <div style={{ marginBottom: 16, padding: '12px 16px', backgroundColor: '#f8f9fa', borderRadius: '6px', borderLeft: '4px solid #007bff' }}>
          <p style={{ margin: 0, fontSize: '14px', color: '#666' }}>
            <strong>Run ID:</strong> <code style={{ backgroundColor: '#e9ecef', padding: '2px 6px', borderRadius: '3px', fontWeight: '600' }}>{runId}</code>
          </p>
        </div>
      )}
      
      {!runId && (
        <div style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 24, marginBottom: 24 }}>
          <h2 style={{ marginTop: 0 }}>Welcome to Meiro MMM</h2>
          <p style={{ color: '#6c757d' }}>Start by uploading a dataset or connecting data sources. When a model run is created via the API, results will appear here.</p>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={() => setStep('upload')} style={{ padding: '10px 16px', background: '#007bff', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>Upload Data</button>
            <button onClick={() => setStep('datasources')} style={{ padding: '10px 16px', background: '#17a2b8', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>Data Sources</button>
            <button onClick={() => setStep('compare')} style={{ padding: '10px 16px', background: '#28a745', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>Compare Models</button>
          </div>
        </div>
      )}

      {!run && mutation.isPending && (
        <div style={{ textAlign: 'center', padding: 60, backgroundColor: '#f8f9fa', borderRadius: '8px', marginBottom: 32 }}>
          <div style={{ 
            width: '48px', 
            height: '48px', 
            border: '4px solid #e3f2fd', 
            borderTopColor: '#007bff', 
            borderRadius: '50%', 
            animation: 'spin 1s linear infinite',
            margin: '0 auto 20px'
          }}></div>
          <p style={{ fontSize: '18px', fontWeight: '600', color: '#333', margin: '0 0 8px' }}>Running model...</p>
          <p style={{ fontSize: '14px', color: '#666', margin: 0 }}>This may take a few moments.</p>
        </div>
      )}
      
      {run && (
        <div>
          {run.status === 'finished' && "r2" in run ? (
            <>
              <h2 style={{ fontSize: '24px', fontWeight: '700', color: '#212529', marginBottom: 20 }}>Model Fit Results</h2>
              <div style={{ backgroundColor: '#ffffff', padding: 24, borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)', marginBottom: 32, border: '1px solid #e9ecef' }}>
                {"r2" in run && (
                  <div style={{ marginBottom: 24, padding: '20px', backgroundColor: '#e7f3ff', borderRadius: '8px', border: '2px solid #007bff' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                      <span style={{ fontSize: '14px', color: '#666', textTransform: 'uppercase', letterSpacing: '0.5px', fontWeight: '600' }}>Model Quality</span>
                      <span style={{ fontSize: '12px', color: '#6c757d', backgroundColor: '#fff', padding: '4px 8px', borderRadius: '4px', fontWeight: '600' }}>STATUS: FINISHED</span>
                    </div>
                    <div style={{ fontSize: '48px', fontWeight: '700', color: '#007bff', lineHeight: '1' }}>
                      {Number(run.r2).toFixed(3)}
                    </div>
                    <p style={{ fontSize: '14px', color: '#666', marginTop: 8 }}>
                      R-squared goodness of fit
                    </p>
                  </div>
                )}
                
                {"contrib" in run && run.contrib && (
                  <div style={{ marginBottom: 24 }}>
                    <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 12, color: '#495057' }}>Channel Contribution</h3>
                    <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                      {run.contrib.map((c: any) => (
                        <li key={c.channel} style={{ 
                          padding: '12px 16px', 
                          backgroundColor: '#f8f9fa', 
                          marginBottom: '8px', 
                          borderRadius: '6px', 
                          display: 'flex', 
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          border: '1px solid #e9ecef'
                        }}>
                          <span style={{ fontWeight: '500', color: '#495057' }}>{c.channel}</span>
                          <span style={{ fontWeight: '700', fontSize: '16px', color: '#007bff' }}>{(c.mean_share*100).toFixed(1)}%</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                
                {"roi" in run && run.roi && (
                  <div>
                    <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 12, color: '#495057' }}>Return on Investment (ROI)</h3>
                    <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                      {run.roi.map((r: any) => (
                        <li key={r.channel} style={{ 
                          padding: '12px 16px', 
                          backgroundColor: '#f8f9fa', 
                          marginBottom: '8px', 
                          borderRadius: '6px', 
                          display: 'flex', 
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          border: '1px solid #e9ecef'
                        }}>
                          <span style={{ fontWeight: '500', color: '#495057' }}>{r.channel}</span>
                          <span style={{ fontWeight: '700', fontSize: '16px', color: '#28a745' }}>{Number(r.roi).toFixed(3)}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </>
          ) : run.status === 'error' ? (
            <div style={{ backgroundColor: '#fff3cd', padding: 20, borderRadius: 8, marginBottom: 24, border: '2px solid #ffc107' }}>
              <h2 style={{ color: '#856404', fontSize: '20px', fontWeight: '600', marginTop: 0 }}>Model Error</h2>
              <p style={{ color: '#856404', fontSize: '14px' }}>{run.detail || 'An error occurred during model fitting'}</p>
            </div>
          ) : (
            <div style={{ backgroundColor: '#d1ecf1', padding: 20, borderRadius: 8, marginBottom: 24, border: '2px solid #0c5460' }}>
              <h2 style={{ color: '#0c5460', fontSize: '20px', fontWeight: '600', marginTop: 0 }}>Model Status: {run.status}</h2>
              <p style={{ color: '#0c5460', fontSize: '14px' }}>Model is currently {run.status === 'queued' ? 'waiting to start' : 'running'}. Please wait...</p>
            </div>
          )}
        </div>
      )}

      {/* MMM Dashboard - show when model is finished */}
      {run && "r2" in run && datasetConfig && (
        <MMMDashboard 
          runId={runId} 
          datasetId={datasetConfig.dataset_id}
        />
      )}

      {/* Budget Optimizer - only show when model is finished */}
      {run && "r2" in run && run.contrib && run.roi && (
        <BudgetOptimizer
          roiData={run.roi}
          contribData={run.contrib}
          baselineKPI={100} // TODO: Get from actual dataset
          runId={runId}
        />
      )}

      {/* Campaign Drilldown - show when model is finished */}
      {run && "r2" in run && (
        <div style={{ marginTop: 24 }}>
          <CampaignDrilldown runId={runId} />
        </div>
      )}
    </div>
  )
}