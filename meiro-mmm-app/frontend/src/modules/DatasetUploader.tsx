import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'

interface DatasetUploadResponse {
  dataset_id: string
  columns: string[]
  preview_rows: Record<string, any>[]
  path: string
}

interface ColumnMapping {
  kpi: string
  spend_channels: string[]
  covariates: string[]
}

interface DatasetUploaderProps {
  onMappingComplete: (mapping: { dataset_id: string, columns: ColumnMapping }) => void
}

async function uploadDataset({ file, datasetId, type }: { file: File, datasetId?: string, type: string }): Promise<DatasetUploadResponse> {
  const formData = new FormData()
  formData.append('file', file)
  if (datasetId) {
    formData.append('dataset_id', datasetId)
  }
  
  const res = await fetch(`/api/datasets/upload?type=${type}`, {
    method: 'POST',
    body: formData,
  })
  
  if (!res.ok) throw new Error('Upload failed')
  return res.json()
}

export default function DatasetUploader({ onMappingComplete }: DatasetUploaderProps) {
  const [file, setFile] = useState<File | null>(null)
  const [datasetType, setDatasetType] = useState<'sales' | 'attribution'>('sales')
  const [dataset, setDataset] = useState<DatasetUploadResponse | null>(null)
  const [kpi, setKpi] = useState<string>('')
  const [spendChannels, setSpendChannels] = useState<string[]>([])
  const [covariates, setCovariates] = useState<string[]>([])
  const [selectedSample, setSelectedSample] = useState<string | null>(null)
  
  const queryClient = useQueryClient()
  
  const uploadMutation = useMutation({
    mutationFn: uploadDataset,
    onSuccess: (data) => {
      setDataset(data)
    },
  })

  const handleLoadSample = async (sampleId: string) => {
    try {
      const res = await fetch(`/api/datasets/${sampleId}`)
      if (!res.ok) throw new Error('Failed to load sample')
      const data = await res.json()
      setDataset(data)
      setSelectedSample(sampleId)
    } catch (error) {
      console.error('Error loading sample:', error)
      alert('Failed to load sample dataset')
    }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      setFile(selectedFile)
    }
  }

  const handleUpload = () => {
    if (!file) return
    uploadMutation.mutate({ file, type: datasetType })
  }

  const toggleSpendChannel = (channel: string) => {
    setSpendChannels(prev => 
      prev.includes(channel) 
        ? prev.filter(c => c !== channel)
        : [...prev, channel]
    )
  }

  const toggleCovariate = (covariate: string) => {
    setCovariates(prev => 
      prev.includes(covariate) 
        ? prev.filter(c => c !== covariate)
        : [...prev, covariate]
    )
  }

  const handleContinue = () => {
    if (!dataset || !kpi || spendChannels.length === 0) {
      alert('Please select at least one KPI and one spend channel')
      return
    }
    
    onMappingComplete({
      dataset_id: dataset.dataset_id,
      columns: {
        kpi,
        spend_channels: spendChannels,
        covariates,
      }
    })
  }

  if (dataset) {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
        <div style={{ marginBottom: 32 }}>
          <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 12 }}>Dataset Mapping</h2>
          <p style={{ fontSize: '14px', color: '#6c757d' }}>
            Dataset ID: <code style={{ backgroundColor: '#f8f9fa', padding: '2px 8px', borderRadius: '4px' }}>{dataset.dataset_id}</code>
          </p>
        </div>
        
        {/* Data Preview Table */}
        <div style={{ 
          marginBottom: 32,
          backgroundColor: '#fff',
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          overflow: 'hidden',
          border: '1px solid #e9ecef'
        }}>
          <div style={{ padding: '16px 20px', backgroundColor: '#f8f9fa', borderBottom: '1px solid #e9ecef' }}>
            <h3 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#495057' }}>Data Preview (5 rows)</h3>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '14px' }}>
              <thead>
                <tr style={{ backgroundColor: '#f8f9fa' }}>
                  {dataset.columns.map(col => (
                    <th key={col} style={{ padding: '12px', border: '1px solid #e9ecef', textAlign: 'left', fontWeight: '600', color: '#495057' }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataset.preview_rows.map((row, idx) => (
                  <tr key={idx} style={{ backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                    {dataset.columns.map(col => (
                      <td key={col} style={{ padding: '12px', border: '1px solid #e9ecef', color: '#495057' }}>
                        {row[col]}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Column Mapping */}
        <div style={{ 
          marginBottom: 24,
          backgroundColor: '#fff',
          padding: 24,
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          border: '1px solid #e9ecef'
        }}>
          <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 16, color: '#495057' }}>Select KPI Column</h3>
          <select 
            value={kpi} 
            onChange={(e) => setKpi(e.target.value)}
            style={{ width: '100%', padding: '12px', fontSize: '15px', border: '2px solid #007bff', borderRadius: '6px', fontWeight: '500' }}
          >
            <option value="">-- Select KPI column --</option>
            {dataset.columns.map(col => (
              <option key={col} value={col}>{col}</option>
            ))}
          </select>
        </div>

        <div style={{ 
          marginBottom: 24,
          backgroundColor: '#fff',
          padding: 24,
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          border: '1px solid #e9ecef'
        }}>
          <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 16, color: '#495057' }}>Select Spend Channels</h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 12 }}>Select one or more columns that represent marketing spend</p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
            {dataset.columns
              .filter(col => col !== kpi)
              .map(col => (
                <label 
                  key={col} 
                  style={{ 
                    display: 'flex', 
                    alignItems: 'center', 
                    cursor: 'pointer',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    backgroundColor: spendChannels.includes(col) ? '#007bff' : '#f8f9fa',
                    color: spendChannels.includes(col) ? '#fff' : '#495057',
                    border: spendChannels.includes(col) ? '2px solid #007bff' : '2px solid #e9ecef',
                    fontWeight: spendChannels.includes(col) ? '600' : '500',
                    transition: 'all 0.2s'
                  }}
                >
                  <input 
                    type="checkbox"
                    checked={spendChannels.includes(col)}
                    onChange={() => toggleSpendChannel(col)}
                    style={{ marginRight: 8, cursor: 'pointer' }}
                  />
                  {col}
                </label>
              ))}
          </div>
        </div>

        <div style={{ 
          marginBottom: 24,
          backgroundColor: '#fff',
          padding: 24,
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          border: '1px solid #e9ecef'
        }}>
          <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 16, color: '#495057' }}>Select Covariates (Optional)</h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 12 }}>Select additional factors that may influence the KPI</p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
            {dataset.columns
              .filter(col => col !== kpi && !spendChannels.includes(col))
              .map(col => (
                <label 
                  key={col} 
                  style={{ 
                    display: 'flex', 
                    alignItems: 'center', 
                    cursor: 'pointer',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    backgroundColor: covariates.includes(col) ? '#28a745' : '#f8f9fa',
                    color: covariates.includes(col) ? '#fff' : '#495057',
                    border: covariates.includes(col) ? '2px solid #28a745' : '2px solid #e9ecef',
                    fontWeight: covariates.includes(col) ? '600' : '500',
                    transition: 'all 0.2s'
                  }}
                >
                  <input 
                    type="checkbox"
                    checked={covariates.includes(col)}
                    onChange={() => toggleCovariate(col)}
                    style={{ marginRight: 8, cursor: 'pointer' }}
                  />
                  {col}
                </label>
              ))}
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12, marginTop: 32 }}>
          <button 
            onClick={handleContinue}
            disabled={!kpi || spendChannels.length === 0}
            style={{ 
              padding: '14px 32px', 
              fontSize: '16px',
              fontWeight: '600',
              backgroundColor: (!kpi || spendChannels.length === 0) ? '#ccc' : '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: (!kpi || spendChannels.length === 0) ? 'not-allowed' : 'pointer',
              boxShadow: (!kpi || spendChannels.length === 0) ? 'none' : '0 2px 4px rgba(0,123,255,0.3)',
              transition: 'all 0.2s'
            }}
            onMouseOver={(e) => {
              if (!(!kpi || spendChannels.length === 0)) {
                e.currentTarget.style.backgroundColor = '#0056b3'
              }
            }}
            onMouseOut={(e) => {
              if (!(!kpi || spendChannels.length === 0)) {
                e.currentTarget.style.backgroundColor = '#007bff'
              }
            }}
          >
            Continue to Modeling
          </button>
        </div>
      </div>
    )
  }

  return (
    <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
      <div style={{ marginBottom: 32 }}>
        <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>Upload Dataset</h2>
        <p style={{ fontSize: '15px', color: '#6c757d' }}>Upload your CSV file or select a sample dataset to get started</p>
      </div>
      
      {/* Sample Dataset Selection */}
      {!dataset && (
        <div style={{ 
          marginBottom: 32,
          padding: 24,
          backgroundColor: '#f8f9fa',
          borderRadius: '8px',
          border: '1px solid #e9ecef'
        }}>
          <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: 16, color: '#495057' }}>Quick Start: Sample Datasets</h3>
          <p style={{ fontSize: '14px', color: '#6c757d', marginBottom: 16 }}>Try the app with pre-loaded sample data</p>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <button
              onClick={() => handleLoadSample('sample-weekly-01')}
              style={{
                padding: '12px 20px',
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
              onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#007bff'}
            >
              Sales (3 channels)
            </button>
            <button
              onClick={() => handleLoadSample('sample-weekly-realistic')}
              style={{
                padding: '12px 20px',
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
              onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#007bff'}
            >
              Sales (6 channels + KPIs)
            </button>
            <button
              onClick={() => handleLoadSample('sample-attribution-weekly')}
              style={{
                padding: '12px 20px',
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
              onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#28a745'}
            >
              Conversions (Attribution)
            </button>
          </div>
        </div>
      )}
      
      {/* Dataset Type Selector */}
      <div style={{ marginBottom: 24, backgroundColor: '#f5f5f5', padding: 16, borderRadius: 8 }}>
        <h3 style={{ marginTop: 0, fontSize: '16px' }}>Model Target:</h3>
        <div style={{ display: 'flex', gap: 24, marginBottom: 12 }}>
          <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
            <input 
              type="radio" 
              name="datasetType"
              value="sales"
              checked={datasetType === 'sales'}
              onChange={(e) => setDatasetType('sales')}
              style={{ marginRight: 8 }}
            />
            <strong>Total Sales</strong>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
            <input 
              type="radio" 
              name="datasetType"
              value="attribution"
              checked={datasetType === 'attribution'}
              onChange={(e) => setDatasetType('attribution')}
              style={{ marginRight: 8 }}
            />
            <strong>Marketing-Driven Conversions (Attribution)</strong>
          </label>
        </div>
        {datasetType === 'attribution' && (
          <p style={{ fontSize: '13px', color: '#666', margin: 0, fontStyle: 'italic' }}>
            This mode uses conversion totals by channel and week from Meiro attribution exports.
          </p>
        )}
      </div>

      <div style={{ marginBottom: 24 }}>
        <input 
          type="file" 
          accept=".csv" 
          onChange={handleFileChange}
          style={{ marginBottom: 16 }}
        />
        <br />
        <button 
          onClick={handleUpload}
          disabled={!file || uploadMutation.isPending}
          style={{ 
            padding: '12px 24px', 
            fontSize: '16px',
            backgroundColor: (!file || uploadMutation.isPending) ? '#ccc' : '#28a745',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: (!file || uploadMutation.isPending) ? 'not-allowed' : 'pointer'
          }}
        >
          {uploadMutation.isPending ? 'Uploading...' : 'Upload CSV'}
        </button>
      </div>
      
      {uploadMutation.isError && (
        <p style={{ color: 'red' }}>Upload failed. Please try again.</p>
      )}
    </div>
  )
}
