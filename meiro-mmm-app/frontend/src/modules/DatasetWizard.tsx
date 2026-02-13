import { useState, useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { apiGetJson, apiRequest } from '../lib/apiClient'

// ── Types ──────────────────────────────────────────────────────────

interface ColumnInfo {
  name: string
  dtype: string
  missing: number
  unique: number
  sample_values: any[]
}

interface Validation {
  dataset_id: string
  n_rows: number
  n_columns: number
  columns: ColumnInfo[]
  date_column: string | null
  date_range: { min: string; max: string; n_periods: number } | null
  format: 'wide' | 'tall'
  suggestions: {
    spend_channels: string[]
    kpi_columns: string[]
    covariates: string[]
  }
  warnings: string[]
}

interface DatasetUploadResponse {
  dataset_id: string
  columns: string[]
  preview_rows: Record<string, any>[]
  path: string
  type: string
}

interface ColumnMapping {
  kpi: string
  spend_channels: string[]
  covariates: string[]
}

interface DatasetWizardProps {
  onComplete: (mapping: { dataset_id: string; columns: ColumnMapping }) => void
}

type WizardStep = 'source' | 'preview' | 'mapping' | 'quality' | 'confirm'

// ── API helpers ────────────────────────────────────────────────────

async function uploadDataset({ file, type }: { file: File; type: string }): Promise<DatasetUploadResponse> {
  const formData = new FormData()
  formData.append('file', file)
  const res = await apiRequest(`/api/datasets/upload?type=${type}`, {
    method: 'POST',
    body: formData,
    fallbackMessage: 'Upload failed',
  })
  return res.json()
}

async function fetchValidation(datasetId: string): Promise<Validation> {
  return apiGetJson<Validation>(`/api/datasets/${datasetId}/validate`, { fallbackMessage: 'Validation failed' })
}

async function loadSampleDataset(sampleId: string): Promise<DatasetUploadResponse> {
  return apiGetJson<DatasetUploadResponse>(`/api/datasets/${sampleId}`, { fallbackMessage: 'Failed to load sample' })
}

// ── Styles ─────────────────────────────────────────────────────────

const card = {
  backgroundColor: '#fff',
  borderRadius: '8px',
  boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
  border: '1px solid #e9ecef',
  padding: 24,
  marginBottom: 24,
} as const

const primaryBtn = {
  padding: '12px 28px',
  fontSize: '15px',
  fontWeight: '600' as const,
  backgroundColor: '#007bff',
  color: 'white',
  border: 'none',
  borderRadius: '6px',
  cursor: 'pointer',
}

const secondaryBtn = {
  ...primaryBtn,
  backgroundColor: '#6c757d',
}

// ── Component ──────────────────────────────────────────────────────

export default function DatasetWizard({ onComplete }: DatasetWizardProps) {
  const [step, setStep] = useState<WizardStep>('source')
  const [file, setFile] = useState<File | null>(null)
  const [datasetType, setDatasetType] = useState<'sales' | 'attribution'>('sales')
  const [dataset, setDataset] = useState<DatasetUploadResponse | null>(null)
  const [validation, setValidation] = useState<Validation | null>(null)
  const [kpi, setKpi] = useState('')
  const [spendChannels, setSpendChannels] = useState<string[]>([])
  const [covariates, setCovariates] = useState<string[]>([])

  const uploadMutation = useMutation({
    mutationFn: uploadDataset,
    onSuccess: (data) => {
      setDataset(data)
      setStep('preview')
    },
  })

  // Auto-validate when dataset is loaded
  useEffect(() => {
    if (dataset?.dataset_id) {
      fetchValidation(dataset.dataset_id)
        .then((v) => {
          setValidation(v)
          // Apply smart suggestions
          if (v.suggestions.kpi_columns.length > 0 && !kpi) {
            setKpi(v.suggestions.kpi_columns[0])
          }
          if (v.suggestions.spend_channels.length > 0 && spendChannels.length === 0) {
            setSpendChannels(v.suggestions.spend_channels)
          }
          if (v.suggestions.covariates.length > 0 && covariates.length === 0) {
            setCovariates(v.suggestions.covariates)
          }
        })
        .catch(() => {})
    }
  }, [dataset?.dataset_id])

  const handleLoadSample = async (sampleId: string) => {
    try {
      const data = await loadSampleDataset(sampleId)
      setDataset(data)
      setStep('preview')
    } catch {
      alert('Failed to load sample dataset')
    }
  }

  const handleUpload = () => {
    if (!file) return
    uploadMutation.mutate({ file, type: datasetType })
  }

  const toggleSpend = (col: string) => {
    setSpendChannels((prev) => (prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]))
  }

  const toggleCovariate = (col: string) => {
    setCovariates((prev) => (prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]))
  }

  const numericColumns = validation
    ? validation.columns
        .filter((c) => ['float64', 'int64', 'float32', 'int32'].includes(c.dtype))
        .map((c) => c.name)
    : dataset
    ? dataset.columns.filter((c) => c !== 'date')
    : []

  // ── Step indicator ─────────────────────────────────────────────

  const steps: { key: WizardStep; label: string }[] = [
    { key: 'source', label: 'Data Source' },
    { key: 'preview', label: 'Preview' },
    { key: 'mapping', label: 'Column Mapping' },
    { key: 'quality', label: 'Quality Check' },
    { key: 'confirm', label: 'Confirm' },
  ]

  const stepIndex = steps.findIndex((s) => s.key === step)

  const StepIndicator = () => (
    <div style={{ display: 'flex', gap: 4, marginBottom: 32 }}>
      {steps.map((s, i) => (
        <div key={s.key} style={{ flex: 1, textAlign: 'center' }}>
          <div
            style={{
              height: 4,
              borderRadius: 2,
              backgroundColor: i <= stepIndex ? '#007bff' : '#e9ecef',
              marginBottom: 8,
              transition: 'background-color 0.3s',
            }}
          />
          <span
            style={{
              fontSize: '12px',
              fontWeight: i === stepIndex ? '700' : '400',
              color: i <= stepIndex ? '#007bff' : '#adb5bd',
              textTransform: 'uppercase',
              letterSpacing: '0.5px',
            }}
          >
            {s.label}
          </span>
        </div>
      ))}
    </div>
  )

  // ── Step 1: Data Source ────────────────────────────────────────

  if (step === 'source') {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
        <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>
          Dataset Wizard
        </h2>
        <p style={{ fontSize: '15px', color: '#6c757d', marginBottom: 24 }}>
          Choose how to load your marketing data for modeling.
        </p>
        <StepIndicator />

        {/* Sample datasets */}
        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Quick Start: Sample Datasets
          </h3>
          <p style={{ fontSize: '14px', color: '#6c757d', marginBottom: 16 }}>
            Try the app with pre-loaded sample data
          </p>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            {[
              { id: 'sample-weekly-01', label: 'Sales (3 channels)', color: '#007bff' },
              { id: 'sample-weekly-realistic', label: 'Sales (6 channels + KPIs)', color: '#007bff' },
              { id: 'sample-weekly-campaigns', label: 'Campaign-level (Tall)', color: '#17a2b8' },
              { id: 'sample-attribution-weekly', label: 'Conversions (Attribution)', color: '#28a745' },
            ].map((s) => (
              <button
                key={s.id}
                onClick={() => handleLoadSample(s.id)}
                style={{
                  padding: '12px 20px',
                  fontSize: '14px',
                  fontWeight: '600',
                  backgroundColor: s.color,
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  cursor: 'pointer',
                }}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        {/* Upload */}
        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Upload CSV
          </h3>

          <div style={{ display: 'flex', gap: 24, marginBottom: 16 }}>
            {(['sales', 'attribution'] as const).map((t) => (
              <label key={t} style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                <input
                  type="radio"
                  name="datasetType"
                  value={t}
                  checked={datasetType === t}
                  onChange={() => setDatasetType(t)}
                  style={{ marginRight: 8 }}
                />
                <strong>{t === 'sales' ? 'Total Sales' : 'Marketing-Driven Conversions'}</strong>
              </label>
            ))}
          </div>

          <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files?.[0] || null)} />
          <div style={{ marginTop: 12 }}>
            <button
              onClick={handleUpload}
              disabled={!file || uploadMutation.isPending}
              style={{
                ...primaryBtn,
                backgroundColor: !file || uploadMutation.isPending ? '#ccc' : '#28a745',
                cursor: !file || uploadMutation.isPending ? 'not-allowed' : 'pointer',
              }}
            >
              {uploadMutation.isPending ? 'Uploading...' : 'Upload CSV'}
            </button>
          </div>
          {uploadMutation.isError && (
            <p style={{ color: '#dc3545', marginTop: 8 }}>Upload failed. Please try again.</p>
          )}
        </div>

        {/* Meiro CDP import note */}
        <div style={{ ...card, backgroundColor: '#f0f7ff', borderColor: '#b8daff' }}>
          <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600', color: '#004085' }}>
            Import from Meiro CDP
          </h3>
          <p style={{ fontSize: '14px', color: '#004085', marginBottom: 0 }}>
            Connect your Meiro CDP instance via the <strong>Data Sources</strong> page to fetch
            customer attribution and event data directly. Exported CDP data will appear as a
            selectable dataset.
          </p>
        </div>
      </div>
    )
  }

  // ── Step 2: Preview ───────────────────────────────────────────

  if (step === 'preview') {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
        <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>
          Dataset Wizard
        </h2>
        <StepIndicator />

        <div style={card}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
            <div>
              <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
                Data Preview
              </h3>
              <p style={{ fontSize: '13px', color: '#6c757d', margin: '4px 0 0' }}>
                Dataset: <code style={{ backgroundColor: '#f8f9fa', padding: '2px 6px', borderRadius: 3 }}>{dataset?.dataset_id}</code>
                {validation && (
                  <>
                    {' '}&middot; {validation.n_rows} rows &middot; {validation.n_columns} columns
                    &middot; Format: <strong>{validation.format}</strong>
                  </>
                )}
              </p>
            </div>
          </div>

          {validation?.date_range && (
            <div style={{ marginBottom: 16, padding: '12px 16px', backgroundColor: '#e7f3ff', borderRadius: 6, fontSize: '14px' }}>
              Date range: <strong>{validation.date_range.min}</strong> to <strong>{validation.date_range.max}</strong>
              {' '}&middot; {validation.date_range.n_periods} periods
            </div>
          )}

          {/* Data table */}
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '13px' }}>
              <thead>
                <tr style={{ backgroundColor: '#f8f9fa' }}>
                  {dataset?.columns.map((col) => (
                    <th key={col} style={{ padding: '10px 12px', border: '1px solid #e9ecef', textAlign: 'left', fontWeight: '600', color: '#495057' }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataset?.preview_rows.map((row, idx) => (
                  <tr key={idx} style={{ backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                    {dataset.columns.map((col) => (
                      <td key={col} style={{ padding: '8px 12px', border: '1px solid #e9ecef', color: '#495057' }}>
                        {row[col] != null ? String(row[col]) : ''}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <button onClick={() => { setDataset(null); setValidation(null); setStep('source') }} style={secondaryBtn}>
            Back
          </button>
          <button onClick={() => setStep('mapping')} style={primaryBtn}>
            Next: Map Columns
          </button>
        </div>
      </div>
    )
  }

  // ── Step 3: Column Mapping ────────────────────────────────────

  if (step === 'mapping') {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
        <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>
          Dataset Wizard
        </h2>
        <StepIndicator />

        {validation?.suggestions && (
          <div style={{ marginBottom: 24, padding: '12px 16px', backgroundColor: '#d4edda', borderRadius: 6, border: '1px solid #c3e6cb', fontSize: '14px', color: '#155724' }}>
            Smart suggestions applied based on column names. Adjust as needed.
          </div>
        )}

        {/* KPI */}
        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Select KPI Column
          </h3>
          <select
            value={kpi}
            onChange={(e) => setKpi(e.target.value)}
            style={{ width: '100%', padding: '12px', fontSize: '15px', border: '2px solid #007bff', borderRadius: '6px', fontWeight: '500' }}
          >
            <option value="">-- Select KPI column --</option>
            {numericColumns.map((col) => (
              <option key={col} value={col}>{col}</option>
            ))}
          </select>
        </div>

        {/* Spend channels */}
        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Select Spend Channels
          </h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 12 }}>
            Select columns that represent marketing spend
          </p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
            {numericColumns
              .filter((col) => col !== kpi)
              .map((col) => {
                const selected = spendChannels.includes(col)
                return (
                  <label
                    key={col}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      cursor: 'pointer',
                      padding: '8px 12px',
                      borderRadius: '6px',
                      backgroundColor: selected ? '#007bff' : '#f8f9fa',
                      color: selected ? '#fff' : '#495057',
                      border: selected ? '2px solid #007bff' : '2px solid #e9ecef',
                      fontWeight: selected ? '600' : '500',
                      transition: 'all 0.2s',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={selected}
                      onChange={() => toggleSpend(col)}
                      style={{ marginRight: 8, cursor: 'pointer' }}
                    />
                    {col}
                  </label>
                )
              })}
          </div>
        </div>

        {/* Covariates */}
        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Select Covariates (Optional)
          </h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 12 }}>
            Additional factors that may influence the KPI
          </p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
            {numericColumns
              .filter((col) => col !== kpi && !spendChannels.includes(col))
              .map((col) => {
                const selected = covariates.includes(col)
                return (
                  <label
                    key={col}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      cursor: 'pointer',
                      padding: '8px 12px',
                      borderRadius: '6px',
                      backgroundColor: selected ? '#28a745' : '#f8f9fa',
                      color: selected ? '#fff' : '#495057',
                      border: selected ? '2px solid #28a745' : '2px solid #e9ecef',
                      fontWeight: selected ? '600' : '500',
                      transition: 'all 0.2s',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={selected}
                      onChange={() => toggleCovariate(col)}
                      style={{ marginRight: 8, cursor: 'pointer' }}
                    />
                    {col}
                  </label>
                )
              })}
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <button onClick={() => setStep('preview')} style={secondaryBtn}>
            Back
          </button>
          <button
            onClick={() => setStep('quality')}
            disabled={!kpi || spendChannels.length === 0}
            style={{
              ...primaryBtn,
              backgroundColor: !kpi || spendChannels.length === 0 ? '#ccc' : '#007bff',
              cursor: !kpi || spendChannels.length === 0 ? 'not-allowed' : 'pointer',
            }}
          >
            Next: Quality Check
          </button>
        </div>
      </div>
    )
  }

  // ── Step 4: Quality Check ─────────────────────────────────────

  if (step === 'quality') {
    const warnings = validation?.warnings || []

    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
        <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>
          Dataset Wizard
        </h2>
        <StepIndicator />

        <div style={card}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
            Data Quality Report
          </h3>

          {/* Summary cards */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 24 }}>
            <div style={{ padding: 16, backgroundColor: '#e7f3ff', borderRadius: 6 }}>
              <div style={{ fontSize: '12px', color: '#666', textTransform: 'uppercase' }}>Rows</div>
              <div style={{ fontSize: '24px', fontWeight: '700', color: '#007bff' }}>{validation?.n_rows || 0}</div>
            </div>
            <div style={{ padding: 16, backgroundColor: '#e7f3ff', borderRadius: 6 }}>
              <div style={{ fontSize: '12px', color: '#666', textTransform: 'uppercase' }}>Columns</div>
              <div style={{ fontSize: '24px', fontWeight: '700', color: '#007bff' }}>{validation?.n_columns || 0}</div>
            </div>
            <div style={{ padding: 16, backgroundColor: '#e7f3ff', borderRadius: 6 }}>
              <div style={{ fontSize: '12px', color: '#666', textTransform: 'uppercase' }}>Format</div>
              <div style={{ fontSize: '24px', fontWeight: '700', color: '#007bff' }}>{validation?.format || '—'}</div>
            </div>
            <div style={{ padding: 16, backgroundColor: warnings.length > 0 ? '#fff3cd' : '#d4edda', borderRadius: 6 }}>
              <div style={{ fontSize: '12px', color: '#666', textTransform: 'uppercase' }}>Warnings</div>
              <div style={{ fontSize: '24px', fontWeight: '700', color: warnings.length > 0 ? '#856404' : '#155724' }}>
                {warnings.length}
              </div>
            </div>
          </div>

          {/* Warnings */}
          {warnings.length > 0 && (
            <div style={{ marginBottom: 24 }}>
              <h4 style={{ fontSize: '16px', fontWeight: '600', color: '#856404', marginBottom: 12 }}>
                Warnings
              </h4>
              {warnings.map((w, i) => (
                <div
                  key={i}
                  style={{
                    padding: '10px 14px',
                    backgroundColor: '#fff3cd',
                    border: '1px solid #ffc107',
                    borderRadius: 4,
                    marginBottom: 8,
                    fontSize: '14px',
                    color: '#856404',
                  }}
                >
                  {w}
                </div>
              ))}
            </div>
          )}

          {/* Column details table */}
          {validation?.columns && (
            <div style={{ overflowX: 'auto' }}>
              <h4 style={{ fontSize: '16px', fontWeight: '600', marginBottom: 12 }}>Column Details</h4>
              <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '13px' }}>
                <thead>
                  <tr style={{ backgroundColor: '#f8f9fa' }}>
                    <th style={{ padding: '10px', border: '1px solid #e9ecef', textAlign: 'left' }}>Column</th>
                    <th style={{ padding: '10px', border: '1px solid #e9ecef', textAlign: 'left' }}>Type</th>
                    <th style={{ padding: '10px', border: '1px solid #e9ecef', textAlign: 'right' }}>Missing</th>
                    <th style={{ padding: '10px', border: '1px solid #e9ecef', textAlign: 'right' }}>Unique</th>
                    <th style={{ padding: '10px', border: '1px solid #e9ecef', textAlign: 'left' }}>Role</th>
                  </tr>
                </thead>
                <tbody>
                  {validation.columns.map((ci) => {
                    let role = ''
                    if (ci.name === kpi) role = 'KPI'
                    else if (spendChannels.includes(ci.name)) role = 'Spend'
                    else if (covariates.includes(ci.name)) role = 'Covariate'
                    else if (ci.name === validation.date_column) role = 'Date'

                    return (
                      <tr key={ci.name}>
                        <td style={{ padding: '8px 10px', border: '1px solid #e9ecef', fontWeight: '500' }}>
                          {ci.name}
                        </td>
                        <td style={{ padding: '8px 10px', border: '1px solid #e9ecef', color: '#6c757d' }}>
                          {ci.dtype}
                        </td>
                        <td
                          style={{
                            padding: '8px 10px',
                            border: '1px solid #e9ecef',
                            textAlign: 'right',
                            color: ci.missing > 0 ? '#dc3545' : '#28a745',
                            fontWeight: ci.missing > 0 ? '600' : '400',
                          }}
                        >
                          {ci.missing}
                        </td>
                        <td style={{ padding: '8px 10px', border: '1px solid #e9ecef', textAlign: 'right' }}>
                          {ci.unique}
                        </td>
                        <td style={{ padding: '8px 10px', border: '1px solid #e9ecef' }}>
                          {role && (
                            <span
                              style={{
                                display: 'inline-block',
                                padding: '2px 8px',
                                borderRadius: 4,
                                fontSize: '12px',
                                fontWeight: '600',
                                backgroundColor:
                                  role === 'KPI' ? '#007bff' : role === 'Spend' ? '#28a745' : role === 'Covariate' ? '#17a2b8' : '#6c757d',
                                color: '#fff',
                              }}
                            >
                              {role}
                            </span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <button onClick={() => setStep('mapping')} style={secondaryBtn}>
            Back
          </button>
          <button onClick={() => setStep('confirm')} style={primaryBtn}>
            Next: Confirm
          </button>
        </div>
      </div>
    )
  }

  // ── Step 5: Confirm & Launch ──────────────────────────────────

  return (
    <div style={{ maxWidth: 920, margin: '0 auto', padding: 24 }}>
      <h2 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>
        Dataset Wizard
      </h2>
      <StepIndicator />

      <div style={card}>
        <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600', color: '#495057' }}>
          Configuration Summary
        </h3>

        <div style={{ display: 'grid', gridTemplateColumns: '140px 1fr', gap: '12px 16px', fontSize: '14px' }}>
          <strong style={{ color: '#6c757d' }}>Dataset:</strong>
          <span>{dataset?.dataset_id}</span>

          <strong style={{ color: '#6c757d' }}>Format:</strong>
          <span>{validation?.format || 'wide'}</span>

          <strong style={{ color: '#6c757d' }}>KPI Column:</strong>
          <span style={{ fontWeight: '600', color: '#007bff' }}>{kpi}</span>

          <strong style={{ color: '#6c757d' }}>Spend Channels:</strong>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {spendChannels.map((ch) => (
              <span
                key={ch}
                style={{ display: 'inline-block', padding: '2px 8px', backgroundColor: '#28a745', color: '#fff', borderRadius: 4, fontSize: '13px', fontWeight: '600' }}
              >
                {ch}
              </span>
            ))}
          </div>

          {covariates.length > 0 && (
            <>
              <strong style={{ color: '#6c757d' }}>Covariates:</strong>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {covariates.map((c) => (
                  <span
                    key={c}
                    style={{ display: 'inline-block', padding: '2px 8px', backgroundColor: '#17a2b8', color: '#fff', borderRadius: 4, fontSize: '13px', fontWeight: '600' }}
                  >
                    {c}
                  </span>
                ))}
              </div>
            </>
          )}

          {validation?.date_range && (
            <>
              <strong style={{ color: '#6c757d' }}>Date Range:</strong>
              <span>
                {validation.date_range.min} to {validation.date_range.max} ({validation.date_range.n_periods} periods)
              </span>
            </>
          )}

          <strong style={{ color: '#6c757d' }}>Rows:</strong>
          <span>{validation?.n_rows || 'N/A'}</span>
        </div>
      </div>

      <div style={{ ...card, backgroundColor: '#e8f5e9', borderColor: '#a5d6a7' }}>
        <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600', color: '#2e7d32' }}>
          Ready to Run
        </h3>
        <p style={{ fontSize: '14px', color: '#2e7d32', marginBottom: 0 }}>
          The model will use <strong>PyMC-Marketing Bayesian MMM</strong> with geometric adstock
          and logistic saturation transforms. If PyMC is not available, it will fall back to Ridge
          regression.
        </p>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <button onClick={() => setStep('quality')} style={secondaryBtn}>
          Back
        </button>
        <button
          onClick={() =>
            onComplete({
              dataset_id: dataset!.dataset_id,
              columns: { kpi, spend_channels: spendChannels, covariates },
            })
          }
          style={{
            ...primaryBtn,
            backgroundColor: '#28a745',
            fontSize: '16px',
            padding: '14px 32px',
          }}
        >
          Run Model
        </button>
      </div>
    </div>
  )
}
