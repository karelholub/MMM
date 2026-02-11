import { useState, useEffect, useRef } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

interface DatasetUploadResponse {
  dataset_id: string
  columns: string[]
  preview_rows: Record<string, any>[]
  path: string
  type?: string
}

interface ColumnMapping {
  kpi: string
  spend_channels: string[]
  covariates: string[]
}

interface ValidateResponse {
  date_column: string | null
  suggestions: {
    date_column: string | null
    kpi_columns: string[]
    kpi_columns_sales?: string[]
    kpi_columns_conversions?: string[]
    spend_channels: string[]
    covariates: string[]
    covariates_binary?: string[]
    covariates_numeric?: string[]
  }
  columns: { name: string; dtype: string; missing: number }[]
}

interface ValidateMappingResponse {
  errors: string[]
  warnings: string[]
  details: { n_weeks?: number; missingness?: Record<string, { count: number; pct: number }>; missingness_top?: { column: string; count: number; pct: number }[]; correlation_issues?: { pair: string[]; correlation: number }[] }
  valid: boolean
}

interface DatasetUploaderProps {
  onMappingComplete: (mapping: { dataset_id: string; columns: ColumnMapping }) => void
}

async function uploadDataset({ file, datasetId, type }: { file: File; datasetId?: string; type: string }): Promise<DatasetUploadResponse> {
  const formData = new FormData()
  formData.append('file', file)
  if (datasetId) formData.append('dataset_id', datasetId)
  const res = await fetch(`/api/datasets/upload?type=${type}`, { method: 'POST', body: formData })
  if (!res.ok) throw new Error('Upload failed')
  return res.json()
}

export default function DatasetUploader({ onMappingComplete }: DatasetUploaderProps) {
  const [file, setFile] = useState<File | null>(null)
  const [datasetType, setDatasetType] = useState<'sales' | 'attribution'>('sales')
  const [dataset, setDataset] = useState<DatasetUploadResponse | null>(null)
  const [dateColumn, setDateColumn] = useState<string>('')
  const [kpi, setKpi] = useState<string>('')
  const [spendChannels, setSpendChannels] = useState<string[]>([])
  const [covariates, setCovariates] = useState<string[]>([])
  const [validation, setValidation] = useState<ValidateMappingResponse | null>(null)
  const [proceedDespiteWarnings, setProceedDespiteWarnings] = useState(false)
  const appliedSuggestions = useRef(false)
  const queryClient = useQueryClient()

  const uploadMutation = useMutation({
    mutationFn: uploadDataset,
    onSuccess: (data) => { setDataset(data); appliedSuggestions.current = false },
  })

  const { data: validateData } = useQuery<ValidateResponse>({
    queryKey: ['validate', dataset?.dataset_id, datasetType],
    queryFn: async () => {
      const res = await fetch(`/api/datasets/${dataset!.dataset_id}/validate?kpi_target=${datasetType}`)
      if (!res.ok) throw new Error('Validation failed')
      return res.json()
    },
    enabled: !!dataset?.dataset_id,
  })

  useEffect(() => {
    if (!validateData?.suggestions || appliedSuggestions.current || !dataset) return
    const s = validateData.suggestions
    if (s.date_column && dataset.columns.includes(s.date_column)) setDateColumn(s.date_column)
    const kpiList = datasetType === 'sales' ? (s.kpi_columns_sales || s.kpi_columns) : (s.kpi_columns_conversions || s.kpi_columns)
    if (kpiList?.length && dataset.columns.includes(kpiList[0])) setKpi(kpiList[0])
    if (s.spend_channels?.length) {
      const valid = s.spend_channels.filter((c: string) => dataset.columns.includes(c))
      if (valid.length) setSpendChannels(valid)
    }
    if (s.covariates?.length) {
      const valid = s.covariates.filter((c: string) => dataset.columns.includes(c))
      setCovariates(valid.slice(0, 5))
    }
    appliedSuggestions.current = true
  }, [validateData, datasetType, dataset])

  const resolvedDateColumn = dateColumn || validateData?.date_column || validateData?.suggestions?.date_column || dataset?.columns.find((c) => /date|week|period|ds/i.test(c)) || (dataset?.columns?.length ? dataset.columns[0] : 'date')

  const validateMappingMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch(`/api/mmm/datasets/${dataset!.dataset_id}/validate-mapping`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date_column: resolvedDateColumn,
          kpi,
          spend_channels: spendChannels,
          covariates,
          kpi_target: datasetType,
        }),
      })
      if (!res.ok) throw new Error('Validation failed')
      return res.json() as Promise<ValidateMappingResponse>
    },
    onSuccess: (data) => setValidation(data),
  })

  const handleLoadSample = async (sampleId: string) => {
    try {
      const res = await fetch(`/api/datasets/${sampleId}`)
      if (!res.ok) throw new Error('Failed to load sample')
      const data = await res.json()
      setDataset(data)
      appliedSuggestions.current = false
    } catch (err) {
      console.error(err)
      alert('Failed to load sample dataset')
    }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0]
    if (f) setFile(f)
  }

  const handleUpload = () => {
    if (file) uploadMutation.mutate({ file, type: datasetType })
  }

  const toggleSpendChannel = (ch: string) => {
    setSpendChannels((prev) => (prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch]))
    setValidation(null)
  }

  const toggleCovariate = (cov: string) => {
    setCovariates((prev) => (prev.includes(cov) ? prev.filter((c) => c !== cov) : [...prev, cov]))
    setValidation(null)
  }

  const canContinue = !!dataset && !!kpi && spendChannels.length > 0 && (dateColumn || validateData?.suggestions?.date_column || dataset.columns.some((c) => /date|week|period/i.test(c)))
  const dateCol = dateColumn || validateData?.date_column || validateData?.suggestions?.date_column || ''

  const handleContinue = () => {
    if (!dataset || !kpi || spendChannels.length === 0) return
    validateMappingMutation.mutate(undefined, {
      onSuccess: (data) => {
        if (data.valid) submitMapping()
        else setValidation(data)
      },
    })
  }

  const submitMapping = () => {
    if (!dataset) return
    onMappingComplete({
      dataset_id: dataset.dataset_id,
      columns: { kpi, spend_channels: spendChannels, covariates },
    })
  }

  const proceedAnyway = () => {
    setProceedDespiteWarnings(false)
    submitMapping()
  }

  const numericColumns = dataset?.columns.filter((c) => c !== dateCol) ?? []
  const spendCandidates = numericColumns.filter((c) => c !== kpi)
  const covCandidates = numericColumns.filter((c) => c !== kpi && !spendChannels.includes(c))

  if (dataset) {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto', padding: t.space.xl }}>
        <div style={{ marginBottom: t.space.lg }}>
          <h2 style={{ margin: 0, fontSize: t.font.size2xl, fontWeight: t.font.weightBold, color: t.color.text }}>Dataset Mapping</h2>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <code style={{ background: t.color.bg, padding: '2px 8px', borderRadius: t.radius.sm }}>{dataset.dataset_id}</code>
            {validateData?.suggestions && <span style={{ marginLeft: t.space.sm, color: t.color.success }}>Smart suggestions applied</span>}
          </p>
        </div>

        {/* Preview */}
        <div style={{ background: t.color.surface, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.lg, overflow: 'hidden', marginBottom: t.space.xl, boxShadow: t.shadowSm }}>
          <div style={{ padding: `${t.space.md}px ${t.space.lg}px`, borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Data preview (5 rows)</div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}`, backgroundColor: t.color.bg }}>
                  {dataset.columns.map((col) => (
                    <th key={col} style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataset.preview_rows.map((row: Record<string, unknown>, idx: number) => (
                  <tr key={idx} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    {dataset.columns.map((col) => (
                      <td key={col} style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.text }}>{row[col] != null ? String(row[col]) : '—'}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Mapping summary card */}
        <div style={{ background: t.color.accentMuted, border: `1px solid ${t.color.accent}`, borderRadius: t.radius.md, padding: t.space.lg, marginBottom: t.space.xl }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>Mapping summary</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.lg, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <span><strong style={{ color: t.color.text }}>Date:</strong> {dateCol || '—'}</span>
            <span><strong style={{ color: t.color.text }}>KPI:</strong> {kpi || '—'}</span>
            <span><strong style={{ color: t.color.text }}>Spend channels:</strong> {spendChannels.length}</span>
            <span><strong style={{ color: t.color.text }}>Covariates:</strong> {covariates.length}</span>
          </div>
        </div>

        {/* Grouped mapping sections */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg, marginBottom: t.space.xl }}>
          <section style={{ background: t.color.surface, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.lg, padding: t.space.xl, boxShadow: t.shadowSm }}>
            <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Date (weekly)</h3>
            <select
              value={dateCol}
              onChange={(e) => { setDateColumn(e.target.value); setValidation(null) }}
              style={{ width: '100%', maxWidth: 320, padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, background: t.color.surface, color: t.color.text }}
            >
              <option value="">— Select date column —</option>
              {dataset.columns.map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </section>

          <section style={{ background: t.color.surface, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.lg, padding: t.space.xl, boxShadow: t.shadowSm }}>
            <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>KPI</h3>
            <p style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Target column based on {datasetType === 'sales' ? 'sales/revenue' : 'conversions'}.</p>
            <select
              value={kpi}
              onChange={(e) => { setKpi(e.target.value); setValidation(null) }}
              style={{ width: '100%', maxWidth: 320, padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, background: t.color.surface, color: t.color.text }}
            >
              <option value="">— Select KPI column —</option>
              {numericColumns.map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </section>

          <section style={{ background: t.color.surface, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.lg, padding: t.space.xl, boxShadow: t.shadowSm }}>
            <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Spend channels</h3>
            <p style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Select one or more columns that represent marketing spend.</p>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
              {spendCandidates.map((col) => {
                const selected = spendChannels.includes(col)
                return (
                  <button
                    key={col}
                    type="button"
                    onClick={() => toggleSpendChannel(col)}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      fontSize: t.font.sizeSm,
                      borderRadius: 999,
                      border: `1px solid ${selected ? t.color.accent : t.color.border}`,
                      background: selected ? t.color.accent : t.color.surface,
                      color: selected ? '#fff' : t.color.textSecondary,
                      cursor: 'pointer',
                      fontWeight: t.font.weightMedium,
                    }}
                  >
                    {col}
                  </button>
                )
              })}
            </div>
          </section>

          <section style={{ background: t.color.surface, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.lg, padding: t.space.xl, boxShadow: t.shadowSm }}>
            <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Covariates (optional)</h3>
            <p style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Binary (e.g. holiday) or numeric (e.g. price index).</p>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
              {covCandidates.map((col) => {
                const selected = covariates.includes(col)
                return (
                  <button
                    key={col}
                    type="button"
                    onClick={() => toggleCovariate(col)}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      fontSize: t.font.sizeSm,
                      borderRadius: 999,
                      border: `1px solid ${selected ? t.color.success : t.color.border}`,
                      background: selected ? t.color.success : t.color.surface,
                      color: selected ? '#fff' : t.color.textSecondary,
                      cursor: 'pointer',
                      fontWeight: t.font.weightMedium,
                    }}
                  >
                    {col}
                  </button>
                )
              })}
            </div>
          </section>
        </div>

        {/* Validation: errors and warnings */}
        {validation && (validation.errors.length > 0 || validation.warnings.length > 0) && (
          <div style={{ marginBottom: t.space.xl }}>
            {validation.errors.length > 0 && (
              <div style={{ background: t.color.dangerMuted, border: `1px solid ${t.color.danger}`, borderRadius: t.radius.md, padding: t.space.lg, marginBottom: t.space.md }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.danger, marginBottom: t.space.sm }}>Blocking errors (fix before continuing)</div>
                <ul style={{ margin: 0, paddingLeft: t.space.xl, fontSize: t.font.sizeSm, color: t.color.text }}>
                  {validation.errors.map((err, i) => (
                    <li key={i}>{err}</li>
                  ))}
                </ul>
              </div>
            )}
            {validation.warnings.length > 0 && (
              <div style={{ background: t.color.warningMuted, border: `1px solid ${t.color.warning}`, borderRadius: t.radius.md, padding: t.space.lg, marginBottom: t.space.md }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.warning, marginBottom: t.space.sm }}>Warnings (you can proceed)</div>
                <ul style={{ margin: 0, paddingLeft: t.space.xl, fontSize: t.font.sizeSm, color: t.color.text }}>
                  {validation.warnings.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
                {validation.details?.missingness_top?.length ? (
                  <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Missingness (top): {validation.details.missingness_top.map((m: { column: string; pct: number }) => `${m.column} ${m.pct}%`).join(', ')}
                  </div>
                ) : null}
              </div>
            )}
          </div>
        )}

        {/* Fix helpers (planned) */}
        <div style={{ background: t.color.bg, border: `1px dashed ${t.color.border}`, borderRadius: t.radius.md, padding: t.space.lg, marginBottom: t.space.xl }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.textSecondary, marginBottom: t.space.xs }}>Handling missing values (planned)</div>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>
            Per-column policies (zero-fill spend, interpolate KPI, drop weeks) are planned. For now: export your dataset, fix missing values or apply transformations in your tool, then re-upload.
          </p>
        </div>

        {/* Actions */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: t.space.md }}>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            {validateData?.date_range && (
              <span>{validateData.date_range.n_periods} periods · {validateData.date_range.min} → {validateData.date_range.max}</span>
            )}
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center' }}>
            {validation && !validation.valid && validation.warnings.length > 0 && validation.errors.length === 0 && (
              <button
                type="button"
                onClick={() => setProceedDespiteWarnings(true)}
                style={{ padding: `${t.space.sm}px ${t.space.lg}px`, fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.warning, background: 'transparent', border: `1px solid ${t.color.warning}`, borderRadius: t.radius.sm, cursor: 'pointer' }}
              >
                Proceed despite warnings
              </button>
            )}
            <button
              type="button"
              onClick={handleContinue}
              disabled={!canContinue || validateMappingMutation.isPending || (validation != null && validation.errors.length > 0)}
              style={{
                padding: `${t.space.md}px ${t.space.xl}px`,
                fontSize: t.font.sizeBase,
                fontWeight: t.font.weightSemibold,
                color: '#fff',
                background: !canContinue || (validation != null && validation.errors.length > 0) ? t.color.border : t.color.accent,
                border: 'none',
                borderRadius: t.radius.sm,
                cursor: !canContinue || (validation != null && validation.errors.length > 0) ? 'not-allowed' : 'pointer',
              }}
            >
              {validateMappingMutation.isPending ? 'Validating…' : 'Continue to Modeling'}
            </button>
          </div>
        </div>

        {/* If user clicked "Proceed despite warnings", show confirm and submit */}
        {proceedDespiteWarnings && validation && validation.errors.length === 0 && (
          <div style={{ marginTop: t.space.md }}>
            <button
              type="button"
              onClick={proceedAnyway}
              style={{ padding: `${t.space.sm}px ${t.space.lg}px`, fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.success, background: t.color.successMuted, border: `1px solid ${t.color.success}`, borderRadius: t.radius.sm, cursor: 'pointer' }}
            >
              Confirm and continue to modeling
            </button>
          </div>
        )}
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
