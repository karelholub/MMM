import { ChangeEvent } from 'react'
import type { CSSProperties } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface GlobalFiltersState {
  dateFrom: string
  dateTo: string
  channel: string
  campaign: string
  device: string
  geo: string
  segment: string
}

interface Option {
  value: string
  label: string
}

interface GlobalFilterBarProps {
  value: GlobalFiltersState
  onChange: (next: GlobalFiltersState) => void
  channels?: string[]
}

const FIELD_STYLE: CSSProperties = {
  minWidth: 140,
  padding: '6px 10px',
  borderRadius: t.radius.sm,
  border: `1px solid ${t.color.borderLight}`,
  backgroundColor: t.color.surface,
  color: t.color.text,
  fontSize: t.font.sizeSm,
}

function asOptions(values: string[]): Option[] {
  return values.map((value) => ({ value, label: value }))
}

function updateSelect(
  event: ChangeEvent<HTMLSelectElement | HTMLInputElement>,
  key: keyof GlobalFiltersState,
  value: GlobalFiltersState,
  onChange: (next: GlobalFiltersState) => void,
) {
  onChange({ ...value, [key]: event.target.value })
}

export default function GlobalFilterBar({
  value,
  onChange,
  channels = [],
}: GlobalFilterBarProps) {
  const channelOptions = [{ value: 'all', label: 'All channels' }, ...asOptions(channels)]
  const campaignOptions = [
    { value: 'all', label: 'All campaigns' },
    { value: 'newsletter', label: 'Newsletter' },
    { value: 'brand', label: 'Brand' },
    { value: 'retargeting', label: 'Retargeting' },
  ]
  const deviceOptions = [
    { value: 'all', label: 'All devices' },
    { value: 'desktop', label: 'Desktop' },
    { value: 'mobile', label: 'Mobile' },
    { value: 'tablet', label: 'Tablet' },
  ]
  const geoOptions = [
    { value: 'all', label: 'All geos' },
    { value: 'us', label: 'United States' },
    { value: 'eu', label: 'Europe' },
    { value: 'apac', label: 'APAC' },
  ]
  const segmentOptions = [
    { value: 'all', label: 'All segments' },
    { value: 'new', label: 'New users' },
    { value: 'returning', label: 'Returning users' },
    { value: 'high_value', label: 'High value' },
  ]

  return (
    <div
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: t.space.sm,
        alignItems: 'center',
      }}
    >
      <input
        type="date"
        value={value.dateFrom}
        onChange={(event) => updateSelect(event, 'dateFrom', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Date from"
      />
      <input
        type="date"
        value={value.dateTo}
        onChange={(event) => updateSelect(event, 'dateTo', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Date to"
      />

      <select
        value={value.channel}
        onChange={(event) => updateSelect(event, 'channel', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Channel filter"
      >
        {channelOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>

      <select
        value={value.campaign}
        onChange={(event) => updateSelect(event, 'campaign', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Campaign filter"
      >
        {campaignOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>

      <select
        value={value.device}
        onChange={(event) => updateSelect(event, 'device', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Device filter"
      >
        {deviceOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>

      <select
        value={value.geo}
        onChange={(event) => updateSelect(event, 'geo', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Geo filter"
      >
        {geoOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>

      <select
        value={value.segment}
        onChange={(event) => updateSelect(event, 'segment', value, onChange)}
        style={FIELD_STYLE}
        aria-label="Segment filter"
      >
        {segmentOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  )
}
