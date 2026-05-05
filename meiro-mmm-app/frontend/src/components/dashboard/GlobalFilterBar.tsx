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
  channels?: Option[]
  campaigns?: Option[]
  devices?: Option[]
  geos?: Option[]
  segments?: Option[]
  showSegment?: boolean
  segmentFallbackLabel?: string
  segmentAriaLabel?: string
}

const FIELD_STYLE: CSSProperties = {
  minWidth: 0,
  width: 'min(220px, 100%)',
  maxWidth: '100%',
  flex: '1 1 160px',
  padding: '6px 10px',
  borderRadius: t.radius.sm,
  border: `1px solid ${t.color.borderLight}`,
  backgroundColor: t.color.surface,
  color: t.color.text,
  fontSize: t.font.sizeSm,
}

function ensureAllOption(values: Option[], fallbackLabel: string): Option[] {
  const options = values.length ? values : []
  return [{ value: 'all', label: fallbackLabel }, ...options]
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
  campaigns = [],
  devices = [],
  geos = [],
  segments = [],
  showSegment = true,
  segmentFallbackLabel = 'All analytical segments',
  segmentAriaLabel = 'Analytical segment filter',
}: GlobalFilterBarProps) {
  const channelOptions = ensureAllOption(channels, 'All channels')
  const campaignOptions = ensureAllOption(campaigns, 'All campaigns')
  const deviceOptions = ensureAllOption(devices, 'All devices')
  const geoOptions = ensureAllOption(geos, 'All geos')
  const segmentOptions = ensureAllOption(segments, segmentFallbackLabel)

  return (
    <div
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: t.space.sm,
        alignItems: 'center',
        minWidth: 0,
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

      {showSegment ? (
        <select
          value={value.segment}
          onChange={(event) => updateSelect(event, 'segment', value, onChange)}
          style={FIELD_STYLE}
          aria-label={segmentAriaLabel}
        >
          {segmentOptions.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      ) : null}
    </div>
  )
}
