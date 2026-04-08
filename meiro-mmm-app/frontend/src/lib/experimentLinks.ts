export function buildIncrementalityPlannerHref(args: {
  channel?: string | null
  conversionKey?: string | null
  startAt?: string | null
  endAt?: string | null
  name?: string | null
  notes?: string | null
  segmentId?: string | null
}): string {
  const params = new URLSearchParams()
  params.set('page', 'incrementality')
  if (args.channel) params.set('planner_channel', String(args.channel))
  if (args.conversionKey) params.set('planner_conversion_key', String(args.conversionKey))
  if (args.startAt) params.set('planner_start', String(args.startAt))
  if (args.endAt) params.set('planner_end', String(args.endAt))
  if (args.name) params.set('planner_name', String(args.name))
  if (args.notes) params.set('planner_notes', String(args.notes))
  if (args.segmentId) params.set('planner_segment_id', String(args.segmentId))
  return `/?${params.toString()}`
}
