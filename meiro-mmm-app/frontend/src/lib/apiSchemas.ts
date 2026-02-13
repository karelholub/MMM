export type SortDirection = 'asc' | 'desc'

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  per_page: number
}

export interface ListQueryOptions {
  page?: number
  perPage?: number
  pageSize?: number
  limit?: number
  search?: string
  sort?: SortDirection
  order?: SortDirection
  domain?: string
}

export function clampPageSize(value: number | undefined, max: number, fallback: number): number {
  if (!Number.isFinite(value)) return fallback
  const asInt = Math.floor(Number(value))
  if (asInt < 1) return 1
  if (asInt > max) return max
  return asInt
}

export function buildListQuery(options: ListQueryOptions, maxPerPage: number = 100): Record<string, string | number> {
  const perPage = clampPageSize(options.perPage ?? options.pageSize ?? options.limit, maxPerPage, 20)
  const page = Math.max(1, Math.floor(options.page ?? 1))
  const query: Record<string, string | number> = {
    page,
    per_page: perPage,
  }
  if (options.search) query.search = options.search
  if (options.sort) query.sort = options.sort
  if (options.order) query.order = options.order
  if (options.domain) query.domain = options.domain
  return query
}
