type ApiPrimitive = string | number | boolean | null | undefined

type ApiRequestOptions = Omit<RequestInit, 'headers'> & {
  headers?: Record<string, string>
  auth?: boolean
  fallbackMessage?: string
}

type CsrfRefreshPayload = {
  csrf_token?: string | null
}

export class ApiError extends Error {
  status: number
  detail?: unknown
  code?: string

  constructor(message: string, status: number, detail?: unknown, code?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
    this.code = code
  }
}

function isBrowser(): boolean {
  return typeof window !== 'undefined'
}

export function getUserContext(): { role: string; userId: string } {
  if (!isBrowser()) return { role: '', userId: 'ui' }
  const params = new URLSearchParams(window.location.search)
  return {
    role: (params.get('role') || window.localStorage.getItem('mmm-user-role') || '').trim(),
    userId: (params.get('user_id') || window.localStorage.getItem('mmm-user-id') || 'ui').trim(),
  }
}

export function authHeaders(): Record<string, string> {
  const { role, userId } = getUserContext()
  const headers: Record<string, string> = {}
  if (role) headers['X-User-Role'] = role
  if (userId) headers['X-User-Id'] = userId
  if (isBrowser()) {
    try {
      const csrf = (window.localStorage.getItem('mmm-csrf-token') || '').trim()
      if (csrf) headers['X-CSRF-Token'] = csrf
    } catch {
      // ignore storage errors
    }
  }
  return headers
}

function mergeHeaders(
  provided?: Record<string, string>,
  includeAuth: boolean = true,
  body?: BodyInit | null,
): Record<string, string> {
  const headers: Record<string, string> = {
    ...(includeAuth ? authHeaders() : {}),
    ...(provided ?? {}),
  }
  if (body && !(body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json'
  }
  return headers
}

export async function parseApiError(res: Response, fallbackMessage: string): Promise<Error> {
  let detailPayload: unknown = undefined
  let message = fallbackMessage
  let code: string | undefined
  try {
    const contentType = res.headers.get('content-type') || ''
    if (contentType.includes('application/json')) {
      const body = await res.json()
      detailPayload = body?.detail ?? body
      const detail = body?.detail
      if (typeof detail === 'string' && detail.trim()) {
        message = detail
      } else if (detail && typeof detail === 'object') {
        if (typeof detail.message === 'string' && detail.message.trim()) {
          message = detail.message
        } else if (typeof detail.code === 'string' && detail.code.trim()) {
          message = detail.code
        }
        if (typeof detail.code === 'string' && detail.code.trim()) {
          code = detail.code
        }
      } else if (typeof body?.message === 'string' && body.message.trim()) {
        message = body.message
      }
      if (!code && typeof body?.code === 'string' && body.code.trim()) {
        code = body.code
      }
    } else {
      const text = (await res.text()).trim()
      detailPayload = text || undefined
      if (text) message = text
    }
  } catch {
    // ignore parse errors and return fallback below
  }
  return new ApiError(message, res.status, detailPayload, code)
}

async function tryRefreshCsrfToken(): Promise<boolean> {
  if (!isBrowser()) return false
  try {
    const res = await fetch('/api/auth/me', { method: 'GET' })
    if (!res.ok) return false
    const body = (await res.json()) as CsrfRefreshPayload
    const csrf = (body?.csrf_token || '').trim()
    if (!csrf) return false
    window.localStorage.setItem('mmm-csrf-token', csrf)
    return true
  } catch {
    return false
  }
}

async function isCsrfFailure(res: Response): Promise<boolean> {
  if (res.status !== 403) return false
  try {
    const cloned = res.clone()
    const contentType = cloned.headers.get('content-type') || ''
    if (!contentType.includes('application/json')) return false
    const body = await cloned.json()
    const detail = body?.detail
    if (typeof detail === 'string') {
      return detail === 'Missing CSRF token' || detail === 'Invalid CSRF token'
    }
  } catch {
    // ignore parse errors
  }
  return false
}

export async function apiRequest(path: string, options: ApiRequestOptions = {}): Promise<Response> {
  const { auth = true, headers, fallbackMessage = `Request failed: ${path}`, body, ...rest } = options
  const method = (rest.method || 'GET').toUpperCase()

  if (auth && method !== 'GET') {
    await tryRefreshCsrfToken()
  }

  const makeRequest = () =>
    fetch(path, {
      ...rest,
      body,
      headers: mergeHeaders(headers, auth, body ?? null),
    })

  let res = await makeRequest()
  if (auth && method !== 'GET' && await isCsrfFailure(res)) {
    const refreshed = await tryRefreshCsrfToken()
    if (refreshed) {
      res = await makeRequest()
    }
  }
  if (!res.ok) throw await parseApiError(res, fallbackMessage)
  return res
}

export async function apiGetJson<T>(path: string, options: Omit<ApiRequestOptions, 'method' | 'body'> = {}): Promise<T> {
  const res = await apiRequest(path, { ...options, method: 'GET' })
  return (await res.json()) as T
}

export async function apiSendJson<T>(
  path: string,
  method: 'POST' | 'PUT' | 'PATCH' | 'DELETE',
  payload?: unknown,
  options: Omit<ApiRequestOptions, 'method' | 'body'> = {},
): Promise<T> {
  const body = payload === undefined ? undefined : JSON.stringify(payload)
  const res = await apiRequest(path, { ...options, method, body })
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export function withQuery(path: string, params: Record<string, ApiPrimitive>): string {
  const q = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return
    q.set(key, String(value))
  })
  const qs = q.toString()
  return qs ? `${path}?${qs}` : path
}
