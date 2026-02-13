type ApiPrimitive = string | number | boolean | null | undefined

type ApiRequestOptions = Omit<RequestInit, 'headers'> & {
  headers?: Record<string, string>
  auth?: boolean
  fallbackMessage?: string
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
  try {
    const contentType = res.headers.get('content-type') || ''
    if (contentType.includes('application/json')) {
      const body = await res.json()
      const detail = body?.detail
      if (typeof detail === 'string' && detail.trim()) return new Error(detail)
      if (detail && typeof detail === 'object') {
        if (typeof detail.message === 'string' && detail.message.trim()) return new Error(detail.message)
        if (typeof detail.code === 'string' && detail.code.trim()) return new Error(detail.code)
      }
      if (typeof body?.message === 'string' && body.message.trim()) return new Error(body.message)
    } else {
      const text = (await res.text()).trim()
      if (text) return new Error(text)
    }
  } catch {
    // ignore parse errors and return fallback below
  }
  return new Error(fallbackMessage)
}

export async function apiRequest(path: string, options: ApiRequestOptions = {}): Promise<Response> {
  const { auth = true, headers, fallbackMessage = `Request failed: ${path}`, body, ...rest } = options
  const res = await fetch(path, {
    ...rest,
    body,
    headers: mergeHeaders(headers, auth, body ?? null),
  })
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
