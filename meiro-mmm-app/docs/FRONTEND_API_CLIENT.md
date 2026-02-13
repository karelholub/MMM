# Frontend API Client Guide

## Purpose
Use `frontend/src/lib/apiClient.ts` for all HTTP requests from React modules/connectors.

## Why
- Standardizes auth + workspace headers.
- Standardizes error parsing and fallback messages.
- Reduces duplicate `fetch` boilerplate.

## Helpers
- `apiGetJson<T>(url, { fallbackMessage? })`
- `apiSendJson<T>(url, method, body?, { fallbackMessage? })`
- `apiRequest(url, { method, headers?, body?, fallbackMessage? })`
- `withQuery(path, params)`
- `getUserContext()` for `user_id` / `role` resolution

## Usage examples
```ts
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

const data = await apiGetJson<MyResponse>(
  withQuery('/api/journeys/definitions', { page: 1, per_page: 50, sort: 'updated_at', order: 'desc' }),
  { fallbackMessage: 'Failed to load journey definitions' },
)

await apiSendJson('/api/journeys/definitions', 'POST', payload, {
  fallbackMessage: 'Failed to create journey definition',
})
```

## Rules
- Do not call `fetch` directly in modules/connectors.
- Always provide a user-facing `fallbackMessage` for mutations and important reads.
- Use `withQuery` instead of manual `URLSearchParams` concatenation.
- Never put secrets in query strings.
