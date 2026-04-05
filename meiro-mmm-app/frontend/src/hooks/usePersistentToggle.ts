import { useEffect, useState } from 'react'

export function usePersistentToggle(key: string, defaultValue = false) {
  const [value, setValue] = useState<boolean>(() => {
    if (typeof window === 'undefined') return defaultValue
    try {
      const raw = window.localStorage.getItem(key)
      if (raw === 'true') return true
      if (raw === 'false') return false
    } catch {
      // Ignore persistence errors and fall back to the default state.
    }
    return defaultValue
  })

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      window.localStorage.setItem(key, value ? 'true' : 'false')
    } catch {
      // Ignore persistence errors and keep the panel usable.
    }
  }, [key, value])

  return [value, setValue] as const
}
