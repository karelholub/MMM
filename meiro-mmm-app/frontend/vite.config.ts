import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/react') || id.includes('node_modules/react-dom')) {
            return 'vendor-react'
          }
          if (id.includes('node_modules/@tanstack')) {
            return 'vendor-query'
          }
          if (id.includes('node_modules/recharts') || id.includes('node_modules/d3-')) {
            return 'vendor-charts'
          }
          if (id.includes('/src/components/dashboard/') || id.includes('/src/components/ads/')) {
            return 'ui-surfaces'
          }
          if (id.includes('/src/lib/') || id.includes('/src/connectors/')) {
            return 'data-access'
          }
          if (id.includes('/src/modules/app/') || id.includes('/src/components/WorkspaceContext')) {
            return 'app-shell'
          }
        },
      },
    },
  },
  server: {
    host: true,
    allowedHosts: ['host.docker.internal'],
    port: 5173,
    proxy: {
      '/api': {
        // Default to localhost for local dev, or use VITE_API_BASE if set (for Docker)
        target: process.env.VITE_API_BASE || 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      }
    }
  }
})
