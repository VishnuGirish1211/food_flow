import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/auth': 'http://localhost:8085',
      '/restaurants': 'http://localhost:8085',
      '/orders': 'http://localhost:8085',
      '/payments': 'http://localhost:8085'
    }
  }
})
