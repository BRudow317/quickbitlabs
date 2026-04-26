import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// https://vite.dev/config/
export default defineConfig({
  base: "/", 
  //process.env.VITE_BASE_PATH || "/",
  plugins: [react(), tailwindcss()],
  server: {
    allowedHosts: [
      "quickbitlabs.com",
      "www.quickbitlabs.com",
      "cloudvoyages.com",
      "www.cloudvoyages.com",
      "millerlandman.com",
      "www.millerlandman.com",
      "miller-land-management.com",
      "www.miller-land-management.com",
      "brudow317.github.io",
      "agileindianapolis.com",
      "www.agileindianapolis.com",
    ],
    // port: 6050,
    watch: {
      ignored: ['**/bin/**','**/lib/**','**/logs/**','**/node_modules/**','**/.git/**','**/build/**'],
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 10240,
  },
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
})
