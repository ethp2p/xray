import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';

export default defineConfig({
  plugins: [solid()],
  server: {
    allowedHosts: true,
    proxy: {
      '/api': {
        target: 'http://localhost:9100',
        changeOrigin: true,
        ws: true,
      },
      '/instrument.v1.InstrumentService': {
        target: 'http://localhost:9100',
        changeOrigin: true,
        ws: true,
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.setHeader('Connection', 'keep-alive');
          });
        },
      },
      '/ws': {
        target: 'http://localhost:9100',
        changeOrigin: true,
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
