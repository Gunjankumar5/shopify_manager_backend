import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    hmr: {
      protocol: 'ws',
      host: 'localhost',
      // Auto-detect based on actual server port
    },
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true
      }
    }
  },
  // Polyfill process and global for browser environment
  define: {
    global: 'globalThis',
    'process.env': '{}',
    'process.env.NODE_ENV': JSON.stringify('development'),
  },
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      '@supabase/supabase-js',
    ],
  },
  build: {
    // Use esbuild minification (stable and performant)
    minify: 'esbuild',
    
    // Split dependencies into logical chunks to avoid very large single bundles
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules')) {
            if (id.includes('react') || id.includes('react-dom')) return 'vendor_react';
            if (id.includes('react-router-dom')) return 'vendor_router';
            if (id.includes('@supabase') || id.includes('supabase')) return 'vendor_supabase';
            if (id.includes('axios')) return 'vendor_axios';
            return 'vendor_misc';
          }
        }
      }
    },
    chunkSizeWarningLimit: 600,
    sourcemap: false,
    assetsInlineLimit: 4096,
    reportCompressedSize: true,
  },
});
