import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig({
    plugins: [react()],
    build: {
        rollupOptions: {
            output: {
                manualChunks: function (id) {
                    if (id.indexOf('node_modules/react-dom/') !== -1 || id.indexOf('node_modules/react/') !== -1)
                        return 'react';
                    if (id.indexOf('node_modules/react-router') !== -1)
                        return 'router';
                    if (id.indexOf('node_modules/recharts') !== -1)
                        return 'recharts';
                },
            },
        },
        chunkSizeWarningLimit: 600,
    },
    server: {
        port: 5173,
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:8000',
                changeOrigin: true,
            },
        },
    },
});
