import { defineConfig } from "vite";

const dingoTarget = process.env.DINGO_BLOCKFROST_URL ?? "http://127.0.0.1:3000";
const dingoMetricsTarget = process.env.DINGO_METRICS_URL ?? "http://127.0.0.1:12798";

// Attached by the dev server to proxied requests, so the shared secret
// stays server-side and never reaches the browser. Dingo refuses a
// non-loopback API bind without authentication, which is what a container
// bind is, so the Compose stack always sets this.
const dingoToken = process.env.DINGO_API_TOKEN;
const authHeaders = dingoToken
  ? { Authorization: `Bearer ${dingoToken}` }
  : undefined;

const blockfrostProxy = {
  target: dingoTarget,
  changeOrigin: true,
  ws: false,
  headers: authHeaders,
};

const metricsProxy = {
  target: dingoMetricsTarget,
  changeOrigin: true,
  ws: false,
};

export default defineConfig({
  server: {
    host: "0.0.0.0",
    port: 5173,
    strictPort: true,
    proxy: {
      "/api/v0": blockfrostProxy,
      "/health": blockfrostProxy,
      "/metrics": metricsProxy,
    },
  },
});
