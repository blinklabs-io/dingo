import { defineConfig } from "vite";

const dingoTarget = process.env.DINGO_BLOCKFROST_URL ?? "http://127.0.0.1:3000";
const dingoMetricsTarget = process.env.DINGO_METRICS_URL ?? "http://127.0.0.1:12798";

const blockfrostProxy = {
  target: dingoTarget,
  changeOrigin: true,
  ws: false,
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
