import { defineConfig } from "vite";
import { nodePolyfills } from "vite-plugin-node-polyfills";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";

const dingoTarget = process.env.DINGO_UTXORPC_URL ?? "http://127.0.0.1:9090";

const rpcProxy = {
  target: dingoTarget,
  changeOrigin: true,
  ws: false,
};

export default defineConfig({
  plugins: [
    wasm(),
    topLevelAwait(),
    nodePolyfills({
      include: ["buffer", "process"],
      globals: {
        Buffer: true,
        global: true,
        process: true,
      },
    }),
  ],
  define: {
    global: "globalThis",
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/utxorpc.v1alpha.query.QueryService": rpcProxy,
      "/utxorpc.v1alpha.submit.SubmitService": rpcProxy,
      "/utxorpc.v1alpha.sync.SyncService": rpcProxy,
      "/utxorpc.v1alpha.watch.WatchService": rpcProxy,
      "/grpc.health.v1.Health": rpcProxy,
    },
  },
});
