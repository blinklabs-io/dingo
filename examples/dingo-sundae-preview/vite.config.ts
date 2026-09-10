import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import { nodePolyfills } from "vite-plugin-node-polyfills";

const messageSigningShim = fileURLToPath(
  new URL("./src/shims/cardanoMessageSigning.ts", import.meta.url),
);

const dingoTarget = process.env.DINGO_UTXORPC_URL ?? "http://127.0.0.1:9090";

// Attached by the dev server to proxied requests, so the shared secret
// stays server-side and never reaches the browser. Dingo refuses a
// non-loopback API bind without authentication, which is what a container
// bind is, so the Compose stack always sets this.
const dingoToken = process.env.DINGO_API_TOKEN;

const rpcProxy = {
  target: dingoTarget,
  changeOrigin: true,
  ws: false,
  headers: dingoToken ? { Authorization: `Bearer ${dingoToken}` } : undefined,
};

export default defineConfig({
  plugins: [
    nodePolyfills({
      include: ["buffer", "process", "util"],
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
  resolve: {
    alias: {
      "@emurgo/cardano-message-signing-browser": messageSigningShim,
      "@emurgo/cardano-message-signing-nodejs": messageSigningShim,
    },
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
