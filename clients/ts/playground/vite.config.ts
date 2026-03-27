import { defineConfig } from "vite";
import { resolve } from "path";

export default defineConfig({
  resolve: {
    alias: {
      // Point at SDK source for live development — no build step needed.
      "@wavehouse/sdk": resolve(__dirname, "../src/index.ts"),
    },
  },
  server: {
    port: 5180,
    open: true,
  },
});
