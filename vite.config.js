import { defineConfig } from "vite";

export default defineConfig({
  root: "uk_address_matcher/labelling/app",
  base: "./",
  build: {
    outDir: "static",
    emptyOutDir: true,
  },
});
