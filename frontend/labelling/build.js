import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { build, context } from "esbuild";

const appRoot = fileURLToPath(
  new URL("../../uk_address_matcher/labelling/app/", import.meta.url),
);
const outputRoot = new URL("../../uk_address_matcher/labelling/app/static/", import.meta.url);
const assetsRoot = new URL("./assets/", outputRoot);
const duckdbRoot = new URL("./node_modules/@duckdb/duckdb-wasm/dist/", import.meta.url);
const watch = process.argv.includes("--watch");

await rm(outputRoot, { recursive: true, force: true });
await mkdir(assetsRoot, { recursive: true });

const assetImports = {
  "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url": "duckdb-mvp.wasm",
  "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url":
    "duckdb-browser-mvp.worker.js",
};

const options = {
  entryPoints: [`${appRoot}app.js`, `${appRoot}app.css`],
  bundle: true,
  format: "esm",
  minify: true,
  nodePaths: [fileURLToPath(new URL("./node_modules", import.meta.url))],
  outdir: fileURLToPath(assetsRoot),
  entryNames: "index",
  plugins: [
    {
      name: "duckdb-assets",
      setup(builder) {
        builder.onResolve({ filter: /\?url$/ }, ({ path }) => ({
          path,
          namespace: "duckdb-assets",
        }));
        builder.onLoad(
          { filter: /.*/, namespace: "duckdb-assets" },
          async ({ path }) => {
            const fileName = assetImports[path];
            if (!fileName) throw new Error(`Unsupported asset import: ${path}`);
            await cp(new URL(fileName, duckdbRoot), new URL(fileName, assetsRoot));
            return {
              contents: `export default new URL(${JSON.stringify(fileName)}, import.meta.url).href;`,
              loader: "js",
            };
          },
        );
      },
    },
  ],
};

async function writeIndex() {
  const html = (await readFile(`${appRoot}index.html`, "utf8"))
    .replace(
      '<link rel="stylesheet" href="./app.css" />',
      '<link rel="stylesheet" crossorigin href="./assets/index.css">',
    )
    .replace(
      '<script type="module" src="./app.js"></script>',
      '<script type="module" crossorigin src="./assets/index.js"></script>',
    );
  await writeFile(new URL("index.html", outputRoot), html);
}

await writeIndex();

if (watch) {
  const watcher = await context(options);
  await watcher.watch();
  const server = await watcher.serve({
    host: "127.0.0.1",
    port: 5173,
    servedir: fileURLToPath(outputRoot),
  });
  console.log(`Labelling app: http://${server.host}:${server.port}`);
} else {
  await build(options);
}
