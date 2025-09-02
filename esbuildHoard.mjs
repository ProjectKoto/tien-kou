import * as esbuild from 'esbuild'
import fs from 'fs/promises'
import * as jsonc from 'jsonc-parser'
import * as common from './esbuildCommon.mjs'

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
const cfg = ({
  ...common.esbuildConfig,
  plugins: [
    ...(common.esbuildConfig.plugins || []),
    // https://github.com/evanw/esbuild/issues/3215
    // {
    //   name: 'selectPackagesExternalHoard',
    //   setup(build) {
    //     build.onResolve({ filter: /^telegram/ }, args => {
    //       return { external: false }
    //     })
    //     build.onResolve({ filter: /^mddb/ }, args => {
    //       return { external: false }
    //     })
    //     build.onResolve({ filter: /^miniflare/ }, args => {
    //       return { external: false }
    //     })
    //     build.onResolve({ filter: /^\.\.?\// }, args => {
    //       return { external: false }
    //     })
    //     build.onResolve({ filter: /^/ }, args => {
    //       return { external: true }
    //     })
    //   },
    // },
  ],
  entryPoints: ['src/hoard/hoard.mts'],
  packages: 'external',
  format: 'esm',
  platform: 'node',
  outdir: 'nodeDist/',
  sourceRoot: 'nodeDist',
  outExtension: {
    ".js": ".mjs",
  },
})

await common.watchOrBuild(cfg)
