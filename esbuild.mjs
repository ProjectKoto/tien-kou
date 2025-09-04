import * as esbuild from 'esbuild'
import fs from 'fs/promises'
import * as jsonc from 'jsonc-parser'

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
/** @type{esbuild.BuildOptions} */
export const makeEsbuildConfig = async (programName) => { return {
  bundle: true,
  keepNames: true,
  
  target: 'es6',
  sourcemap: true,
  metafile: true,
  alias: jsonc.parse(await fs.readFile('./wrangler.jsonc', {encoding: 'utf-8'})).alias,
  loader: {
    '.html': 'file',
  },
  plugins: [{
    name: 'rebuild-notify',
    setup(build) {
      build.onEnd(result => {
        console.log(`${(() => {
          const date = new Date()
          return new Date(date.getTime() - (date.getTimezoneOffset() * 60000)).toISOString().replace(/Z$/g, '')
        })()} ${programName} build ended with ${result.errors.length} errors`)
        // HERE: somehow restart the server from here, e.g., by sending a signal that you trap and react to inside the server.
      })
    },
  }],
} }

let esbuildConfig
esbuildConfig = await makeEsbuildConfig('hoard')

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
export const hoardBuildCfg = ({
  ...esbuildConfig,
  plugins: [
    ...(esbuildConfig.plugins || []),
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
  sourceRoot: '',
  outExtension: {
    ".js": ".mjs",
  },
})

esbuildConfig = await makeEsbuildConfig('nodeRtServe')

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
export const nodeRtServeBuildCfg = ({
  ...esbuildConfig,
  external: [ '__STATIC_CONTENT_MANIFEST', 'sqlite3', 'better-sqlite3', 'pg', 'mysql', 'mysql2', 'tedious', 'pg-query-stream', 'oracledb', 'aws-sdk', 'mock-aws-s3', 'nock' ],
  // external: [ '__STATIC_CONTENT_MANIFEST' ],
  entryPoints: ['src/serve/nodeRuntimeServe.mts'],
  format: 'cjs',
  platform: 'node',
  outdir: 'nodeDist/',
  sourceRoot: 'nodeDist',
  outExtension: {
    // ".js": ".mjs",
    ".js": ".cjs",
  },
})

esbuildConfig = await makeEsbuildConfig('webExt')

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
export const webExtBuildCfg = ({
  ...esbuildConfig,
  external: [ '__STATIC_CONTENT_MANIFEST', 'sqlite3', 'better-sqlite3', 'pg', 'mysql', 'mysql2', 'tedious', 'pg-query-stream', 'oracledb', 'aws-sdk', 'mock-aws-s3', 'nock' ],
  // external: [ '__STATIC_CONTENT_MANIFEST' ],
  entryPoints: ['src/webExtMain.mjs'],
  format: 'esm',
  outdir: 'webExtDist/',
  sourceRoot: 'webExtDist',
})

export const watch = async (config) => {
  const ctx = await esbuild.context(config)
  await ctx.watch()
}

export const main = async () => {
  const which = process.argv[process.argv.length - 2]

  let configList
  if (which === 'hoard') {
    configList = [hoardBuildCfg]
  } else if (which === 'nodeRtServe') {
    configList = [nodeRtServeBuildCfg]
  } else if (which === 'webExt') {
    configList = [webExtBuildCfg]
  } else if (which === 'all') {
    configList = [hoardBuildCfg, nodeRtServeBuildCfg, webExtBuildCfg]
  } else {
    throw new Error('unrecognized arg: ' + which)
  }

  if (process.argv[process.argv.length - 1] === 'watch') {
    for (const config of configList) {
      watch(config)
    }
  } else {
    for (const config of configList) {
      try {
        await esbuild.build(config)
      } catch (e) {
        console.error(e)
      }
    }
  }
}

await main()
