import * as esbuild from 'esbuild'
import fs from 'fs/promises'
import * as jsonc from 'jsonc-parser'

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
/** @type{esbuild.BuildOptions} */
export const esbuildConfig = {
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
        console.log(`build ended with ${result.errors.length} errors`)
        // HERE: somehow restart the server from here, e.g., by sending a signal that you trap and react to inside the server.
      })
    },
  }],
}

export const watchOrBuild = async (config) => {
  if (process.argv[process.argv.length - 1] === 'watch') {
    const ctx = await esbuild.context(config)
    ctx.watch()
  } else {
    await esbuild.build(config)
  }
}

