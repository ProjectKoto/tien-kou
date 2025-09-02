import * as esbuild from 'esbuild'
import fs from 'fs/promises'
import * as jsonc from 'jsonc-parser'
import * as common from './esbuildCommon.mjs'

// dump esbuild options by adding log to wrangler bundleWorker() then imitate it
const cfg = ({
  ...common.esbuildConfig,
  external: [ '__STATIC_CONTENT_MANIFEST', 'sqlite3', 'better-sqlite3', 'pg', 'mysql', 'mysql2', 'tedious', 'pg-query-stream', 'oracledb', 'aws-sdk', 'mock-aws-s3', 'nock' ],
  // external: [ '__STATIC_CONTENT_MANIFEST' ],
  entryPoints: ['src/webExtMain.mjs'],
  format: 'esm',
  outdir: 'webExtDist/',
  sourceRoot: 'webExtDist',
})

await common.watchOrBuild(cfg)
