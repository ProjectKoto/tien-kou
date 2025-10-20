import path from "node:path"
import fs from "node:fs"
import { AnyObj, getParentPath, TkContext, TkError } from "./common.mts"
import replaceAll from 'string.prototype.replaceall'
import KeyvSqlite from "@keyv/sqlite"
import Keyv from "keyv"
import { HC, MiddleCacheHandler, KD } from "../serve/serveDef.mts"
import { SqliteCache as SqliteLruCache } from 'cache-sqlite-lru-ttl'
import '../nodeEnv.mts'

export const nodeResolvePath = (p: string) => {
  // return url.fileURLToPath(import.meta.resolve(p))
  // return path.resolve(__dirname, p)
  return path.resolve(".", p)
}

export const pathExists = async (p: string) => {
  return await fs.promises.access(p).then(() => true, () => false)
}


export const ensurePathDirExists = async (p: string) => {
  if (!await pathExists(p)) {
    await fs.promises.mkdir(p, { recursive: true })
  }
}

export const ensureParentDirExists = async (p: string) => {
  return await ensurePathDirExists(getParentPath(p))
}

export interface TkContextHoard extends TkContext {
  rcloneW: (...args: (string | AnyObj)[]) => Promise<any>
}

export const isPath2InDir1 = (path1: string, path2: string) => {
  const relative = path.relative(path1, path2)
  return relative && !relative.startsWith('..') && !path.isAbsolute(relative)
}

export const calcValidateFileSystemPathSync = (basePath: string, assetOriginFilePath: string) => {
  const fileSystemPath = path.normalize(path.join(basePath, assetOriginFilePath))
  const topDirPath = path.normalize(path.join(basePath, (replaceAll(assetOriginFilePath, '\\', '/') as string).split('/').filter(x => x)[0]))
  if (!isPath2InDir1(basePath, topDirPath)) {
    throw new TkError("bad path")
  }
  if (!isPath2InDir1(topDirPath, fileSystemPath)) {
    throw new TkError("bad path")
  }
  return fileSystemPath
}

export const defaultStaticGenBaseDir = './data/staticGen'


export const KeyvMiddleCacheHandler = HC<MiddleCacheHandler>()(async (_: KD<never>) => {

  let kResolve: (value: Keyv) => void = undefined!
  const kp: Promise<Keyv> = new Promise<Keyv>(r => { kResolve = r })

  kResolve(new Keyv({
    store: new KeyvSqlite({
      uri: 'sqlite://' + process.env.PROCENV_CACHE_KEYV_SQLITE_PATH!,
    }),
  }))

  return {
    fetchDataVersion: async (_ctx) => {
      const kv = await kp
      const fetchedKvDataVersion = await kv.get("dataVersion")
      if (fetchedKvDataVersion !== null && fetchedKvDataVersion !== undefined && fetchedKvDataVersion !== "") {
        return fetchedKvDataVersion
      }
      return undefined
    },
    evictForNewDataVersion: async (_ctx: TkContext) => {
      const kv = await kp
      await kv.clear()
    },
    getInCache: async <T,>(_ctx: TkContext, k: string): Promise<T | undefined> => {
      const kv = await kp
      const v = await kv.get(k)
      if (v === undefined || v === null) {
        return undefined
      }
      return v
    },
    putInCache: async <T,>(_ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      const kv = await kp
      await kv.set(k, v)
    },
  }
})

export const LruSqliteMiddleCacheHandler = HC<MiddleCacheHandler>()(async (_: KD<never>) => {

  let lsResolve: (value: SqliteLruCache) => void = undefined!
  const lsp: Promise<SqliteLruCache> = new Promise<SqliteLruCache>(r => { lsResolve = r })

  lsResolve(new SqliteLruCache({
    database: process.env.PROCENV_CACHE_LRU_SQLITE_PATH!,
    maxItems: 2000,
    compress: false,
  }))

  return {
    fetchDataVersion: async (_ctx) => {
      const ls = await lsp
      const fetchedKvDataVersion = await ls.get("dataVersion")
      if (fetchedKvDataVersion !== null && fetchedKvDataVersion !== undefined && fetchedKvDataVersion !== "") {
        return fetchedKvDataVersion
      }
      return undefined
    },
    evictForNewDataVersion: async (_ctx: TkContext) => {
      const ls = await lsp
      await ls.clear()
    },
    getInCache: async <T,>(_ctx: TkContext, k: string): Promise<T | undefined> => {
      const ls = await lsp
      const v = await ls.get(k)
      if (v === undefined || v === null) {
        return undefined
      }
      return v
    },
    putInCache: async <T,>(_ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      const ls = await lsp
      await ls.set(k, v)
    },
  }
})

