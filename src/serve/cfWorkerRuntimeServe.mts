
import * as hono from 'hono'
import { AnyObj, l, le, TkError } from '../lib/common.mts'
import { HonoWithErrorHandler } from '../lib/hack.mts'
import { AbstractTkSqlLiquidHonoApp, hc, TkContextHl, TkContextHlGetTkEnvHandler } from './honoIntegrate.mts'
import { RuntimeCachedLiquidHandler } from './liquidIntegrate.mts'
import { AbstractTkSqlAssetFetchHandler, EA, HT, KD, MainJsRuntimeCacheHandler, MainTkCtxHandler, MiddleCacheHandler, MultiIntanceCachePolicyHandler, NoMiddleCacheHandler, QueryLiveAssetSqlCommonParam, TienKouApp, TienKouAssetFetchHandler, TkAppStartInfo, TkAssetInfo, TkAssetNotFoundError, WebRedirHeavyAssetHandler } from './serveDef.mts'
import { TkContext } from '../lib/common.mts'
import { LiquidSqlFilterRegHandler, SqlTkDataPersistHandler, TkSqlAssetCategoryLogicHandler } from './tkAssetCategoryLogic.mts'
import { TursoSqlDbHandler } from './tursoSql.mts'
import { LiquidTelegramMsgFilterRegHandler } from './tgIntegrate'
import isArrayBuffer from 'is-array-buffer'

type CfweBindings = AnyObj
type Cfwe = { "Bindings": CfweBindings, "Variables": AnyObj }

// TkContext with Hono & Liquid & Cloudflare Worker Env
export interface TkContextHlc extends TkContextHl<Cfwe> {
  cfWorkerEnv: Cfwe,
  we: Cfwe,
}

const cfwe = (ctx: TkContext | undefined): CfweBindings => {
  let we: CfweBindings | undefined = undefined
  let hc_: hono.Context<Cfwe> | undefined = undefined
  if (we === undefined) {
    if (ctx !== undefined) {
      we = (ctx as TkContextHlc).we
    }
  }
  if (we === undefined) {
    try {
      hc_ = hc(ctx)
    } catch (e) {
      void(e)
    }
  }
  if (we === undefined) {
    if (hc_ !== undefined) {
      we = hc_.env
    }
  }

  if (we === undefined) {
    throw new TkError("Can't get Cloudflare Workers env")
  }
  return we
}

const CloudflareWorkerKvCacheHandler = HT<MiddleCacheHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (_ctx0) => {

  })

  return {
    // TODO: LOCK???
    fetchDataVersion: async (ctx) => {
      const we = cfwe(ctx)
      const fetchedKvDataVersion = await we.KV.get("dataVersion")
      if (fetchedKvDataVersion !== null && fetchedKvDataVersion !== undefined && fetchedKvDataVersion !== "") {
        return fetchedKvDataVersion
      }
      return undefined
      // const newDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
      // console.log("KvCache:fetchDataVersion:new", newDataVersion)
      // await we.KV.put("dataVersion", newDataVersion)
      // return newDataVersion
    },
    evictForNewDataVersion: async (ctx: TkContext, backstoreDataVersion: number) => {
      const we = cfwe(ctx)
      await we.KV.delete("dataVersion")
      // await we.KV.put("dataVersion", "")

      const cacheKvList = []
      let currCursor = undefined
      while (true) {
        // max 1000
        const currPage = await we.KV.list({ prefix: "middleCacheVal:", cursor: currCursor }) as AnyObj
        cacheKvList.push(...currPage.keys)
        if (!currPage.list_complete && currPage.cursor) {
          currCursor = currPage.cursor
          continue
        } else {
          break
        }
      }
      for (const k of cacheKvList) {
        try {
          await we.KV.delete(k.name)
        } catch (e) {
          le('KV.delete', e)
        }
        // await we.kv.put(k.name, "undefined")
      }

      console.log("KvCache:evictForNewDataVersion:new", backstoreDataVersion)
      await we.KV.put("dataVersion", backstoreDataVersion)
    },
    getInCache: async <T,>(ctx: TkContext, k: string): Promise<T | undefined> => {
      // TODO: LOCK???
      const we = cfwe(ctx)
      return JSON.parse(await we.KV.get("middleCacheVal:" + k))
    },
    putInCache: async <T,>(ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      // TODO: LOCK???
      const we = cfwe(ctx)
      await we.KV.put("middleCacheVal:" + k, JSON.stringify(v, function (k, v) {
        if (isArrayBuffer(v)) {
          return {
            type: 'Buffer',
            data: [...new Uint8Array(v)]
          }
        }
        return v
      }))
    },
  }
})

const RoutedMiddleCacheHandler = HT<MiddleCacheHandler>()(async ({ TkFirstCtxProvideHandler, CloudflareWorkerKvCacheHandler, NoMiddleCacheHandler }: KD<"TkFirstCtxProvideHandler", { CloudflareWorkerKvCacheHandler: MiddleCacheHandler, NoMiddleCacheHandler: MiddleCacheHandler }>) => {

  let cacheLevel = "kv"

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (ctx0) => {
    cacheLevel = cfwe(ctx0).CACHE_LEVEL || cacheLevel
  })

  const map: Record<string, MiddleCacheHandler> = {
    "kv": CloudflareWorkerKvCacheHandler,
    "no": NoMiddleCacheHandler,
  }

  return {
    fetchDataVersion: async (ctx) => {
      return await map[cacheLevel].fetchDataVersion(ctx)
    },
    evictForNewDataVersion: async (ctx: TkContext, backstoreDataVersion: number) => {
      return await map[cacheLevel].evictForNewDataVersion(ctx, backstoreDataVersion)
    },
    getInCache: async <T,>(ctx: TkContext, k: string): Promise<T | undefined> => {
      return await map[cacheLevel].getInCache(ctx, k)
    },
    putInCache: async <T,>(ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      await map[cacheLevel].putInCache(ctx, k, v)
    },
  }
})

const CloudflareWorkerTienKouAssetFetchHandler = HT<TienKouAssetFetchHandler>()(async ({ HeavyAssetHandler, SqlDbHandler }: KD<"HeavyAssetHandler" | "SqlDbHandler">): Promise<TienKouAssetFetchHandler> => {

  const super_ = await AbstractTkSqlAssetFetchHandler({ SqlDbHandler })

  return EA(super_, {
    fetchLiveHeavyAssetBytes: async (_: { originFilePath: string }): Promise<{ asset_raw_bytes: ArrayBuffer }> => {
      throw new TkAssetNotFoundError('fetchLiveHeavyAsset not implemented').shouldLog()
    },
    fetchStaticAsset: async ({ tkCtx, locatorTopDir, locatorSubPath }: { tkCtx?: TkContext, locatorTopDir: string, locatorSubPath: string }): Promise<ArrayBuffer> => {
      return await (await (cfwe(tkCtx).ASSETS.fetch as typeof fetch)(new Request(new URL("/static/" + locatorTopDir + locatorSubPath, hc(tkCtx).req.url)))).arrayBuffer()
    },

    queryLiveAsset: async (param: QueryLiveAssetSqlCommonParam): Promise<TkAssetInfo[]> => {
      const sqlResult = await super_.queryLiveAssetSqlCommon(param)

      for (const x of sqlResult) {
        if (x.is_asset_heavy === 1) {
          x.redirect_url = await HeavyAssetHandler.makeHeavyAssetUrl(x as TkAssetInfo)
        }
      }

      // l("final sqlResult", sqlResult)
      return sqlResult
    },
  })
})


const TienKouCloudflareWorkerApp = HT<TienKouApp<HonoWithErrorHandler<Cfwe>
>>()(async ({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
  IntegratedCachePolicyHandler,
  TkCtxHandler,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkCtxHandler">) => {

  const super_ = await AbstractTkSqlLiquidHonoApp<HonoWithErrorHandler<Cfwe>>()({
    TienKouAssetFetchHandler,
    LiquidHandler,
    TienKouAssetCategoryLogicHandler,
    LiquidFilterRegisterHandlerList,
    IntegratedCachePolicyHandler,
    TkContextHlGetEHandler: {
      getTkEnvGetter: async () => honoCtx => {
        return honoCtx.env
      },
    } as TkContextHlGetTkEnvHandler<Cfwe>,
    TkCtxHandler,
  })

  return EA(super_, {
    start: async (): Promise<TkAppStartInfo<HonoWithErrorHandler<Cfwe>>> => {
      return {
        defaultExportObject: super_.honoApp
      }
    }
  })
})

const appExportedObjPromise = (async () => {

  const TkCtxHandler = await MainTkCtxHandler({})
  const TkFirstCtxProvideHandler = TkCtxHandler

  const SqlDbHandler = await TursoSqlDbHandler({
    TkFirstCtxProvideHandler,
  })

  const TkDataPersistHandler = await SqlTkDataPersistHandler({
    TkFirstCtxProvideHandler,
    SqlDbHandler,
  })

  const MiddleCacheHandler = await RoutedMiddleCacheHandler({
    TkFirstCtxProvideHandler,
    CloudflareWorkerKvCacheHandler: await CloudflareWorkerKvCacheHandler({
      TkFirstCtxProvideHandler,
    }),
    NoMiddleCacheHandler: await NoMiddleCacheHandler({
      TkFirstCtxProvideHandler,
    }),
  })

  const RuntimeCacheHandler = await MainJsRuntimeCacheHandler({
    TkFirstCtxProvideHandler,
  })

  const IntegratedCachePolicyHandler = await MultiIntanceCachePolicyHandler({
    MiddleCacheHandler,
    RuntimeCacheHandler,
    TkDataPersistHandler,
  })

  const HeavyAssetHandler = await WebRedirHeavyAssetHandler({
    TkFirstCtxProvideHandler,
  })

  const TienKouAssetFetchHandler = await CloudflareWorkerTienKouAssetFetchHandler({
    SqlDbHandler,
    HeavyAssetHandler,
  })

  const LiquidHandler = await RuntimeCachedLiquidHandler({
    TkFirstCtxProvideHandler,
    RuntimeCacheHandler,
  })
  
  const LiquidFilterRegisterHandlerList = [
    await LiquidSqlFilterRegHandler({
      SqlDbHandler,
    }),
    await LiquidTelegramMsgFilterRegHandler({}),
  ]

  const TienKouAssetCategoryLogicHandler = await TkSqlAssetCategoryLogicHandler({
    TienKouAssetFetchHandler,
  })

  const app = await TienKouCloudflareWorkerApp({
    TienKouAssetFetchHandler,
    IntegratedCachePolicyHandler,
    LiquidHandler,
    LiquidFilterRegisterHandlerList,
    TienKouAssetCategoryLogicHandler,
    TkCtxHandler,
  })
  
  const startInfo = await app.start()
  return startInfo.defaultExportObject
})()

export type ExportFetchOnlyHandler<CfweT> = { fetch: Exclude<ExportedHandler<CfweT>["fetch"], undefined> }

const proxyPromiseFetchHandlerAsSync = <CfweT,>(a: Promise<ExportFetchOnlyHandler<CfweT>>): ExportFetchOnlyHandler<CfweT> => {
  return {
    async fetch(request, env, ctx): Promise<Response> {
      const fh = await a
      return await fh.fetch(request, env, ctx)
    },
  }
}

export default proxyPromiseFetchHandlerAsSync(appExportedObjPromise)
