
import * as hono from 'hono'
import isArrayBuffer from 'is-array-buffer'
import { AnyObj, l, LazyVal, lazyValue, le, TkContext, TkError } from '../lib/common.mts'
import { HonoWithErrorHandler } from '../lib/hack.mts'
import { AbstractTkSqlLiquidHonoApp, hc, HonoEnvTypeWithTkCtx, TkContextHl, TkContextHlGetTkEnvHandler } from './honoIntegrate.mts'
import { RuntimeCachedLiquidHandler } from './liquidIntegrate.mts'
import { AbstractTkSqlAssetFetchHandler, EAH, HC, KD, MainJsRuntimeCacheHandler, MainTkCtxHandler, MiddleCacheHandler, MultiIntanceCachePolicyHandler, NoMiddleCacheHandler, QueryLiveAssetSqlCommonParam, TienKouApp, TienKouAssetFetchHandler, TkAppStartInfo, TkAssetInfo, TkAssetNotFoundError, WebRedirHeavyAssetHandler } from './serveDef.mts'
import { LiquidTelegramMsgFilterRegHandler } from './tgIntegrate'
import { LiquidSqlFilterRegHandler, SqlTkDataPersistHandler, TkSqlAssetCategoryLogicHandler } from './tkAssetCategoryLogic.mts'
import { TursoSqlDbHandler } from './tursoSql.mts'

type CfweBindings = AnyObj
type Cfwe = HonoEnvTypeWithTkCtx<CfweBindings>

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

// Middle Cache value
const kvCacheKeyPrefix = 'mc:'

const CloudflareWorkerKvCacheHandler = HC<MiddleCacheHandler>()(async ({ TkFirstCtxProvideHandler, TkEachCtxNotifyHandler }: KD<"TkFirstCtxProvideHandler" | "TkEachCtxNotifyHandler">) => {

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (_ctx0) => {

  })

  const slotCount = 2
  const decideSlotForCacheKey = (_k: string) => {
    // const kHash = quickStrHash(k)
    // const kSlot = kHash % slotCount
    // return kSlot

    // if (k.startsWith('slot1:')) {
    //   return 1
    // }
    return 0
  }

  const r: MiddleCacheHandler = {
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

      // all write could fail
      // try {
      //   l('KV.delete dataVersion')
      //   await we.KV.delete("dataVersion")
      //   // await we.KV.put("dataVersion", "")
      // } catch (e) {
      //   le('evictForNewDataVersion: KV.delete', e)
      // }

      l("KvCache:evictForNewDataVersion:new", backstoreDataVersion)

      try {
        l('KV.put dataVersion', backstoreDataVersion)
        await we.KV.put("dataVersion", backstoreDataVersion)
      } catch (e) {
        le('evictForNewDataVersion: KV.put', e)
      }

      const doDeleteMiddleCacheKvPairs = true

      if (doDeleteMiddleCacheKvPairs) {
        const cacheKvList = []
        let currCursor = undefined
        while (true) {
          // max 1000
          const currPage = await we.KV.list({ prefix: kvCacheKeyPrefix, cursor: currCursor }) as AnyObj
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
            l('KV.delete', k.name)
            await we.KV.delete(k.name)
          } catch (e) {
            le('evictForNewDataVersion: KV.delete', e)
          }
          // await we.kv.put(k.name, "undefined")
        }
      }
    },
    getInCache: async <T,>(ctx: TkContext, k: string): Promise<T | undefined> => {
      const cfkvCaches: (LazyVal<Promise<AnyObj | undefined>> & { isModified: undefined | boolean })[] = (ctx as AnyObj)['cfkvCaches']

      const kSlot = decideSlotForCacheKey(k)
      const cache = await cfkvCaches[kSlot]()
      if (cache === undefined) {
        return undefined
      }
      return cache[k]

      
    },
    putInCache: async <T,>(ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      const cfkvCaches: (LazyVal<Promise<AnyObj | undefined>> & { isModified: undefined | boolean })[] = (ctx as AnyObj)['cfkvCaches']

      const kSlot = decideSlotForCacheKey(k)
      const cacheLazy = cfkvCaches[kSlot]
      const cache = await cacheLazy()
      if (cache === undefined) {
        return
      }
      cache[k] = v
      cacheLazy.isModified = true
    },
  }

  TkEachCtxNotifyHandler.listenOnEachTkContextArrived(async (ctx) => {
    // TODO: LOCK???
    const cfkvCaches: (LazyVal<Promise<AnyObj | undefined>> & { isModified: undefined | boolean })[] = []
    ;(ctx as AnyObj)['cfkvCaches'] = cfkvCaches

    const we = cfwe(ctx)

    const dataVersion = lazyValue(() => {
      return r.fetchDataVersion(ctx)
    })

    ;(ctx as AnyObj)['dataVersionForCfkvCaches'] = dataVersion

    for (let i = 0; i < slotCount; i++) {
      const fixedI = i
      cfkvCaches.push(lazyValue(() => {
        return (async (): Promise<AnyObj | undefined> => {
          const dv = await dataVersion()
          if (dv !== undefined) {
            const cacheValStr: string | null = await we.KV.get(kvCacheKeyPrefix + dv.toString() + ":s" + fixedI.toString())
            if (cacheValStr == null) {
              return {} as AnyObj
            } else {
              return JSON.parse(cacheValStr) as AnyObj
            }
          } else {
            return undefined
          }
        })()
      }) as (LazyVal<Promise<AnyObj | undefined>> & { isModified: undefined | boolean }))
    }

  })

  TkEachCtxNotifyHandler.listenOnEachTkContextEnded(async (ctx) => {
    // TODO: LOCK???
    const cfkvCaches: (LazyVal<Promise<AnyObj | undefined>> & { isModified: undefined | boolean })[] = (ctx as AnyObj)['cfkvCaches']
    const we = cfwe(ctx)

    const dataVersion = (ctx as AnyObj)['dataVersionForCfkvCaches'] as LazyVal<Promise<number | undefined>>

    for (let i = 0; i < slotCount; i++) {
      const fixedI = i
      const currCacheLazy = cfkvCaches[i]
      if (currCacheLazy.isCalled && currCacheLazy.isModified) {
        const dv = await dataVersion()
        if (dv !== undefined) {
          const currCacheUnlazy = await currCacheLazy()
          if (currCacheUnlazy !== undefined) {
            const k = kvCacheKeyPrefix + dv.toString() + ":s" + fixedI.toString()
            l('KV.put', k)
            await we.KV.put(k, JSON.stringify(currCacheUnlazy, function (k, v) {
              if (isArrayBuffer(v)) {
                return {
                  type: 'Buffer',
                  data: [...new Uint8Array(v)]
                }
              }
              return v
            }))
          }
        }
      }
    }
  })

  return r
})

const RoutedMiddleCacheHandler = HC<MiddleCacheHandler>()(async ({ TkFirstCtxProvideHandler, CloudflareWorkerKvCacheHandler, NoMiddleCacheHandler }: KD<"TkFirstCtxProvideHandler", { CloudflareWorkerKvCacheHandler: MiddleCacheHandler, NoMiddleCacheHandler: MiddleCacheHandler }>) => {

  let cacheLevel = "kv"
  // let cacheLevel = "no"

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

const CloudflareWorkerTienKouAssetFetchHandler = HC<TienKouAssetFetchHandler>()(async ({ HeavyAssetHandler, SqlDbHandler }: KD<"HeavyAssetHandler" | "SqlDbHandler">): Promise<TienKouAssetFetchHandler> => {

  const super_ = await AbstractTkSqlAssetFetchHandler({ SqlDbHandler })

  return EAH(super_, {
    fetchLiveHeavyAssetBytes: async (_: { originFilePath: string }): Promise<{ asset_raw_bytes: ArrayBuffer | Buffer<ArrayBufferLike> }> => {
      throw new TkAssetNotFoundError('fetchLiveHeavyAsset not implemented').shouldLog()
    },
    fetchStaticAsset: async ({ tkCtx, locatorTopDir, locatorSubPath }: { tkCtx?: TkContext, locatorTopDir: string, locatorSubPath: string }): Promise<ArrayBuffer | Buffer<ArrayBufferLike>> => {
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


const TienKouCloudflareWorkerApp = HC<TienKouApp<HonoWithErrorHandler<Cfwe>
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

  return EAH(super_, {
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
  const TkEachCtxNotifyHandler = TkCtxHandler

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
      TkEachCtxNotifyHandler,
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

const cfCache = caches.default

// https://developers.cloudflare.com/cache/how-to/cache-keys/
// const cachedReqHeaders = ['Origin', 'x-http-method-override', 'x-http-method', 'x-method-override', 'x-forwarded-host', 'x-host', 'x-original-url', 'x-rewrite-url', 'forwarded']
const cachedReqHeaders: string[] = []

function cfMakeReqCacheKey(request: Request<unknown, IncomingRequestCfProperties<unknown>>) {
  const requestCacheKeyHeaders = {} as Record<string, string>
  for (const reqCacheHeader of cachedReqHeaders) {
    const hValue = request.headers.get(reqCacheHeader)
    if (hValue) {
      requestCacheKeyHeaders[reqCacheHeader] = hValue
    }
  }

  const requestCacheKey = new Request(request.url, {
    method: request.method,
    headers: requestCacheKeyHeaders,
  })
  return requestCacheKey
}


const proxyPromiseFetchHandlerAsSync = <CfweT,>(a: Promise<ExportFetchOnlyHandler<CfweT>>): ExportFetchOnlyHandler<CfweT> => {
  return {
    async fetch(request, env, ctx): Promise<Response> {
      let requestToPass = request
      const requestCacheKey = cfMakeReqCacheKey(request)
      const tryMatch = await cfCache.match(requestCacheKey, { ignoreMethod: false })
      let eTagHeaderManipulated = false
      if (tryMatch) {
        l('cf req cache hit', request.url)
        const cachedETag = tryMatch.headers.get('ETag')
        if (cachedETag) {
          l('cf req cache hit has valid etag', request.url, cachedETag)
          if (!request.headers.get('If-None-Match')) {
            l('cf orig req has no If-None-Match, add it', request.url, cachedETag)
            eTagHeaderManipulated = true
            requestToPass = new Request(request.url, request)
            requestToPass.headers.set('If-None-Match', cachedETag)
          }
        }
      }
      const fh = await a
      const resp = await fh.fetch(requestToPass, env, ctx)
      if (resp.status == 304 && tryMatch && eTagHeaderManipulated) {
        l('cf etag test resp is 304, return cached resp')
        return tryMatch
      }
      if (resp.status == 200) {
        l('cf put resp in cache')
        const respClone = resp.clone()
        const respToCache = new Response(respClone.body, respClone)
        respToCache.headers.delete('Cache-Control')
        respToCache.headers.delete('Expires')
        respToCache.headers.delete('Last-Modified')
        await cfCache.put(requestCacheKey, respToCache)
      } else {
        l('resp.status', resp.status)
      }
      return resp
    },
  }
}

export default proxyPromiseFetchHandlerAsSync(appExportedObjPromise)

