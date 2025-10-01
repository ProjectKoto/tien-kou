
import AsyncLock from 'async-lock'
import Emittery from "emittery"
import { ContentfulStatusCode } from "hono/utils/http-status"
import * as liquid from "liquidjs"
import { Liquid, LiquidOptions } from "liquidjs"
import { Ao, AnyObj, l, makeConcatenatablePath, makeConcatenatablePathList, sqlGlobPatternEscape, TkError, TkErrorHttpAware, TkErrorHttpAwareOptions, TkContext, truncateStrByLen } from "../lib/common.mts"
import { TagClass, TagImplOptions } from 'liquidjs/dist/template'

type KnownHandlerTypesMap0 = {
  MiddleCacheHandler: MiddleCacheHandler,
  RuntimeCacheHandler: RuntimeCacheHandler,
  TkFirstCtxProvideHandler: TkFirstCtxProvideHandler,
  TkEachCtxNotifyHandler: TkEachCtxNotifyHandler,
  TkCtxHandler: TkCtxHandler,
  SqlDbHandler: SqlDbHandler,
  TkDataPersistHandler: TkDataPersistHandler,
  IntegratedCachePolicyHandler: IntegratedCachePolicyHandler,
  LiquidHandler: LiquidHandler,
  HeavyAssetHandler: HeavyAssetHandler,
  TienKouAssetFetchHandler: TienKouAssetFetchHandler,
  TienKouAssetCategoryLogicHandler: TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandler: LiquidFilterRegisterHandler,
  TkProvideCtxFromNothingHandler: TkProvideCtxFromNothingHandler,
}

type KnownHandlerTypesMap = KnownHandlerTypesMap0 & {
  [K in keyof KnownHandlerTypesMap0 as `${(K & string)}List`]: Array<KnownHandlerTypesMap0[K]>
}

// Known-handler-type Dependencies object
export type KD<T extends keyof KnownHandlerTypesMap, OtherParam extends AnyObj = AnyObj> = Pick<KnownHandlerTypesMap, T> & OtherParam
export type KD2<T extends Partial<{ [K in keyof KnownHandlerTypesMap]: unknown }>, OtherParam extends AnyObj> = Pick<KnownHandlerTypesMap, keyof T & keyof KnownHandlerTypesMap> & OtherParam
export type KD3<T extends (keyof KnownHandlerTypesMap)[]> = { [P in keyof T as T[P] & string]: KnownHandlerTypesMap[T[P] & (keyof KnownHandlerTypesMap)]; }

export type KDNoDep = KD<never>


// Handler Constructor Types
type HandlerConstructorCurryFuncReturnTypeConforming<ReturnTypeShouldSatisfy> = {

  // <RT extends ReturnTypeShouldSatisfy, T extends keyof KnownHandlerTypesMap, OtherParam extends AnyObj>(f: (param: KD<T, OtherParam>) => Promise<RT>): (param: KD<T, OtherParam>) => Promise<ReturnTypeShouldSatisfy>

  <T extends keyof KnownHandlerTypesMap, OtherParam extends AnyObj>(f: (param: KD<T, OtherParam>) => Promise<ReturnTypeShouldSatisfy>): (param: KD<T, OtherParam>) => Promise<ReturnTypeShouldSatisfy>

}

type HandlerConstructorCurryFuncReturnTypeAsIs<ReturnTypeShouldSatisfy> = {

  <RT extends ReturnTypeShouldSatisfy, T extends keyof KnownHandlerTypesMap, OtherParam extends AnyObj>(f: (param: KD<T, OtherParam>) => Promise<RT>): (param: KD<T, OtherParam>) => Promise<RT>

  // <RT extends ReturnTypeShouldSatisfy>(f: () => Promise<RT>): () => Promise<RT>
}

// Handler Constructor
export const HC = <ReturnTypeShouldSatisfy,>() => ((f: unknown) => {
  return f
}) as HandlerConstructorCurryFuncReturnTypeConforming<ReturnTypeShouldSatisfy>

// Handler Constructor keep-Original-return-type
export const HCO = <ReturnTypeShouldSatisfy,>() => ((f: unknown) => {
  return f
}) as HandlerConstructorCurryFuncReturnTypeAsIs<ReturnTypeShouldSatisfy>

export type PartialWithNull<T> = {
  [P in keyof T]: T[P] | null;
}

// Abstract Handler Constructor
export const AHC = <ReturnTypeShouldSatisfy,>() => HCO<PartialWithNull<ReturnTypeShouldSatisfy>>()

// export const AHT = <ReturnTypeShouldSatisfy,>() => <T extends keyof KnownHandlerTypesMap, OtherParam extends AnyObj>(f: (param: KD<T, OtherParam>) => Promise<PartialWithNull<ReturnTypeShouldSatisfy>>): (param: KD<T, OtherParam>) => Promise<PartialWithNull<ReturnTypeShouldSatisfy>> => {
//   return f
// }

// canNot-be-Promise This
export const NPT = <T,>(this_: T | PromiseLike<T>) => this_ as T

// Extends Abstract Handler
export type Unpromise<T> = T extends PromiseLike<infer _R> ? never : T
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const EAH = <SuperHandlerType extends Record<string, any>, DerivedType extends Record<string, any>>(super_: SuperHandlerType, subImpl: Omit<Unpromise<DerivedType>, { [MethodName in keyof SuperHandlerType]: SuperHandlerType[MethodName] extends null ? never: MethodName }[keyof SuperHandlerType]>) => {
  return Object.assign(super_, subImpl) as Unpromise<DerivedType>
}

// Abstract Extends Abstract Handler
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const AEAH = <SuperHandlerType extends Record<string, any>, SubImpl extends Record<string, any>>(super_: SuperHandlerType, subImpl: SubImpl) => {
  return Object.assign(super_, subImpl) as Omit<SuperHandlerType, keyof SubImpl> & SubImpl
}


export interface TkFirstCtxProvideHandler {

  get isFirstCtxArrived(): boolean

  get firstCtx(): TkContext | undefined

  listenOnFirstCtxForInit(l: (ctx0: TkContext) => Promise<void>): void

  listenOnFirstCtxFullyReady(l: (ctx0: TkContext) => Promise<void>): void

  waitForFirstCtxInit(): Promise<TkContext>

}

export interface TkEachCtxNotifyHandler {

  listenOnEachTkContextArrived(l: (ctx: TkContext) => Promise<void>): void
  listenOnEachTkContextEnded(l: (ctx: TkContext) => Promise<void>): void

}

export interface TkCtxHandler extends TkFirstCtxProvideHandler, TkEachCtxNotifyHandler {

  triggerProcessTkCtx(ctx: TkContext): Promise<void>
  triggerProcessTkCtxOnEnd(ctx: TkContext): Promise<void>

}

export interface TkProvideCtxFromNothingHandler {

  fetchTkCtxFromNothing(): Promise<TkContext>

}

export type SqlArgValue = null | string | number | bigint | ArrayBuffer | boolean | Uint8Array | Date
export interface SqlDbHandler {
  
  sql({ tkCtx, sql, args }: { tkCtx?: TkContext, sql: string, args: SqlArgValue[] }): Promise<Ao[]>

}

export interface CacheHandler {
  getInCache<T>(ctx: TkContext, k: string): Promise<T | undefined>

  putInCache<T>(ctx: TkContext, k: string, v: T | undefined): Promise<void>
}

/*
 * multiple runtime cache ->
 * multiple runtime cache -> one middle cache -> data persist
 * multiple runtime cache ->
 * 
 * so middle cache evict logic can't evict runtime cache; 
 * runtime cache should check every time (on EVERY request) middle cache version to know whether to evict self.
 * why runtime cache has to exist? because liquid's default cache mechanism is js runtime local. it can't live on middle cache.
 * 
 * i.e. runtime cache is "automatic/active-evict" cache.
 */
export interface RuntimeCacheHandler extends CacheHandler {
  
  listenOnEvict(l: () => Promise<void>): void,

  // checkMiddleCacheAndEvict(ctx: TkContext): Promise<void>
  checkIfShouldEvict(backstoreDataVersion: number | undefined): Promise<boolean>


  // checkAndDoEvict(ctx: TkContext, backstoreDataVersion: number | undefined): Promise<boolean>
  checkAndDoEvict(ctx: TkContext, backstoreDataVersion: number): Promise<boolean>

  // note: only evicts for current runtime (Node.JS/Cloudflare Worker/...) process. May exists other instances that don't get evicted.
  evictForNewDataVersion(ctx: TkContext, backstoreDataVersion: number | undefined): Promise<void>

  fetchDataVersion(ctx: TkContext): Promise<number | undefined>

}


const AbstractRuntimeCacheHandler = AHC<RuntimeCacheHandler>()(async (_: KD<never>) => {
  return {
    listenOnEvict: null,
    checkIfShouldEvict: null,
    checkAndDoEvict: async function(ctx, backstoreDataVersion) {
      const npThis = NPT(this)
      const shouldEvict = await npThis.checkIfShouldEvict!(backstoreDataVersion)
      if (shouldEvict) {
        await npThis.evictForNewDataVersion!(ctx, backstoreDataVersion)
      }
      return shouldEvict
    },
    evictForNewDataVersion: null,
    getInCache: null,
    putInCache: null,
    fetchDataVersion: null,
  }
})

// ~~~all MiddleCacheHandler are "manual/passive-evict" cache; they need instant manual evict by calling evictForNewDataVersion on version update to ensure they are correct.~~~
export interface MiddleCacheHandler extends CacheHandler {

  fetchDataVersion(ctx: TkContext): Promise<number | undefined>

  evictForNewDataVersion(ctx: TkContext, backstoreDataVersion: number): Promise<void>

}


export interface TkDataPersistHandler {
  
  fetchDataVersion(ctx: TkContext): Promise<number>

  fetchKv<T>(ctx: TkContext, k: string): Promise<T | undefined>

  storeKv<T>(ctx: TkContext, k: string, v: T | undefined): Promise<void>

}

export interface IntegratedCachePolicyHandler {

  listenOnRuntimeCacheEvict(l: () => Promise<void>): void,

  fetchDataVersion(ctx: TkContext): Promise<number>,

  checkAndDoEvictRuntimeCache(ctx: TkContext): Promise<boolean>

  evictForNewDataVersion(ctx: TkContext): Promise<void>,

  getCachedVal<T>(ctx: TkContext, k: string, getIfMissing: () => Promise<T>): Promise<T>

}

export interface LiquidHandler {

  get isLiquidReady(): boolean

  get liquid(): Liquid | undefined

  liquidReadyPromise: Promise<Liquid>

  listenOnLiquidPreCreate: (l: (ctx0: TkContext, liquidOptions: LiquidOptions) => Promise<void>) => void

  customizeLiquidOpt: (l: (ctx0: TkContext, liquidOptions: LiquidOptions) => Promise<void>) => void

  listenOnLiquidPostCreate: (l: (liquid: Liquid) => Promise<void>) => void
  
  registerFilterPostCreate(name: string, filter: liquid.FilterImplOptions): void

  registerTagPostCreate(name: string, filter: TagClass | TagImplOptions): void

  initBaseLiquidOptions(inputLiquidOptions: LiquidOptions): Promise<void>

}

export interface FetchLocatableContentOpt {
  shouldReadOpenAssets?: boolean
}

export interface FetchBackstageAssetOpt {
  shouldFetchLiveHeavyAssetBytes?: boolean
  liveLightOnly?: boolean
}

export interface FetchGenericLocatableAssetOpt {
  shouldFetchLiveHeavyAssetBytes?: boolean
}

// See DEVNOTE.md
export interface TienKouAssetCategoryLogicHandler {

  // throws: TkAssetNotDedicatedError TkAssetNotFoundError
  fetchLocatableContent(subPath: string, extraOpt: FetchLocatableContentOpt): Promise<TkAssetInfo>

  // throws: TkAssetNotFoundError
  fetchBackstageAsset(subPath: string, extraOpt: FetchBackstageAssetOpt): Promise<TkAssetInfo>

  // throws: TkAssetNotFoundError
  fetchGenericLocatableAsset(subPath: string, extraOpt: FetchGenericLocatableAssetOpt): Promise<TkAssetInfo>

}

export interface LiquidFilterRegisterHandler {
  
  doRegister(registerFilterPostCreate: (name: string, filter: liquid.FilterImplOptions) => void) : void

}

export interface HeavyAssetHandler {
  
  makeHeavyAssetUrl(asset: TkAssetInfo): Promise<string>

}

export type TkAssetInfo = Ao

export interface TienKouAssetFetchHandler {
  fetchLiveHeavyAssetBytes(_: { tkCtx?: TkContext, originFilePath: string }): Promise<{ asset_raw_bytes: ArrayBuffer | Buffer<ArrayBufferLike> }>
  fetchStaticAsset(_: { tkCtx?: TkContext, locatorTopDir: string, locatorSubPath: string }): Promise<ArrayBuffer | Buffer<ArrayBufferLike>>
  queryLiveAsset<P extends QueryLiveAssetCommonParam>(param: P): Promise<TkAssetInfo[]>
}

export interface TkAppStartInfo<EO> {
  defaultExportObject: EO
  waitForAppEndPromise?: Promise<void>
}

export interface TienKouApp<EO> {
  start(): Promise<TkAppStartInfo<EO>>
}

const tkSubErrorOverwriteDefaultStatus = (args: unknown[], status: ContentfulStatusCode) => {
  let options: TkErrorHttpAwareOptions | undefined = undefined
  for (let i = 0; i < args.length; i++) {
    const a = args[i]
    if (typeof a === 'object') {
      options = {
        ...a
      }
      args[i] = options
    }
  }
  if (options === undefined) {
    options = {}
    args.push(options)
  }
  options.defaultStatus = status
}

export class TkAssetNotDedicatedError extends TkErrorHttpAware {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 404)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export class TkAssetNotFoundError extends TkErrorHttpAware {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 404)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export class TkAssetIsDirectoryError extends TkErrorHttpAware {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 400)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export class TkAssetIsHeavyError extends TkErrorHttpAware {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 503)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export class TkReqError extends TkErrorHttpAware {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 400)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export class TkInvalidReqError extends TkReqError {
  constructor(...args: unknown[]) {
    tkSubErrorOverwriteDefaultStatus(args, 400)
    super(...(args as [string, ContentfulStatusCode, AnyObj]))
  }
}

export const MainTkCtxHandler = HC<TkCtxHandler>()(async (_: KD<never>) => {
  
  let firstCtx: TkContext | undefined = undefined

  let initFirstCtxSingleFlightPromise: Promise<void> | undefined = undefined

  const firstCtxArrivedEvent = new Emittery()
  const firstCtxFullyReadyEvent = new Emittery()
  const eachCtxArrivedEvent = new Emittery()
  const eachCtxEndedEvent = new Emittery()

  let firstCtxInitedPromiseResolver: ((value: TkContext | PromiseLike<TkContext>) => void) | undefined = undefined
  const firstCtxInitedPromise = new Promise<TkContext>(r => {
    firstCtxInitedPromiseResolver = r
  })

  const checkAndInitFirstCtx = async (ctx: TkContext): Promise<void> => {
    // TODO: wait for runtime layer initialized?
    if (!initFirstCtxSingleFlightPromise) {
      initFirstCtxSingleFlightPromise = r.initByFirstCtx(ctx)
    }
    await initFirstCtxSingleFlightPromise
  }

  const r = {
    get isFirstCtxArrived(): boolean {
      return firstCtx !== undefined
    },
    get firstCtx(): TkContext | undefined {
      return firstCtx
    },
    listenOnFirstCtxForInit: (l: (ctx0: TkContext) => Promise<void>): void => {
      firstCtxArrivedEvent.on('firstCtxArrived', async () => {
        await l(firstCtx!)
      })
    },
    listenOnFirstCtxFullyReady: (l: (ctx0: TkContext) => Promise<void>): void => {
      firstCtxFullyReadyEvent.on('firstCtxFullyReady', async () => {
        await l(firstCtx!)
      })
    },
    waitForFirstCtxInit: async (): Promise<TkContext> => {
      return await firstCtxInitedPromise
    },
    initByFirstCtx: async (ctx0: TkContext): Promise<void> => {
      firstCtx = ctx0
      await firstCtxArrivedEvent.emitSerial('firstCtxArrived')
      if (firstCtxInitedPromiseResolver) {
        firstCtxInitedPromiseResolver(firstCtx!)
      }
      await firstCtxFullyReadyEvent.emitSerial('firstCtxFullyReady')
    },
    
    listenOnEachTkContextArrived: (l: (ctx: TkContext) => Promise<void>): void => {
      eachCtxArrivedEvent.on('eachCtxArrived', async (ctx) => {
        await l(ctx)
      })
    },
    listenOnEachTkContextEnded: (l: (ctx: TkContext) => Promise<void>): void => {
      eachCtxEndedEvent.on('eachCtxEnded', async (ctx) => {
        await l(ctx)
      })
    },

    triggerProcessTkCtx: async (ctx: TkContext): Promise<void> => {
      await checkAndInitFirstCtx(ctx)
      await eachCtxArrivedEvent.emitSerial('eachCtxArrived', ctx)
    },

    triggerProcessTkCtxOnEnd: async (ctx: TkContext): Promise<void> => {
      await eachCtxEndedEvent.emitSerial('eachCtxEnded', ctx)
    },
    
  }

  return r
})

/*
 * multiple runtime cache ->
 * multiple runtime cache -> one middle cache -> data persist
 * multiple runtime cache ->
 * 
 */

export const MainJsRuntimeCacheHandler = HC<RuntimeCacheHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  const super_ = await AbstractRuntimeCacheHandler({})

  const evictEvent = new Emittery()

  let cacheMap = {} as AnyObj

  let jsRuntimeCacheVersion: number | undefined = undefined

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (_ctx0: TkContext) => {

  })

  const lock = new AsyncLock()

  return EAH(super_, {
    listenOnEvict: (listener: () => Promise<void>): void => {
      evictEvent.on('evict', async () => {
        await listener()
      })
    },
    fetchDataVersion: async function (_ctx: TkContext): Promise<number | undefined> {
      return jsRuntimeCacheVersion
    },
    checkIfShouldEvict: async (backstoreDataVersion: number | undefined): Promise<boolean> => {
      let result: boolean | undefined = undefined
      await lock.acquire('1', async () => { 
        result = jsRuntimeCacheVersion === undefined || backstoreDataVersion === undefined || backstoreDataVersion !== jsRuntimeCacheVersion
      })
      return result!
    },
    // checkMiddleCacheAndEvict: async (ctx: TkContext) => {
    //   const newSiteVersion = await MiddleCacheHandler.fetchDataVersion(ctx)
    //   if (newSiteVersion !== jsRuntimeCacheVersion) {
    //     jsRuntimeCacheVersion = newSiteVersion
    //     await evictEvent.emitSerial('evict')
    //   }
    // },
    evictForNewDataVersion: async (ctx: TkContext, backstoreDataVersion: number) => {
      await lock.acquire('1', async () => { 
        jsRuntimeCacheVersion = backstoreDataVersion 
        cacheMap = {} as AnyObj
        // note: only evicts for current runtime (Node.JS/Cloudflare Worker/...) process. May exists other instances that don't get evicted.
        await evictEvent.emitSerial('evict')
      })
    },
    getInCache: async <T,>(ctx: TkContext, k: string): Promise<T | undefined> => {
      return cacheMap[k]
    },
    putInCache: async <T,>(ctx: TkContext, k: string, v: T | undefined): Promise<void> => {
      cacheMap[k] = v
    },
  })
})


export const NoMiddleCacheHandler = HC<MiddleCacheHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler" >) => {

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (_ctx0) => {

  })

  return {
    fetchDataVersion: async (_ctx) => {
      return undefined
    },
    evictForNewDataVersion: async (_ctx: TkContext) => {
      // no-op
    },
    getInCache: async <T,>(_ctx: TkContext, _k: string): Promise<T | undefined> => {
      return undefined
    },
    putInCache: async <T,>(_ctx: TkContext, _k: string, _v: T | undefined): Promise<void> => {
      // no-op
    },
  }
})

export const getAndFillCachedValOneLayer = async <T,> (ctx: TkContext, cacheLayer: CacheHandler, k: string, getIfMissing: () => Promise<T>): Promise<T> => {
  let result: T | undefined = undefined
  result = await cacheLayer.getInCache(ctx, k)
  if (result !== undefined && result != null) {
    return result
  }
  try {
    result = await getIfMissing()
    return result
  } finally {
    if (result !== undefined && result != null) {
      await cacheLayer.putInCache(ctx, k, result)
    }
  }
}

// checks MiddleCache each time to ensure RuntimeCache is evicted if outdated.
export const MultiIntanceCachePolicyHandler = HC<IntegratedCachePolicyHandler>()(async ({
  RuntimeCacheHandler,
  MiddleCacheHandler,
  TkDataPersistHandler,
}: KD<"RuntimeCacheHandler" | "MiddleCacheHandler" | "TkDataPersistHandler">) => {
  return {
    listenOnRuntimeCacheEvict: (l: () => Promise<void>): void => {
      RuntimeCacheHandler.listenOnEvict(l)
    },

    fetchDataVersion: async function(ctx: TkContext): Promise<number> {
      // TODO: lock
      let cacheDataVersion = await RuntimeCacheHandler.fetchDataVersion(ctx)
      if (cacheDataVersion === undefined) {
        cacheDataVersion = await MiddleCacheHandler.fetchDataVersion(ctx)
      }
      if (cacheDataVersion === undefined) {
        cacheDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
      }
      return cacheDataVersion
    },

    checkAndDoEvictRuntimeCache: async (ctx) => {
      // TODO: lock
      let cacheDataVersion = await MiddleCacheHandler.fetchDataVersion(ctx)
      if (cacheDataVersion === undefined) {
        cacheDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
        await MiddleCacheHandler.evictForNewDataVersion(ctx, cacheDataVersion)
      }
      return await RuntimeCacheHandler.checkAndDoEvict(ctx, cacheDataVersion)
    },

    evictForNewDataVersion: async (ctx: TkContext): Promise<void> => {
      const newDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
      await MiddleCacheHandler.evictForNewDataVersion(ctx, newDataVersion)
      await RuntimeCacheHandler.evictForNewDataVersion(ctx, newDataVersion)
      // await RuntimeCacheHandler.evictForNewDataVersion(ctx, undefined)
      // await RuntimeCacheHandler.evictForNewDataVersion(ctx, await MiddleCacheHandler.fetchDataVersion(ctx))
    },

    getCachedVal: async <T,> (ctx: TkContext, k: string, getIfMissing: () => Promise<T>): Promise<T> => {
      return await getAndFillCachedValOneLayer(ctx, RuntimeCacheHandler, k, async () => {
        return await getAndFillCachedValOneLayer(ctx, MiddleCacheHandler, k, getIfMissing)
      })
    },
  }
})

// won't check MiddleCache each time, because evictForNewDataVersion already evicts both cache, and nothing need to be synced because it is single instance.
export const SingleInstanceCachePolicyHandler = HC<IntegratedCachePolicyHandler>()(async ({
  RuntimeCacheHandler,
  MiddleCacheHandler,
  TkDataPersistHandler,
}: KD<"RuntimeCacheHandler" | "MiddleCacheHandler" | "TkDataPersistHandler">) => {
  return {
    listenOnRuntimeCacheEvict: (l: () => Promise<void>): void => {
      RuntimeCacheHandler.listenOnEvict(l)
    },

    fetchDataVersion: async function(ctx: TkContext): Promise<number> {
      // TODO: lock
      let cacheDataVersion = await RuntimeCacheHandler.fetchDataVersion(ctx)
      if (cacheDataVersion === undefined) {
        cacheDataVersion = await MiddleCacheHandler.fetchDataVersion(ctx)
      }
      if (cacheDataVersion === undefined) {
        cacheDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
      }
      return cacheDataVersion
    },

    checkAndDoEvictRuntimeCache: async (_ctx) => {
      return false
    },

    evictForNewDataVersion: async (ctx: TkContext): Promise<void> => {
      const newDataVersion = await TkDataPersistHandler.fetchDataVersion(ctx)
      await MiddleCacheHandler.evictForNewDataVersion(ctx, newDataVersion)
      await RuntimeCacheHandler.evictForNewDataVersion(ctx, newDataVersion)
      // await RuntimeCacheHandler.evictForNewDataVersion(ctx, undefined)
      // await RuntimeCacheHandler.evictForNewDataVersion(ctx, await MiddleCacheHandler.fetchDataVersion(ctx))
    },

    getCachedVal: async <T,> (ctx: TkContext, k: string, getIfMissing: () => Promise<T>): Promise<T> => {
      return await getAndFillCachedValOneLayer(ctx, RuntimeCacheHandler, k, async () => {
        return await getAndFillCachedValOneLayer(ctx, MiddleCacheHandler, k, getIfMissing)
      })
    },
  }
})

export const WebRedirHeavyAssetHandler = HC<HeavyAssetHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  let heavyAssetPrefix: string

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (ctx0: TkContext) => {
    heavyAssetPrefix = ctx0.e.HEAVY_ASSET_WEB_REDIR_URL_PREFIX || "https://raw.githubusercontent.com/Your_username/Repo_name/Your_branch/"
  })

  return {
    makeHeavyAssetUrl: async (asset: TkAssetInfo): Promise<string> => {
      return heavyAssetPrefix + asset.origin_file_path
    },
  }
})

export const StubHeavyAssetHandler = HC<HeavyAssetHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async (_ctx0: TkContext) => {
  })

  return {
    makeHeavyAssetUrl: async (_asset: TkAssetInfo): Promise<string> => {
      throw new TkErrorHttpAware("makeHeavyAssetUrl not implemented")
    },
  }
})

export interface QueryLiveAssetCommonParam {
  tkCtx?: TkContext,
  locatorTopDirs: string[],
  locatorSubPaths?: string[],
  locatorSubAncestors?: string[],
  locatorSubParents?: string[],
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldIncludeDirectories?: boolean | Boolean,
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldIncludeDerivingParent?: boolean | Boolean,
  extensions?: string[],
  orderBy?: "condTimeDesc" | "condTimeAsc" | "condLocatorAsc" | "condLocatorDesc" | "timeDesc" | "timeAsc" | "locatorAsc" | "locatorDesc" | string,
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldFetchRawBytes?: boolean | Boolean,
  pageNum?: number,
  pageSize?: number,
}

export interface QueryLiveAssetSqlCommonParam extends QueryLiveAssetCommonParam {
  customSql?: string,
  customSqlArgs?: SqlArgValue[],
}

export const AbstractTkSqlAssetFetchHandler = AHC<TienKouAssetFetchHandler>()(async ({ SqlDbHandler } : KD<"SqlDbHandler">) => {
  return {

    fetchLiveHeavyAssetBytes: null,

    fetchStaticAsset: null,

    queryLiveAsset: null,

    queryLiveAssetSqlCommon: async (
      {
        tkCtx,
        locatorTopDirs,
        locatorSubPaths,
        locatorSubAncestors,
        locatorSubParents,
        shouldIncludeDirectories,
        shouldIncludeDerivingParent,
        extensions,
        customSql,
        customSqlArgs,
        orderBy,
        shouldFetchRawBytes,
        pageNum,
        pageSize,
      }: QueryLiveAssetSqlCommonParam) => {
      // l("queryLiveAssetSqlCommon", {
      //   tkCtx,
      //   locatorTopDirs,
      //   locatorSubPaths,
      //   locatorSubAncestors,
      //   locatorSubParents,
      //   shouldIncludeDirectories,
      //   shouldIncludeDerivingParent,
      //   extensions,
      //   customSql,
      //   customSqlArgs,
      //   orderBy,
      //   shouldFetchRawBytes,
      //   pageNum,
      //   pageSize,
      // })

      locatorTopDirs = makeConcatenatablePathList(locatorTopDirs)

      const sqlFragmentList: string[] = []
      const sqlArgs: SqlArgValue[] = []

      let conflictArgsCount = 0
      let conflictArgsDesc = ''
      if (locatorSubPaths) {
        conflictArgsDesc += 'locatorSubPaths, '
        conflictArgsCount++
      }
      if (locatorSubAncestors) {
        conflictArgsDesc += 'locatorSubAncestors, '
        conflictArgsCount++
      }
      if (locatorSubParents) {
        conflictArgsDesc += 'locatorSubParents, '
        conflictArgsCount++
      }

      if (conflictArgsCount > 1) {
        throw new TkError(`more than one arg not null: ${conflictArgsDesc}`)
      }

      sqlFragmentList.push(`
      SELECT * FROM (
        SELECT *, MIN(top_dir_index) FROM (
      `)

      const multipleConditionsSqlFragments: string[] = []

      for (let i = 0; i < locatorTopDirs.length; i++) {
        if (locatorSubPaths) {
          if (locatorSubPaths.length === 0) {
            locatorSubPaths = ['']
          }
          for (let j = 0; j < locatorSubPaths.length; j++) {
            const currSubPath = locatorSubPaths[j]

            multipleConditionsSqlFragments.push(`
              SELECT * FROM
              (SELECT ${i} AS top_dir_index, ${j} AS second_cond_index) idx INNER JOIN (
                SELECT
                  FALSE AS is_directory,
                  asset_raw_path AS asset_raw_path,
                  asset_locator AS asset_locator,
                  asset_type AS asset_type,
                  asset_store_tag AS asset_store_tag,
                  asset_size AS asset_size,
                  is_asset_heavy AS is_asset_heavy,
                  has_derived_children AS has_derived_children,
                  deriving_parent_id AS deriving_parent_id,
                  ${shouldFetchRawBytes ? 'asset_raw_bytes' : 'NULL'} AS asset_raw_bytes,
                  origin_file_path AS origin_file_path,
                  origin_file_extension AS origin_file_extension,
                  metadata AS metadata,
                  links AS links,
                  publish_time_by_metadata AS publish_time_by_metadata,
                  is_deleted_by_hoard AS is_deleted_by_hoard,
                  update_time_by_hoard AS update_time_by_hoard
                FROM files WHERE
                  asset_locator = ?
                AND (is_deleted_by_hoard IS NULL OR is_deleted_by_hoard != 1)
                ORDER BY update_time_by_hoard DESC
                LIMIT 1
              ) d
            `)

            sqlArgs.push(
              locatorTopDirs[i] + currSubPath,
            )

            multipleConditionsSqlFragments.push(`
              SELECT * FROM
              (SELECT ${i} AS top_dir_index, ${j} AS second_cond_index) idx INNER JOIN (
                SELECT
                  TRUE AS is_directory,
                  NULL AS asset_raw_path,
                  ? AS asset_locator,
                  'directory' AS asset_type,
                  NULL AS asset_store_tag,
                  asset_size AS asset_size,
                  FALSE AS is_asset_heavy,
                  NULL AS has_derived_children,
                  NULL AS deriving_parent_id,
                  NULL AS asset_raw_bytes,
                  NULL AS origin_file_path,
                  NULL AS origin_file_extension,
                  NULL AS metadata,
                  NULL AS links,
                  NULL AS publish_time_by_metadata,
                  is_deleted_by_hoard AS is_deleted_by_hoard,
                  update_time_by_hoard AS update_time_by_hoard
                FROM files WHERE
                  asset_locator GLOB ?
                AND (is_deleted_by_hoard IS NULL OR is_deleted_by_hoard != 1)
                ORDER BY update_time_by_hoard DESC
                LIMIT 1
              ) d
            `)

            sqlArgs.push(
              locatorTopDirs[i] + currSubPath,
              sqlGlobPatternEscape(locatorTopDirs[i] + makeConcatenatablePath(currSubPath)) + '*',
            )
          }
        }
        
        if (locatorSubAncestors) {
          locatorSubAncestors = makeConcatenatablePathList(locatorSubAncestors)
          for (let j = 0; j < locatorSubAncestors.length; j++) {
            multipleConditionsSqlFragments.push(`
              SELECT * FROM
              (SELECT ${i} AS top_dir_index, ${j} AS second_cond_index) idx INNER JOIN (
                SELECT
                  FALSE AS is_directory,
                  asset_raw_path AS asset_raw_path,
                  asset_locator AS asset_locator,
                  asset_type AS asset_type,
                  asset_store_tag AS asset_store_tag,
                  asset_size AS asset_size,
                  is_asset_heavy AS is_asset_heavy,
                  has_derived_children AS has_derived_children,
                  deriving_parent_id AS deriving_parent_id,
                  ${shouldFetchRawBytes ? 'asset_raw_bytes' : 'NULL'} AS asset_raw_bytes,
                  origin_file_path AS origin_file_path,
                  origin_file_extension AS origin_file_extension,
                  metadata AS metadata,
                  links AS links,
                  publish_time_by_metadata AS publish_time_by_metadata,
                  is_deleted_by_hoard AS is_deleted_by_hoard,
                  update_time_by_hoard AS update_time_by_hoard
                FROM files WHERE
                  asset_locator GLOB ?
                AND (is_deleted_by_hoard IS NULL OR is_deleted_by_hoard != 1)
              ) d
            `)

            sqlArgs.push(
              sqlGlobPatternEscape(locatorTopDirs[i] + locatorSubAncestors[j]) + '*',
            )
          }
        }

        if (locatorSubParents) {
          locatorSubParents = makeConcatenatablePathList(locatorSubParents)
          for (let j = 0; j < locatorSubParents.length; j++) {
            multipleConditionsSqlFragments.push(`
              SELECT * FROM
              (SELECT ${i} AS top_dir_index, ${j} AS second_cond_index) idx INNER JOIN (
                SELECT
                  CASE slash_index
                  WHEN 0 THEN FALSE
                  ELSE TRUE
                  END AS is_directory,

                  CASE slash_index
                  WHEN 0 THEN asset_raw_path
                  ELSE NULL
                  END AS asset_raw_path,
                  
                  CASE slash_index
                  WHEN 0 THEN (parent_concat_locator || following_locator)
                  ELSE (parent_concat_locator || SUBSTR(following_locator, 1, slash_index - 1))
                  END AS asset_locator,

                  CASE slash_index
                  WHEN 0 THEN asset_type
                  ELSE 'directory'
                  END AS asset_type,

                  asset_store_tag AS asset_store_tag,

                  CASE slash_index
                  WHEN 0 THEN asset_size
                  ELSE 0
                  END AS asset_size,

                  CASE slash_index
                  WHEN 0 THEN is_asset_heavy
                  ELSE FALSE
                  END AS is_asset_heavy,

                  CASE slash_index
                  WHEN 0 THEN has_derived_children
                  ELSE FALSE
                  END AS has_derived_children,

                  CASE slash_index
                  WHEN 0 THEN deriving_parent_id
                  ELSE NULL
                  END AS deriving_parent_id,

                  CASE slash_index
                  WHEN 0 THEN ${shouldFetchRawBytes ? 'asset_raw_bytes' : 'NULL'}
                  ELSE NULL
                  END AS asset_raw_bytes,

                  CASE slash_index
                  WHEN 0 THEN origin_file_path
                  ELSE NULL
                  END AS origin_file_path,

                  CASE slash_index
                  WHEN 0 THEN origin_file_extension
                  ELSE NULL
                  END AS origin_file_extension,

                  CASE slash_index
                  WHEN 0 THEN metadata
                  ELSE NULL
                  END AS metadata,

                  CASE slash_index
                  WHEN 0 THEN links
                  ELSE NULL
                  END AS links,

                  CASE slash_index
                  WHEN 0 THEN publish_time_by_metadata
                  ELSE NULL
                  END AS publish_time_by_metadata,

                  is_deleted_by_hoard AS is_deleted_by_hoard,

                  MAX(update_time_by_hoard) AS update_time_by_hoard
                FROM
                (
                  SELECT
                    INSTR(following_locator, '/') AS slash_index,
                    *
                  FROM
                  (
                    SELECT *, SUBSTR(asset_locator, 1, ?) AS parent_concat_locator, SUBSTR(asset_locator, ?) AS following_locator FROM files WHERE
                      asset_locator GLOB ?
                    AND (is_deleted_by_hoard IS NULL OR is_deleted_by_hoard != 1)
                  ) with_following_locator
                ) with_slash_index
                GROUP BY asset_locator
              ) d
            `)

            // unicode code point length, to align with sqlite
            const parentConcatLocatorLen = [...(locatorTopDirs[i] + locatorSubParents[j])].length
            sqlArgs.push(
              parentConcatLocatorLen,
              parentConcatLocatorLen + 1,
              sqlGlobPatternEscape(locatorTopDirs[i] + locatorSubParents[j]) + '*',
            )
          }
        }
      }

      sqlFragmentList.push(multipleConditionsSqlFragments.join(' UNION ALL '))
      sqlFragmentList.push(`) multi_cond_result
        WHERE 1=1
      `)

      if (shouldIncludeDerivingParent === true || shouldIncludeDerivingParent && shouldIncludeDerivingParent.valueOf() === true) {
        // sqlFragmentList.push(`
        //   AND (has_derived_children)
        // `)
      } else if (shouldIncludeDerivingParent === false || shouldIncludeDerivingParent && shouldIncludeDerivingParent.valueOf() === false) {
        sqlFragmentList.push(`
          AND (has_derived_children IS NULL OR NOT(has_derived_children))
        `)
      }

      if (shouldIncludeDirectories === true || shouldIncludeDirectories && shouldIncludeDirectories.valueOf() === true) {
        // sqlFragmentList.push(`
        //   AND (is_directory)
        // `)
      } else if (shouldIncludeDirectories === false || shouldIncludeDirectories && shouldIncludeDirectories.valueOf() === false) {
        sqlFragmentList.push(`
          AND (is_directory IS NULL OR NOT(is_directory))
        `)
      }

      if (extensions) {
        if (extensions.length === 0) {
          sqlFragmentList.push(`
            AND (is_directory OR has_derived_children OR 1=0)
          `)
        } else {
          sqlFragmentList.push(`
            AND (is_directory OR has_derived_children OR origin_file_extension IN ( ${extensions.map(_ => '?').join(', ')} ))
          `)
          sqlArgs.push(...extensions)
        }
      }

      sqlFragmentList.push(`
        GROUP BY asset_locator
      ) grouped_result
        WHERE 1 = 1
      `)

      if (customSql) {
        sqlFragmentList.push(customSql)
        if (customSqlArgs !== undefined) {
          sqlArgs.push(...customSqlArgs)
        }
      }

      if (orderBy === 'condTimeDesc') {
        sqlFragmentList.push(`
          ORDER BY second_cond_index ASC, publish_time_by_metadata DESC, update_time_by_hoard DESC, asset_locator ASC
        `)
      } else if (orderBy === 'condTimeAsc') {
        sqlFragmentList.push(`
          ORDER BY second_cond_index ASC, publish_time_by_metadata ASC, update_time_by_hoard ASC, asset_locator ASC
        `)
      } else if (orderBy === 'condLocatorAsc') {
        sqlFragmentList.push(`
          ORDER BY second_cond_index ASC, asset_locator ASC, publish_time_by_metadata DESC, update_time_by_hoard DESC
        `)
      } else if (orderBy === 'condLocatorDesc') {
        sqlFragmentList.push(`
          ORDER BY second_cond_index ASC, asset_locator DESC, publish_time_by_metadata DESC, update_time_by_hoard DESC
        `)
      } else if (orderBy === 'timeDesc') {
        sqlFragmentList.push(`
          ORDER BY publish_time_by_metadata DESC, second_cond_index ASC, publish_time_by_metadata DESC, update_time_by_hoard DESC, asset_locator ASC
        `)
      } else if (orderBy === 'timeAsc') {
        sqlFragmentList.push(`
          ORDER BY publish_time_by_metadata ASC, second_cond_index ASC, publish_time_by_metadata ASC, update_time_by_hoard ASC, asset_locator ASC
        `)
      } else if (orderBy === 'locatorAsc') {
        sqlFragmentList.push(`
          ORDER BY asset_locator ASC, second_cond_index ASC, publish_time_by_metadata DESC, publish_time_by_metadata DESC, update_time_by_hoard DESC
        `)
      } else if (orderBy === 'locatorDesc') {
        sqlFragmentList.push(`
          ORDER BY asset_locator DESC, second_cond_index ASC, publish_time_by_metadata DESC, publish_time_by_metadata DESC, update_time_by_hoard DESC
        `)
      } else if (orderBy === '') {
        // no order
      } else if (orderBy) {
        sqlFragmentList.push(`
          ORDER BY ${orderBy}
        `)
      } else {
        sqlFragmentList.push(`
          ORDER BY second_cond_index ASC, publish_time_by_metadata DESC, update_time_by_hoard DESC, asset_locator ASC
        `)
      }
      if (pageNum !== undefined && pageNum !== null) {
        if (pageNum < 0) {
          pageNum = 0
        }
        if (!pageSize) {
          pageSize = 10
        }
        if (pageSize < 0 || pageSize > 700) {
          pageSize = 700
        }
        sqlFragmentList.push(`
          LIMIT ? OFFSET ?
        `)
        sqlArgs.push(pageSize, pageNum * pageSize)
      }

      l("executing sql", truncateStrByLen(sqlFragmentList.join(' ').replace(/\s+/g, ' '), 60), truncateStrByLen(JSON.stringify(sqlArgs), 50))
      // l("executing sql", sqlFragmentList.join('\n'), sqlArgs)

      const sqlResult = await SqlDbHandler.sql({
        tkCtx,
        sql: sqlFragmentList.join('\n'),
        args: sqlArgs,
      })

      for (const x of sqlResult) {
        if (x.asset_locator) {
          x.web_url_path_tidy = x.asset_locator.split('/').slice(1).join('/')
          x.web_url_path_unencoded = '/' + x.web_url_path_tidy
          x.web_url_path = '/' + x.asset_locator.split('/').slice(1).map(encodeURI).join('/')
        }
        if (x.metadata) {
          x.metadata = JSON.parse(x.metadata)
        }
        if (x.links) {
          x.links = JSON.parse(x.links)
        }
      }

      // l("sqlResult", sqlResult)
      
      return sqlResult
    },
  }
})


export * as default from './serveDef.mts'


