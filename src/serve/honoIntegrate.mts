import * as hono from "hono"
import { Hono } from "hono"
import { contextStorage as honoContextStorage, getContext as honoGetContext } from "hono/context-storage"
import * as honoTypes from "hono/types"
import * as liquid from "liquidjs"
import mimeDbJson from 'mime-db/db.json'
import mimeType from 'mime-types'
import { AnyObj, dedicatedAssetExtNames, l, lazyValue, le, listableAssetExtNames, markdownExtNames, TkError, TkErrorHttpAware, um } from "../lib/common.mts"
import { HonoWithErrorHandler } from "../lib/hack.mts"
import { AbstractTkSqlLiquidApp, ResultGenContext } from "./liquidIntegrate.mts"
import { AHT, EA, KD, TienKouApp, TkInvalidReqError } from "./serveDef.mts"
import { TkContext } from '../lib/common.mts'

export interface TkContextHlGetTkEnvHandler<HE extends hono.Env> {
  getTkEnvGetter: () => Promise<(honoCtx: hono.Context<HE>) => Record<string, string | undefined>>
}

export interface ResultGenContextHl<HE extends hono.Env,> extends ResultGenContext {
  tkCtx: () => TkContextHl<HE>,
  rgc: ResultGenContextHl<HE>,
  reqPath: string,
  reqPathTidy: string,
  mainTemplateRelPath: string,
  markdownExtNames: Iterable<string>,
  listableAssetExtNames: Iterable<string>,
  dedicatedAssetExtNames: Iterable<string>,
}

// TkContext with Hono & Liquid <HonoEnv>
export type TkContextHl<HE extends hono.Env> = TkContext & {
  honoCtx: hono.Context<HE>,
  hc: hono.Context<HE>,
  resultGenContext: ResultGenContextHl<HE>,
  rgc: ResultGenContextHl<HE>,
}

// Get Hono Context
export const hc = <T extends hono.Env,>(ctx: TkContext | undefined): hono.Context<T> => {
  let hc_: hono.Context<T> | undefined = undefined
  if (hc_ === undefined) {
    if (ctx !== undefined) {
      hc_ = (ctx as TkContextHl<T>).hc
    }
  }
  if (hc_ === undefined) {
    try {
      hc_ = honoGetContext<T>()
    } catch (e) {
      void(e)
    }
  }
  
  if (hc_ === undefined) {
    throw new TkError("Can't get Hono Context")
  }

  return hc_
}

export type HonoEnvVariablesType<HE extends hono.Env> = {
  tkCtx: TkContextHl<HE>
}

export type HonoEnvTypeWithTkCtx<B extends object> = {
  Bindings?: B
  Variables: HonoEnvVariablesType<HonoEnvTypeWithTkCtx<B>>
}

export const AbstractTkSqlLiquidHonoApp = <EO,> () => AHT<TienKouApp<EO>>()(async <HE extends HonoEnvTypeWithTkCtx<object>, >({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
  IntegratedCachePolicyHandler,
  TkContextHlGetEHandler,
  TkCtxHandler,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkCtxHandler",
  {
    TkContextHlGetEHandler: TkContextHlGetTkEnvHandler<HE>
  }>) => {

  const super_ = await AbstractTkSqlLiquidApp<EO>()({
    TienKouAssetFetchHandler,
    LiquidHandler,
    TienKouAssetCategoryLogicHandler,
    LiquidFilterRegisterHandlerList,
    IntegratedCachePolicyHandler,
    TkProvideCtxFromNothingHandler: {
      fetchTkCtxFromNothing: async () => {
        const honoCtx = honoGetContext<HE>()
        return honoCtx.get("tkCtx")
      },
    }
  })

  const honoApp = new Hono<HE>() as HonoWithErrorHandler<HE>

  honoApp.use(honoContextStorage())

  const origHonoErrorHandler = honoApp.errorHandler
  honoApp.onError((err, c) => {
    // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
    let realErr: Error | { message: String & { isUserMessage: boolean } } | honoTypes.HTTPResponseError = err
    if (err instanceof liquid.RenderError) {
      realErr = err.originalError ?? err
    }
  
    if (realErr instanceof TkErrorHttpAware) {
      const code = realErr.status
      if (realErr.mShouldLog) {
        console.error(`TkError: ${JSON.stringify(realErr)} msg=${JSON.stringify(realErr.message)} userMsg=${JSON.stringify(realErr.tkUserMessage || '')}`)
      }
      if (realErr.tkUserMessage) {
        return c.text('Error occurred: ' + realErr.tkUserMessage, code)
      }
      return c.text('Error occurred. Something is wrong with server, or the input may be invalid.', code)
    }
    le(err)
    let stack: string | string[] = err.stack ?? '( No Stack )'
    if (typeof(stack) === 'string') {
      stack = stack.split('\n')
    }
    le('Stack: ', ...stack)
    return origHonoErrorHandler.apply(honoApp, [err, c])
  })

  const convertHonoCtxToTkCtx = async (honoCtx: hono.Context<HE>) => {
    // hono already done urldecode to this.
    const reqPath = honoCtx.req.path

    const urlGetter = lazyValue(() => new URL(honoCtx.req.url))

    const queryRawGetter = lazyValue(() => urlGetter().search)

    const queryGetter = lazyValue(() => {
      return Object.fromEntries(urlGetter().searchParams.entries())
    })

    const queryMultiValueGetter = lazyValue(() => {
      const result:{ [x: string]: string[] } = {}
      for (const [k, v] of urlGetter().searchParams.entries()) {
        let vArr = result[k]
        if (vArr === undefined) {
          vArr = []
          result[k] = vArr
        }
        vArr.push(v)
      }
      return result
    })

    const ifNoneMatchHeaderGetter = lazyValue(() => {
      return honoCtx.req.header('If-None-Match')
    })

    const serverDeployVersionGetter = lazyValue(() => {
      const he = honoCtx?.env
      if (he) {
        const cfVerMetadata = (he as AnyObj)["CF_VERSION_METADATA"]
        if (cfVerMetadata) {
          const { id: versionId } =  cfVerMetadata
          return (versionId || "cfUnknown").toString()
        }
      }
      return "otherUnknown"
    })

    l('reqPath', reqPath)
  
    if (!reqPath.startsWith('/')) {
      throw new TkInvalidReqError("bad path")
    }
    
    const resultGenContext: ResultGenContextHl<HE> = {
      // get containing tkCtx
      tkCtx: () => tkCtx,
      rgc: undefined as unknown as ResultGenContextHl<HE>,
      reqPath,
      reqPathTidy: reqPath.split('/').filter(x => x !== '').join('/'),
      reqQuery: () => { return queryGetter() },
      reqSearch: () => { return queryGetter() },
      reqQueryRaw: () => { return queryRawGetter() },
      reqSearchRaw: () => { return queryRawGetter() },
      reqQueryStr: () => { return queryRawGetter() },
      reqSearchStr: () => { return queryRawGetter() },
      reqQueryMulti: () => { return queryMultiValueGetter() },
      reqSearchMulti: () => { return queryMultiValueGetter() },
      serverDeployVersion: () => { return serverDeployVersionGetter() },
      dataVersion: async () => { return IntegratedCachePolicyHandler.fetchDataVersion(tkCtx) },
      reqIfNoneMatchHeader: () => { return ifNoneMatchHeaderGetter() },
      mainTemplateRelPath: "main.tmpl.html",
      markdownExtNames,
      listableAssetExtNames,
      dedicatedAssetExtNames,
      backpatches: {} as AnyObj,
      backpatchValueMap: {} as AnyObj,
    }

    resultGenContext.rgc = resultGenContext

    const tkEnvGetter = await TkContextHlGetEHandler.getTkEnvGetter()

    const tkCtx: TkContextHl<HE> = {
      honoCtx,
      hc: honoCtx,
      resultGenContext: resultGenContext,
      rgc: resultGenContext,
      get tkEnv() {
        return tkEnvGetter(honoCtx)
      },
      get e() {
        return this.tkEnv
      },
    }

    honoCtx.set('tkCtx', tkCtx)

    return tkCtx
  }

  const processOnHonoCtxReceive = async (honoCtx: hono.Context<HE>) => {
    const tkCtx = await convertHonoCtxToTkCtx(honoCtx)

    await TkCtxHandler.triggerProcessTkCtx(tkCtx)

    return { tkCtx, rgc: tkCtx.rgc, resultGenContext: tkCtx.resultGenContext }
  }

  const processTkCtxPostReqHandle = async (tkCtx: TkContextHl<HE>) => {
    await TkCtxHandler.triggerProcessTkCtxOnEnd(tkCtx)
  }

  const serveGenericLocatableAsset = async <HE extends hono.Env>({ hc, rgc }: { hc: hono.Context, rgc: ResultGenContextHl<HE> }, subPath: string) => {
    const fetchResult = await super_.fetchGenericLocatableAssetWrapper(rgc, subPath, {
      shouldThrowIfNotFound: true
    })


    if (fetchResult === undefined) {
      throw new TkErrorHttpAware(um`Error fetching asset`)
    } if (fetchResult.asset_raw_bytes !== undefined && fetchResult.asset_raw_bytes !== null) {
      // TODO: CONTENT TYPE FROM PATH/EXTENSION/BYTES
      let mime = mimeType.contentType(fetchResult.origin_file_extension || 'bin')
      if (typeof mime === 'boolean') {
        const octetType: keyof typeof mimeDbJson = "application/octet-stream"
        mime = octetType
      }
      hc.header('Content-Type', mime)
      return hc.body(fetchResult.asset_raw_bytes)
    } else if (fetchResult.redirect_url) {
      return hc.redirect(fetchResult.redirect_url)
    } else {
      throw new TkErrorHttpAware(um`Error fetching asset`)
    }
  }

  const handlerServeGenericLocatableAsset = async (c: hono.Context) => {
    const { tkCtx, rgc } = await processOnHonoCtxReceive(c)
    try {
      return await serveGenericLocatableAsset(tkCtx, rgc.reqPathTidy)
    } finally {
      await processTkCtxPostReqHandle(tkCtx)
    }
  }

  honoApp.get('/favicon.ico', handlerServeGenericLocatableAsset)
  honoApp.get('/favicon.png', handlerServeGenericLocatableAsset)
  honoApp.get('/favicon.jpg', handlerServeGenericLocatableAsset)
  
  honoApp.on("ALL", ['/.well-known', '/.well-known/*'], async c => {
    return c.text(".well-known not found", 404)
  })
  
  honoApp.get('/admin/:op', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    try {
      const rgc = resultGenContext
      void(rgc)
    
      const op = c.req.param("op")
    
      if (op === "refreshSiteVersion") {
        await IntegratedCachePolicyHandler.evictForNewDataVersion(tkCtx)
        return c.text("refreshed " + (await IntegratedCachePolicyHandler.fetchDataVersion(tkCtx)).toString())
      } else {
        return c.text("invalid op", 404)
      }
    } finally {
      await processTkCtxPostReqHandle(tkCtx)
    }
  })
  
  honoApp.get('/*', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    try {
      const rgc = resultGenContext
    
      await IntegratedCachePolicyHandler.checkAndDoEvictRuntimeCache(tkCtx)
    
      let renderResult = (await (await LiquidHandler.liquidReadyPromise).renderFile(rgc.mainTemplateRelPath, rgc, {
        globals: { rgc, now: new Date().getTime() },
      })) as string

      l('backpatches', rgc['backpatches'])
      l('backpatchValueMap', rgc['backpatchValueMap'])
      if (rgc['backpatches'] && Object.keys(rgc['backpatches']).length && rgc['backpatchValueMap']) {
        const backpatches = [ ... Object.values(rgc['backpatches']) ] as {
          name: string,
          outputOffset: number,
        }[]

        const backpatchValueMap = rgc['backpatchValueMap'] as AnyObj
        
        backpatches.sort((a, b) => {
          return a.outputOffset - b.outputOffset
        })

        const segments: string[] = []
        let cursor = 0
        for (const backpatch of backpatches) {
          if (cursor >= renderResult.length) {
            break
          }
          if (cursor >= backpatch.outputOffset) {
            continue
          }
          segments.push(renderResult.substring(cursor, backpatch.outputOffset))
          const backpatchValue = (backpatchValueMap[backpatch.name] ?? '_').toString()
          segments.push(backpatchValue)
          cursor = backpatch.outputOffset + 1
        }
        if (cursor < renderResult.length) {
          segments.push(renderResult.substring(cursor, renderResult.length))
        }
        renderResult = segments.join('')
      }
    
      // console.log(`renderResult: ${renderResult}`)
      // l('rgc', rgc)
    
      if (rgc.respSend304) {
        return c.body(null, 304)
      }

      if (rgc.respGenericLocatableAssetSubPath) {
        return await serveGenericLocatableAsset(tkCtx, rgc.respGenericLocatableAssetSubPath)
      }
      
      if (rgc.respETag) {
        c.header('Cache-Control', 'max-age=0, must-revalidate')
        c.header('ETag', rgc.respETag)
      }

      return c.html(renderResult, rgc.respStatusCode || 200)
    } finally {
      await processTkCtxPostReqHandle(tkCtx)
    }
  })

  return EA(super_, {
    honoApp,
  })

})

