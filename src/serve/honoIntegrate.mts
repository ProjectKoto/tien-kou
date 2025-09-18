import * as hono from "hono"
import { Hono } from "hono"
import { contextStorage as honoContextStorage, getContext as honoGetContext } from "hono/context-storage"
import * as honoTypes from "hono/types"
import * as liquid from "liquidjs"
import mimeDbJson from 'mime-db/db.json'
import mimeType from 'mime-types'
import { dedicatedAssetExtNames, l, le, listableAssetExtNames, markdownExtNames, TkError, TkErrorHttpAware, um } from "../lib/common.mts"
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

export const AbstractTkSqlLiquidHonoApp = <EO,> () => AHT<TienKouApp<EO>>()(async <HE extends hono.Env, >({
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
        // simulate one
        const honoCtx = honoGetContext()
        return {
          tkEnv: honoCtx.env,
          e: honoCtx.env,
          honoCtx: honoCtx,
          hc: honoCtx,
          get resultGenContext(): ResultGenContextHl<hono.Env> {
            // TODO: save rgc in AsyncLocalContext and get
            throw new TkErrorHttpAware("getting resultGenContext from TkProvideCtxFromNothingHandler not implemented") 
          },
          get rgc(): ResultGenContextHl<hono.Env> {
            // TODO: save rgc in AsyncLocalContext and get
            throw new TkErrorHttpAware("getting resultGenContext from TkProvideCtxFromNothingHandler not implemented")
          },
        } as TkContextHl<hono.Env>
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
      mainTemplateRelPath: "main.tmpl.html",
      markdownExtNames,
      listableAssetExtNames,
      dedicatedAssetExtNames,
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

    return tkCtx
  }

  const processOnHonoCtxReceive = async (honoCtx: hono.Context<HE>) => {
    const tkCtx = await convertHonoCtxToTkCtx(honoCtx)

    await TkCtxHandler.triggerProcessTkCtx(tkCtx)

    return { tkCtx, rgc: tkCtx.rgc, resultGenContext: tkCtx.resultGenContext }
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

    return await serveGenericLocatableAsset(tkCtx, rgc.reqPathTidy)
  }

  honoApp.get('/favicon.ico', handlerServeGenericLocatableAsset)
  honoApp.get('/favicon.png', handlerServeGenericLocatableAsset)
  honoApp.get('/favicon.jpg', handlerServeGenericLocatableAsset)
  
  honoApp.on("ALL", ['/.well-known', '/.well-known/*'], async c => {
    return c.text(".well-known not found", 404)
  })
  
  honoApp.get('/admin/:op', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    const rgc = resultGenContext
    void(rgc)
  
    const op = c.req.param("op")
  
    if (op === "refreshSiteVersion") {
      await IntegratedCachePolicyHandler.evictForNewDataVersion(tkCtx)
      return c.text("refreshed")
    } else {
      return c.text("invalid op", 404)
    }
  })
  
  honoApp.get('/*', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    const rgc = resultGenContext
  
    await IntegratedCachePolicyHandler.checkAndDoEvictRuntimeCache(tkCtx)
  
    const renderResult = await (await LiquidHandler.liquidReadyPromise).renderFile(rgc.mainTemplateRelPath, rgc, {
      globals: { rgc },
    })
  
    // console.log(`renderResult: ${renderResult}`)
    l('rgc', rgc)
  
    if (rgc.respGenericLocatableAssetSubPath) {
      return await serveGenericLocatableAsset(tkCtx, rgc.respGenericLocatableAssetSubPath)
    }
  
    return c.html(renderResult, rgc.respStatusCode || 200)
  })

  return EA(super_, {
    honoApp,
  })

})

