import * as hono from "hono"
import { Hono } from "hono"
import { contextStorage as honoContextStorage, getContext as honoGetContext } from "hono/context-storage"
import * as honoTypes from "hono/types"
import * as liquid from "liquidjs"
import mimeDbJson from 'mime-db/db.json'
import mimeType from 'mime-types'
import { AnyObj, dedicatedAssetExtNames, isTienKouResponseCacheable, l, lazyValue, le, listableAssetExtNames, LruInMemory, markdownExtNames, tellCacheIgnoreUrlPath, TkContext, TkError, TkErrorHttpAware, toArrayBuffer, um } from "../lib/common.mts"
import { HonoWithErrorHandler } from "../lib/hack.mts"
import { AbstractTkSqlLiquidApp, ResultGenContext } from "./liquidIntegrate.mts"
import { AEAH, AHC, HC, KD, TienKouApp, TkInvalidReqError, MiddleCacheHandler, notImplementedCtx } from "./serveDef.mts"
import * as honoCookie from 'hono/cookie'
import * as honoUtilsCookie from "hono/utils/cookie"
import * as jshttpContentType from "content-type"

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
  serverDeployVersion: () => string,
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
  tkCtx: TkContextHl<HE>,
}

export type HonoEnvTypeWithTkCtx<B extends object> = {
  Bindings?: B
  Variables: HonoEnvVariablesType<HonoEnvTypeWithTkCtx<B>>
}

export type HonoProvideHandler<HE extends hono.Env, > = {
  getHono(): HonoWithErrorHandler<HE>
}

export const MainHonoProvideHandler = <HE extends hono.Env,> () => HC<HonoProvideHandler<HE>>()(async (_: KD<never>) => {
  const honoApp = new Hono<HE>() as unknown as HonoWithErrorHandler<HE>
  return {
    getHono: () => {
      return honoApp
    }
  }
})

export const AbstractTkSqlLiquidHonoApp = <EO,> () => AHC<TienKouApp<EO>>()(async <HE extends HonoEnvTypeWithTkCtx<object>, >({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
  IntegratedCachePolicyHandler,
  TkContextHlGetEHandler,
  TkCtxHandler,
  HonoProvideHandler,
  HonoMiddlewares,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkCtxHandler",
  {
    TkContextHlGetEHandler: TkContextHlGetTkEnvHandler<HE>,
    HonoProvideHandler: HonoProvideHandler<HE>,
    HonoMiddlewares: hono.MiddlewareHandler[] | undefined | null,
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

  const honoApp = HonoProvideHandler.getHono()

  honoApp.use(honoContextStorage())
  if (HonoMiddlewares) {
    for (const m of HonoMiddlewares) {
      honoApp.use(m)
    }
  }

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

  LiquidHandler.registerFilterPostCreate("reqGetHeader", async function (headerName: string) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    const honoReq = rgc.honoReq as hono.HonoRequest
    return honoReq.header(headerName)
  })

  LiquidHandler.registerFilterPostCreate("reqGetCookie", async function (cookieName: string, ...rest) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    const honoCtx = (rgc.tkCtx() as TkContextHl<hono.Env>).honoCtx
    return honoCookie.getCookie(honoCtx, cookieName, ...rest)
  })

  const commonHonoSetCookieCheck: (c: hono.Context, name: string, value: string, opt?: honoUtilsCookie.CookieOptions) => void = (c: hono.Context, name: string, value: string, opt?: honoUtilsCookie.CookieOptions) => {
    if (value && encodeURIComponent(value).length > 4090) {
      throw new TkErrorHttpAware(um("error: setting cookie " + name + ": encoded cookie value reaches 4090 bytes, can't set"), 400)
    }
    honoCookie.setCookie(c, name, value, opt)
  }

  LiquidHandler.registerFilterPostCreate("respSetCookie", async function (value: string, cookieName: string, ...rest) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    const honoCtx = (rgc.tkCtx() as TkContextHl<hono.Env>).honoCtx
    const cookieOptions: honoUtilsCookie.CookieOptions = {}
    for (let oi = 1; oi < rest.length; oi += 2) {
      (cookieOptions as AnyObj)[rest[oi - 1] as string] = rest[oi]
    }
    return commonHonoSetCookieCheck(honoCtx, cookieName, value, cookieOptions)
  })

  LiquidHandler.registerFilterPostCreate("respDeleteCookie", async function (cookieName: string, ...rest) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    const honoCtx = (rgc.tkCtx() as TkContextHl<hono.Env>).honoCtx
    const cookieOptions: honoUtilsCookie.CookieOptions = {}
    for (let oi = 1; oi < rest.length; oi += 2) {
      (cookieOptions as AnyObj)[rest[oi - 1] as string] = rest[oi]
    }
    return honoCookie.deleteCookie(honoCtx, cookieName, cookieOptions)
  })

  const convertHonoCtxToTkCtx = async (honoCtx: hono.Context<HE>) => {
    // hono already done urldecode to this.
    const reqPath = honoCtx.req.path

    const urlGetter = lazyValue(() => new URL(honoCtx.req.url))

    const queryRawGetter = lazyValue(() => urlGetter().search)

    const queryGetter = lazyValue(() => {
      return Object.fromEntries(urlGetter().searchParams.entries())
    })
    const pathParamMapGetter = lazyValue(() => {
      return honoCtx.req.param()
    })
    const headerLowerMapGetter = lazyValue(() => {
      return honoCtx.req.header()
    })
    const parsedContentTypeGetter = lazyValue(() => {
      const contentType = honoCtx.req.raw.headers.get('Content-Type')
      if (!contentType) {
        return undefined
      }
      return jshttpContentType.parse(contentType, { parameters: true })
    })
    const forceUtf8ReqMimeGetter = lazyValue(() => {
      const ct = parsedContentTypeGetter()
      if (ct === undefined) {
        return undefined
      }
      if (ct.parameters && ct.parameters.charset && ct.parameters.charset !== 'utf-8') {
        throw new TkErrorHttpAware("forceUtf8ReqMimeGetter: request Content-Type charset is only UTF-8 supported, if provided. got " + ct.parameters.charset)
      }
      return ct.type
    })
    const bodyFormGetter = lazyValue(() => {
      return honoCtx.req.parseBody()
    })
    const bodyFormMultiValueGetter = lazyValue(() => {
      return honoCtx.req.parseBody({ all: true })
    })
    const unifiedFormGetter = lazyValue(() => {
      const form: Record<string, string> = {}
      try {
        Object.assign(form, honoCtx.req.parseBody() ?? {})
      } catch (_e: unknown) {
        //
      }
      try {
        Object.assign(form, pathParamMapGetter() ?? {})
      } catch (_e: unknown) {
        //
      }
      try {
        Object.assign(form, queryGetter() ?? {})
      } catch (_e: unknown) {
        //
      }
      return form
    })
    const allCookiesGetter = lazyValue(() => {
      return honoCookie.getCookie(honoCtx)
    })

    const urlSchemeHostGetter = lazyValue(() => {
      const origUrl = urlGetter()
      const urlSchemeHostOnly = new URL('https://www.example.com')
      urlSchemeHostOnly.host = origUrl.host
      urlSchemeHostOnly.hostname = origUrl.hostname
      urlSchemeHostOnly.pathname = '/'
      urlSchemeHostOnly.password = ''
      urlSchemeHostOnly.hash = ''
      urlSchemeHostOnly.port = origUrl.port
      urlSchemeHostOnly.protocol = origUrl.protocol
      urlSchemeHostOnly.search = ''
      let urlSchemeHostOnlyStr = urlSchemeHostOnly.href
      if (urlSchemeHostOnlyStr.endsWith('/')) {
        urlSchemeHostOnlyStr = urlSchemeHostOnlyStr.slice(0, urlSchemeHostOnlyStr.length - 1)
      }
      return urlSchemeHostOnlyStr
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

    const serverStartTime = Date.now()
    const serverVersionFallback = "v" + serverStartTime.toString()
    const serverDeployVersionGetter = lazyValue<string>(() => {
      const he = honoCtx?.env
      if (he) {
        const cfVerMetadata = (he as AnyObj)["CF_VERSION_METADATA"]
        if (cfVerMetadata) {
          const { id: versionId } =  cfVerMetadata
          return (versionId || "cfWorkerUnknownServerVer").toString()
        }
      }
      return serverVersionFallback
    })

    l(honoCtx.req.method, 'reqPath', reqPath)
  
    if (!reqPath.startsWith('/')) {
      throw new TkInvalidReqError("bad path")
    }

    const resultGenContext: ResultGenContextHl<HE> = {
      // get containing tkCtx
      tkCtx: () => tkCtx,
      rgc: undefined as unknown as ResultGenContextHl<HE>,
      honoReq: honoCtx.req, // is it safe?
      req: honoCtx.req.raw, // is it safe?
      reqMethod: honoCtx.req.method,
      reqUrl: honoCtx.req.url,
      reqUrlSchemeHostOnly: () => { return urlSchemeHostGetter() },
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
      reqPathParamMap: () => { return pathParamMapGetter() },
      reqHeaderLowerMap: () => { return headerLowerMapGetter() },
      reqParsedContentType: () => { return parsedContentTypeGetter() },
      reqForceUtf8Mime: () => { return forceUtf8ReqMimeGetter() },
      reqBodyForm: () => { return bodyFormGetter() },
      reqBodyFormMulti: () => { return bodyFormMultiValueGetter() },
      reqUnifiedForm: () => { return unifiedFormGetter() },
      reqAllCookies: () => { return allCookiesGetter() },
      serverDeployVersion: () => { return serverDeployVersionGetter() },
      dataVersion: async () => { return IntegratedCachePolicyHandler.fetchDataVersion(tkCtx) },
      reqIfNoneMatchHeader: () => { return ifNoneMatchHeaderGetter() },
      serverProfileName: () => tkCtx.tkEnv.PROFILE_NAME || 'release',
      mainTemplateRelPath: "main.tmpl.html",
      genStaticTemplateRelPath: "genStatic.tmpl.html",
      markdownExtNames,
      listableAssetExtNames,
      dedicatedAssetExtNames,
      backpatches: {} as AnyObj,
      backpatchValueMap: {} as AnyObj,
      now: new Date().getTime(),
      tkCurrInstAppCtx: TkCtxHandler.appSharedCtxCurrInstOnly,
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
    }

    let assetBytes = fetchResult.asset_processed_bytes
    if (assetBytes === undefined || assetBytes === null) {
      assetBytes = fetchResult.asset_raw_bytes
    }
    
    if (assetBytes !== undefined && assetBytes !== null) {
      // TODO: CONTENT TYPE FROM PATH/EXTENSION/BYTES
      let mime = mimeType.contentType(fetchResult.origin_file_extension || 'bin')
      if (typeof mime === 'boolean') {
        const octetType: keyof typeof mimeDbJson = "application/octet-stream"
        mime = octetType
      }
      hc.header('Content-Type', mime)
      return hc.body(assetBytes)
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

  honoApp.on("ALL", ['/sqlDb/:secretPath/*'], async c => {
    const { tkCtx } = await processOnHonoCtxReceive(c)
    const secretPath = c.req.param("secretPath")
    if (!(tkCtx.e.TURSO_EXPOSE_SECRET_PATH && tkCtx.e.TURSO_EXPOSE_SECRET_PATH === secretPath && (tkCtx.e.TURSO_DATABASE_URL_FOR_SERVE_BACKEND || tkCtx.e.TURSO_DATABASE_URL))) {
      return c.text("path not found", 404)
    }

    const origReqPath = c.req.path
    const origReqPathSplit = origReqPath.split('/')
    // take, until we get first two non-empty path component
    let origReqPathFirstTwoNonEmptyCompI = 0
    let origReqPathNonEmptyCompCount = 0
    for (; origReqPathFirstTwoNonEmptyCompI < origReqPathSplit.length; origReqPathFirstTwoNonEmptyCompI++) {
      if (origReqPathSplit[origReqPathFirstTwoNonEmptyCompI]) {
        origReqPathNonEmptyCompCount++
      }
      if (origReqPathNonEmptyCompCount >= 2) {
        break
      }
    }
    let resolvedForwardedPath: string | undefined = undefined
    if (origReqPathNonEmptyCompCount == 2 && origReqPathFirstTwoNonEmptyCompI < origReqPathSplit.length) {
      const stripPrefixLen = origReqPathSplit.slice(0, origReqPathFirstTwoNonEmptyCompI + 1).join('/').length
      if (stripPrefixLen < origReqPath.length) {
        resolvedForwardedPath = origReqPath.slice(stripPrefixLen)
      }
    }
    if (!resolvedForwardedPath || !resolvedForwardedPath.startsWith('/')) {
      return c.text("path not found", 404)
    }
    const tursoUrlStr = (tkCtx.e.TURSO_DATABASE_URL_FOR_SERVE_BACKEND || tkCtx.e.TURSO_DATABASE_URL)!
    let newUrlStr: string
    if (tursoUrlStr.startsWith('libsql')) {
      const tmpUrlStr = "http" + tursoUrlStr.slice(6)
      const tmpUrl = new URL(tmpUrlStr)
      if (tmpUrl.searchParams.get('tls') === '0') {
        newUrlStr = tmpUrlStr
      } else if (tmpUrl.searchParams.get('tls') === '1') {
        newUrlStr = "https" + tursoUrlStr.slice(6)
      } else if (tkCtx.e.TURSO_DISABLE_TLS === '1' || tkCtx.e.TURSO_DISABLE_TLS === 'true') {
        newUrlStr = tmpUrlStr
      } else {
        newUrlStr = "https" + tursoUrlStr.slice(6)
      }
    } else {
      newUrlStr = tursoUrlStr
    }
    {
      const tmpUrl2 = new URL(newUrlStr)
      tmpUrl2.search = ''
      newUrlStr = tmpUrl2.toString()
    }
    if (newUrlStr.endsWith('/')) {
      newUrlStr = newUrlStr.slice(0, newUrlStr.length - 1)
    }
    newUrlStr = newUrlStr + resolvedForwardedPath
    const newReq: Request = new Request(newUrlStr, {
      body: await c.req.bytes(),
      headers: c.req.raw.headers,
      method: c.req.raw.method,
    })
    newReq.headers.set("Connection", "close")
    newReq.headers.delete('If-None-Match')
    newReq.headers.delete('Host')
    newReq.headers.delete('Upgrade')
    l(`forward sqlDb path to libsql-server/sqld: ${origReqPath} -> ${newUrlStr}`)
    // l('oldReq', c.req.raw)
    // l('newReq', newReq)
    const forwardedResp = await fetch(newUrlStr, newReq)
    // l('forwardedResp', forwardedResp)
    const forwardedRespClone = forwardedResp.clone()
    c.res = new Response(await forwardedRespClone.bytes(), forwardedRespClone)
    c.res.headers.delete('Content-Length')
    c.res.headers.delete('Content-Encoding')
    c.res.headers.delete('Transfer-Encoding')
    c.res.headers.delete('ETag')
    c.res.headers.delete('Cache-Control')
    c.res.headers.delete('Expires')
    c.res.headers.delete('Last-Modified')
    c.res.headers.delete('Upgrade')
    c.res.headers.set('Connection', 'close')
  })
  
  honoApp.get('/admin/:op', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    try {
      const rgc = resultGenContext
      void(rgc)
    
      const op = c.req.param("op")
    
      if (op === "refreshSiteVersion") {
        await IntegratedCachePolicyHandler.evictForNewDataVersion(tkCtx)
        return c.text("refreshed " + (await IntegratedCachePolicyHandler.fetchDataVersion(tkCtx)).toString() + " " + rgc.serverDeployVersion())
      } else if (op === 'genStatic' && TkCtxHandler.appSharedCtxCurrInstOnly.isStaticGenFeatureEnabled) {
        const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
        const rgc = resultGenContext
      
        await IntegratedCachePolicyHandler.checkAndDoEvictRuntimeCache(tkCtx)

        ;(await (await LiquidHandler.liquidReadyPromise).renderFile(rgc.genStaticTemplateRelPath, rgc, {
          globals: {
            c: rgc,
            ...rgc,
            rgc,
          },
        })) as string

        return c.body('operation ends', 200)
      } else {
        return c.text("invalid op", 404)
      }
    } finally {
      await processTkCtxPostReqHandle(tkCtx)
    }
  })
  
  honoApp.all('/*', async (c) => {
    const { tkCtx, resultGenContext } = await processOnHonoCtxReceive(c)
    try {
      const rgc = resultGenContext
    
      await IntegratedCachePolicyHandler.checkAndDoEvictRuntimeCache(tkCtx)

      if (rgc.reqPathTidy === 'hello') {
        return c.text(await (await LiquidHandler.liquidReadyPromise).parseAndRender('{%- echo "Hello world\\n" -%}'))
      }
    
      let renderResult = (await (await LiquidHandler.liquidReadyPromise).renderFile(rgc.mainTemplateRelPath, rgc, {
        globals: {
          c: rgc,
          ...rgc,
          rgc,
        },
      })) as string

      // l('backpatches', rgc['backpatches'])
      // l('backpatchValueMap', rgc['backpatchValueMap'])
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
    
      // 304 Not Modified
      if (rgc.respSend304) {
        return c.body(null, 304)
      }

      if (rgc.respSend302) {
        return c.redirect(rgc.respSend302, 302)
      }

      if (rgc.respGenericLocatableAssetSubPath) {
        return await serveGenericLocatableAsset(tkCtx, rgc.respGenericLocatableAssetSubPath)
      }
      
      if (rgc.respETag) {
        c.header('Cache-Control', 'max-age=0, must-revalidate')
        c.header('ETag', rgc.respETag)
      }

      if (rgc.respContentType) {
        c.header('Content-Type', rgc.respContentType)
      } else {
        c.header('Content-Type', 'text/html; charset=UTF-8')
      }

      if (rgc.respSetCookies && Array.isArray(rgc.respSetCookies)) {
        const rgcSetCookies: [string, string, ...unknown[]][] = rgc.respSetCookies
        for (const kv of rgcSetCookies) {
          if (kv && Array.isArray(kv) && kv.length >= 2) {
            const cookieOptions: honoUtilsCookie.CookieOptions = {}
            for (let oi = 3; oi < kv.length; oi += 2) {
              (cookieOptions as AnyObj)[kv[oi - 1] as string] = kv[oi]
            }
            commonHonoSetCookieCheck(c, kv[0], kv[1], cookieOptions)
          }
        }
      }

      let respBody: (string | ArrayBuffer | ReadableStream<unknown> | Uint8Array<ArrayBuffer>) = renderResult
      if (rgc.overwriteRespBody) {
        respBody = rgc.overwriteRespBody
      }

      if (rgc.respDoNotCache) {
        c.header('Tk-Internal-Resp-Do-Not-Cache', '1')
      }

      return c.body(respBody, rgc.respStatusCode || 200)
    } finally {
      await processTkCtxPostReqHandle(tkCtx)
    }
  })

  return AEAH(super_, {
    honoApp,
  })

})

const honoReqCacheLru = new LruInMemory<string, Promise<undefined | unknown>>(2500)

export const honoReqCache = async ({
  MiddleCacheHandler
}: {
  MiddleCacheHandler: MiddleCacheHandler,
}): Promise<hono.MiddlewareHandler> => {
  return async (c, next) => {
    const cacheFetchResult = await (async () => {
      if (c.req.path.length > 500) {
        return false
      }
      if (c.req.method.length > 30) {
        return false
      }
      const url = new URL(c.req.url)
      if (url.pathname.length > 500) {
        return false
      }
      if (tellCacheIgnoreUrlPath(url.pathname)) {
        return false
      }
      if (url.search.length > 500) {
        return false
      }
      const cacheKey = c.req.method.length.toString() + c.req.method + url.pathname.length.toString() + url.pathname + url.search
      
      let cachedValue: unknown

      const existingPendingPromise = honoReqCacheLru.get(cacheKey)
      if (existingPendingPromise !== undefined) {
        cachedValue = await existingPendingPromise
      } else {
        const newPromise = MiddleCacheHandler.getInCache(notImplementedCtx, cacheKey)
        honoReqCacheLru.set(cacheKey, newPromise)
        try {
          cachedValue = await newPromise
        } finally {
          honoReqCacheLru.delete(cacheKey)
        }
      }

      if (cachedValue === null || cachedValue === undefined) {
        return {
          cacheKey,
          val: undefined,
        }
      } else {
        return {
          cacheKey,
          val: cachedValue,
        }
      }
    })()

    if (cacheFetchResult === false) {
      await next()
    } else {
      if (cacheFetchResult.val) {
        l('cache hit', c.req.url)
        const resp = cacheFetchResult.val as {
          headers: [n: string, v: string][],
          status: number,
          body: Buffer,
        }
        // for (const h of resp.headers) {
        //   c.header(h[0], h[1])
        // }
        return new Response(resp.body, {
          headers: resp.headers,
          status: resp.status,
        })
      } else {
        await next()
        const isRespCacheable = isTienKouResponseCacheable(c.req.raw, c.res)
        c.res.headers.delete('Tk-Internal-Resp-Do-Not-Cache')
        if (c.res.body !== null && isRespCacheable) {
          const respClone = c.res.clone()
          const respToCache = new Response(respClone.body, respClone)
          respToCache.headers.delete('Cache-Control')
          respToCache.headers.delete('Expires')
          respToCache.headers.delete('Last-Modified')
          const abBody = await toArrayBuffer(respToCache.body!)
          if (abBody.byteLength <= 1024 * 1024 * 2) {
            await MiddleCacheHandler.putInCache(notImplementedCtx, cacheFetchResult.cacheKey, {
              headers: Array.from(respToCache.headers.entries()),
              status: respToCache.status,
              body: Buffer.from(abBody),
            })
          }
        }
      }
    }
  }
}
