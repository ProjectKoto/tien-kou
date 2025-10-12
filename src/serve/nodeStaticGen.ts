
import os from 'node:os'
import fs from 'node:fs'
import path from 'node:path'
import { le, TkErrorHttpAware } from "../lib/common.mts"
import { defaultStaticGenBaseDir, ensureParentDirExists, ensurePathDirExists, toArrayBuffer } from "../lib/nodeCommon.mts"
import { HonoProvideHandler, TkContextHl } from "./honoIntegrate.mts"
import { ResultGenContext } from "./liquidIntegrate.mts"
import { HC, KD, LiquidFilterRegisterHandler, TkAppStartInfo, TkCtxHandler } from "./serveDef.mts"
import { Hono } from 'hono'

export const LiquidStaticGenFilterRegHandler = HC<LiquidFilterRegisterHandler>()(async ({
  HonoProvideHandler,
  TkAppSharedMutableCtxHandler,
// eslint-disable-next-line @typescript-eslint/no-explicit-any
} : KD<"TkAppSharedMutableCtxHandler", { HonoProvideHandler: HonoProvideHandler<any> }>) => {
  return {
    doRegister: (reg) => {
      reg("genStaticPageWithAncestors", async function genStaticPageWithAncestors(unencodedWebPath: string, search?: string | undefined, overwriteDestPath?: string | undefined) {
        if (!TkAppSharedMutableCtxHandler.appSharedMutableCtx.isStaticGenFeatureEnabled) {
          throw new TkErrorHttpAware("genStaticPageWithAncestors is not enabled")
        }

        const honoApp = HonoProvideHandler.getHono()
        const rgc = this.context.getSync(['rgc']) as ResultGenContext
        const staticGenBaseDir = (rgc.tkCtx() as TkContextHl<never>).tkEnv.NODE_STATIC_GEN_BASE_PATH || defaultStaticGenBaseDir
        const honoEnv = (rgc.tkCtx() as TkContextHl<never>).honoCtx.env
          

        type GenStaticPageCtx = {
          pageAlreadyGenMemo: undefined | Record<string, boolean>
        }

        if (rgc.genStaticPageCtx === undefined) {
          rgc.genStaticPageCtx = {}
        }

        const gsCtx = rgc.genStaticPageCtx as GenStaticPageCtx
        
        if (gsCtx.pageAlreadyGenMemo === undefined) {
          gsCtx.pageAlreadyGenMemo = {}
        }
        const pageAlreadyGenMemo = gsCtx.pageAlreadyGenMemo!
        const pathParts = unencodedWebPath.split('/').filter(x => x !== '')
        const pathTidy = pathParts.join('/')
        const overwriteDestPathTidy = overwriteDestPath === undefined ? undefined : overwriteDestPath.split('/').filter(x => x !== '').join('/')

        try {
          
          for (let i = 0; i < pathParts.length; i++) {
            const ancestorPathTidy = pathParts.slice(0, i).join('/')
            if (!pageAlreadyGenMemo[ancestorPathTidy]) {
              await genStaticPageWithAncestors.call(this, ancestorPathTidy)
            }
          }

          const currUrl = new URL('http://pseudo-tien-kou-app.home.arpa/' + encodeURI(pathTidy) + (search || ''))
          const resp = await honoApp.fetch(new Request(currUrl), honoEnv)

          if (resp.status === 302) {
            const location = resp.headers.get('Location')
            if (location) {
              const locationUrl = new URL(location, currUrl)
              const targetLocationPathTidy = locationUrl.pathname.split('/').filter(x => x !== '').join('/')
              // if (pageAlreadyGenMemo[targetLocationPathTidy]) {
              // }

              const modifiedUrlSearchParams = new URLSearchParams(locationUrl.searchParams)
              modifiedUrlSearchParams.set('isStaticGen302Target', '1')
              let modifiedUrlSearchParamsStr = modifiedUrlSearchParams.toString()
              if (modifiedUrlSearchParamsStr) {
                modifiedUrlSearchParamsStr = '?' + modifiedUrlSearchParamsStr
              }

              await genStaticPageWithAncestors.call(this, targetLocationPathTidy, modifiedUrlSearchParamsStr, pathTidy)

              // try {
              //   await fs.promises.cp(path.join(staticGenBaseDir, './' + targetLocationPathTidy), path.join(staticGenBaseDir, './' + pathTidy), {
              //     recursive: true,
              //     errorOnExist: true,
              //     dereference: true,
              //   })
              // } catch (e) {
              //   le('genStaticPageWithAncestors', unencodedWebPath, search, '302 cp err: ', e)
              // }
            }
          } else if (resp.status === 200) {
            const contentType = resp.headers.get('Content-Type')
            let bodyBuffer
            if (resp.body) {
              bodyBuffer = Buffer.from(await toArrayBuffer(resp.body))
            } else {
              bodyBuffer = Buffer.from("Nothing?")
            }
            if (contentType && /^text\/html(\s*;.*)?$/.test(contentType)) {
              await ensurePathDirExists(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy)))
              await fs.promises.writeFile(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy), './index.html'), bodyBuffer)
            } else if (contentType && /^application\/rss+xml(\s*;.*)?$/.test(contentType)) {
              await ensureParentDirExists(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy)))
              await fs.promises.writeFile(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy)), bodyBuffer)
            } else {
              await ensureParentDirExists(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy)))
              await fs.promises.writeFile(path.join(staticGenBaseDir, './' + (overwriteDestPathTidy || pathTidy)), bodyBuffer)
            }
          }
        } catch (e) {
          le('genStaticPageWithAncestors', unencodedWebPath, search, 'error', e)
        } finally {
          pageAlreadyGenMemo[pathTidy] = true
        }
      })

    }
  }
})

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const nodeGenStatic = (tkEnv: Record<string, string | undefined>, TkCtxHandler: TkCtxHandler, honoApp: Hono<any>, postTask: () => Promise<void>) => async (): Promise<TkAppStartInfo<undefined>> => {
  const genStaticUrl = new URL('http://pseudo-tien-kou-app.home.arpa/admin/genStatic')

  TkCtxHandler.appSharedMutableCtx.isStaticGenFeatureEnabled = true

  const theTask = (async () => {
    await fs.promises.rm(tkEnv.NODE_STATIC_GEN_BASE_PATH || defaultStaticGenBaseDir, { force: true, recursive: true, })
    await honoApp.fetch(new Request(genStaticUrl))
    await postTask()
  })()
  
  return {
    defaultExportObject: undefined,
    waitForAppEndPromise: theTask,
  }
}
