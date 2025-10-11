
import { Hono } from "hono"
import { ResultGenContext } from "./liquidIntegrate.mts"
import { HC, KD, LiquidFilterRegisterHandler } from "./serveDef.mts"
import fs from 'node:fs'
import path from 'node:path'
import { ensureParentDirExists, ensurePathDirExists, staticGenBaseDir, toArrayBuffer } from "../lib/nodeCommon.mts"
import { l, le, TkErrorHttpAware } from "../lib/common.mts"

export const LiquidStaticGenFilterRegHandler = HC<LiquidFilterRegisterHandler>()(async ({ honoGetter, enabledGetter } : KD<never, { honoGetter: () => Hono, enabledGetter: () => boolean }>) => {
  return {
    doRegister: (reg) => {
      reg("genStaticPageWithAncestors", async function genStaticPageWithAncestors(webReqPath: string) {
        const honoApp = honoGetter()
        const isEnabled = enabledGetter()
        if (!isEnabled) {
          throw new TkErrorHttpAware("genStaticPageWithAncestors is not enabled")
        }
        const rgc = this.context.getSync(['rgc']) as ResultGenContext

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
        const pathParts = decodeURI(webReqPath).split('/').filter(x => x !== '')
        const pathTidy = pathParts.join('/')

        try {
          
          for (let i = 0; i < pathParts.length; i++) {
            const ancestorPathTidy = pathParts.slice(0, i).join('/')
            if (!pageAlreadyGenMemo[ancestorPathTidy]) {
              await genStaticPageWithAncestors.call(this, '/' + ancestorPathTidy)
            }
          }

          const resp = await honoApp.fetch(new Request(new URL('http://pseudo-tien-kou-app.home.arpa' + webReqPath)), rgc.tkCtx().honoCtx.env)

          if (resp.status === 302) {
            const location = resp.headers.get('Location')
            if (location) {
              const targetLocationPathTidy = new URL(location).pathname.split('/').filter(x => x !== '').join('/')
              if (!pageAlreadyGenMemo[targetLocationPathTidy]) {
                await genStaticPageWithAncestors.call(this, '/' + targetLocationPathTidy)
              }

              try {
                await fs.promises.cp(path.join(staticGenBaseDir, './' + targetLocationPathTidy), path.join(staticGenBaseDir, './' + pathTidy))
              } catch (e) {
                le('genStaticPageWithAncestors', webReqPath, '302 cp err: ', e)
              }
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
              await ensurePathDirExists(path.join(staticGenBaseDir, './' + pathTidy))
              await fs.promises.writeFile(path.join(staticGenBaseDir, './' + pathTidy, './index.html'), bodyBuffer)
            } else {
              await ensureParentDirExists(path.join(staticGenBaseDir, './' + pathTidy))
              await fs.promises.writeFile(path.join(staticGenBaseDir, './' + pathTidy), bodyBuffer)
            }
          }
        } catch (e) {
          le('genStaticPageWithAncestors', webReqPath, 'error', e)
        } finally {
          pageAlreadyGenMemo[pathTidy] = true
        }
      })

    }
  }
})
