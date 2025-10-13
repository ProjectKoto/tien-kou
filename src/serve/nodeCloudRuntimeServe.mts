import { serve as nodeServe } from '@hono/node-server'
import fs from "node:fs"
import path from "node:path"
import { AnyObj, delayInitVal, l, makeConcatenatableRelPath, TkContext } from "../lib/common.mts"
import { HonoWithErrorHandler } from "../lib/hack.mts"
import { calcValidateFileSystemPathSync, defaultStaticGenBaseDir, nodeResolvePath } from '../lib/nodeCommon.mts'
import { tkEnvFromDevVarsFile } from '../nodeEnv.mts'
import { AbstractTkSqlLiquidHonoApp, HonoEnvTypeWithTkCtx, HonoProvideHandler, MainHonoProvideHandler, TkContextHlGetTkEnvHandler } from "./honoIntegrate.mts"
import { RuntimeCachedLiquidHandler } from "./liquidIntegrate.mts"
import { LiquidStaticGenFilterRegHandler, nodeGenStatic } from './nodeStaticGen'
import { AbstractTkSqlAssetFetchHandler, EAH, HC, KD, MainJsRuntimeCacheHandler, MainTkCtxHandler, NoMiddleCacheHandler, QueryLiveAssetSqlCommonParam, SingleInstanceCachePolicyHandler, TienKouApp, TienKouAssetFetchHandler, TkAppStartInfo, TkAssetInfo, TkAssetIsDirectoryError, TkAssetNotFoundError, WebRedirHeavyAssetHandler } from "./serveDef.mts"
import { LiquidTelegramMsgFilterRegHandler } from './tgIntegrate'
import { LiquidSqlFilterRegHandler, SqlTkDataPersistHandler, TkSqlAssetCategoryLogicHandler } from "./tkAssetCategoryLogic.mts"
import { TursoSqlDbHandler } from './tursoSql.mts'
import { gitSyncStaticGen } from '../lib/nodeGitUtil.mts'

if (process.platform === "freebsd") {
  console.error("freebsd not supported, readFile not returing EISDIR")
  process.exit(1)
}


const NodeJsCloudTienKouAssetFetchHandler = HC<TienKouAssetFetchHandler>()(async ({ SqlDbHandler,  TkFirstCtxProvideHandler, HeavyAssetHandler }: KD<"SqlDbHandler" | "TkFirstCtxProvideHandler" | "HeavyAssetHandler">) => {

  const super_ = await AbstractTkSqlAssetFetchHandler({ SqlDbHandler })

  const staticAssetFileSystemBasePath = delayInitVal<string>()

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async ctx0 => {
    staticAssetFileSystemBasePath.val = path.normalize(nodeResolvePath(ctx0.e.NODE_LOCAL_FS_STATIC_ASSET_BASE_PATH!))
  })
  
  return EAH(super_, {
    fetchLiveHeavyAssetBytes: async (_: { originFilePath: string }): Promise<{ asset_raw_bytes: ArrayBuffer | Buffer<ArrayBufferLike> }> => {
      throw new TkAssetNotFoundError('fetchLiveHeavyAsset not implemented').shouldLog()
    },
    fetchStaticAsset: async ({ locatorTopDir, locatorSubPath }: { tkCtx?: TkContext, locatorTopDir: string, locatorSubPath: string }): Promise<ArrayBuffer | Buffer<ArrayBufferLike>> => {
      const fileSystemPath = calcValidateFileSystemPathSync(staticAssetFileSystemBasePath.val, makeConcatenatableRelPath(locatorTopDir) + makeConcatenatableRelPath(locatorSubPath))
     
      try {
        const fileBytes = await fs.promises.readFile(fileSystemPath)
        return fileBytes
      } catch (e) {
        if ((e as AnyObj | undefined)?.code === "EISDIR") {
          throw new TkAssetIsDirectoryError('current static asset is a directory')
        }
        throw new TkAssetNotFoundError('fetch static asset not found, possibly because of file not found')
          .shouldLog((e as AnyObj).toString())
      }
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

const TienKouNodeJsCloudHonoApp = HC<TienKouApp<undefined>>()(async ({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
  IntegratedCachePolicyHandler,
  TkCtxHandler,
  HonoProvideHandler,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkCtxHandler", {
  HonoProvideHandler: HonoProvideHandler<HonoEnvTypeWithTkCtx<AnyObj>>
}>) => {

  const tkEnv = await tkEnvFromDevVarsFile()

  const super_ = await AbstractTkSqlLiquidHonoApp<HonoWithErrorHandler<HonoEnvTypeWithTkCtx<AnyObj>>>()({
    TienKouAssetFetchHandler,
    LiquidHandler,
    TienKouAssetCategoryLogicHandler,
    LiquidFilterRegisterHandlerList,
    IntegratedCachePolicyHandler,
    TkContextHlGetEHandler: {
      getTkEnvGetter: async () => _ => {
        return {
          ...tkEnv
        }
      },
    } as TkContextHlGetTkEnvHandler<HonoEnvTypeWithTkCtx<AnyObj>>,
    TkCtxHandler,
    HonoProvideHandler,
  })

  const realServeHttp = async (): Promise<TkAppStartInfo<undefined>> => {
    (super_.honoApp as AnyObj)['hostname'] = tkEnv.NODE_CLOUD_RT_LISTEN_HOST || '127.0.0.1'
    ;(super_.honoApp as AnyObj)['port'] = Number.parseInt(tkEnv.NODE_CLOUD_RT_LISTEN_PORT || "8571")

    const nodeServer = nodeServe(super_.honoApp, (info) => {
      l(`Listening on http://${info.address}:${info.port}`)
    })
    
    return {
      defaultExportObject: undefined,
      waitForAppEndPromise: new Promise((rs, _rj) => {
        nodeServer.on('close', rs)
      })
    }
  }

  return EAH<typeof super_, TienKouApp<undefined>>(super_, {
    start: async (): Promise<TkAppStartInfo<undefined>> => {
      if ((tkEnv.PROCENV_TK_SERVE_SUB_MODE || '') === 'genStatic') {
        
        return await nodeGenStatic(tkEnv, TkCtxHandler, super_.honoApp, async () => {
          await gitSyncStaticGen(tkEnv)
        })()
      } else {
        return await realServeHttp()
      }
    }
  })

})

const nodeMain = async () => {

  const TkCtxHandler = await MainTkCtxHandler({})
  const TkFirstCtxProvideHandler = TkCtxHandler

  const SqlDbHandler = await TursoSqlDbHandler({
    TkFirstCtxProvideHandler,
  })

  const TkDataPersistHandler = await SqlTkDataPersistHandler({
    TkFirstCtxProvideHandler,
    SqlDbHandler,
  })

  const MiddleCacheHandler = await NoMiddleCacheHandler({
    TkFirstCtxProvideHandler,
  })

  const RuntimeCacheHandler = await MainJsRuntimeCacheHandler({
    TkFirstCtxProvideHandler,
  })

  const IntegratedCachePolicyHandler = await SingleInstanceCachePolicyHandler({
    MiddleCacheHandler,
    RuntimeCacheHandler,
    TkDataPersistHandler,
  })

  const HeavyAssetHandler = await WebRedirHeavyAssetHandler({
    TkFirstCtxProvideHandler,
  })

  const TienKouAssetFetchHandler = await NodeJsCloudTienKouAssetFetchHandler({
    TkFirstCtxProvideHandler,
    SqlDbHandler,
    HeavyAssetHandler,
  })

  const LiquidHandler = await RuntimeCachedLiquidHandler({
    TkFirstCtxProvideHandler,
    RuntimeCacheHandler,
  })

  const HonoProvideHandler = await MainHonoProvideHandler<HonoEnvTypeWithTkCtx<AnyObj>>()({})
  
  const LiquidFilterRegisterHandlerList = [
    await LiquidSqlFilterRegHandler({
      SqlDbHandler,
    }),
    await LiquidTelegramMsgFilterRegHandler({}),
    await LiquidStaticGenFilterRegHandler({
      HonoProvideHandler,
      TkAppSharedMutableCtxHandler: TkCtxHandler,
    }),
  ]

  const TienKouAssetCategoryLogicHandler = await TkSqlAssetCategoryLogicHandler({
    TienKouAssetFetchHandler,
  })

  const app = await TienKouNodeJsCloudHonoApp({
    TienKouAssetFetchHandler,
    IntegratedCachePolicyHandler,
    LiquidHandler,
    LiquidFilterRegisterHandlerList,
    TienKouAssetCategoryLogicHandler,
    TkCtxHandler,
    HonoProvideHandler,
  })

  const startInfo = await app.start()
  if (startInfo.waitForAppEndPromise !== undefined) {
    await startInfo.waitForAppEndPromise
  }
  process.exit(0)
}

// await nodeMain()
nodeMain()
