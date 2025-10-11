import { serve as nodeServe } from '@hono/node-server'
import { MarkdownDB } from "mddb"
import fs from "node:fs"
import path from "node:path"
import { AnyObj, delayInitVal, l, makeConcatenatableRelPath, TkError } from "../lib/common.mts"
import { HonoWithErrorHandler } from "../lib/hack.mts"
import { AbstractTkSqlLiquidHonoApp, HonoEnvTypeWithTkCtx, TkContextHlGetTkEnvHandler } from "./honoIntegrate.mts"
import { MainLiquidHandler } from "./liquidIntegrate.mts"
import { AbstractTkSqlAssetFetchHandler, EAH, HC, KD, MainJsRuntimeCacheHandler, MainTkCtxHandler, NoMiddleCacheHandler, QueryLiveAssetSqlCommonParam, SingleInstanceCachePolicyHandler, SqlDbHandler, StubHeavyAssetHandler, TienKouApp, TienKouAssetFetchHandler, TkAppStartInfo, TkAssetInfo, TkAssetIsDirectoryError, TkAssetNotFoundError } from "./serveDef.mts"
import { TkContext } from '../lib/common.mts'
import { LiquidSqlFilterRegHandler, SqlTkDataPersistHandler, TkSqlAssetCategoryLogicHandler } from "./tkAssetCategoryLogic.mts"
import { calcValidateFileSystemPathSync, nodeResolvePath } from '../lib/nodeCommon.mts'
import { tkEnvFromDevVarsFile } from '../nodeEnv.mts'
import { LiquidTelegramMsgFilterRegHandler } from './tgIntegrate'
import { Hono } from 'hono'
import { LiquidStaticGenFilterRegHandler } from './nodeStaticGen'

if (process.platform === "freebsd") {
  console.error("freebsd not supported, readFile not returing EISDIR")
  process.exit(1)
}

const MddbSqliteSqlDbHandler = HC<SqlDbHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  let mddb: MarkdownDB | undefined = undefined

  let mddbReadyResolver: ((value: MarkdownDB) => void)
  const mddbReadyPromise = new Promise<MarkdownDB>(r => { mddbReadyResolver = r })

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async _ctx0 => {
    mddb = await new MarkdownDB({
      client: "sqlite3",
      connection: {
        filename: "markdown.db",
      },
    }).init()
    if (mddbReadyResolver) {
      mddbReadyResolver(mddb)
    }
  })

  return {
    sql: async ({ sql, args }: { sql: string, args: string[] }) => {
      return (await (await mddbReadyPromise).db.raw(sql, args))
    },
  }
})


const NodeJsLocalFsTienKouAssetFetchHandler = HC<TienKouAssetFetchHandler>()(async ({ SqlDbHandler,  TkFirstCtxProvideHandler }: KD<"SqlDbHandler" | "TkFirstCtxProvideHandler">) => {

  const super_ = await AbstractTkSqlAssetFetchHandler({ SqlDbHandler })

  const liveAssetFileSystemRootPath = delayInitVal<string>()
  const staticAssetFileSystemRootPath = delayInitVal<string>()

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async ctx0 => {
    liveAssetFileSystemRootPath.val = path.normalize(nodeResolvePath(ctx0.e.NODE_LOCAL_FS_LIVE_ASSET_ROOT_PATH!))
    staticAssetFileSystemRootPath.val = path.normalize(nodeResolvePath(ctx0.e.NODE_LOCAL_FS_STATIC_ASSET_ROOT_PATH!))
  })
  
  return EAH(super_, {
    fetchLiveHeavyAssetBytes: async function ({ originFilePath }: { originFilePath: string }): Promise<{ asset_raw_bytes: ArrayBuffer | Buffer<ArrayBufferLike> }> {
      const fileSystemPath = calcValidateFileSystemPathSync(liveAssetFileSystemRootPath.val, originFilePath)
      return {
        asset_raw_bytes: await fs.promises.readFile(fileSystemPath),
      }
    },
    fetchStaticAsset: async ({ locatorTopDir, locatorSubPath }: { tkCtx?: TkContext, locatorTopDir: string, locatorSubPath: string }): Promise<ArrayBuffer | Buffer<ArrayBufferLike>> => {
      const fileSystemPath = calcValidateFileSystemPathSync(staticAssetFileSystemRootPath.val, makeConcatenatableRelPath(locatorTopDir) + makeConcatenatableRelPath(locatorSubPath))
     
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

    queryLiveAsset: async function (param: QueryLiveAssetSqlCommonParam): Promise<TkAssetInfo[]> {
      const sqlResult = await super_.queryLiveAssetSqlCommon(param)
      
      for (const x of sqlResult) {
        if (x.is_asset_heavy === 1) {
          if (param.shouldFetchRawBytesIfLight && (x.asset_raw_bytes === undefined || x.asset_raw_bytes === null)) {
            x.asset_raw_bytes = (await this.fetchLiveHeavyAssetBytes({ originFilePath: x.origin_file_path })).asset_raw_bytes
          }
        }
      }
    
      // l("final sqlResult", sqlResult)
      return sqlResult
    },
  })
})

const TienKouNodeJsLocalFsHonoApp = HC<TienKouApp<undefined>>()(async ({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
  IntegratedCachePolicyHandler,
  TkCtxHandler,
  staticGenEnabledSetter,
  honoSetter,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkCtxHandler", {
  staticGenEnabledSetter: (_: boolean) => void,
  honoSetter: (_: Hono) => void,
}>) => {

  const tkEnv = await tkEnvFromDevVarsFile()

  // applyTkEnvToProcessEnv(tkEnv)

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
  })

  honoSetter(super_.honoApp as unknown as Hono)

  const realServeHttp = async (): Promise<TkAppStartInfo<undefined>> => {
    (super_.honoApp as AnyObj)['hostname'] = tkEnv.NODE_LOCAL_FS_RT_LISTEN_HOST || '127.0.0.1'
    ;(super_.honoApp as AnyObj)['port'] = Number.parseInt(tkEnv.NODE_LOCAL_FS_RT_LISTEN_PORT || "8569")

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

  staticGenEnabledSetter(false)
  const genStatic = async (): Promise<TkAppStartInfo<undefined>> => {

    const genStaticUrl = new URL('http://pseudo-tien-kou-app.home.arpa/admin/genStatic')


    super_.option.isStaticGenFeatureEnabled = true
    staticGenEnabledSetter(true)

    const theTask = (async () => {
      await super_.honoApp.fetch(new Request(genStaticUrl))
    })()
    
    return {
      defaultExportObject: undefined,
      waitForAppEndPromise: theTask,
    }
  }

  return EAH<typeof super_, TienKouApp<undefined>>(super_, {
    start: async (): Promise<TkAppStartInfo<undefined>> => {
      if ((tkEnv.PROCENV_TK_SUB_MODE || '') === 'genStatic') {
        return await genStatic()
      } else {
        return await realServeHttp()
      }
    }
  })

})

const nodeMain = async () => {

  const TkCtxHandler = await MainTkCtxHandler({})
  const TkFirstCtxProvideHandler = TkCtxHandler

  const SqlDbHandler = await MddbSqliteSqlDbHandler({
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

  const HeavyAssetHandler = await StubHeavyAssetHandler({
    TkFirstCtxProvideHandler,
  })

  const TienKouAssetFetchHandler = await NodeJsLocalFsTienKouAssetFetchHandler({
    TkFirstCtxProvideHandler,
    SqlDbHandler,
    HeavyAssetHandler,
  })

  const LiquidHandler = await MainLiquidHandler({
    TkFirstCtxProvideHandler,
  })

  let staticGenEnabled = false
  let honoApp: Hono | undefined = undefined
  const LiquidFilterRegisterHandlerList = [
    await LiquidSqlFilterRegHandler({
      SqlDbHandler,
    }),
    await LiquidTelegramMsgFilterRegHandler({}),
    await LiquidStaticGenFilterRegHandler({
      enabledGetter: () => staticGenEnabled,
      honoGetter: () => honoApp!,
    }),
  ]

  const TienKouAssetCategoryLogicHandler = await TkSqlAssetCategoryLogicHandler({
    TienKouAssetFetchHandler,
  })

  const app = await TienKouNodeJsLocalFsHonoApp({
    TienKouAssetFetchHandler,
    IntegratedCachePolicyHandler,
    LiquidHandler,
    LiquidFilterRegisterHandlerList,
    TienKouAssetCategoryLogicHandler,
    TkCtxHandler,
    honoSetter: (v) => { honoApp = v },
    staticGenEnabledSetter: (v) => { staticGenEnabled = v },
  })
  
  const startInfo = await app.start()
  if (startInfo.waitForAppEndPromise !== undefined) {
    await startInfo.waitForAppEndPromise
  }
  process.exit(0)
}

// await nodeMain()
nodeMain()
