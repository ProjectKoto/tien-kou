import { allKnownAssetExtNames, dedicatedAssetExtNames, isEndWithExtensionList, isInExtensionList, stripExtensionList, strippedInLocatorExtNames, TkError } from "../lib/common.mts"
import { FetchBackstageAssetOpt, FetchGenericLocatableAssetOpt, FetchLocatableContentOpt, HC, KD, LiquidFilterRegisterHandler, TienKouAssetCategoryLogicHandler, TkAssetInfo, TkAssetIsHeavyError, TkAssetNotDedicatedError, TkAssetNotFoundError, TkDataPersistHandler } from "./serveDef.mts"

export const isDedicatedAsset = (subPath: string) => {
  return isEndWithExtensionList(subPath, dedicatedAssetExtNames, allKnownAssetExtNames)
}

export const SqlTkDataPersistHandler = HC<TkDataPersistHandler>()(async({ TkFirstCtxProvideHandler, SqlDbHandler, }: KD<"TkFirstCtxProvideHandler" | "SqlDbHandler">): Promise<TkDataPersistHandler> => {

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async _ctx0 => {
  })

  return {
    fetchDataVersion: async (ctx): Promise<number> => {
      return (
        await SqlDbHandler.sql({
          tkCtx: ctx,
          sql: `
            SELECT last_update_timestamp FROM tk_meta_meta_info
              ORDER BY id DESC
              LIMIT 1
          `,
          args: [  ],
        })
      )[0].last_update_timestamp.toString()
    },
    fetchKv: async (ctx, k) => {
      // bacause nothing should be directly changed online; should change it in mddb
      throw new TkError('not implemented')
      const r =
        await SqlDbHandler.sql({
          tkCtx: ctx,
          sql: `
            SELECT v FROM tk_meta_kv
              WHERE k = ?
              ORDER BY id DESC
              LIMIT 1
          `,
          args: [ k ],
        })
      
      if (r.length === 0) {
        return undefined
      }

      return JSON.parse(r[0].v)
    },

    storeKv: async (ctx, k, v) => {
      // bacause nothing should be directly changed online; should change it in mddb
      throw new TkError('not implemented')
      await SqlDbHandler.sql({
        tkCtx: ctx,
        sql: `
          INSERT INTO tk_meta_kv(id, k, v)
            VALUES(NULL, ?, ?)
            ON CONFLICT(k) DO UPDATE SET
              v = excluded.v
        `,
        args: [ k, JSON.stringify(v) ],
      })
    },
  }
})


export const LiquidSqlFilterRegHandler = HC<LiquidFilterRegisterHandler>()(async ({ SqlDbHandler } : KD<"SqlDbHandler">) => {
  return {
    doRegister: (reg) => {
      reg("sql", async function (sql, ...args) {
        return await SqlDbHandler.sql({ sql, args })
      })
    }
  }
})

export const TkSqlAssetCategoryLogicHandler = HC<TienKouAssetCategoryLogicHandler>()(async ({ TienKouAssetFetchHandler }: KD<"TienKouAssetFetchHandler">) => {
  return {

    // throws: TkAssetNotDedicatedError TkAssetNotFoundError TkAssetIsHeavyError
    fetchLocatableContent: async (subPath: string, extraOpt: FetchLocatableContentOpt): Promise<TkAssetInfo> => {

      // // Because ????? 
      // if (!isDedicatedAsset(subPath)) {
      //   throw new TkAssetNotDedicatedError()
      // }

      let locatorTopDirsToSearch = ["guarded/", "open/"]
      if (!extraOpt.shouldReadOpenAssets) {
        locatorTopDirsToSearch = ["guarded/"]
      }

      const [_, subPathTryStrippedExt, ext] = stripExtensionList(subPath, allKnownAssetExtNames)

      const queryResultList = await TienKouAssetFetchHandler.queryLiveAsset({
        locatorTopDirs: locatorTopDirsToSearch,
        locatorSubPaths: (subPathTryStrippedExt !== subPath && isInExtensionList(ext, strippedInLocatorExtNames)) ? [subPath, subPathTryStrippedExt] : [subPath],
        shouldIncludeDerivingParent: true,
        shouldFetchRawBytes: true,
      })

      if (queryResultList.length === 0) {
        throw new TkAssetNotFoundError()
      }

      const asset = queryResultList[0]

      if (!asset.is_directory && !isDedicatedAsset(asset.origin_file_path)) {
        throw new TkAssetNotDedicatedError()
      }

      if (asset.is_asset_heavy) {
        throw new TkAssetIsHeavyError("asset is heavy")
      }

      const matchedTopDir = locatorTopDirsToSearch[asset.top_dir_index]

      asset.fetched_asset_type = (matchedTopDir === 'guarded/') ? 'liveLightGuarded' : 'liveLightOpen'

      return asset
    },

    // throws: TkAssetNotFoundError
    fetchBackstageAsset: async (subPath: string, extraOpt: FetchBackstageAssetOpt): Promise<TkAssetInfo> => {
      const queryResultList = await TienKouAssetFetchHandler.queryLiveAsset({
        locatorTopDirs: ["backstage/"],
        locatorSubPaths: [subPath],
        shouldIncludeDerivingParent: true,
        shouldFetchRawBytes: true,
      })

      let asset: TkAssetInfo

      if (queryResultList.length === 0) {
        // not found, search static
        if (extraOpt.liveLightOnly) {
          throw new TkAssetNotFoundError("not found")
        }
        asset = await TienKouAssetFetchHandler.fetchStaticAsset({
          locatorTopDir: "backstage/",
          locatorSubPath: subPath,
        })
        asset.fetched_asset_type = 'staticBackstage'
        return asset
      }

      asset = queryResultList[0]

      if (asset.is_asset_heavy) {
        if (asset.asset_raw_bytes === undefined || asset.asset_raw_bytes === null) {
          if (extraOpt.shouldFetchLiveHeavyAssetBytes && !extraOpt.liveLightOnly) {
            asset = {
              ...asset,
              ...await TienKouAssetFetchHandler.fetchLiveHeavyAssetBytes({
                originFilePath: asset.origin_file_path,
              })
            }
          }
        }

        asset.fetched_asset_type = 'liveHeavyBackstage'
      } else {
        asset.fetched_asset_type = 'liveLightBackstage'
      }
      return asset
    },

    // throws: TkAssetNotFoundError
    fetchGenericLocatableAsset: async (subPath: string, extraOpt: FetchGenericLocatableAssetOpt): Promise<TkAssetInfo> => {

      const locatorTopDirsToSearch = ["guarded/", "open/"]

      const result = await TienKouAssetFetchHandler.queryLiveAsset({
        locatorTopDirs: locatorTopDirsToSearch,
        locatorSubPaths: [subPath],
        shouldIncludeDerivingParent: false,
        shouldFetchRawBytes: true,
      })

      let asset: TkAssetInfo

      if (result.length === 0) {
        // not found, search static
        asset = await TienKouAssetFetchHandler.fetchStaticAsset({
          locatorTopDir: "open/",
          locatorSubPath: subPath,
        })
        asset.fetched_asset_type = 'staticOpen'
        return asset
      }

      asset = result[0]

      const matchedTopDir = locatorTopDirsToSearch[asset.top_dir_index]

      if (asset.is_asset_heavy) {
        if ((asset.asset_raw_bytes === undefined || asset.asset_raw_bytes === null) && extraOpt.shouldFetchLiveHeavyAssetBytes) {
          asset = {
            ...asset,
            ...await TienKouAssetFetchHandler.fetchLiveHeavyAssetBytes!({
              originFilePath: asset.origin_file_path,
            })
          }
        }
        asset.fetched_asset_type = (matchedTopDir === 'guarded/') ? 'liveHeavyGuarded' : 'liveHeavyOpen'
      } else {
        asset.fetched_asset_type = (matchedTopDir === 'guarded/') ? 'liveLightGuarded' : 'liveLightOpen'
      }

      return asset
    },
  }
})
