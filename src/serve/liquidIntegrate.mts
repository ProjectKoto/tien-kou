import Emittery from "emittery"
import * as liquid from "liquidjs"
import { Liquid, LiquidOptions } from "liquidjs"
import { LiquidCache } from "liquidjs/dist/cache"
import { default as markdownit } from 'markdown-it'
import { sed } from "sed-lite"
import replaceAll from 'string.prototype.replaceall'
import { AnyObj, allKnownAssetExtNames, bytesLikeToString, extensionListStrToSet, isAssetExtensionInList, isEndWithExtensionList, jsonPrettyStringify, liquidExtName, sqlGlobPatternEscape, sqlLikePatternEscape, TkErrorHttpAware, um } from "../lib/common.mts"
import { AHT, FetchBackstageAssetOpt, FetchGenericLocatableAssetOpt, FetchLocatableContentOpt, HT, KD, LiquidHandler, TienKouApp, TkAssetInfo, TkAssetIsHeavyError, TkAssetNotDedicatedError, TkAssetNotFoundError, TkContext } from './serveDef.mts'
import { isDedicatedAsset } from "./tkAssetCategoryLogic.mts"
import { tgMessageToHtml } from "../lib/tgCommon.mts"

export const isSqlAssetOpen = (asset: TkAssetInfo): boolean => {
  return (asset.fetched_asset_type as string).endsWith('Open')
}

export const MainLiquidHandler = HT<LiquidHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  let liquid: Liquid | undefined = undefined
  let liquidOptions: LiquidOptions | undefined = undefined

  const liquidPreCreateEvent = new Emittery()
  const liquidPostCreateEvent = new Emittery()
  const liquidFullyReadyEvent = new Emittery()

  let liquidReadyResolver: ((value: Liquid) => void)
  const liquidReadyPromise = new Promise<Liquid>(r => { liquidReadyResolver = r })

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async _ctx0 => {
    
    if (liquidOptions === undefined) {
      throw new Error('liquidOptions === undefined in MainLiquidHandler.listenOnFirstCtxForInit???')
    }

    await liquidPreCreateEvent.emitSerial('liquidPreCreate')
    liquid = new Liquid(liquidOptions)
    await r.mandatoryCustomizeLiquidOpt(liquidOptions)
    await liquidPostCreateEvent.emitSerial('liquidPostCreate')
    if (liquidReadyResolver) {
      liquidReadyResolver(liquid)
    }
    await liquidFullyReadyEvent.emitSerial('liquidFullyReady')
    
  })

  const listenOnLiquidPreCreate = (l: (ctx0: TkContext, liquidOptions: LiquidOptions) => Promise<void>): void => {
    liquidPreCreateEvent.on('liquidPreCreate', async () => {
      if (TkFirstCtxProvideHandler.firstCtx === undefined) {
        throw new Error('TkFirstCtxProvideHandler.firstCtx === undefined when listenOnLiquidPreCreate???')
      }
      if (liquidOptions === undefined) {
        throw new Error('liquidOptions === undefined when listenOnLiquidPreCreate???')
      }
      await l(TkFirstCtxProvideHandler.firstCtx, liquidOptions)
    })
  }

  const r = {
    get isLiquidReady() { return liquid !== undefined },
    get liquid() { return liquid },
    liquidReadyPromise,

    listenOnLiquidPreCreate,

    customizeLiquidOpt: listenOnLiquidPreCreate,

    mandatoryCustomizeLiquidOpt: async (liquidOptions: LiquidOptions): Promise<void> => {
      liquidOptions.cache = false
    },

    listenOnLiquidPostCreate: (l: (liquid: Liquid) => Promise<void>): void => {
      liquidPostCreateEvent.on('liquidPostCreate', async () => {
        if (liquid === undefined) {
          throw new Error('liquid === undefined when listenOnLiquidPostCreate???')
        }
        await l(liquid)
      })
    },

    registerFilterPostCreate: (name: string, filter: liquid.FilterImplOptions): void => {
      r.listenOnLiquidPostCreate(async (liquid) => {
        liquid.registerFilter(name, filter)
      })
    },

    initBaseLiquidOptions: async (inputLiquidOptions: LiquidOptions): Promise<void> => {
      liquidOptions = {
        ...inputLiquidOptions
      }
    },
  }
  r satisfies LiquidHandler
  return r
})

export const RuntimeCachedLiquidHandler = HT<LiquidHandler>()(async ({ TkFirstCtxProvideHandler, RuntimeCacheHandler }: KD<"TkFirstCtxProvideHandler" | "RuntimeCacheHandler">): Promise<LiquidHandler> => {

  const super_ = await MainLiquidHandler({ TkFirstCtxProvideHandler })

  super_.mandatoryCustomizeLiquidOpt = async (opt: LiquidOptions): Promise<void> => {
    opt.cache = true
  }

  RuntimeCacheHandler.listenOnEvict(async () => {
    const liquid = super_.liquid
    if (liquid) {
      // liquid.cache ???
      if (liquid.options.cache) {
        const lqCache = liquid.options.cache as LiquidCache & { clear: () => void }
        if (lqCache.clear) {
          lqCache.clear()
        } else {
          console.log("warn: !lqCache.clear ?????")
        }
      }
    }
  })

  return {
    ...super_,
  }

})


export interface TkSqlLiquidAssetQueryCommonParam {
  shouldThrowIfNotFound?: boolean
  shouldThrowIfNotExpectedCategory?: boolean,
  shouldConvertToString?: boolean,
}

export type WrapperOpt<T> = T & TkSqlLiquidAssetQueryCommonParam


export type ResultGenContext = AnyObj

export const AbstractTkSqlLiquidApp = <EO,> () => AHT<TienKouApp<EO>>()(async ({
  TienKouAssetFetchHandler,
  LiquidHandler,
  TienKouAssetCategoryLogicHandler,
  LiquidFilterRegisterHandlerList,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList">) => {

  const parseExtraOptFromLiquidShapeParams = <T extends AnyObj,>(optDefault: T, liquidShapeParams: unknown[]) => {
    const opt: T = {
      ...optDefault,
    }
    for (const p of liquidShapeParams) {
      if (p !== undefined && p !== null && Array.isArray(p) && p.length >= 2) {
        const k = p[0]
        if (typeof k === 'string') {
          (opt as AnyObj)[k] = p[1]
        }
      }
    }
    return opt
  }

  let justResolvedTemplateString: string | undefined = undefined

  const liquidMainOption: LiquidOptions = {
    cache: true,
    outputEscape: "escape",
    extname: liquidExtName,
    fs: {
      sep: '/',
  
      readFileSync(_file) {
        throw new Error("no impl for readFileSync")
      },
  
      async readFile (fileRelPath) {
        if (fileRelPath === 'justResolvedTemplate') {
          if (justResolvedTemplateString === undefined) {
            throw new TkErrorHttpAware("justResolvedTemplateString is undefined")
          }
          return justResolvedTemplateString
        }
  
        const queryResultList = await TienKouAssetFetchHandler.queryLiveAsset({
          locatorTopDirs: ['backstage/'],
          locatorSubPaths: [fileRelPath],
          shouldIncludeDerivingParent: false,
          shouldFetchRawBytes: true,
        })
  
        if (queryResultList.length === 0) {
          throw new TkAssetNotFoundError('live asset not found').shouldLog()
        }
    
        if (queryResultList[0].is_asset_heavy === 1) {
          throw new TkAssetIsHeavyError('live asset is heavy').shouldLog()
        }
  
        if (!queryResultList[0].asset_raw_bytes) {
          throw new TkErrorHttpAware('no asset_raw_bytes').shouldLog()
        }
  
        return bytesLikeToString(queryResultList[0].asset_raw_bytes)
      },
  
      existsSync() {
        throw new Error("no impl for existsSync")
      },
  
      async exists() {
        return true
      },
  
      contains(_root, _file) {
        return true
      },
  
      resolve(root, file, ext) {
        let fileRelPath = file
        if (!(fileRelPath.endsWith(ext))) {
          fileRelPath += ext
        }
        return fileRelPath
      },
  
      dirname(x) {
        const parts = replaceAll(x, '\\', '/').split('/') as string[]
        return parts.join('/')
      },
  
    },
    parseLimit: 1e8, // 100M?
    renderLimit: 3000, // limit each render to be completed in 1s
    memoryLimit: 32e6, // 32M
  }

  await LiquidHandler.initBaseLiquidOptions(liquidMainOption)

  LiquidHandler.registerFilterPostCreate("ctxSet", async function (v, k: string) {
    (this.context.environments as AnyObj)[k] = v
    return v
  })

  LiquidHandler.registerFilterPostCreate("ctxGet", async function (k) {
    return this.context.getSync([k])
  })

  LiquidHandler.registerFilterPostCreate("sleep", async function (a, t) {
    if (t === undefined) {
      t = a
    }
    if (t === undefined) {
      t = "1000"
    }
    const t1 = parseInt(t)
    if (!isNaN(t1) && t1 >= 0) {
      console.log('Sleeping...')
      await new Promise(r => { setTimeout(r, t1 <= 120000 ? t1 : 120000) })
      console.log('Sleep done.')
    }
    return a
  })

  // - Liquid result gen related functions/filters

  // -- fetch LocatableContent

  const fetchLocatableContentWrapper = async (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchLocatableContentOpt>) => {
    const pseudoRgc = {}
    const rgc = (rgcOptional ?? pseudoRgc) as {
      isNotLocatableContent?: boolean,
      isLocatableContent?: boolean,
      isLocatableContentNotFound?: boolean,
      isLocatableContentFound?: boolean,
      isLocatableContentDirectory?: boolean,
      isLocatableContentNotDirectory?: boolean,
      isLocatableContentHasDerivedChildren?: boolean,
      isLocatableContentHasNotDerivedChildren?: boolean,
      locatableContentAssetType?: string,
      isLocatableContentOpen?: boolean,
      isAssetHeavy?: boolean,
    }
    extraOpt = extraOpt ?? {}
  
    let asset: TkAssetInfo
    try {
      asset = await TienKouAssetCategoryLogicHandler.fetchLocatableContent(subPath, extraOpt)
    } catch (e) {
      if (e instanceof TkAssetNotDedicatedError) {
        if (extraOpt.shouldThrowIfNotExpectedCategory) {
          throw e
        } else {
          rgc.isNotLocatableContent = true
          rgc.isLocatableContent = false
          rgc.isLocatableContentNotFound = false
          rgc.isLocatableContentFound = false
          rgc.isLocatableContentDirectory = false
          rgc.isLocatableContentNotDirectory = false
          rgc.isLocatableContentHasDerivedChildren = false
          rgc.isLocatableContentHasNotDerivedChildren = false
          rgc.locatableContentAssetType = 'liveHeavy___'
          rgc.isLocatableContentOpen = false
          rgc.isAssetHeavy = false
          return undefined
        }
      } else if (e instanceof TkAssetNotFoundError) {
        if (!extraOpt.shouldThrowIfNotFound) {
          rgc.isNotLocatableContent = false
          rgc.isLocatableContent = true
          rgc.isLocatableContentNotFound = true
          rgc.isLocatableContentFound = false
          rgc.isLocatableContentDirectory = false
          rgc.isLocatableContentNotDirectory = true
          rgc.isLocatableContentHasDerivedChildren = false
          rgc.isLocatableContentHasNotDerivedChildren = true
          rgc.locatableContentAssetType = ''
          rgc.isLocatableContentOpen = false
          rgc.isAssetHeavy = false
          return undefined
        } else {
          throw new TkAssetNotFoundError("asset not found").setUserMessage('404: this URL doesn\'t relate to anything, so nothing is found... for now.')
        }
      } else {
        throw e
      }
    }
  
    if (asset.is_asset_heavy) {
      if (extraOpt.shouldThrowIfNotExpectedCategory) {
        throw new TkAssetIsHeavyError("asset is heavy")
      } else {
        rgc.isNotLocatableContent = true
        rgc.isLocatableContent = false
        rgc.isLocatableContentNotFound = false
        rgc.isLocatableContentFound = false
        rgc.isLocatableContentDirectory = false
        rgc.isLocatableContentNotDirectory = false
        rgc.isLocatableContentHasDerivedChildren = false
        rgc.isLocatableContentHasNotDerivedChildren = false
        rgc.locatableContentAssetType = ''
        rgc.isLocatableContentOpen = false
        rgc.isAssetHeavy = true
        return undefined
      }
    }
      
    rgc.isNotLocatableContent = false
    rgc.isLocatableContent = true
    rgc.isLocatableContentNotFound = false
    rgc.isLocatableContentFound = true
    rgc.locatableContentAssetType = asset.fetched_asset_type
    rgc.isLocatableContentOpen = isSqlAssetOpen(asset)
    rgc.isLocatableContentDirectory = !!asset.is_directory
    rgc.isLocatableContentNotDirectory = !asset.is_directory
    rgc.isLocatableContentHasDerivedChildren = !!asset.has_derived_children
    rgc.isLocatableContentHasNotDerivedChildren = !asset.has_derived_children
    rgc.isAssetHeavy = false
  
    let result: TkAssetInfo | string = asset
    if (extraOpt.shouldConvertToString) {
      result = bytesLikeToString(asset.asset_raw_bytes)
    }
  
    return result
  }

  {
    const filterFuncCommon: (defaultShouldConvertToString: boolean) => liquid.FilterImplOptions = (defaultShouldConvertToString) => async function (a, ...rest) {
      const { rgc } = this.context.getSync(['tkCtx']) as ResultGenContext
      const subPath = a ?? this.context.getSync(['locatableContentSubPath'])

      if (typeof subPath !== 'string') {
        throw new TkErrorHttpAware('in fetchLocatableContent(AsString): locatableContentSubPath is not a string')
      }

      const extraOpt = parseExtraOptFromLiquidShapeParams<WrapperOpt<FetchLocatableContentOpt>>({
        shouldReadOpenAssets: true,
        shouldThrowIfNotFound: false,
        shouldConvertToString: defaultShouldConvertToString,
      }, rest)

      return await fetchLocatableContentWrapper(rgc, subPath, extraOpt)
    }

    LiquidHandler.registerFilterPostCreate("fetchLocatableContent", filterFuncCommon(false))

    LiquidHandler.registerFilterPostCreate("fetchLocatableContentAsString", filterFuncCommon(true))
  }

  // -- fetch BackstageAsset

  const fetchBackstageAssetWrapper = async (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchBackstageAssetOpt> & {
    pathClassValidator: ((subPath: string) => boolean) | null,
    afterFetchHook: ((result: TkAssetInfo | string) => Promise<void>) | null,
    classNamePascal: string,
    classNameCamel: string,
  }) => {

    const {
      pathClassValidator,
      afterFetchHook,
      classNamePascal,
      classNameCamel,
    } = extraOpt

    const classCorrectFieldName = 'is' + classNamePascal
    const notFoundFieldName = 'is' + classNamePascal + 'NotFound'
    const foundFieldName = 'is' + classNamePascal + 'Found'
    const isHeavyFieldName = 'is' + classNamePascal + 'Heavy'
    const typeFieldName = classNameCamel + 'Type'

    const pseudoRgc = {}
    const rgc = (rgcOptional ?? pseudoRgc) as AnyObj
    extraOpt = extraOpt ?? {}

    if (pathClassValidator) {
      if (!pathClassValidator(subPath)) {
        rgc[classCorrectFieldName] = false
        rgc[notFoundFieldName] = false
        rgc[foundFieldName] = true
        rgc[isHeavyFieldName] = false
        rgc[typeFieldName] = ''

        return undefined
      }
    }

    let asset: TkAssetInfo
    try {
      asset = await TienKouAssetCategoryLogicHandler.fetchBackstageAsset(subPath, extraOpt)
    } catch (e) {
      if (e instanceof TkAssetNotFoundError) {
        if (pathClassValidator) {
          // needless to call pathClassValidator again, because bad path won't pass above
          rgc[classCorrectFieldName] = true
        }
        rgc[notFoundFieldName] = true
        rgc[foundFieldName] = false
        rgc[isHeavyFieldName] = false
        rgc[typeFieldName] = ''


        if (!extraOpt.shouldThrowIfNotFound) {
          return undefined
        } else {
          throw e
        }
      } else {
        if (pathClassValidator) {
          // needless to call pathClassValidator again, because bad path won't pass above
          rgc[classCorrectFieldName] = true
        }
        rgc[notFoundFieldName] = false
        rgc[foundFieldName] = false
        rgc[isHeavyFieldName] = false
        rgc[typeFieldName] = ''

        throw e
      }
    }

    if (asset.is_asset_heavy) {
      if (asset.asset_raw_bytes === undefined || asset.asset_raw_bytes === null) {
        if (extraOpt.shouldConvertToString) {
          throw new TkAssetIsHeavyError("didn't fetched asset bytes, possibly because asset is heavy")
        }
      }

      rgc[isHeavyFieldName] = true
    } else {
      rgc[isHeavyFieldName] = false
    }

    if (pathClassValidator) {
      // needless to call pathClassValidator again, because bad path won't pass above
      rgc[classCorrectFieldName] = true
    }

    rgc[notFoundFieldName] = false
    rgc[foundFieldName] = true
    rgc[typeFieldName] = asset.fetched_asset_type

    let result: TkAssetInfo | string = asset
    if (extraOpt.shouldConvertToString) {
      result = bytesLikeToString(asset.asset_raw_bytes)
    }

    if (afterFetchHook) {
      await afterFetchHook(result)
    }
    
    return result
  }

  const isTemplate = (x: string) => {
    return isEndWithExtensionList(x, liquidExtName, allKnownAssetExtNames)
  }

  let fetchCommonBackstageAssetCallInfo
  let fetchTemplateCallInfo

  {
    const backstageClassSubPathFieldName = (className: string) => className + 'SubPath'

    fetchCommonBackstageAssetCallInfo = (() => {
      const className = 'backstageAsset' as const
      
      return {
        className,
        subPathFieldName: backstageClassSubPathFieldName(className),
        func: async (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchBackstageAssetOpt>) => {
          return await fetchBackstageAssetWrapper(rgcOptional, subPath, {
            shouldConvertToString: false,
            ...extraOpt,
            pathClassValidator: null,
            liveLightOnly: false,
            afterFetchHook: null,
            classNameCamel: className,
            classNamePascal: className[0].toUpperCase() + className.substring(1),
          })
        },
      }
    })()

    fetchTemplateCallInfo = (() => {
      const className = 'template' as const
      
      return {
        className,
        subPathFieldName: backstageClassSubPathFieldName(className),
        func: async (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchBackstageAssetOpt>) => {
          return (await fetchBackstageAssetWrapper(rgcOptional, subPath, {
            shouldConvertToString: true,
            ...extraOpt,
            pathClassValidator: path => isTemplate(path),
            liveLightOnly: true,
            afterFetchHook: async (result) => {
              justResolvedTemplateString = result as string
            },
            classNameCamel: className,
            classNamePascal: className[0].toUpperCase() + className.substring(1),
          })) as string | undefined
        },
      }
    })()
  }

  {
    const filterFuncCommon: (defaultShouldConvertToString: boolean, classCallInfo: { className: string, subPathFieldName: string, func: (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchBackstageAssetOpt>) => Promise<string | AnyObj | undefined>}) => liquid.FilterImplOptions = (defaultShouldConvertToString, classCallInfo) => async function (a, ...rest) {
      const { rgc } = this.context.getSync(['tkCtx']) as ResultGenContext
      const subPath = a ?? this.context.getSync([classCallInfo.subPathFieldName])

      if (typeof subPath !== 'string') {
        throw new TkErrorHttpAware('subPath is not a string')
      }

      const extraOpt = parseExtraOptFromLiquidShapeParams<WrapperOpt<FetchBackstageAssetOpt>>({
        shouldFetchLiveHeavyAssetBytes: true,
        shouldThrowIfNotFound: false,
        shouldConvertToString: defaultShouldConvertToString,
      }, rest)

      return await classCallInfo.func(rgc, subPath, extraOpt)
    }

    LiquidHandler.registerFilterPostCreate("fetchBackstageAsset", filterFuncCommon(false, fetchCommonBackstageAssetCallInfo))

    LiquidHandler.registerFilterPostCreate("fetchBackstageAssetAsString", filterFuncCommon(true, fetchCommonBackstageAssetCallInfo))

    LiquidHandler.registerFilterPostCreate("fetchTemplate", filterFuncCommon(false, fetchTemplateCallInfo))

    LiquidHandler.registerFilterPostCreate("fetchTemplateAsString", filterFuncCommon(true, fetchTemplateCallInfo))
  }

  // -- fetch GenericLocatable

  const fetchGenericLocatableAssetWrapper = async (rgcOptional: ResultGenContext, subPath: string, extraOpt: WrapperOpt<FetchGenericLocatableAssetOpt>) => {

    const pseudoRgc = {}
    const rgc = (rgcOptional ?? pseudoRgc) as {
      isGenericLocatableAssetHeavy?: boolean,
      isGenericLocatableAssetStatic?: boolean,
      genericLocatableAssetType?: string,
      isGenericLocatableAssetNotFound?: boolean,
      isGenericLocatableAssetFound?: boolean,
    }
    extraOpt = extraOpt ?? {}

    let asset: TkAssetInfo
    try {
      asset = await TienKouAssetCategoryLogicHandler.fetchGenericLocatableAsset(subPath, extraOpt)
    } catch (e) {
      if (e instanceof TkAssetNotFoundError) {
        rgc.isGenericLocatableAssetHeavy = false
        rgc.isGenericLocatableAssetStatic = false
        rgc.genericLocatableAssetType = ''
        rgc.isGenericLocatableAssetNotFound = true
        rgc.isGenericLocatableAssetFound = false


        if (!extraOpt.shouldThrowIfNotFound) {
          return undefined
        } else {
          throw e
        }
      } else {
        rgc.isGenericLocatableAssetHeavy = false
        rgc.isGenericLocatableAssetStatic = false
        rgc.genericLocatableAssetType = ''
        rgc.isGenericLocatableAssetNotFound = false
        rgc.isGenericLocatableAssetFound = false

        throw e
      }
    }

    if (asset.fetched_asset_type.startsWith('static')) {
      rgc.isGenericLocatableAssetHeavy = false
      rgc.isGenericLocatableAssetStatic = true
    } else if (asset.is_asset_heavy) {
      if (asset.asset_raw_bytes === undefined || asset.asset_raw_bytes === null) {
        if (extraOpt.shouldConvertToString) {
          throw new TkAssetIsHeavyError("didn't fetched asset bytes, possibly because asset is heavy")
        }
      }
      rgc.isGenericLocatableAssetHeavy = true
      rgc.isGenericLocatableAssetStatic = false
    } else {
      rgc.isGenericLocatableAssetHeavy = false
      rgc.isGenericLocatableAssetStatic = false
    }

    rgc.isGenericLocatableAssetFound = true
    rgc.isGenericLocatableAssetNotFound = false
    rgc.genericLocatableAssetType = asset.fetched_asset_type


    const result: TkAssetInfo = asset
    if (extraOpt.shouldConvertToString) {
      throw new TkErrorHttpAware("shouldConvertToString not supported for fetchGenericLocatableAssetWrapper")
    }

    return result
  }

  {
    LiquidHandler.registerFilterPostCreate("fetchGenericLocatableAsset", async function (a, ...rest) {

      const { rgc } = this.context.getSync(['tkCtx']) as ResultGenContext
      const subPath = a ?? this.context.getSync(['genericLocatableAssetSubPath'])

      if (typeof subPath !== 'string') {
        throw new TkErrorHttpAware('in fetchLocatableContent: locatableContentSubPath is not a string')
      }

      const extraOpt = parseExtraOptFromLiquidShapeParams<WrapperOpt<FetchGenericLocatableAssetOpt>>({
        
      }, rest)

      return await fetchGenericLocatableAssetWrapper(rgc, subPath, extraOpt)
    })

  }

  LiquidHandler.registerFilterPostCreate("isDedicatedAsset", isDedicatedAsset)
  LiquidHandler.registerFilterPostCreate("sed", function (a, sedScript) {
    return sed(sedScript)(a)
  })
  LiquidHandler.registerFilterPostCreate("isEndWithExtension", (a, b) => isEndWithExtensionList(a, b, allKnownAssetExtNames))
  LiquidHandler.registerFilterPostCreate("isEndWithExtensionList", (a, b) => isEndWithExtensionList(a, b, allKnownAssetExtNames))
  LiquidHandler.registerFilterPostCreate("isEndWithExtensionRaw", isEndWithExtensionList)
  LiquidHandler.registerFilterPostCreate("isEndWithExtensionListRaw", isEndWithExtensionList)
  LiquidHandler.registerFilterPostCreate("isAssetExtensionInList", isAssetExtensionInList)
  LiquidHandler.registerFilterPostCreate("isAssetExtension", isAssetExtensionInList)

  const md = markdownit({
    html: true,
    xhtmlOut: true,
    linkify: true,
  })

  const mdSafe = markdownit({
    html: false,
    xhtmlOut: true,
    linkify: true,
  })

  LiquidHandler.registerFilterPostCreate("md", async function (a) {
    const result = md.render(bytesLikeToString(a))
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdInline", async function (a) {
    const result = md.renderInline(bytesLikeToString(a))
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdSafe", async function (a) {
    const result = mdSafe.render(bytesLikeToString(a))
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdSafeInline", async function (a) {
    const result = mdSafe.renderInline(bytesLikeToString(a))
    return result
  })

  LiquidHandler.registerFilterPostCreate("throwWithUserMessage", async function (a) {
    throw new TkErrorHttpAware(um(a))
  })

  LiquidHandler.registerFilterPostCreate("forceFalse", function (a) { return false })
  LiquidHandler.registerFilterPostCreate("forceTrue", function (a) { return true })
  LiquidHandler.registerFilterPostCreate("sqlLikePatternEscape", sqlLikePatternEscape)
  LiquidHandler.registerFilterPostCreate("sqlGlobPatternEscape", sqlGlobPatternEscape)
  

  LiquidHandler.registerFilterPostCreate("listAssetChildren", async function (parent, shouldIncludeDirectories, shouldIncludeDerivingParent, extensionListStr, orderBy, pageNum, pageSize, customSql, ...customSqlArgs) {
    const result = await TienKouAssetFetchHandler.queryLiveAsset({
      locatorTopDirs: ["guarded/", "open/"],
      locatorSubParents: [parent],
      shouldIncludeDirectories,
      shouldIncludeDerivingParent,
      extensions: [...extensionListStrToSet(extensionListStr)],
      shouldFetchRawBytes: true,
      orderBy,
      pageNum,
      pageSize,
      // TODO: this is sql specific
      customSql,
      customSqlArgs,
    })
    return result
  })

  LiquidHandler.registerFilterPostCreate("listAssetDescendants", async function (ancestor, orderBy, pageNum, pageSize, customSql, ...customSqlArgs) {
    return await TienKouAssetFetchHandler.queryLiveAsset({
      locatorTopDirs: ["guarded/", "open/"],
      locatorSubAncestors: [ancestor],
      shouldIncludeDerivingParent: true,
      shouldFetchRawBytes: true,
      orderBy,
      pageNum,
      pageSize,
      // TODO: this is sql specific
      customSql,
      customSqlArgs,
    })
  })

  LiquidHandler.registerFilterPostCreate("tgMsgRender", async function (x) {
    return await tgMessageToHtml(x)
  })

  LiquidHandler.registerFilterPostCreate("btos", bytesLikeToString)

  LiquidHandler.registerFilterPostCreate("json", function (o) {
    return jsonPrettyStringify(o)
  })

  if (LiquidFilterRegisterHandlerList && LiquidFilterRegisterHandlerList.length > 0) {
    for (const frh of LiquidFilterRegisterHandlerList) {
      frh.doRegister((a, b) => {
        LiquidHandler.registerFilterPostCreate(a, b)
      })
    }
  }
  
  return {
    start: null,
    fetchLocatableContentWrapper,
    fetchBackstageAssetWrapper,
    fetchGenericLocatableAssetWrapper,
    fetchTemplateWrapper: fetchTemplateCallInfo.func,
  }
})
