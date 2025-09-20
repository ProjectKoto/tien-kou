import Emittery from "emittery"
import * as liquid from "liquidjs"
import { Liquid, LiquidOptions } from "liquidjs"
import { LiquidCache } from "liquidjs/dist/cache"
import { default as markdownit } from 'markdown-it'
import { sed } from "sed-lite"
import replaceAll from 'string.prototype.replaceall'
import { AnyObj, allKnownAssetExtNames, bytesLikeToString, extensionListStrToSet, isAssetExtensionInList, isEndWithExtensionList, jsonPrettyStringify, liquidExtName, sqlGlobPatternEscape, sqlLikePatternEscape, TkErrorHttpAware, um, stripExtensionList, l, le } from "../lib/common.mts"
import { AHT, EA, FetchBackstageAssetOpt, FetchGenericLocatableAssetOpt, FetchLocatableContentOpt, HT, KD, LiquidHandler, QueryLiveAssetCommonParam, TienKouApp, TkAssetInfo, TkAssetIsHeavyError, TkAssetNotDedicatedError, TkAssetNotFoundError } from './serveDef.mts'
import { TkContext } from '../lib/common.mts'
import { isDedicatedAsset } from "./tkAssetCategoryLogic.mts"
import { FilterHandler, FilterOptions, TagClass, TagImplOptions } from "liquidjs/dist/template"

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
    // TODO: is using r
    await r.mandatoryCustomizeLiquidOpt(liquidOptions)
    liquid = new Liquid(liquidOptions)
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
      if (liquidOptions.cache) {
        l(`warn: liquidOptions.cache is forcibly set to false, because MainLiquidHandler has no runtime cache lifecycle integration.`)
      }
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

    registerTagPostCreate: (name: string, tag: TagClass | TagImplOptions): void => {
      r.listenOnLiquidPostCreate(async (liquid) => {
        liquid.registerTag(name, tag)
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

  RuntimeCacheHandler.listenOnEvict(async () => {
    const liquid = super_.liquid
    if (liquid) {
      // liquid.cache ???
      if (liquid.options.cache) {
        const lqCache = liquid.options.cache as LiquidCache & { clear: () => void }
        if (lqCache.clear) {
          lqCache.clear()
          l("done lqCache.clear()")
        } else {
          l("warn: !lqCache.clear ?????")
        }
      }
    }
  })

  return EA(super_, {
    mandatoryCustomizeLiquidOpt: async (opt: LiquidOptions): Promise<void> => {
      opt.cache = true
    },
  })

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
  IntegratedCachePolicyHandler,
  TkProvideCtxFromNothingHandler,
}: KD<"LiquidHandler" | "TienKouAssetFetchHandler" | "TienKouAssetCategoryLogicHandler" | "LiquidFilterRegisterHandlerList" | "IntegratedCachePolicyHandler" | "TkProvideCtxFromNothingHandler">) => {

  const parseExtraOptFromLiquidShapeParams = <T extends AnyObj,>(optDefault: T, liquidShapeParams: unknown[], aliasMap: ({[x: string]: string}) = {}) => {
    const opt: T = {
      ...optDefault,
    }
    for (const p of liquidShapeParams) {
      if (p !== undefined && p !== null && Array.isArray(p) && p.length >= 2) {
        let k = p[0]
        let kMapped = aliasMap[k]
        if (kMapped) {
          k = kMapped
        }
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
        
        const templateAsset = await IntegratedCachePolicyHandler.getCachedVal(
          await TkProvideCtxFromNothingHandler.fetchTkCtxFromNothing(),
          'liquidMain:template::' + fileRelPath,
          async () => {
            const queryResultList = await TienKouAssetFetchHandler.queryLiveAsset({
              locatorTopDirs: ['backstage/'],
              locatorSubPaths: [fileRelPath],
              shouldIncludeDerivingParent: false,
              shouldFetchRawBytes: true,
            })
      
            if (queryResultList.length === 0) {
              throw new TkAssetNotFoundError('some related template not found: ' + fileRelPath).shouldLog()
            }
        
            if (queryResultList[0].is_asset_heavy === 1) {
              throw new TkAssetIsHeavyError('some related template is heavy:' + fileRelPath).shouldLog()
            }
      
            if (!queryResultList[0].asset_raw_bytes) {
              throw new TkErrorHttpAware('no asset_raw_bytes').shouldLog()
            }
            return queryResultList[0]
          }
        )
  
        return bytesLikeToString(templateAsset.asset_raw_bytes)
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
    // renderLimit: 3000, // limit each render to be completed in
    renderLimit: 10000, // limit each render to be completed in
    // memoryLimit: 32e6, // 32M,
    memoryLimit: 320e6,
    relativeReference: false,
  }

  await LiquidHandler.initBaseLiquidOptions(liquidMainOption)

  LiquidHandler.registerFilterPostCreate("rgcSet", async function (v, k: string) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    rgc[k] = v
    return v
  })

  LiquidHandler.registerFilterPostCreate("rgcGet", async function (k) {
    const rgc = (this.context.globals as AnyObj)['rgc']
    if (!rgc) {
      throw new TkErrorHttpAware('no rgc')
    }
    return rgc[k]
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

  LiquidHandler.registerFilterPostCreate("l", async function (...rest) {
    l('liquidLog', ...rest)
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
          rgc.locatableContentAssetType = 'live_______'
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
      const rgc = this.context.getSync(['rgc']) as ResultGenContext
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
      const rgc = this.context.getSync(['rgc']) as ResultGenContext
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

      const rgc = this.context.getSync(['rgc']) as ResultGenContext
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

  LiquidHandler.registerFilterPostCreate("toBoolean", function (a) { return !!a })
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

  LiquidHandler.registerFilterPostCreate("queryLiveAssetByRawLocators", async function (a, ...rest) {
    const rgc = this.context.getSync(['rgc']) as ResultGenContext
      const assetLocators = a ?? this.context.getSync(['liveAssetLocators'])

      if (typeof assetLocators !== 'object' || !Array.isArray(assetLocators)) {
        le('assetLocators is not an array', a, assetLocators)
        throw new TkErrorHttpAware('assetLocators is not an array')
      }

      const extraOpt = parseExtraOptFromLiquidShapeParams<WrapperOpt<QueryLiveAssetCommonParam>>({
        tkCtx: rgc.tkCtx(),
        locatorSubPaths: assetLocators,
        locatorTopDirs: [''],
        shouldIncludeDerivingParent: true,
        shouldFetchRawBytes: true,
        shouldThrowIfNotFound: false,
        shouldConvertToString: true,
      }, rest, {
        locatorPaths: 'locatorSubPaths',
      })


    const result = await TienKouAssetFetchHandler.queryLiveAsset(extraOpt)
    return result
  })

  LiquidHandler.registerFilterPostCreate("arrToObjByKey", function (a: AnyObj[], k1: string, k2: string | undefined) {
    const result: AnyObj = {}
    for (const el of a) {
      let t1 = el[k1]
      if (k2 !== undefined) {
        if (t1 !== undefined && t1 !== null) {
          t1 = t1[k2]
        }
      }
      if (t1 !== undefined && t1 !== null) {
        if (!(t1 in result)) {
          result[t1] = el
        }
      }
    }
    return result
  })

  const arrayInplaceSortFunc = (desc: boolean) => function (a: AnyObj[], k1: string, k2: string | undefined) {
    a.sort((el1, el2) => {
      let t1 = el1[k1]
      if (k2 !== undefined) {
        if (t1 !== undefined && t1 !== null) {
          t1 = t1[k2]
        }
      }
      let t2 = el2[k1]
      if (k2 !== undefined) {
        if (t2 !== undefined && t2 !== null) {
          t2 = t2[k2]
        }
      }
      let result
      if (t1 !== undefined && t1 !== null) {
        if (t2 !== undefined && t2 !== null) {
          result = t1 > t2 ? 1 : t1 < t2 ? -1 : 0
        } else {
          result = 1
        }
      } else {
        if (t2 !== undefined && t2 !== null) {
          result = -1
        } else {
          result = 0
        }
      }
      if (desc) {
        result = -result
      }
      return result
    })
    return a
  }
  
  LiquidHandler.registerFilterPostCreate("arrInplaceSortAscByKey", arrayInplaceSortFunc(false))
  LiquidHandler.registerFilterPostCreate("arrInplaceSortDescByKey", arrayInplaceSortFunc(true))

  LiquidHandler.registerFilterPostCreate("arrToGroupingObjByKey", function (a: AnyObj[], k1: string, k2: string | undefined) {
    const result: AnyObj = {}
    for (const el of a) {
      let t1 = el[k1]
      if (k2 !== undefined) {
        if (t1 !== undefined && t1 !== null) {
          t1 = t1[k2]
        }
      }
      if (t1 !== undefined && t1 !== null) {
        if (!(t1 in result)) {
          result[t1] = []
        }
        result[t1].push(el)
      }
    }
    return result
  })

  LiquidHandler.registerFilterPostCreate("newObj", function (a: undefined) {
    return {}
  })

  LiquidHandler.registerFilterPostCreate("gkeys", function (a) {
    l('keys', a)
    return Object.keys(a)
  })

  LiquidHandler.registerFilterPostCreate("values", function (a) {
    return Object.values(a)
  })

  LiquidHandler.registerFilterPostCreate("newArr", function (a: undefined) {
    return []
  })

  const arrayInplacePush: liquid.FilterImplOptions = function (a: AnyObj[], ...rest) {
    for (const b of rest) {
      a.push(b)
    }
    return a
  }
  const arrayInplacePushTo: liquid.FilterImplOptions = function (a, b: any[]) {
    b.push(a)
    return b
  }

  LiquidHandler.registerFilterPostCreate("inplacePush", arrayInplacePush)
  LiquidHandler.registerFilterPostCreate("inplace_push", arrayInplacePush)
  LiquidHandler.registerFilterPostCreate("inplacePushTo", arrayInplacePushTo)
  LiquidHandler.registerFilterPostCreate("inplace_push_to", arrayInplacePushTo)

  LiquidHandler.registerFilterPostCreate("applyFilters", async function(a, b: [string, ...any][] | undefined) {
    if (b === undefined) {
      return a
    }
    const filters = this.liquid.filters
    for (const fAndArgs of b) {
      let f: FilterHandler = filters[fAndArgs[0]] as FilterHandler
      if ((f as unknown as FilterOptions).handler) {
        f = (f as unknown as FilterOptions).handler
      }
      a = await f.call(this, a, ...fAndArgs.slice(1))
    }
    return a
  })

  LiquidHandler.registerFilterPostCreate("objSet", function (a: AnyObj, k: string, v) {
    if (v === undefined) {
      delete a[k]
    } else {
      a[k] = v
    }
  })

  LiquidHandler.registerFilterPostCreate("objSetIfAbsent", function (a: AnyObj, k: string, v) {
    if (a[k] !== undefined) {
      return
    }
    if (v === undefined) {
      delete a[k]
    } else {
      a[k] = v
    }
  })

  LiquidHandler.registerFilterPostCreate("setToObj", function (v, a: AnyObj, k: string) {
    if (v === undefined) {
      delete a[k]
    } else {
      a[k] = v
    }
  })

  LiquidHandler.registerFilterPostCreate("setToObjIfAbsent", function (v, a: AnyObj, k: string) {
    if (a[k] !== undefined) {
      return
    }
    if (v === undefined) {
      delete a[k]
    } else {
      a[k] = v
    }
  })

  LiquidHandler.registerFilterPostCreate("listAssetDescendants", async function (ancestor, shouldIncludeDerivingParent, orderBy, pageNum, pageSize, customSql, ...customSqlArgs) {
    return await TienKouAssetFetchHandler.queryLiveAsset({
      locatorTopDirs: ["guarded/", "open/"],
      locatorSubAncestors: [ancestor],
      shouldIncludeDerivingParent,
      shouldFetchRawBytes: true,
      orderBy,
      pageNum,
      pageSize,
      // TODO: this is sql specific
      customSql,
      customSqlArgs,
    })
  })

  // non-empty prepend
  LiquidHandler.registerFilterPostCreate("nPrepend", async function (s, a) {
    if (!s || typeof a !== 'string' || typeof s !== 'string') {
      return s
    }
    return a + s
  })

  // non-empty append
  LiquidHandler.registerFilterPostCreate("nAppend", async function (s, a) {
    if (!s || typeof a !== 'string' || typeof s !== 'string') {
      return s
    }
    return s + a
  })

  // non-empty then return
  LiquidHandler.registerFilterPostCreate("nRet", async function (s, r) {
    if (!s) {
      return s
    }
    return r
  })

  LiquidHandler.registerFilterPostCreate("startsWith", async function (s, s1) {
    if (!s || typeof s !== 'string' || typeof s1 !== 'string') {
      return false
    }
    return s.startsWith(s1)
  })

  LiquidHandler.registerFilterPostCreate("endsWith", async function (s, s1) {
    if (!s || typeof s !== 'string' || typeof s1 !== 'string') {
      return false
    }
    return s.endsWith(s1)
  })

  LiquidHandler.registerFilterPostCreate("btos", bytesLikeToString)

  LiquidHandler.registerFilterPostCreate("json", function (o) {
    return jsonPrettyStringify(o)
  })

  // Quickmacro wrapper

  const liquidRenderTagHashAdd = function (this: liquid.Tag & TagImplOptions, key: string, content: string) {
    ;(this.renderTag.hash as liquid.Hash).hash[key] = {
      content,
      kind: 1024, //quoted
      input: '<pseudo>',
      begin: 1,
      end: 2,
      file: undefined,
      getText() {
        return content
      },
      getPosition(): number[] {
        return [1,2]
      },
      size() {
        return 1
      }
    } as liquid.Token
  }

  const processQuickmacroInc = function (this: liquid.Tag & TagImplOptions) {
    let inc = this.renderTag.file as string | undefined
    if (inc === undefined || typeof inc !== 'string') {
      return
    }
    const parts = inc.split('?', 2)
    
    let subMacro: string | undefined

    if (parts.length === 2) {
      inc = parts[0]
      subMacro = parts[1]
    } else {
      const parts = inc.split('#', 2)
      if (parts.length === 2) {
        inc = parts[0]
        subMacro = parts[1]
      } else {
        let firstNonDigit = 0
        for (; firstNonDigit < inc.length; firstNonDigit++) {
          const currChar = inc[firstNonDigit]
          if (!(currChar >= '0' && currChar <= '9')) {
            break
          }
        }
        if (firstNonDigit < inc.length && firstNonDigit > 0) {
          inc = inc.substring(0, firstNonDigit)
          subMacro = inc.substring(firstNonDigit)
        }
      }
    }

    if (!inc) {
      inc = 'index'
    }

    if (inc.endsWith('/')) {
      inc = inc + 'index'
    }

    if (subMacro !== undefined) {
      liquidRenderTagHashAdd.call(this, 'subMacro', subMacro)
    }

    const [isEndWith, fileAfterStrip, _] = stripExtensionList(inc, liquidExtName)
    this.renderTag.file = "inc/q/" + fileAfterStrip + (isEndWith ? liquidExtName : "")
  }

  const liquidRenderTagHashFallback = function (this: liquid.Tag & TagImplOptions, key1: string, fallbackToKey: string) {
    if (!((this.renderTag.hash as liquid.Hash).hash[key1]) && ((this.renderTag.hash as liquid.Hash).hash[fallbackToKey])) {
      (this.renderTag.hash as liquid.Hash).hash[key1] = (this.renderTag.hash as liquid.Hash).hash[fallbackToKey]
    }
  }

  LiquidHandler.registerTagPostCreate("qw", {
    parse(tagToken: liquid.TagToken, remainingTokens: liquid.TopLevelToken[]) {
      ;(this as liquid.Tag & TagImplOptions).renderTag = new liquid.RenderTag(tagToken, remainingTokens, this.liquid, this.liquid.parser)

      processQuickmacroInc.call(this)

      ;(this as liquid.Tag & TagImplOptions).tpls = []
      let closed = false
      while(remainingTokens.length) {
        const token = remainingTokens.shift()
        if (token && ((token as AnyObj).name === 'endqw' || (token as AnyObj).name === 'wq')) {
          closed = true
          break
        }
        // parse token into template
        // parseToken() may consume more than 1 tokens
        // e.g. {% if %}...{% endif %}
        if (token !== undefined) {
          const tpl = ((this as liquid.Tag & TagImplOptions).liquid.parser as liquid.Parser).parseToken(token, remainingTokens)
          ;(this as liquid.Tag & TagImplOptions).tpls.push(tpl)
        }
      }
      if (!closed) throw new Error(`tag ${tagToken.getText()} not closed`)
    },
    * render(context, emitter) {
      const html: string = yield this.liquid.renderer.renderTemplates(this.tpls, context, undefined)

      liquidRenderTagHashAdd.call(this, 'qInner', html)
      liquidRenderTagHashFallback.call(this, 'subMacro', 's')
      
      yield * ((this as AnyObj).renderTag).render(context, emitter)
    },

    * children (partials: boolean, sync: boolean) {
      yield * (this as AnyObj).renderTag.children(partials, sync)
    },

    partialScope () {
      return (this as AnyObj).renderTag.partialScope()
    },

    * arguments () {
      yield * (this as AnyObj).renderTag.arguments()
    },
  })

  // Quickmacro
  LiquidHandler.registerTagPostCreate("q", {
    parse(tagToken: liquid.TagToken, remainingTokens: liquid.TopLevelToken[]) {
      ;(this as liquid.Tag & TagImplOptions).renderTag = new liquid.RenderTag(tagToken, remainingTokens, this.liquid, this.liquid.parser)

      processQuickmacroInc.call(this)
    },
    * render(context, emitter) {
      liquidRenderTagHashFallback.call(this, 'qInner', 'qi')
      liquidRenderTagHashFallback.call(this, 'subMacro', 's')
      yield * ((this as AnyObj).renderTag).render(context, emitter)
    },

    * children (partials: boolean, sync: boolean) {
      yield * (this as AnyObj).renderTag.children(partials, sync)
    },

    partialScope () {
      return (this as AnyObj).renderTag.partialScope()
    },

    * arguments () {
      yield * (this as AnyObj).renderTag.arguments()
    },
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
