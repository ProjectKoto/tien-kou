import Emittery from "emittery"
import * as liquid from "liquidjs"
import { Liquid, LiquidOptions } from "liquidjs"
import { LiquidCache } from "liquidjs/dist/cache"
import { default as markdownit } from 'markdown-it'
import { Options as MditOptions } from 'markdown-it'
import { default as markdownItHashtag } from 'markdown-it-hashtag'
import { default as markdownItLinkAttributes } from 'markdown-it-link-attributes'
import { sed } from "sed-lite"
import replaceAll from 'string.prototype.replaceall'
import { AnyObj, allKnownAssetExtNames, bytesLikeToStringIgnoreError, extensionListStrToSet, isAssetExtensionInList, isEndWithExtensionList, jsonPrettyStringify, liquidExtName, sqlGlobPatternEscape, sqlLikePatternEscape, TkErrorHttpAware, um, stripExtensionList, l, le, quickStrHash, replaceAsync, unescapeSimple, removeLocatableAssetPathTopDir } from "../lib/common.mts"
import { AHC, EAH, FetchBackstageAssetOpt, FetchGenericLocatableAssetOpt, FetchLocatableContentOpt, HC, KD, LiquidHandler, QueryLiveAssetCommonParam, TienKouApp, TkAssetInfo, TkAssetIsHeavyError, TkAssetNotDedicatedError, TkAssetNotFoundError } from './serveDef.mts'
import { TkContext } from '../lib/common.mts'
import { isDedicatedAsset } from "./tkAssetCategoryLogic.mts"
import { FilterHandler, FilterOptions, TagClass, TagImplOptions } from "liquidjs/dist/template"
import * as liquidTokensType from "liquidjs/dist/tokens"
import * as mditRenderer from "markdown-it/lib/renderer.mjs"
import * as mditToken from "markdown-it/lib/token.mjs"
// import * as liquidjsJs from "liquidjs"

export const isSqlAssetOpen = (asset: TkAssetInfo): boolean => {
  return (asset.fetched_asset_type as string).endsWith('Open')
}

// copied and modified from liquidjs error.ts
function mkLiquidFilePosContext(token: liquid.Token) {
  


  const [line, col] = token.getPosition()
  const lines = token.input.split('\n')
  const begin = Math.max(line - 2, 1)
  const end = Math.min(line + 3, lines.length)

  const iRange: (number)[] = [-2]
  for (let i = begin; i <= end; i++) {
    iRange.push(i)
  }
  iRange.push(-1)
  
  const context = iRange
    .map(lineNumber => {
      if (lineNumber === -2) {
        let msg = '\n'
        if (token.file) msg += `liquid token context, file: ${token.file}`
        const [line, col] = token.getPosition()
        msg += `, line: ${line}, col: ${col}\n`
        return msg
      }
      if (lineNumber === -1) {
        return ''
      }
      const rowIndicator = (lineNumber === line) ? '>> ' : '   '
      const num = String(lineNumber).padStart(String(end).length)
      let text = `${rowIndicator}${num}| `

      const colIndicator = lineNumber === line
        ? '\n' + '^'.padStart(col + text.length)
        : ''

      text += lines[lineNumber - 1]
      text += colIndicator
      return text
    })
    .join('\n')

  return context
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const liquidFilterCommonProcessGetRgc = (liquidFilterImplThis: any /* liquidjs/dist/template FilterImpl */) => {
  const rgc = liquidFilterImplThis.context.getSync(['rgc']) as ResultGenContext
  // Fic: filter invocation context
  ;(rgc.tkCtx() as unknown as Record<string, string>)['liquidLastFic'] = mkLiquidFilePosContext(liquidFilterImplThis.token)
  return rgc
}

export const MainLiquidHandler = HC<LiquidHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">) => {

  let liquid: Liquid | undefined = undefined
  let liquidOptions: LiquidOptions | undefined = undefined

  const liquidPreCreateEvent = new Emittery()
  const liquidPostCreateEvent = new Emittery()
  const liquidFullyReadyEvent = new Emittery()

  let liquidReadyResolver: ((value: Liquid) => void)
  const liquidReadyPromise = new Promise<Liquid>(r => { liquidReadyResolver = r })

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async ctx0 => {
    
    if (liquidOptions === undefined) {
      throw new Error('liquidOptions === undefined in MainLiquidHandler.listenOnFirstCtxForInit???')
    }

    await liquidPreCreateEvent.emitSerial('liquidPreCreate')
    // TODO: is using r
    await r.mandatoryCustomizeLiquidOpt(liquidOptions)
    liquidOptions.timezoneOffset = (() => {
      const tzOffsetStr = ctx0.tkEnv.LIQUID_DATE_TIMEZONE_OFFSET_MINUTE
      if (tzOffsetStr) {
        return -Number.parseInt(tzOffsetStr)
      }
      return undefined
    })()
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

export const RuntimeCachedLiquidHandler = HC<LiquidHandler>()(async ({ TkFirstCtxProvideHandler, RuntimeCacheHandler }: KD<"TkFirstCtxProvideHandler" | "RuntimeCacheHandler">): Promise<LiquidHandler> => {

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

  return EAH(super_, {
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


export type ResultGenContext = {
  tkCtx: () => TkContext
} & AnyObj

export const AbstractTkSqlLiquidApp = <EO,> () => AHC<TienKouApp<EO>>()(async ({
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
        const kMapped = aliasMap[k]
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
    strictFilters: true,
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
              shouldFetchRawBytesIfLight: true,
            })
      
            if (queryResultList.length === 0) {
              throw new TkAssetNotFoundError('some related template not found: ' + fileRelPath).shouldLog()
            }
        
            if (queryResultList[0].is_asset_heavy === 1) {
              throw new TkAssetIsHeavyError('some related template is heavy:' + fileRelPath).shouldLog()
            }

            let assetBytes = queryResultList[0].asset_processed_bytes
            if (assetBytes === undefined || assetBytes === null) {
              assetBytes = queryResultList[0].asset_raw_bytes
            }
      
            if (assetBytes === undefined || assetBytes === null) {
              throw new TkErrorHttpAware('no asset_processed_bytes / asset_raw_bytes').shouldLog()
            }
            return queryResultList[0]
          }
        )
  
        return bytesLikeToStringIgnoreError(templateAsset.asset_processed_bytes ?? templateAsset.asset_raw_bytes)
      },
  
      existsSync() {
        throw new Error("no impl for existsSync")
      },
  
      async exists() {
        return true
      },
  
      async contains(_base, _file) {
        return true
      },
  
      resolve(base, file, ext) {
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
    // renderLimit: 17000, // limit each render to be completed in
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
    let logArgs = rest
    if (logArgs && logArgs.length >= 2) {
      logArgs = [rest[1], rest[0], ...rest.slice(2)]
    }
    l('liquidLog', ...logArgs)
  })

  LiquidHandler.registerFilterPostCreate("pageLog", async function (...rest) {
    let logArgs = rest
    if (logArgs && logArgs.length >= 2) {
      logArgs = [rest[1], rest[0], ...rest.slice(2)]
    }
    l('liquidPageLog', ...logArgs)
    throw new Error('pageLog as a liquid filter is not supported. please use the "tag" version.')
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

    extraOpt.tkCtx = extraOpt.tkCtx ?? rgcOptional?.tkCtx()
  
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
      result = bytesLikeToStringIgnoreError(asset.asset_processed_bytes ?? asset.asset_raw_bytes)
    }
  
    return result
  }

  {
    const filterFuncCommon: (defaultShouldConvertToString: boolean) => liquid.FilterImplOptions = (defaultShouldConvertToString) => async function (a, ...rest) {
      const rgc = liquidFilterCommonProcessGetRgc(this)
      const subPath = a ?? this.context.getSync(['locatableContentSubPath'])

      if (typeof subPath !== 'string') {
        throw new TkErrorHttpAware('in fetchLocatableContent(AsString): locatableContentSubPath is not a string')
      }

      const extraOpt = parseExtraOptFromLiquidShapeParams<WrapperOpt<FetchLocatableContentOpt>>({
        shouldReadOpenAssets: true,
        shouldThrowIfNotFound: false,
        shouldFetchRawBytesIfLight: true,
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

    extraOpt.tkCtx = extraOpt.tkCtx ?? rgcOptional?.tkCtx()

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
      const assetBytes = asset.asset_processed_bytes ?? asset.asset_raw_bytes
      if (assetBytes === undefined || assetBytes === null) {
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
      result = bytesLikeToStringIgnoreError(asset.asset_processed_bytes ?? asset.asset_raw_bytes)
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
      const rgc = liquidFilterCommonProcessGetRgc(this)
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

    extraOpt.tkCtx = extraOpt.tkCtx ?? rgcOptional?.tkCtx()

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
      const assetBytes = asset.asset_processed_bytes ?? asset.asset_raw_bytes
      if (assetBytes === undefined || assetBytes === null) {
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

      const rgc = liquidFilterCommonProcessGetRgc(this)
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

  // from markdownItLinkAttributes source code
  const markdownPrependUrlPrefix = function (md: markdownit) {
    const defaultRender = md.renderer.rules.link_open || function (
      tokens,
      idx,
      options,
      env,
      self
    ) {
      return self.renderToken(tokens, idx, options)
    }

    md.renderer.rules.link_open = function (tokens, idx, options, env, self) {
      const attrIndex = tokens[idx].attrIndex('href')

      if (attrIndex >= 0) {
        const attrs = tokens[idx].attrs
        if (attrs) {
          const existingVal = attrs[attrIndex][1]
          if (existingVal) {
            attrs[attrIndex][1] = env.rgc.urlPrefixWhenIsRss + existingVal
          }
        }
      }

      // pass token to default renderer.
      return defaultRender(tokens, idx, options, env, self)
    }
  }

  const markdownPrependUrlPrefixForHtml = function (md: markdownit) {
    const htmlBlockDefaultRender = md.renderer.rules.html_block || function (
      tokens,
      idx,
      options,
      env,
      self
    ) {
      return self.renderToken(tokens, idx, options)
    }
    const htmlInlineDefaultRender = md.renderer.rules.html_inline || function (
      tokens,
      idx,
      options,
      env,
      self
    ) {
      return self.renderToken(tokens, idx, options)
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const commonHtmlHandlerGen = (origRenderRule: mditRenderer.RenderRule) => function (tokens: mditToken.default[], idx: number, options: MditOptions, env: any, self: mditRenderer.default) {
      const existingContent = tokens[idx].content

      if (existingContent) {
        let newContent = replaceAll(existingContent, 'tien-kou-marker-for-url-prefix-replace="1" href="/', 'tien-kou-marker-for-url-prefix-replace="1" href="' + env.rgc.urlPrefixWhenIsRss + '/')
        newContent = replaceAll(newContent, 'tien-kou-marker-for-url-prefix-replace="1" src="/', 'tien-kou-marker-for-url-prefix-replace="1" src="' + env.rgc.urlPrefixWhenIsRss + '/')

        tokens[idx].content = newContent
      }

      return origRenderRule(tokens, idx, options, env, self)
    }

    md.renderer.rules.html_block = commonHtmlHandlerGen(htmlBlockDefaultRender)
    md.renderer.rules.html_inline = commonHtmlHandlerGen(htmlInlineDefaultRender)
  }

  const md = markdownit({
    html: true,
    xhtmlOut: true,
    linkify: true,
  })
    // See mddb parseFile.ts extractTags
    .use(markdownItHashtag, {
      hashtagRegExp: /(?:[^#"'\s]*[^#"'\s0-9][^#"'\s]*)/.source,
      preceding:     /^|\s+|\n+|\r+/.source,
    })
    .use(markdownPrependUrlPrefix)
    .use(markdownPrependUrlPrefixForHtml)
    .use(markdownItLinkAttributes, {
      attrs: {
        target: '_blank',
        // rel: 'noopener',
      }
    })

  md.renderer.rules.hashtag_open = function(tokens, idx, _options, env) {
    const tagName = tokens[idx].content
    return '<a  target="_blank" href="' + encodeURI(env.rgc.urlPrefixWhenIsRss) + '/tag/' + encodeURIComponent(tagName) + '" class="tag">'
  }

  const mdSafe = markdownit({
    html: false,
    xhtmlOut: true,
    linkify: true,
  })
    // See mddb parseFile.ts extractTags
    .use(markdownItHashtag, {
      hashtagRegExp: /(?:[^#"'\s]*[^#"'\s0-9][^#"'\s]*)/.source,
      preceding:     /^|\s+|\n+|\r+/.source,
    })
    .use(markdownPrependUrlPrefix)
    .use(markdownPrependUrlPrefixForHtml)
    .use(markdownItLinkAttributes, {
      attrs: {
        target: '_blank',
        // rel: 'noopener',
      }
    })

  md.renderer.rules.hashtag_open = function(tokens, idx, _options, env) {
    const tagName = tokens[idx].content
    return '<a  target="_blank" href="' + encodeURI(env.rgc.urlPrefixWhenIsRss) + '/tag/' + encodeURIComponent(tagName) + '" class="tag">'
  }

  LiquidHandler.registerFilterPostCreate("md", async function (a) {
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const result = md.render(bytesLikeToStringIgnoreError(a), { rgc })
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdInline", async function (a) {
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const result = md.renderInline(bytesLikeToStringIgnoreError(a), { rgc })
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdSafe", async function (a) {
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const result = mdSafe.render(bytesLikeToStringIgnoreError(a), { rgc })
    return result
  })

  LiquidHandler.registerFilterPostCreate("mdSafeInline", async function (a) {
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const result = mdSafe.renderInline(bytesLikeToStringIgnoreError(a), { rgc })
    return result
  })

  LiquidHandler.registerFilterPostCreate("throwWithUserMessage", async function (a) {
    throw new TkErrorHttpAware(um(a))
  })

  LiquidHandler.registerFilterPostCreate("toBoolean", function (a) { return !!a })
  LiquidHandler.registerFilterPostCreate("not", function (a) { return !a })
  LiquidHandler.registerFilterPostCreate("forceFalse", function (a) { return false })
  LiquidHandler.registerFilterPostCreate("forceTrue", function (a) { return true })
  LiquidHandler.registerFilterPostCreate("sqlLikePatternEscape", sqlLikePatternEscape)
  LiquidHandler.registerFilterPostCreate("sqlGlobPatternEscape", sqlGlobPatternEscape)
  

  LiquidHandler.registerFilterPostCreate("listAssetChildren", async function (parent, shouldIncludeDirectories, shouldIncludeDerivingParent, extensionListStr, orderBy, pageNum, pageSize, customSql, ...customSqlArgs) {
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const result = await TienKouAssetFetchHandler.queryLiveAsset({
      tkCtx: rgc.tkCtx(),
      locatorTopDirs: ["guarded/", "open/"],
      locatorSubParents: [parent],
      shouldIncludeDirectories,
      shouldIncludeDerivingParent,
      shouldIncludeWithoutPubTime: true,
      extensions: [...extensionListStrToSet(extensionListStr)],
      shouldFetchRawBytesIfLight: true,
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
    const rgc = liquidFilterCommonProcessGetRgc(this)
    const assetLocators = a ?? this.context.getSync(['liveAssetLocators'])

    if (assetLocators !== undefined && (typeof assetLocators !== 'object' || !Array.isArray(assetLocators))) {
      le('assetLocators is not an array', a, assetLocators)
      throw new TkErrorHttpAware('assetLocators is not an array')
    }

    const extraOpt = parseExtraOptFromLiquidShapeParams<QueryLiveAssetCommonParam>({
      tkCtx: rgc.tkCtx(),
      locatorSubPaths: assetLocators,
      locatorTopDirs: ["guarded/", "open/"],
      shouldIncludeDerivingParent: true,
      shouldFetchRawBytesIfLight: true,
      shouldIncludeWithoutPubTime: true,
    }, rest, {
      locatorPaths: 'locatorSubPaths',
    })

    const result = await TienKouAssetFetchHandler.queryLiveAsset(extraOpt)

    return result
  })

  LiquidHandler.registerFilterPostCreate("removeLocatableAssetPathTopDir", async function (a: string) {
    if (a === null || a === undefined) {
      return a
    }
    return removeLocatableAssetPathTopDir(a, true)
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

  LiquidHandler.registerFilterPostCreate("keys", function (a) {
    // l('keys', a)
    return Object.keys(a)
  })

  LiquidHandler.registerFilterPostCreate("values", function (a) {
    return Object.values(a)
  })

  LiquidHandler.registerFilterPostCreate("newArr", function (repetition: number | undefined, el: unknown | undefined) {
    if (repetition === undefined) {
      return []
    }
    if (typeof el !== 'object') {
      return Array(repetition).fill(el)
    }
    const result = Array(repetition)
    for (let i = 0; i < repetition; i++) {
      result[i] = { ...el }
    }
    return result
  })

  LiquidHandler.registerFilterPostCreate("coerceToArr", function (a) {
    if (a === undefined || a === null) {
      return []
    }
    if (Array.isArray(a)) {
      return a
    }
    return a.toString().split(',')
  })

  const makeSetCompatibleWithLiquidContainsOperator = (...args: unknown[]) => {
    const s = new Set(...(args as Array<never>))
    ;(s as AnyObj).indexOf = function(v: unknown) {
      const result = (this as Set<unknown>).has(v) ? 0 : -1
      return result
    }
    return s
  }

  LiquidHandler.registerFilterPostCreate("toSet", function (arr) {
    if (!arr) {
      return makeSetCompatibleWithLiquidContainsOperator()
    }
    if (!Array.isArray(arr)) {
      throw new TkErrorHttpAware("in filter toSet: arr is not an array, it is " + arr.toString())
    }
    return makeSetCompatibleWithLiquidContainsOperator(arr)
  })

  const arrayInplacePush: liquid.FilterImplOptions = function (a: AnyObj[], ...rest) {
    for (const b of rest) {
      a.push(b)
    }
    return a
  }
  const arrayInplacePop: liquid.FilterImplOptions = function (a: AnyObj[]) {
    return a.pop()
  }
  const arrayInplaceRemoveLast: liquid.FilterImplOptions = function (a: AnyObj[]) {
    a.pop()
    return a
  }
  const arrayInplacePushTo: liquid.FilterImplOptions = function (a, b: unknown[]) {
    b.push(a)
    return b
  }

  LiquidHandler.registerFilterPostCreate("inplacePush", arrayInplacePush)
  LiquidHandler.registerFilterPostCreate("inplace_push", arrayInplacePush)
  LiquidHandler.registerFilterPostCreate("inplacePushTo", arrayInplacePushTo)
  LiquidHandler.registerFilterPostCreate("inplace_push_to", arrayInplacePushTo)
  LiquidHandler.registerFilterPostCreate("inplacePop", arrayInplacePop)
  LiquidHandler.registerFilterPostCreate("inplaceRemoveLast", arrayInplaceRemoveLast)

  LiquidHandler.registerFilterPostCreate("applyFilters", async function(a, b: [string, ...unknown[]][] | undefined) {
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

  LiquidHandler.registerFilterPostCreate("objAssign", function (a: AnyObj, b: AnyObj) {
    return Object.assign(a, b)
  })

  LiquidHandler.registerFilterPostCreate("objAssignIfAbsent", function (a: AnyObj, b: AnyObj) {
    for (const [k, v] of Object.entries(b)) {
      if (a[k] === undefined) {
        a[k] = v
      }
    }
    return a
  })

  LiquidHandler.registerFilterPostCreate("setToObj", function (v, a: AnyObj, k: string) {
    if (v === undefined) {
      delete a[k]
    } else {
      a[k] = v
    }
  })

  LiquidHandler.registerFilterPostCreate("cloneObj", function (a: AnyObj) {
    return {...a}
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
    const rgc = liquidFilterCommonProcessGetRgc(this)
    return await TienKouAssetFetchHandler.queryLiveAsset({
      tkCtx: rgc.tkCtx(),
      locatorTopDirs: ["guarded/", "open/"],
      locatorSubAncestors: [ancestor],
      shouldIncludeDerivingParent,
      shouldFetchRawBytesIfLight: true,
      shouldIncludeWithoutPubTime: false,
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

  LiquidHandler.registerFilterPostCreate("toString", async function (a) {
    return a.toString()
  })

  LiquidHandler.registerFilterPostCreate("quickStrHash", async function (...rest) {
    return quickStrHash(rest.join(''))
  })

  LiquidHandler.registerFilterPostCreate("btos", bytesLikeToStringIgnoreError)

  LiquidHandler.registerFilterPostCreate("json", function (o) {
    return jsonPrettyStringify(o)
  })

  LiquidHandler.registerFilterPostCreate("resolveRelPathInUrlManner", function (a: string, b: string) {
    const aStartWithSlash = a.startsWith('/')
    return new URL(b, 'http://example.com' + (aStartWithSlash ? '' : '/') + a).pathname.slice(aStartWithSlash ? 0 : 1)
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

  const decideQuickmacroInfoFromIncStr = function (incStr: string | undefined): { renderFile: string, subMacro: string | undefined } | undefined {
    if (incStr === undefined || typeof incStr !== 'string') {
      return undefined
    }
    let parts = incStr.split('?')
    if (parts.length > 1) {
      parts = [parts[0], parts.slice(1).join('?')]
    }
    
    let subMacro: string | undefined

    if (parts.length === 2) {
      incStr = parts[0]
      subMacro = parts[1]
    } else {
      let parts = incStr.split('#')
      if (parts.length > 1) {
        parts = [parts[0], parts.slice(1).join('#')]
      }
      if (parts.length === 2) {
        incStr = parts[0]
        subMacro = parts[1]
      } else {
        let firstCharIOfBaseName = incStr.lastIndexOf('/')
        if (firstCharIOfBaseName >= 0) {
          firstCharIOfBaseName += 1
        } else {
          firstCharIOfBaseName = 0
        }
        let firstNonDigitOfBaseName = firstCharIOfBaseName
        for (; firstNonDigitOfBaseName < incStr.length; firstNonDigitOfBaseName++) {
          const currChar = incStr[firstNonDigitOfBaseName]
          if (!(currChar >= '0' && currChar <= '9')) {
            break
          }
        }
        if (firstNonDigitOfBaseName < incStr.length && firstNonDigitOfBaseName > firstCharIOfBaseName) {
          subMacro = incStr.substring(firstNonDigitOfBaseName)
          incStr = incStr.substring(0, firstNonDigitOfBaseName)
        }
      }
    }

    if (!incStr) {
      incStr = 'index'
    }

    if (incStr.endsWith('/')) {
      incStr = incStr + 'index'
    }

    const [isEndWith, fileAfterStrip, _] = stripExtensionList(incStr, liquidExtName)
    return {
      renderFile: "inc/q/" + fileAfterStrip + (isEndWith ? liquidExtName : ""),
      subMacro
    }
  }

  const processQuickmacroInc = function (this: liquid.Tag & TagImplOptions) {
    const incStr = this.renderTag.file as string | undefined
   
    const info = decideQuickmacroInfoFromIncStr(incStr)
    if (info === undefined) {
      return
    }

    if (info.subMacro !== undefined) {
      liquidRenderTagHashAdd.call(this, 'subMacro', info.subMacro)
    }

    this.renderTag.file = info.renderFile
  }

  const liquidRenderTagHashFallback = function (this: liquid.Tag & TagImplOptions, key1: string, fallbackToKey: string) {
    if (!((this.renderTag.hash as liquid.Hash).hash[key1]) && ((this.renderTag.hash as liquid.Hash).hash[fallbackToKey])) {
      (this.renderTag.hash as liquid.Hash).hash[key1] = (this.renderTag.hash as liquid.Hash).hash[fallbackToKey]
    }
  }

  LiquidHandler.registerTagPostCreate("preloadTmpl", {
    parse(tagToken: liquid.TagToken, remainingTokens: liquid.TopLevelToken[]) {
      ;(this as liquid.Tag & TagImplOptions).renderTag = new liquid.RenderTag(tagToken, remainingTokens, this.liquid, this.liquid.parser)
    },
    * render(_context, _emitter) {
      yield this.liquid.options.fs.readFile(this.renderTag.file as string)
    },
  })
  
  LiquidHandler.registerTagPostCreate("qw", {
    parse(tagToken: liquid.TagToken, remainingTokens: liquid.TopLevelToken[]) {
      ;(this as liquid.Tag & TagImplOptions).renderTag = new liquid.RenderTag(tagToken, remainingTokens, this.liquid, this.liquid.parser)

      processQuickmacroInc.call(this)

      ;(this as liquid.Tag & TagImplOptions).tpls = []
      let closed = false
      while(remainingTokens.length) {
        const token = remainingTokens.shift()
        if (token && liquid.TypeGuards.isTagToken(token) && ((token as AnyObj).name === 'endqw' || (token as AnyObj).name === 'wq')) {
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

  // use %h (hat) to represent literal ^
  // use %c (comma) to represent literal ,
  const sourceQuickmacroSyntax = /q\^([^^]+)\^([^^]+)\^/gs

  LiquidHandler.registerFilterPostCreate("processSourceQuickmacroSyntax", async function(a: string, shouldProcess: boolean, b: [string, ...unknown[]][] | undefined) {
    if (!shouldProcess) {
      return a
    }
    return await replaceAsync(a, sourceQuickmacroSyntax, async (theMatch, qName, qParamStr) => {
      const info = decideQuickmacroInfoFromIncStr(qName)
      if (info === undefined) {
        return theMatch
      }
      qParamStr = unescapeSimple(qParamStr, '^', "%", "h")
      const qParamList = qParamStr.split(',').map(x => unescapeSimple(x, ',', "%", "c"))
      const qParamMap = {} as Record<string, string>
      for (const qParamOneStr of qParamList) {
        const qParamParts = qParamOneStr.split('=')
        if (qParamParts.length <= 1) {
          qParamMap['qInner'] = qParamOneStr
        } else {
          const qParamKey = qParamParts[0]
          const qParamVal = qParamParts.slice(1).join('=')
          qParamMap[qParamKey] = qParamVal
        }
      }
      if (qParamMap['qInner'] === undefined) {
        const tryQInner = qParamMap['qi']
        if (tryQInner !== undefined) {
          qParamMap['qInner'] = tryQInner
        }
      }
      if (qParamMap['subMacro'] === undefined) {
        if (info.subMacro !== undefined) {
          qParamMap['subMacro'] = info.subMacro
        }
      }
      if (qParamMap['subMacro'] === undefined) {
        const trySubMacro = qParamMap['s']
        if (trySubMacro !== undefined) {
          qParamMap['subMacro'] = trySubMacro
        }
      }

      try {
        // from liquidjs src/tags/render.ts render
        const childCtx = this.context.spawn()
        const scope = childCtx.bottom()
        Object.assign(scope, qParamMap)
        return (await this.liquid.renderFile(info.renderFile, childCtx, this.liquid.options)) as string
      } catch (e: unknown) {
        le('processSourceQuickmacroSyntax', e)
        return theMatch
      }
    })
  })

  LiquidHandler.registerTagPostCreate("backpatch", class BackpatchTag extends liquid.Tag {
    
    identifier: liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken
    backpatchName: string

    constructor(tagToken: liquid.TagToken, remainingTokens: liquid.TopLevelToken[], liquid: Liquid) {
      super(tagToken, remainingTokens, liquid)
      this.identifier = this.readBackpatchName()
      this.backpatchName = this.identifier.content
    }
    // modified from Liquid.js capture tag
    private readBackpatchName (): liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken {
      let ident = this.tokenizer.readIdentifier() as liquidTokensType.IdentifierToken | undefined
      if (ident?.content) return ident
      ident = this.tokenizer.readQuoted()
      if (ident) return ident
      throw this.tokenizer.error('invalid backpatch name')
    }
    render(context: liquid.Context, emitter: liquid.Emitter) {
      const rgc = context.getSync(['rgc']) as ResultGenContext
      if (rgc['backpatches'] === undefined) {
        rgc['backpatches'] = {}
      }
      rgc['backpatches'][this.backpatchName] = {
        name: this.backpatchName,
        outputOffset: emitter.buffer.length,
      }
      emitter.write('@')
    }
    
  })

  // modified from Liquid.js capture tag
  const backpatchValTagClass = class BackpatchValTag extends liquid.Tag {
    identifier: liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken
    backpatchName: string
    templates: liquid.Template[] = []
    constructor (tagToken: liquid.TagToken, remainTokens: liquid.TopLevelToken[], liqInst: Liquid, parser: liquid.Parser) {
      super(tagToken, remainTokens, liqInst)
      this.identifier = this.readBackpatchName()
      this.backpatchName = this.identifier.content

      while (remainTokens.length) {
        const token = remainTokens.shift()!
        if (token && liquid.TypeGuards.isTagToken(token)) {
          const tokenTagNameLower = ((token as AnyObj).name as string).toLowerCase()
          if (tokenTagNameLower === 'endbackpatchval' || tokenTagNameLower === 'endbackpatchvalue' || tokenTagNameLower === 'endbpval' || tokenTagNameLower === 'endbpvalue') {
            return
          }
        }
        this.templates.push(parser.parseToken(token, remainTokens))
      }
      throw new Error(`tag ${tagToken.getText()} not closed`)
    }

    private readBackpatchName (): liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken {
      let ident: liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken | undefined = this.tokenizer.readIdentifier()
      if (ident.content) return ident
      ident = this.tokenizer.readQuoted()
      if (ident) return ident
      throw this.tokenizer.error('invalid capture name')
    }

    * render (ctx: liquid.Context): Generator<unknown, void, string> {
      const r = this.liquid.renderer
      const html = yield r.renderTemplates(this.templates, ctx)
      const rgc = ctx.getSync(['rgc']) as ResultGenContext
      if (rgc['backpatchValueMap'] === undefined) {
        rgc['backpatchValueMap'] = {}
      }
      rgc['backpatchValueMap'][this.backpatchName] = html
    }

    public children (): liquid.Template[] {
      return this.templates
    }

    public * localScope (): Iterable<string | liquidTokensType.IdentifierToken | liquidTokensType.QuotedToken> {
      yield this.identifier
    }
  }
  LiquidHandler.registerTagPostCreate("backpatchVal", backpatchValTagClass)
  LiquidHandler.registerTagPostCreate("backpatchValue", backpatchValTagClass)

  // eXecute value processing / filtering
  LiquidHandler.registerTagPostCreate("x", class TagX extends liquid.Tag {
    private value: liquid.Value

    constructor (token: liquid.TagToken, remainTokens: liquid.TopLevelToken[], liquidInst: Liquid) {
      super(token, remainTokens, liquidInst)
      this.value = new liquid.Value(this.tokenizer.readFilteredValue(), this.liquid)
    }
    * render (ctx: liquid.Context): Generator<unknown, void, unknown> {
      yield this.value.value(ctx, this.liquid.options.lenientIf)
    }

    public * arguments (): liquid.Arguments {
      yield this.value
    }

  })

  LiquidHandler.registerTagPostCreate("l", class TagL extends liquid.Tag {
    private rawValue: liquid.Value

    constructor (token: liquid.TagToken, remainTokens: liquid.TopLevelToken[], liquidInst: Liquid) {
      super(token, remainTokens, liquidInst)
      const fv = this.tokenizer.readFilteredValue()
      this.rawValue = new liquid.Value(fv, this.liquid)
    }
    * render (ctx: liquid.Context): Generator<unknown, void, unknown> {
      const rawVal = yield this.rawValue.value(ctx, this.liquid.options.lenientIf)
      l('liquidLogT', rawVal)
    }

    public * arguments (): liquid.Arguments {
      yield this.rawValue
    }
  })

  // liquid does not export FilterToken...
  class LiquidMockFilterToken2 extends liquid.Token {
    public constructor(
      public name: string,
      public args: liquid.ValueToken | ([string?, liquid.ValueToken?])[],
      input: string,
      begin: number,
      end: number,
      file?: string
    ) {
      super(liquid.TokenKind.Filter, input, begin, end, file)
    }
  }

  LiquidHandler.registerTagPostCreate("pageLog", class TagPageLog extends liquid.Tag {
    private rawValue: liquid.Value
    private escapedValue: liquid.Value

    constructor (token: liquid.TagToken, remainTokens: liquid.TopLevelToken[], liquidInst: Liquid) {
      super(token, remainTokens, liquidInst)
      const fv = this.tokenizer.readFilteredValue()
      this.rawValue = new liquid.Value(fv, this.liquid)
      this.escapedValue = new liquid.Value(fv, this.liquid)
      // copied from liquidjs template/output.ts
      const filters = this.escapedValue.filters
      const outputEscape = this.liquid.options.outputEscape
      if (!filters[filters.length - 1]?.raw && outputEscape) {
        // unaligned package of ts/js in liquidjs?
        // // eslint-disable-next-line @typescript-eslint/no-explicit-any
        // const token = new (liquidjsJs as any).FilterToken(outputEscape.toString(), [], '', 0, 0)
        const token = new LiquidMockFilterToken2(outputEscape.toString(), [], '', 0, 0)
        // liquid does not export FilterToken...
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        filters.push(new liquid.Filter(token as any, outputEscape, this.liquid))
      }
    }
    * render (ctx: liquid.Context, emitter: liquid.Emitter): Generator<unknown, void, unknown> {
      const rawVal = yield this.rawValue.value(ctx, this.liquid.options.lenientIf)
      const escapedVal = yield this.escapedValue.value(ctx, this.liquid.options.lenientIf)
      l('liquidPageLogT', rawVal)
      emitter.write('<div class="liquid-page-log">' + escapedVal + '</div>')
    }

    public * arguments (): liquid.Arguments {
      yield this.rawValue
    }

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
