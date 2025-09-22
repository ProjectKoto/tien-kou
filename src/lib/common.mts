import { HTTPException } from "hono/http-exception"
import { ContentfulStatusCode } from "hono/utils/http-status"
import isArrayBuffer from "is-array-buffer"
import replaceAll from 'string.prototype.replaceall'

export type AnyObj = Record<keyof ReturnType<JSON["parse"]>, ReturnType<JSON["parse"]>>
export type Ao = AnyObj

export interface TkContext {
  tkEnv: Record<string, string | undefined>
  e: Record<string, string | undefined>
}

const extensionMemo: Record<string, Set<string>> = {}

// only one
export const liquidExtName = ".tmpl.html"

export const jsonPrettyPrint = (x: unknown) => { l(jsonPrettyStringify(x)) }
export const jsonPrettyStringify = (x: unknown) => {
  return JSON.stringify(x, (k, v) => {
    if (Array.isArray(v)) {
      if (v.length > 50) {
        return [...v.slice(0, 50), '...more']
      }
    }
    if (v?.type === 'Buffer') { // we can't use Buffer.isBuffer(v) cuz https://github.com/nodejs/node-v0.x-archive/issues/5110
      return '[Buffer]'
    }
    return v
  }, 2)
}

export const truncateStrByLen = (s: string, len: number) => {
  if (s.length > len) {
    // return s.substring(0, len) + ' .. ' + s.substring(s.length - len, s.length)
    return s.substring(0, len) + ' ..'
  }
  return s
}

export const l = (...args: unknown[]) => {
  console.log("[" + new Date().toLocaleString( 'sv', { timeZoneName: 'short' } ) + "]", ...args)
}

export const le = (...args: unknown[]) => {
  console.log("[" + new Date().toLocaleString( 'sv', { timeZoneName: 'short' } ) + "]", ...args)
}



export const sqlLikePatternEscape = (a: string) => {
  a = replaceAll(a, '\\', '\\\\')
  a = replaceAll(a, '%', '\\%')
  a = replaceAll(a, '_', '\\_')
  return a
}

export const sqlGlobPatternEscape = (a: string) => {
  return a.replace(/\[\]\*\?/g, (s) => {
    if (s === '[') {
      return '[[]'
    }
    if (s === ']') {
      return '[]]'
    }
    if (s === '*') {
      return '[*]'
    }
    if (s === '?') {
      return '[*]'
    }
    throw new Error('impossible situation in sqlGlobPatternEscape')
  })
}

export type ExtensionListLike = string[] | Set<string> | string

export const extensionListStrToSet = function (extensionList: ExtensionListLike) {
  if (Array.isArray(extensionList)) {
    return new Set(extensionList)
  }
  if (extensionList instanceof Set) {
    return extensionList
  }
  if (typeof(extensionList) !== 'string') {
    throw new Error("extensionListStrToSet: extensionList is not a string")
  }
  let extensionSet = extensionMemo[extensionList]
  if (extensionSet === undefined) {
    const extensionReallyArray = extensionList.split(',')
    for (let i = 0; i < extensionReallyArray.length; i++) {
      let extension = extensionReallyArray[i]
      if (extension.startsWith(".")) {
        extension = extension.substring(1)
      }
      extension = extension.toLowerCase()

      extensionReallyArray[i] = extension
    }
    
    extensionSet = new Set(extensionReallyArray.filter(x => x))
    extensionMemo[extensionList] = extensionSet
  }
  return extensionSet
}

export const stripExtensionList = (theFilePath: string, extensionList: ExtensionListLike): [strippedKnownExtension: boolean, filePathKnownExtensionStripped: string, theExtension: string] => {
  const extensionSet = extensionListStrToSet(extensionList)
  
  const filePathComponents = theFilePath.split("/")
  const parts = filePathComponents[filePathComponents.length - 1].split(".")
  if (parts.length >= 3) {
    const extension2 = (parts[parts.length - 2] + "." + parts[parts.length - 1]).toLowerCase()
    if (extensionSet.has(extension2)) {
      return [true, [...filePathComponents.slice(0, filePathComponents.length - 1), parts.slice(0, parts.length - 2).join(".")].join("/"), extension2]
    }
  }
  if (parts.length >= 2) {
    const extension1 = (parts[parts.length - 1]).toLowerCase()
    if (extensionSet.has(parts[parts.length - 1].toLowerCase())) {
      return [true, [...filePathComponents.slice(0, filePathComponents.length - 1), parts.slice(0, parts.length - 1).join(".")].join("/"), extension1]
    }
    return [false, theFilePath, extension1]
  }
  
  return [false, theFilePath, ""]
}

export const isAssetExtensionInList = (asset: { origin_file_extension: string }, extensionList: ExtensionListLike) => {
  return isInExtensionList(asset.origin_file_extension, extensionList)
}

export const isEndWithExtensionList = (theFilePath: string, extensionListStr: ExtensionListLike, allKnownExtList: ExtensionListLike) => {
  if (!allKnownExtList) {
    return stripExtensionList(theFilePath, extensionListStr)[0]
  }
  const [_, __, ext] = stripExtensionList(theFilePath, allKnownExtList)
  return isInExtensionList(ext, extensionListStr)
}

export const isInExtensionList = (lowerCaseExtNameWithoutPrefixDot: string, extensionList: ExtensionListLike) => {
  const extensionSet = extensionListStrToSet(extensionList)
  return extensionSet.has(lowerCaseExtNameWithoutPrefixDot)
}

export const markdownExtNames = extensionListStrToSet(".md,.markdown")
export const dedicatedAssetExtNames = extensionListStrToSet([...markdownExtNames, liquidExtName, ".liquidjs"].join(","))
export const allKnownAssetExtNames = extensionListStrToSet([...dedicatedAssetExtNames, ".pug.html", ".ejs.html"].join(","))
export const listableAssetExtNames = extensionListStrToSet([...markdownExtNames, "html"].join(","))
export const strippedInLocatorExtNames = extensionListStrToSet([...markdownExtNames, "html"].join(","))

/**
 * %y/%m/%y-%m-%d
 */
export const datePath = (pathPrefix: string, pathSuffix: string, pathFormat: string, date: Date | undefined) => {
  if (date === undefined) {
    date = new Date()
  }
  const y = date.getFullYear().toString().padStart(4, '0')
  const m = (date.getMonth() + 1).toString().padStart(2, '0')
  const d = (date.getDate()).toString().padStart(2, '0')

  if (pathPrefix.length > 0) {
    for (let i = pathPrefix.length - 1; i >= 0; i--) {
      if (pathPrefix[i] !== '/' && pathPrefix !== '\\') {
        pathPrefix = pathPrefix.substring(0, i + 1)
        break
      }
    }
  }

  if (pathFormat.length > 0) {
    for (let i = pathFormat.length - 1; i >= 0; i--) {
      if (pathFormat[i] !== '/' && pathFormat !== '\\') {
        pathFormat = pathFormat.substring(0, i + 1)
        break
      }
    }
    for (let i = 0; i < pathFormat.length; i++) {
      if (pathFormat[i] !== '/' && pathFormat !== '\\') {
        pathFormat = pathFormat.substring(i)
        break
      }
    }
  }

  return pathPrefix + "/" + replaceAll(replaceAll(replaceAll(pathFormat, "%y", y), "%m", m), "%d", d) + pathSuffix
}

export const genTimestampString = (date: Date | undefined) => {
  if (date === undefined) {
    date = new Date()
  }
  const y = date.getFullYear().toString().padStart(4, '0')
  const mon = (date.getMonth() + 1).toString().padStart(2, '0')
  const d = (date.getDate()).toString().padStart(2, '0')
  const h = date.getHours().toString().padStart(2, '0')
  const min = date.getMinutes().toString().padStart(2, '0')
  const s = date.getSeconds().toString().padStart(2, '0')

  return `${y}-${mon}-${d} ${h}:${min}:${s}`
}

export const getParentPath = (p: string) => {
  p = replaceAll(p, '\\', '/')
  let root = ''
  if (p.length > 0 && p[0] === '/') {
    root = '/'
  }
  const parts = p.split('/').filter((x: string) => x)
  if (parts.length > 0) {
    parts.pop()
  }
  return root + parts.join("/")
}

// eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
export type BoxedUserMessageStr = String & { isUserMessage: boolean }

export const um = (errorMessage: { valueOf: () => string } | TemplateStringsArray, ...values: unknown[]) => {
  let strVal: string

  if (typeof(errorMessage) === 'string') {
    strVal = errorMessage.valueOf()
  } else if (errorMessage !== undefined && errorMessage !== null && Array.isArray(errorMessage) && errorMessage.length > 0) {
    // https://stackoverflow.com/questions/68152638/what-is-the-default-tag-function-for-template-literals
    strVal = errorMessage
      .flatMap((part, i) =>
        i < values.length ? [part, String(values[i])] : [part],
      )
      .join('')
  } else {
    throw new TkErrorHttpAware("um error")
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  const messageBoxed: String & { isUserMessage?: boolean } = new String(strVal)
  messageBoxed.isUserMessage = true
  return messageBoxed as BoxedUserMessageStr
}

export type TkErrorHttpAwareOptions = { message?: string, mShouldLog?: boolean, defaultStatus?: ContentfulStatusCode }

export class TkErrorHttpAware extends HTTPException implements TkErrorIntf {

  public mShouldLog: boolean = false
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  public tkUserMessage?: string | String

  constructor()
  constructor(arg1: string | BoxedUserMessageStr)
  constructor(arg1: ContentfulStatusCode)
  constructor(arg1: TkErrorHttpAwareOptions)

  constructor(arg1: string | BoxedUserMessageStr, arg2: ContentfulStatusCode)
  constructor(arg1: string | BoxedUserMessageStr, arg2: TkErrorHttpAwareOptions)
  constructor(arg1: ContentfulStatusCode, arg2: string | BoxedUserMessageStr)
  constructor(arg1: ContentfulStatusCode, arg2: TkErrorHttpAwareOptions)
  constructor(arg1: TkErrorHttpAwareOptions, arg2: string | BoxedUserMessageStr)
  constructor(arg1: TkErrorHttpAwareOptions, arg2: ContentfulStatusCode)

  constructor(arg1: string | BoxedUserMessageStr, arg2: ContentfulStatusCode, arg3: TkErrorHttpAwareOptions)
  constructor(arg1: string | BoxedUserMessageStr, arg2: TkErrorHttpAwareOptions, arg3: ContentfulStatusCode)
  constructor(arg1: ContentfulStatusCode, arg2: string | BoxedUserMessageStr, arg3: TkErrorHttpAwareOptions)
  constructor(arg1: ContentfulStatusCode, arg2: TkErrorHttpAwareOptions, arg3: string | BoxedUserMessageStr)
  constructor(arg1: TkErrorHttpAwareOptions, arg2: string | BoxedUserMessageStr, arg3: ContentfulStatusCode)
  constructor(arg1: TkErrorHttpAwareOptions, arg2: ContentfulStatusCode, arg3: string | BoxedUserMessageStr)


  constructor(arg1?: string | TkErrorHttpAwareOptions | ContentfulStatusCode | BoxedUserMessageStr, arg2?: string | TkErrorHttpAwareOptions | ContentfulStatusCode | BoxedUserMessageStr, arg3?: string | TkErrorHttpAwareOptions | ContentfulStatusCode | BoxedUserMessageStr) {

    let message: string | undefined = undefined
    let userMessage: BoxedUserMessageStr | undefined = undefined
    let status: ContentfulStatusCode | undefined = undefined
    let options: TkErrorHttpAwareOptions = {}

    for (const a of [arg1, arg2, arg3]) {
      if (typeof(a) === 'string') {
        message = a
      } else if (a instanceof String) {
        message = a.valueOf()
        if ((a as BoxedUserMessageStr).isUserMessage) {
          userMessage = a
        }
      } else if (typeof(a) === 'object') {
        options = {
          ...a
        }
      } else if (typeof(a) === 'number') {
        status = a
      }
    }

    if (message) {
      options.message = message
    }
    if (status === undefined) {
      status = options.defaultStatus ?? 500
    }
    super(status, options)
    TkError.prototype["postProcessAfterSuperConstruct"].call(this, options, userMessage)
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldLog(arg1?: string | String): TkErrorHttpAware {
    return TkError.prototype.shouldLog.call(this, arg1) as unknown as TkErrorHttpAware
  }

  shouldNotLog(): TkErrorHttpAware {
    return TkError.prototype.shouldNotLog.call(this) as unknown as TkErrorHttpAware
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  setUserMessage(arg1: string | String): TkErrorHttpAware {
    return TkError.prototype.setUserMessage.call(this, arg1) as unknown as TkErrorHttpAware
  }
}

interface TkErrorIntf {
  mShouldLog: boolean
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  tkUserMessage?: string | String

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldLog(arg1?: string | String): TkErrorIntf
  shouldNotLog(): TkErrorIntf
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  setUserMessage(arg1: string | String): TkErrorIntf
}

export class TkError extends Error implements TkErrorIntf {

  public mShouldLog: boolean = false
  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  public tkUserMessage?: string | String

  constructor()
  constructor(arg1: string | BoxedUserMessageStr)
  constructor(arg1: { message?: string, mShouldLog?: boolean, cause?: unknown })

  constructor(arg1: string | BoxedUserMessageStr, arg2: { message?: string, mShouldLog?: boolean, cause?: unknown })
  constructor(arg1: { message?: string, mShouldLog?: boolean, cause?: unknown }, arg2: string | BoxedUserMessageStr)

  constructor(arg1?: string | BoxedUserMessageStr | { message?: string, mShouldLog?: boolean, cause?: unknown }, arg2?: string | BoxedUserMessageStr | { message?: string, mShouldLog?: boolean, cause?: unknown }) {

    let message: string | undefined = undefined
    let userMessage: BoxedUserMessageStr | undefined = undefined
    let options: { message?: string, mShouldLog?: boolean, cause?: unknown } = {}

    for (const a of [arg1, arg2]) {
      if (typeof(a) === 'string') {
        message = a
      } else if (a instanceof String) {
        message = a.valueOf()
        if ((a as BoxedUserMessageStr).isUserMessage) {
          userMessage = a
        }
      } else if (typeof(a) === 'object') {
        options = {
          ...a
        }
      }
    }

    if (message) {
      options.message = message
    }
    // @ts-expect-error Expected 0-1 arguments, but got 2.ts(2554)
    super(message, options)
    this.postProcessAfterSuperConstruct(options, userMessage)
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  private postProcessAfterSuperConstruct(options: { mShouldLog?: boolean }, userMessage: string | String | undefined) {
    this.mShouldLog = true
    if (options && (options.mShouldLog === false)) {
      this.mShouldLog = false
    }
    if (userMessage !== undefined) {
      this.setUserMessage(userMessage)
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  shouldLog(arg1?: string | String) {
    this.mShouldLog = true
    if (typeof(arg1) === 'string' || arg1 instanceof String) {
      this.message = arg1.valueOf()
    } else if (typeof(arg1) === 'object') {
      (this as AnyObj).cause = arg1
    }
    return this
  }

  shouldNotLog() {
    this.mShouldLog = false
    return this
  }

  // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
  setUserMessage(arg1: string | String) {
    this.tkUserMessage = arg1
    return this
  }
}

const textDecoder = new TextDecoder('utf-8')

export const bytesLikeToString = (bytesLike: string | Buffer | ArrayBuffer | { type: "Buffer" } | unknown) => {
  if (bytesLike === undefined) {
    throw new TkErrorHttpAware(`bytesLikeToString: input is undefined`)
  }

  if (typeof(bytesLike) === 'string') {
    return bytesLike
  }

  if (bytesLike && typeof bytesLike === 'object' && (bytesLike as AnyObj)['type'] === 'Buffer') {
    return Buffer.from(bytesLike as Buffer).toString('utf-8')
  }
  
  if (Buffer.isBuffer(bytesLike)) {
    return bytesLike.toString('utf-8')
  }

  if (isArrayBuffer(bytesLike)) {
    return textDecoder.decode(bytesLike)
  }

  throw new TkErrorHttpAware(`bytesLikeToString: invalid input: ${bytesLike === null ? "<null>" : JSON.stringify(bytesLike)}`)
}

export const makeConcatenatablePath = (x: string) => (!x || x === '/') ? '' : (x[x.length - 1] !== '/' ? (x + '/') : x)

export const makeConcatenatablePathList = (l: string[]) => {
  if (!l || l.length === 0) {
    l = ['']
  } else {
    l = l.map(makeConcatenatablePath)
  }
  return l
}

export const delayInitVal = <T,>() => {
  let val: T | undefined = undefined
  return {
    get val() {
      if (val === undefined) {
        throw new TkErrorHttpAware('not initialzed')
      }
      return val
    },
    set val(nv: T) {
      val = nv
    },
  }
}

// credit https://www.npmjs.com/package/lazy-value

export type LazyVal<T> = {
  (): T,
  isCalled: boolean,
  result: T | undefined,
}

export const lazyValue = function <T>(function_: () => T): LazyVal<T> {
  const r = (() => {
    if (!r.isCalled) {
      r.isCalled = true
      r.result = function_()
    }

    return r.result
  }) as LazyVal<T>

  r.isCalled = false
  r.result = undefined

  return r
}

// credit: https://github.com/bryc/code/blob/master/jshash/experimental/cyrb53.js
export const cyrb53 = function (str: string, seed = 0) {
  let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed
  for (let i = 0, ch; i < str.length; i++) {
    ch = str.charCodeAt(i)
    h1 = Math.imul(h1 ^ ch, 2654435761)
    h2 = Math.imul(h2 ^ ch, 1597334677)
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507)
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909)
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507)
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909)
  return 4294967296 * (2097151 & h2) + (h1 >>> 0)
}

export const quickStrHash = cyrb53

