import { AnyObj, l, le } from '../lib/common.mts'
import '../nodeEnv.mts'

import { setGlobalDispatcher, ProxyAgent } from 'undici'
import { tkEnvFromDevVarsFile } from "../nodeEnv.mts"
import { TkContext } from '../lib/common.mts'
import { startMddbHoard } from "./mddbHoard.mts"
import { startTgHoard } from "./tgHoard.mts"

import process from "node:process"
import rcloneJs from 'rclone.js'
import { TkContextHoard } from '../lib/nodeCommon.mjs'

const makeRcloneJsWrapper = (tkCtx: TkContext) => {
  const tkEnv = tkCtx.e

  const defaultEnvForRclone = {
  } as AnyObj

  const defaultOptionForRclone = {
  } as AnyObj

  for (const k of Object.keys(tkEnv)) {
    if (k.startsWith('HOARD_RCLONE_CONFIG_')) {
      const v = tkEnv[k]
      defaultEnvForRclone[k.substring('HOARD_RCLONE_CONFIG_'.length - 'RCLONE_CONFIG_'.length)] = v
    }
    if (k === 'HOARD_RCLONE_EXECUTABLE') {
      const v = tkEnv[k]
      defaultOptionForRclone['rcloneExecutable'] = v
    }
  }

  if (tkEnv['HTTP_PROXY']) {
    defaultEnvForRclone['HTTP_PROXY'] = tkEnv['HTTP_PROXY']
    defaultEnvForRclone['HTTPS_PROXY'] = tkEnv['HTTP_PROXY']
    defaultEnvForRclone['ALL_PROXY'] = tkEnv['HTTP_PROXY']
  }

  return async (...args: (string | AnyObj)[]) => {
    const argsEmpty = args.length === 0
    let flags = args.pop();

    if (!!flags && typeof flags === 'object' && flags.constructor === Object && flags.constructor !== Number) {
      flags = {
        ...defaultOptionForRclone,
        ...flags,
        env: {
          ...defaultEnvForRclone,
          ...(flags.env || { ...process.env }),
        }
      }
      args.push(flags!)
    } else {
      if (!argsEmpty) {
        args.push(flags!)
      }
    }
    return await rcloneJs.promises(...args)
  }
}

const main = async () => {
  const tkEnv = await tkEnvFromDevVarsFile()

  // applyTkEnvToProcessEnv(tkEnv)

  const tkCtx: TkContextHoard = {
    tkEnv,
    get e() {
      return this.tkEnv
    },
    rcloneW: undefined!,
  }
  tkCtx.rcloneW = makeRcloneJsWrapper(tkCtx)

  if (tkEnv.HTTP_PROXY) {
    setGlobalDispatcher(new ProxyAgent(tkEnv.HTTP_PROXY))
  }

  const onUpdate = async () => {
    const dataUpdateCbUrlStr = tkEnv.HOARD_DATA_UPDATE_CALLBACK_URL_LIST
    if (dataUpdateCbUrlStr) {
      const timeoutSignal = AbortSignal.timeout(6000)
      const urls = dataUpdateCbUrlStr.split(';')
      const pList = [] as Promise<unknown>[]
      for (let url of urls) {
        url = url.trim()
        pList.push((async () => {
          try {
            await fetch(url, {
              signal: timeoutSignal,
            })
            l(`http callback dataUpdateCbUrl success: ${url}`)
          } catch (e) {
            le(`http callback dataUpdateCbUrl err: ${url}`, e)
          }
        })())
      }
      // (async () => {
      //   try {
      //     await Promise.all(pList)
      //   } catch (e) {
      //     le('http callback dataUpdateCbUrl err', e)
      //   }
      // })()
    }
  }
      
  await startMddbHoard(tkCtx, onUpdate)
  await startTgHoard(tkCtx, onUpdate)

}

main()
// await main()
