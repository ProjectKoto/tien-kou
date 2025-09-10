import { l, le } from '../lib/common.mts'
import '../nodeEnv.mts'

import { setGlobalDispatcher, ProxyAgent } from 'undici'
import { tkEnvFromDevVarsFile } from "../nodeEnv.mts"
import { TkContext } from "../serve/serveDef.mts"
import { startMddbHoard } from "./mddbHoard.mts"
import { startTgHoard } from "./tgHoard.mts"

const main = async () => {
  const tkEnv = await tkEnvFromDevVarsFile()

  // applyTkEnvToProcessEnv(tkEnv)

  const tkCtx: TkContext = {
    tkEnv,
    get e() {
      return this.tkEnv
    },
  }

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
