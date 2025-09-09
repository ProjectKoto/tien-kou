
import { tgMessageToHtml } from "../lib/tgCommon.mts"
import { HT, KD, LiquidFilterRegisterHandler } from "./serveDef.mts"

export const LiquidTelegramMsgFilterRegHandler = HT<LiquidFilterRegisterHandler>()(async (_: KD<never>) => {
  return {
    doRegister: (reg) => {
      reg("tgMsg", async function (x) {
        return await tgMessageToHtml(x)
      })
    }
  }
})
