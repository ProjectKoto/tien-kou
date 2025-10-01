
import { tgMessageToHtml } from "../lib/tgCommon.mts"
import { HC, KD, LiquidFilterRegisterHandler } from "./serveDef.mts"

export const LiquidTelegramMsgFilterRegHandler = HC<LiquidFilterRegisterHandler>()(async (_: KD<never>) => {
  return {
    doRegister: (reg) => {
      reg("tgMsg", async function (x) {
        return await tgMessageToHtml(x)
      })
    }
  }
})
