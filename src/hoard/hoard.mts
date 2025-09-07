import '../nodeEnv.mts'

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
      
  await startMddbHoard(tkCtx)
  await startTgHoard(tkCtx)

}

main()
// await main()
