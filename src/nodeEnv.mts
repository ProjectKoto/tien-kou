'use strict'
import process from "node:process"
import dotenv from 'dotenv'
import fs from 'node:fs'

export const tkEnvFromDevVarsFile = async (): Promise<Record<string, string | undefined>> => {
  const e = dotenv.parse(await fs.promises.readFile(process.env.ALT_DEV_VARS_FILE || ".dev.vars", { encoding: 'utf-8' }))
  const pendingProcEnvUpdates: Record<string, string> = {}
  for (const k in process.env) {
    const v = process.env[k]
    if (k.startsWith('PROCENV_') && v) {
      pendingProcEnvUpdates[k] = v
    }
  }
  for (const k of Object.keys(e)) {
    if (k.startsWith('PROCENV_') && !pendingProcEnvUpdates[k]) {
      process.env[k] = e[k]
    }
  }
  Object.assign(e, pendingProcEnvUpdates)
  return e
}

export const applyTkEnvToProcessEnv = (tkEnv: Record<string, string | undefined>) => {
  for (const k in tkEnv) {
    process.env[k] = tkEnv[k]
  }
}
