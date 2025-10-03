'use strict'
import process from "node:process"
import dotenv from 'dotenv'
import fs from 'node:fs'

export const tkEnvFromDevVarsFile = async (): Promise<Record<string, string | undefined>> => {
  const e = dotenv.parse(await fs.promises.readFile(".dev.vars", { encoding: 'utf-8' }))
  for (const k of Object.keys(e)) {
    if (k.startsWith('PROCENV_')) {
      process.env[k] = e[k]
    }
  }
  return e
}

export const applyTkEnvToProcessEnv = (tkEnv: Record<string, string | undefined>) => {
  for (const k in tkEnv) {
    process.env[k] = tkEnv[k]
  }
}
