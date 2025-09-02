'use strict'
import process from "node:process"
import dotenv from 'dotenv'
import fs from 'node:fs'

const tkEnv = dotenv.parse(fs.readFileSync(".dev.vars", { encoding: 'utf-8' }))

for (const k in tkEnv) {
  process.env[k] = tkEnv[k]
}
