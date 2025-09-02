import path from "node:path"
import fs from "node:fs"
import { getParentPath } from "./common.mts"

export const nodeResolvePath = (p: string) => {
  // return url.fileURLToPath(import.meta.resolve(p))
  // return path.resolve(__dirname, p)
  return path.resolve(".", p)
}

export const pathExists = async (p: string) => {
  return await fs.promises.access(p).then(() => true, () => false)
}


export const ensurePathDirExists = async (p: string) => {
  if (!await pathExists(p)) {
    await fs.promises.mkdir(p, { recursive: true })
  }
}

export const ensureParentDirExists = async (p: string) => {
  return await ensurePathDirExists(getParentPath(p))
}


