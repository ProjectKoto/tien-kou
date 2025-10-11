import path from "node:path"
import fs from "node:fs"
import { AnyObj, getParentPath, TkContext, TkError } from "./common.mts"
import replaceAll from 'string.prototype.replaceall'

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

export interface TkContextHoard extends TkContext {
  rcloneW: (...args: (string | AnyObj)[]) => Promise<any>
}

export const isPath2InDir1 = (path1: string, path2: string) => {
  const relative = path.relative(path1, path2)
  return relative && !relative.startsWith('..') && !path.isAbsolute(relative)
}

export const calcValidateFileSystemPathSync = (rootPath: string, assetOriginFilePath: string) => {
  const fileSystemPath = path.normalize(path.join(rootPath, assetOriginFilePath))
  const topDirPath = path.normalize(path.join(rootPath, (replaceAll(assetOriginFilePath, '\\', '/') as string).split('/').filter(x => x)[0]))
  if (!isPath2InDir1(rootPath, topDirPath)) {
    throw new TkError("bad path")
  }
  if (!isPath2InDir1(topDirPath, fileSystemPath)) {
    throw new TkError("bad path")
  }
  return fileSystemPath
}

export const staticGenBaseDir = './staticGen'

// credit: deno authors MIT https://jsr.io/@std/streams/1.0.13/to_array_buffer.ts
function bufferConcat(buffers: readonly Uint8Array[]): Uint8Array {
  let length = 0
  for (const buffer of buffers) {
    length += buffer.length
  }
  const output = new Uint8Array(length)
  let index = 0
  for (const buffer of buffers) {
    output.set(buffer, index)
    index += buffer.length
  }

  return output
}

export async function toArrayBuffer(
  readableStream: ReadableStream<Uint8Array>,
): Promise<ArrayBuffer> {
  const reader = readableStream.getReader()
  const chunks: Uint8Array[] = []

  while (true) {
    const { done, value } = await reader.read()

    if (done) {
      break
    }

    chunks.push(value)
  }

  return bufferConcat(chunks).buffer as ArrayBuffer
}
