import * as turso from "@libsql/client"
import { Queue as AsyncQueue } from 'async-await-queue'
import { BigInteger as LibraryBigInteger } from "big-integer"
import crypto from "crypto"
import fs from "fs"
import * as input from "input"
import { MarkdownDB } from "mddb"
import pRetry from 'p-retry'
import path from "path"
import process from "process"
import randomBigint from 'random-bigint'
import replaceAll from 'string.prototype.replaceall'
import telegram from "telegram"
import tgEvents from "telegram/events/index.js"
import { StoreSession } from "telegram/sessions/index.js"
import { throttle } from 'throttle-debounce'
import url from "url"
import '../nodeEnv.mts'

import { FileInfo } from "mddb/dist/src/lib/process"
import { allKnownAssetExtNames, AnyObj, datePath, dedicatedAssetExtNames, genTimestampString, isInExtensionList, jsonPrettyPrint, l, le, markdownExtNames, stripExtensionList, strippedInLocatorExtNames } from '../lib/common.mts'
import { ensureParentDirExists, ensurePathDirExists, nodeResolvePath, pathExists } from "../lib/nodeCommon.mts"
import { TkContext } from "../serve/serveDef.mts"
import { applyTkEnvToProcessEnv, tkEnvFromDevVarsFile } from "../nodeEnv.mts"

const main = async () => {
  const tkEnv = await tkEnvFromDevVarsFile()

  // applyTkEnvToProcessEnv(tkEnv)

  const tkCtx: TkContext = {
    tkEnv,
    get e() {
      return this.tkEnv
    },
  }
      
  let tursoc: turso.Client | undefined = undefined

  const dbPath = "markdown.db"

  tursoc = turso.createClient({
    url: tkEnv.TURSO_DATABASE_URL!,
    authToken: tkEnv.TURSO_AUTH_TOKEN,
  })

  const origClient = await new MarkdownDB({
    client: "sqlite3",
    connection: {
      filename: dbPath,
    },
  }).init()

  const tableCreateForceOrder: Record<string, number> = { 'files': -20, 'tags': -10 }


  const kvColumns = [
    "id INT NOT NULL PRIMARY KEY",
    "k VARCHAR(32768) UNIQUE",
    "v LONGTEXT",
  ]

  const metaMetaInfoColumns = [
    "id INT NOT NULL PRIMARY KEY",
    "site_name TEXT",
    "last_update_timestamp DATETIME",
    "curr_buffer_index INTEGER",
  ]

  const indexSqlList = [
    `
      CREATE INDEX IF NOT EXISTS idx_file_asset_raw_path ON files ( asset_raw_path )
    `,
    `
      CREATE INDEX IF NOT EXISTS idx_file_asset_locator ON files ( asset_locator )
    `,
    `
      CREATE INDEX IF NOT EXISTS idx_file_origin_file_path ON files ( origin_file_path )
    `,
    `
      CREATE INDEX IF NOT EXISTS idx_file_update_time ON files ( update_time_by_hoard )
    `,
    `
      CREATE INDEX IF NOT EXISTS idx_file_is_deleted_publish_time ON files ( is_deleted_by_hoard, publish_time_by_metadata )
    `,
  ]

  const doRefreshMetaMetaInfo = async () => {
    await mddb.db.raw(`CREATE TABLE IF NOT EXISTS tk_meta_kv (
      ${kvColumns.join(', \n')}
    );`)
    await mddb.db.raw(`CREATE TABLE IF NOT EXISTS tk_meta_meta_info (
      ${metaMetaInfoColumns.join(', \n')}
    );`)
    for (const kvColumnDef of kvColumns) {
      try {
        await mddb.db.raw(`ALTER TABLE tk_meta_kv ADD COLUMN ${kvColumnDef};`)
      } catch (_e) {
        // l(`ALTERing tk_meta_meta_info error: "" ${e} "". Usually it doesn't matter...`)
      }
    }
    for (const metaMetaInfoColumnDef of metaMetaInfoColumns) {
      try {
        await mddb.db.raw(`ALTER TABLE tk_meta_meta_info ADD COLUMN ${metaMetaInfoColumnDef};`)
      } catch (_e) {
        // l(`ALTERing tk_meta_meta_info error: "" ${e} "". Usually it doesn't matter...`)
      }
    }
    
    await mddb.db.raw(`INSERT OR IGNORE INTO tk_meta_meta_info (id) VALUES (1);`)
    await mddb.db.raw(`UPDATE tk_meta_meta_info SET last_update_timestamp = ?;`, [Date.now()])

    for (const indexSql of indexSqlList) {
      await mddb.db.raw(indexSql)
    }
  }

  let syncToTursoLastTime: number | undefined = undefined

  let newBufferIndex: number

  type SqlLike = (string | { sql: string; args: string[]; })

  const doSyncToTurso = async () => {
    await doRefreshMetaMetaInfo()

    if (tursoc !== undefined) {
      let tursoBatchWrapper
      if (tkEnv.HOARD_DB_BATCH_DEBUG_MODE === "1") {
        tursoBatchWrapper = async (sqlExecutor: turso.Transaction | turso.Client | undefined, sqls: SqlLike[]) => {
          if (sqlExecutor === undefined) {
            sqlExecutor = tursoc
          }
          for (const sql of sqls) {
            l("the sql", sql)
            if (sql === "sleep") {
              l("do sleep")
              await new Promise(r => setTimeout(r, 410))
              continue
            }
            await (sqlExecutor as turso.Client).batch([sql], "write")
          } 
        }
      } else {
        tursoBatchWrapper = async (sqlExecutor: turso.Transaction | turso.Client | undefined, sqls: SqlLike[]) => {
          sqls = sqls.filter(x => x !== "sleep")
          await (sqlExecutor as turso.Client).batch(sqls, "write")
        }
      }
      
      {
        try {
          await pRetry(
            async () => {
              const currBufferIndex = (await tursoc.execute(`SELECT curr_buffer_index FROM tk_meta_meta_info`)).rows[0].curr_buffer_index
              l("currBufferIndex", currBufferIndex)
              newBufferIndex = currBufferIndex === 0 ? 1 : 0
            }, {
              onFailedAttempt: error => {
                le(`doSyncToTursoIncr-queryCurrBufferIndex: attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`)
              },
              retries: 3 }
          )
        } catch (e) {
          console.error('Fetching remote curr_buffer_index error', e)
          newBufferIndex = 0
        }
      }
      const newBufferIndexStr = newBufferIndex!.toString()
      l("newBufferIndexStr", newBufferIndexStr)

      syncToTursoLastTime = Date.now()
      const reimportSqlDdlList: SqlLike[] = []
      const reimportSqlDataList: SqlLike[] = []
      if (tkEnv.SHOULD_HOARD_DROP_REMOTE_META_META === "1") {
        reimportSqlDdlList.push(`DROP TABLE IF EXISTS tk_meta_meta_info;`)
      }
      const tables = await mddb.db.raw(`SELECT * FROM sqlite_master WHERE type='table';`) as { tbl_name: string, sql: string }[]
      tables.sort((a, b) => (tableCreateForceOrder[a.tbl_name] || 0) - (tableCreateForceOrder[b.tbl_name] || 0))
      for (const t of tables) {
        if (t.tbl_name === 'tk_meta_meta_info') {
          reimportSqlDdlList.push(
            t.sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS'),
          )
          continue
        }
        reimportSqlDdlList.push(`DROP TABLE IF EXISTS ${t.tbl_name}_${newBufferIndexStr};`)
        reimportSqlDdlList.push(
          t.sql
            .replace(/^(CREATE TABLE `?)([^` ]+)(`?)/i, "$1$2_" + newBufferIndexStr + "$3")
            .replace(/(\sreferences\s+`?[^` ]+)(`?)/ig, "$1_" + newBufferIndexStr + "$2")  
          ,
          `DELETE FROM ${t.tbl_name}_${newBufferIndexStr};`,
        )
        reimportSqlDdlList.push(
          (tkEnv.SHOULD_HOARD_DROP_REMOTE_FILES_TABLE_INSTEAD_OF_VIEW === "1") ? `DROP TABLE IF EXISTS files` : `DROP VIEW IF EXISTS files`,
          `CREATE VIEW IF NOT EXISTS files AS
              SELECT * FROM files_0 WHERE
                (SELECT curr_buffer_index FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1) = 0
            UNION
              SELECT * FROM files_1 WHERE
                (SELECT curr_buffer_index FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1) = 1
          `,
        )
        reimportSqlDdlList.push(
          `DROP VIEW IF EXISTS tk_meta_kv`,
          `CREATE VIEW IF NOT EXISTS tk_meta_kv AS
              SELECT * FROM tk_meta_kv_0 WHERE
                (SELECT curr_buffer_index FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1) = 0
            UNION
              SELECT * FROM tk_meta_kv_1 WHERE
                (SELECT curr_buffer_index FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1) = 1
          `,
        )

        const currTableAllData = (await mddb.db.raw(`SELECT * FROM ${t.tbl_name};`)) as AnyObj[]
        if (currTableAllData.length > 0) {
          const batchSize = 3
          for (let i = 0; i < currTableAllData.length; i += batchSize) {
            const currBatchI = i
            const currBatch = currTableAllData.slice(currBatchI, currBatchI + batchSize)

            reimportSqlDataList
              .push({
                get sql() {
                  l(`${t.tbl_name}_${newBufferIndexStr} table: [${currBatchI}/${currTableAllData.length}]`)

                  return `INSERT INTO ${t.tbl_name}_${newBufferIndexStr} VALUES ` + currBatch.map(oneRow => ('( ' + Object.values(oneRow).map((_) => '?').join(', ') + ' )')).join(', ') + ';'
                },
                args: currBatch.flatMap(oneRow => Object.values(oneRow)),
              })


            if (t.tbl_name === "files") {
              reimportSqlDataList.push("sleep")
            }
            
          }
        }
      }

      const metaMetaTableAllData = (await mddb.db.raw(`SELECT * FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1;`)) as AnyObj[]
      metaMetaTableAllData[0]["curr_buffer_index"] = newBufferIndex
      reimportSqlDataList
        .push({
          sql: `INSERT OR REPLACE INTO tk_meta_meta_info VALUES ` + metaMetaTableAllData.map(oneRow => ('( ' + Object.values(oneRow).map((_) => '?').join(', ') + ' )')).join(', ') + ';',
          args: metaMetaTableAllData.flatMap(oneRow => Object.values(oneRow)),
        })
      
      for (const indexSql of indexSqlList) {
        reimportSqlDataList.push(indexSql.replace(/(\s+[Oo][Nn]\s+`?[^\s]+)(`?\s+)/, "$1_" + newBufferIndex + "$2"))
      }

      // reimportSqlList.forEach(x => {
      //   jsonPrettyPrint(x)
      // })

      await pRetry(async () => {
        // reimportSqlDdlList
        {
          const transaction = await tursoc.transaction("write")
          try {
            await tursoBatchWrapper(transaction, reimportSqlDdlList)

            await transaction.commit()
          } finally {
            // make sure to close the transaction, even if an exception was thrown
            transaction.close()
          }
        }
        // reimportSqlDataList
        {
          await tursoBatchWrapper(tursoc, reimportSqlDataList)
          // const transaction = await tursoc.transaction("write");
          // try {
          //   await tursoBatchWrapper(transaction, reimportSqlDataList)

          //   await transaction.commit();
          // } finally {
          //   // make sure to close the transaction, even if an exception was thrown
          //   transaction.close();
          // }
        }
      }, {
        onFailedAttempt: error => {
          le(error.message)
          le(`doSyncToTurso: attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`)
        },
        retries: 3 })
    }
  }

  const doSyncToTursoIncr = async () => {
    await doRefreshMetaMetaInfo()

    if (tursoc !== undefined) {
      const syncSqlList: SqlLike[] = []
      {
        const currTableAllData = (await mddb.db.raw(`SELECT * FROM tk_meta_meta_info;`)) as AnyObj[]
        if (currTableAllData.length > 0) {
          currTableAllData.forEach(oneRow => {
            delete oneRow.curr_buffer_index
          })
          syncSqlList
            .push({
              sql: `INSERT OR REPLACE INTO tk_meta_meta_info (` + Object.keys(currTableAllData[0]).map(x => '`' + x + '`').join(', ') + ', `curr_buffer_index`) VALUES ' + currTableAllData.map(oneRow => ('( ' + Object.values(oneRow).map((_) => '?').join(', ') + ', (SELECT curr_buffer_index FROM tk_meta_meta_info ORDER BY id DESC LIMIT 1) )')).join(', ') + ';',
              args: currTableAllData.flatMap(oneRow => Object.values(oneRow)),
            })
        }
      }

      {
        const currTableNewData = (await mddb.db.raw(`SELECT * FROM files WHERE update_time_by_hoard >= ?;`, syncToTursoLastTime ?? 0)) as AnyObj[]
        if (currTableNewData.length > 0) {
          const batchSize = 3
          for (let i = 0; i < currTableNewData.length; i += batchSize) {
            const currBatchI = i
            const currBatch = currTableNewData.slice(currBatchI, currBatchI + batchSize)

            syncSqlList
              .push({
                get sql() {
                  l(`files_${newBufferIndex} table: [${currBatchI}/${currTableNewData.length}]`)

                  return `DELETE FROM files_${newBufferIndex} WHERE _id IN (` + currBatch.map(_oneRow => '?').join(', ') + ');'
                },
                args: currBatch.flatMap(oneRow => oneRow._id),
              })

            for (const oneRow of currBatch) {
              if (oneRow.is_deleted_by_hoard) {
                l(`pending delete: ${oneRow.asset_raw_path}`)
              } else {
                l(`pending add/update: ${oneRow.asset_raw_path}`)
              }
            }

            syncSqlList
              .push({
                sql: `INSERT INTO files_${newBufferIndex} VALUES ` + currBatch.map(oneRow => ('( ' + Object.values(oneRow).map((_) => '?').join(', ') + ' )')).join(', ') + ';',
                args: currBatch.flatMap(oneRow => Object.values(oneRow)),
              })
          }
        }
      }

      // syncSqlList.forEach(x => {
      //   jsonPrettyPrint(x)
      // })
      await pRetry(async () => {
        await tursoc.batch(syncSqlList, "write")
      }, {
        onFailedAttempt: error => {
          le(`doSyncToTursoIncr: attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`)
        },
        retries: 3 })
    }
  }

  const mddb = await (async (origClient) => {
    const origSaveDataToDisk = origClient["saveDataToDisk"]
    origClient["saveDataToDisk"] = async function(...args: unknown[]) {
      try {
        l("initial indexing, start updating")
        const ret = await origSaveDataToDisk.apply(this, args)
        await doSyncToTurso()
        l("updated")
        return ret
      } catch (e) {
        console.error(e)
        process.exit(1)
      }
    }
    const origSaveDataToDiskIncr = origClient["saveDataToDiskIncr"]
    origClient["saveDataToDiskIncr"] = async function(...args: unknown[]) {
      try {
        l("change detected, start updating")
        const ret = await origSaveDataToDiskIncr.apply(this, args as [])
        await doSyncToTursoIncr()
        l("updated")
        return ret
      } catch (e) {
        console.error(e)
      }
    }
    return origClient
  })(origClient)

  l("initial indexFolder start")

  const genChildAssetFromLines = (() => {
    const Initial = 0
    const InOneChildDirectivePossible = 1
    const InOneChildSource = 2
    const regexTimestampPrefix = /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d /i

    // credits: https://github.com/tremby/json-multi-parse MIT
    const JSON_PARSE_ERROR_REGEXES = [
      /^()()Unexpected .* in JSON at position (\d+)$/, // Node 8..18, Chrome 69
      /^()()Unexpected non-whitespace character after JSON at position (\d+)($|\n)/, // Chromium 113
      /^JSON.parse: unexpected non-whitespace character after JSON data at line (\d+) column (\d+) of the JSON data()$/, // Firefox 62
    ]

    function splitByIndex(input: string, index: number) {
      if (index < 0 || index >= input.length) {
        throw new Error(`Character index ${index} out of range`)
      }
      return [input.substring(0, index), input.substring(index)]
    }

    function splitByLineAndChar(input: string, lineIndex: number, charIndex: number) {
      if (lineIndex < 0) {
        throw new Error(`Line index ${lineIndex} out of range`)
      }
      if (charIndex < 0) {
        throw new Error(`Character index ${charIndex} out of range`)
      }
    
      // Find the start of the line we are interested in
      let lineStartIndex = 0
      for (let l = lineIndex; l > 0; l--) {
        lineStartIndex = input.indexOf('\n', lineStartIndex)
        if (lineStartIndex === -1) {
          throw new Error(`Line index ${lineIndex} out of range`)
        }
        lineStartIndex++
      }
    
      // Check the character number we want is within this line
      const nextNl = input.indexOf('\n', lineStartIndex)
      if (lineStartIndex + charIndex >= input.length || nextNl !== -1 && nextNl <= lineStartIndex + charIndex) {
        throw new Error(`Character index ${charIndex} out of range for line ${lineIndex}`)
      }
    
      return splitByIndex(input, lineStartIndex + charIndex)
    }
    
    const consumeJson = (input: string, i: number) => {
      const inputSub = input.substring(i)
      if (inputSub.trim().length === 0) {
        return undefined
      }
    
      try {
        const result = JSON.parse(inputSub)
        const endRemainLength = inputSub.length - inputSub.trimEnd().length
        return {
          result,
          newI: i + inputSub.length - endRemainLength,
        }
      } catch (error) {
        let match = null
        for (const regex of JSON_PARSE_ERROR_REGEXES) {
          match = (error as Error).message.match(regex)
          if (match) {
            break
          }
        }
        if (!match) {
          console.error("Consuming JSON", error)
          return undefined
        }
    
        const chunks = match[3]
          ? splitByIndex(inputSub, parseInt(match[3], 10))
          : splitByLineAndChar(inputSub, parseInt(match[1], 10) - 1, parseInt(match[2], 10) - 1)
    
        const result = JSON.parse(chunks[0])
        const endRemainLength = chunks[0].length - chunks[0].trimEnd().length
        return {
          result,
          newI: i + chunks[0].length - endRemainLength,
        }
      }
    }

    const modifiedFilePathToUrl = (filePath: string) => {
      let url = filePath
        .replace(/\.(mdx|md)$/, "")
        .replace(/\\/g, "/") // replace windows backslash with forward slash
        .replace(/(\/)?index$/, "") // remove index from the end of the permalink
      url = url.length > 0 ? url : "/" // for home page
      return encodeURI(url)
    }

    return async (fileInfo: FileInfo, lines: string[]) => {
      let state = Initial
      const childFileInfoList = []
      let currentChildFileInfo: FileInfo = {} as FileInfo
      let currentChildFileAccumulatedSourceLines: string[] = []
      const parentSourceLines: string[] = []

      const endOneChild = () => {
        // further removed in process.js:processFile
        currentChildFileInfo._sourceWithoutMatter = currentChildFileAccumulatedSourceLines.join('\n')
        currentChildFileInfo.asset_raw_bytes = Buffer.from(currentChildFileInfo._sourceWithoutMatter, "utf-8")
        currentChildFileInfo.asset_type = currentChildFileInfo.metadata?.type || null
        currentChildFileInfo.tags = (currentChildFileInfo.metadata?.tags || [])
        currentChildFileInfo.asset_size = currentChildFileInfo.asset_raw_bytes.byteLength
      }

      const childPathSep = fileInfo.metadata?.derivedChildrenPathSep || '/'
      const childPathSepIsSlash = childPathSep === '/'

      const childPathDedup: Record<string, number> = {}
      for (const line of lines) {
        let i = 0
        const lineLength = line.length
        if (lineLength >= 20) {
          if (regexTimestampPrefix.test(line).valueOf()) {
            endOneChild()

            // new child
            state = InOneChildDirectivePossible
            const childName = line.substring(0, 19)
            const childPublishDate = new Date(childName)
            let childExtensionWithDot = (fileInfo.origin_file_extension || "").toLowerCase()
            if (childExtensionWithDot) {
              childExtensionWithDot = "." + childExtensionWithDot
            } else {
              childExtensionWithDot = ""
            }
            let childRawPath = fileInfo.asset_raw_path + '!!!' + childName + childExtensionWithDot
            let childLocator = fileInfo.asset_locator + childPathSep + childName /* + childExtensionWithDot */
            let childPathAppearCount = childPathDedup[childRawPath]
            if (childPathAppearCount === undefined) {
              childPathAppearCount = 0
            }
            childPathAppearCount += 1
            childPathDedup[childRawPath] = childPathAppearCount

            if (childPathAppearCount > 1) {
              childRawPath = fileInfo.asset_raw_path + '!!!' + childName + "_" + childPathAppearCount.toString() + childExtensionWithDot
              childLocator = fileInfo.asset_locator + childPathSep + childName + "_" + childPathAppearCount.toString() /* + childExtensionWithDot */
            }

            const childPathEncoded = Buffer.from(childRawPath, "utf-8").toString()
            currentChildFileInfo = {
              ...fileInfo,
              _id: crypto.createHash("sha1").update(childPathEncoded).digest("hex"),
              asset_raw_path: childRawPath,
              asset_locator: childLocator,
              // will assign later
              asset_type: undefined,
              metadata: structuredClone(fileInfo.metadata) || {},
              // will assign later
              tags: [],
              links: [],
              tasks: [],
              asset_size: undefined,
              publish_time_by_metadata: childPublishDate,
              has_derived_children: false,
              deriving_parent_id: fileInfo._id,
              // deriving_parent: fileInfo.asset_path,
            }
            currentChildFileInfo.metadata!.isDerivableIntoChildren = false
            currentChildFileInfo.metadata!.derivedChildrenPathSep = undefined
            childFileInfoList.push(currentChildFileInfo)
            currentChildFileAccumulatedSourceLines = []
            i = 20
          }
        }

        if (state === Initial) {
          parentSourceLines.push(line)
        }

        if (state === InOneChildDirectivePossible) {
          let first = true

          while (state === InOneChildDirectivePossible) {
            if (first) {
              first = false
            } else {
              if (lineLength - i >= 1 && line[i] === ' ') {
                do {
                  i++
                } while (lineLength - i >= 1 && line[i] === ' ')
              } else {
                state = InOneChildSource
                break
              }
            }

            if (lineLength - i >= 2) {
              const char0 = line[i]
              const char1 = line[i + 1]
              if (char0 === '{') {
                const cjr = consumeJson(line, i)
                if (cjr === undefined) {
                  state = InOneChildSource
                  break
                } else {
                  if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                    state = InOneChildSource
                    break
                  } else {
                    Object.assign(currentChildFileInfo.metadata!, cjr.result)
                    i = cjr.newI
                    state = InOneChildSource
                    if (i < line.length) {
                      i++ // must be space " "
                    }
                    break
                  }
                }
              } else if (char0 === '#') {
                // tag
                if (char1 === '"') {
                  const cjr = consumeJson(line, i + 1)
                  if (cjr === undefined) {
                    state = InOneChildSource
                    break
                  } else {
                    if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                      state = InOneChildSource
                      break
                    } else {
                      if (currentChildFileInfo.metadata!.tags === undefined) {
                        currentChildFileInfo.metadata!.tags = []
                      }
                      if (Array.isArray(currentChildFileInfo.metadata!.tags)) {
                        currentChildFileInfo.metadata!.tags.push(cjr.result.toString())
                      }
                      i = cjr.newI
                    }
                  }
                } else {
                  const spaceIndex = line.indexOf(" ", i + 1)
                  let currTag = undefined
                  if (spaceIndex === -1) {
                    currTag = line.substring(i + 1)
                  } else {
                    currTag = line.substring(i + 1, spaceIndex)
                  }

                  if (currTag === '') {
                    state = InOneChildSource
                    break
                  } else {
                    if (currentChildFileInfo.metadata!.tags === undefined) {
                      currentChildFileInfo.metadata!.tags = []
                    }
                    if (Array.isArray(currentChildFileInfo.metadata!.tags)) {
                      currentChildFileInfo.metadata!.tags.push(currTag)
                    }
                    i = i + 1 + currTag.length
                  }
                }
              } else {
                const lineSub = line.substring(i)
                if (/^[_a-zA-Z][_a-zA-Z0-9]{0,63}=/.test(lineSub)) {
                  const equalSignRelIndex = lineSub.indexOf("=")
                  if (equalSignRelIndex === -1) {
                    state = InOneChildSource
                    break
                  }
                  const keyName = lineSub.substring(0, equalSignRelIndex)
                  const charAfterEqualSign = (i + equalSignRelIndex + 1 < line.length) ? line[i + equalSignRelIndex + 1] : ''
                  if (charAfterEqualSign === '"' || charAfterEqualSign === '[' || charAfterEqualSign === '{') {
                    const cjr = consumeJson(line, i + equalSignRelIndex + 1)
                    if (cjr === undefined) {
                      state = InOneChildSource
                      break
                    } else {
                      if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                        state = InOneChildSource
                        break
                      } else {
                        currentChildFileInfo.metadata![keyName] = cjr.result
                        i = cjr.newI
                      }
                    }
                  } else {
                    const spaceIndex = line.indexOf(" ", i + equalSignRelIndex + 1)
                    if (spaceIndex === -1) {
                      currentChildFileInfo.metadata![keyName] = line.substring(i + equalSignRelIndex + 1)
                      i = line.length
                    } else {
                      currentChildFileInfo.metadata![keyName] = line.substring(i + equalSignRelIndex + 1, spaceIndex)
                      i = spaceIndex
                    }
                  }
                } else {
                  state = InOneChildSource
                  break
                }
              }
            } else {
              state = InOneChildSource
              break
            }
          }
        }

        if (state === InOneChildSource) {
          if (i < line.length) {
            currentChildFileAccumulatedSourceLines.push(line.substring(i))
          }
        }
        // l(line, state)
      }

      endOneChild()

      fileInfo.asset_raw_bytes = Buffer.from(parentSourceLines.join('\n') + '\n')
      fileInfo.asset_size = fileInfo.asset_raw_bytes.byteLength
      return childFileInfoList
    }
  })()

  // have background parts
  await mddb.indexFolder({
    folderPath: nodeResolvePath("./liveAsset"),
    ignorePatterns: [/Excalidraw/, /\.obsidian/, /DS_Store/],
    customConfig: {
      handleDedicated: async (filePath) => {
        const [_, pathStrippedExt, extension] = stripExtensionList(filePath, allKnownAssetExtNames)
        const isBackstage = filePath.startsWith('backstage/')
        let processedAssetLocator
        const isDedicated = isInExtensionList(extension, dedicatedAssetExtNames)
        if (isBackstage) {
          processedAssetLocator = filePath
        } else if (isInExtensionList(extension, strippedInLocatorExtNames)) {
          processedAssetLocator = pathStrippedExt
        } else  {
          processedAssetLocator = filePath
        }
          
        return [isDedicated, processedAssetLocator, extension]
      },
      isExtensionMarkdown: async (ext) => {
        return isInExtensionList(ext, markdownExtNames)
      },
      deriveChildFileInfo: async (fileInfo, sourceWithoutMatter, metadata) => {
        if (metadata.isDerivableIntoChildren) {
          fileInfo.has_derived_children = true
          // fileInfo.deriving_parent = undefined
          return await genChildAssetFromLines(fileInfo, sourceWithoutMatter.split("\n"))
        } else {
          return []
        }
      },
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      markdownExtraHandler: async (filePath, getSourceFunc, fileInfo, fileInfoList, { ast, metadata, links, tags }) => {
        // l("ast", JSON.stringify(ast, null, 2))
      },
      otherHandlers: [
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        async (filePath, getSourceFunc, fileInfo) => {

        },
      ],
    },
    watch: true
  })

  l("initial indexFolder done")
  l("[Watching changes]")


  ;(async () => {
    // eslint-disable-next-line no-constant-condition
    if (false) {
      return
    }

    if (!tkEnv.TELEGRAM_HOARD_DEST_ROOT_PATH) {
      throw new Error("TELEGRAM_HOARD_DEST_ROOT_PATH missing")
    }
    const tgDest = replaceAll(tkEnv.TELEGRAM_HOARD_DEST_ROOT_PATH, '\\', '/')
    l("tgDest", tgDest)
    const tgMediaDest = replaceAll(path.join(tgDest, "media"), '\\', '/')
    l("tgMediaDest", tgMediaDest)

    if (!tkEnv.TELEGRAM_PUB_HOARD_DEST_ROOT_PATH) {
      throw new Error("TELEGRAM_PUB_HOARD_DEST_ROOT_PATH missing")
    }
    const tgPubDest = replaceAll(tkEnv.TELEGRAM_PUB_HOARD_DEST_ROOT_PATH, '\\', '/')
    l("tgPubDest", tgPubDest)
    const tgPubMediaDest = replaceAll(path.join(tgPubDest, "media"), '\\', '/')
    l("tgPubMediaDest", tgPubMediaDest)

    await ensurePathDirExists(tgMediaDest)
    await ensurePathDirExists(tgPubMediaDest)

    const storeSession = new StoreSession("tg_session")
    const client = new telegram.TelegramClient(storeSession, parseInt(tkEnv.TELEGRAM_API_ID!), tkEnv.TELEGRAM_API_HASH!, {
      // useWSS: false, // Important. Most proxies cannot use SSL.
      useWSS: true,
      proxy: {
        ip: "127.0.0.1", // Proxy host (IP or hostname)
        port: parseInt(tkEnv.TELEGRAM_PROXY_PORT!), // Proxy port
        secret: "", // If used MTProxy then you need to provide a secret (or zeros).
        socksType: 5, // If used Socks you can choose 4 or 5.
        timeout: 2, // Timeout (in seconds) for connection,
      },
    })
    await client.start({
      phoneNumber: async () => await input.text("tg phoneNumber?"),
      password: async () => await input.text("tg password?"),
      phoneCode: async () => await input.text("tg phoneCode?"),
      onError: (err) => console.error(err),
    })
    if (!client.connected) {
      throw new Error("Telegram is not connected, there may be further error info above")
    }
    if (!(await client.checkAuthorization())) {
      throw new Error("Telegram !client.checkAuthorization")
    }
    if (!(await client.isUserAuthorized())) {
      throw new Error("Telegram !client.isUserAuthorized")
    }
    const me = await client.getMe()
    l("You should now be connected to Telegram. current userId=", me.id, " userName=", me.username)
    const meIdStr = me.id.toString()

    const throttledSendHoarded = throttle(5000, async () => {
      const randomId = randomBigint(128)!
      await client.invoke(
        new telegram.Api.messages.SendMessage({
          peer: new telegram.Api.InputPeerSelf(),
          message: "hoarded",
          randomId: randomId as unknown as LibraryBigInteger,
        })
      )
    })

    const getGjSessionPeerInfo = async (peerLike: telegram.Api.TypePeer) => {
      // (id, hash, username, phone, name, date)
      const result = await storeSession.getEntityRowsById(telegram.utils.getPeerId(peerLike), true)
      if (result && Array.isArray(result) && result.length >= 6) {
        result[1] = null
        result[3] = null
        result[5] = null
      }
      return result
    }

    const resolveMessagePeerInplace = async (theMessage: telegram.Api.Message) => {
      if (theMessage.fromId && typeof theMessage.fromId === "object") {
        try {
          (theMessage.fromId as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.fromId)
          theMessage.originalArgs.fromId.gjSessionPeerInfo = (theMessage.fromId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fromId.originalArgs.gjSessionPeerInfo = (theMessage.fromId as AnyObj).gjSessionPeerInfo
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fromId err", e)
        }
      }
      if (theMessage.peerId && typeof theMessage.peerId === "object") {
        try {
          (theMessage.peerId as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.peerId)
          theMessage.originalArgs.peerId.gjSessionPeerInfo = (theMessage.peerId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.peerId.originalArgs.gjSessionPeerInfo = (theMessage.peerId as AnyObj).gjSessionPeerInfo
        } catch (e) {
          le("resolveMessagePeerInplace: resolve peerId err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.fromId && typeof theMessage.fwdFrom.fromId === "object") {
        try {
          (theMessage.fwdFrom.fromId as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.fwdFrom.fromId)
          theMessage.originalArgs.fwdFrom.fromId.gjSessionPeerInfo = (theMessage.fwdFrom.fromId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.fromId.gjSessionPeerInfo = (theMessage.fwdFrom.fromId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.fromId.originalArgs.gjSessionPeerInfo = (theMessage.fwdFrom.fromId as AnyObj).gjSessionPeerInfo
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.fromId err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.savedFromPeer && typeof theMessage.fwdFrom.savedFromPeer === "object") {
        try {
          (theMessage.fwdFrom.savedFromPeer as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.fwdFrom.savedFromPeer)
          theMessage.originalArgs.fwdFrom.savedFromPeer.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromPeer as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.savedFromPeer.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromPeer as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.savedFromPeer.originalArgs.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromPeer as AnyObj).gjSessionPeerInfo
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.savedFromPeer err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.savedFromId && typeof theMessage.fwdFrom.savedFromId === "object") {
        try {
          (theMessage.fwdFrom.savedFromId as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.fwdFrom.savedFromId)
          theMessage.originalArgs.fwdFrom.savedFromId.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.savedFromId.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromId as AnyObj).gjSessionPeerInfo
          theMessage.originalArgs.fwdFrom.originalArgs.savedFromId.originalArgs.gjSessionPeerInfo = (theMessage.fwdFrom.savedFromId as AnyObj).gjSessionPeerInfo
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.savedFromId err", e)
        }
      }

      await (async (theMessage0) => {
        const theMessage = theMessage0 as AnyObj

        if (theMessage.savedPeerId && typeof theMessage.savedPeerId === "object") {
          try {
            (theMessage.savedPeerId as AnyObj).gjSessionPeerInfo = await getGjSessionPeerInfo(theMessage.savedPeerId)
            theMessage.originalArgs.savedPeerId.gjSessionPeerInfo = (theMessage.savedPeerId as AnyObj).gjSessionPeerInfo
            theMessage.originalArgs.savedPeerId.originalArgs.gjSessionPeerInfo = (theMessage.savedPeerId as AnyObj).gjSessionPeerInfo
          } catch (e) {
            le("resolveMessagePeerInplace: resolve savedPeerId err", e)
          }
        }
      })(theMessage)
      
    }

    const resolveRepliedMessage = async (theMessage: telegram.Api.Message) => {
      await new Promise(r => setTimeout(r, 1500))

      // WARN: when current user havn't joined this peer (channel), this FETCHES THE WRONG MESSAGE
      // let repliedMessage = await theMessage?.getReplyMessage()
      let repliedMessage: telegram.Api.Message | undefined = undefined

      if (repliedMessage === undefined) {
        // will cause FLOOD_WAIT. CAUTION!!!
        // await client.getEntity(...)

        l("theMessage?.replyTo?.replyToPeerId", theMessage?.replyTo?.replyToPeerId)
        const inputPeer = await client.getInputEntity(theMessage?.replyTo?.replyToPeerId as telegram.Api.TypePeer)
        l("inputPeer", inputPeer)
        // const inputPeer = await client.getInputEntity(new telegram.Api.Channel({
        //   id: (theMessage?.replyTo?.replyToPeerId as telegram.Api.PeerChannel).channelId
        // } as telegram.Api.Channel))

        if (theMessage?.replyTo?.replyToMsgId) {
          repliedMessage = (await client.getMessages(inputPeer, {
            ids: new telegram.Api.InputMessageID({ id: theMessage?.replyTo?.replyToMsgId }),
          }))[0]
        }
      }

      repliedMessage = repliedMessage || {} as telegram.Api.Message
      await resolveMessagePeerInplace(repliedMessage)
      return repliedMessage
    }

    const mediaDownQ = new AsyncQueue(1, 1000)
    const appendFileQ = new AsyncQueue(1, 0)

    // TODO: check if message handler has queue logic, ensure it sequential and has time interval
    async function msgEvent(event: tgEvents.NewMessageEvent) {
      if ((event?.message?.peerId as telegram.Api.PeerUser)?.userId?.toString() !== meIdStr) {
        return
      }
      if ((event?.message?.message ?? '') === "hoarded") {
        return
      }
      const now = new Date()
      let messageTextForLog = (event?.message?.message ?? '')
      if (messageTextForLog.length >= 64) {
        messageTextForLog = messageTextForLog.substring(0, 61) + "..."
      }
      messageTextForLog = JSON.stringify(messageTextForLog)
      l(`telegram: processing message ${messageTextForLog}`)
      const m0 = {...(event?.message ?? {})}
      const m: (typeof m0) & { client?: AnyObj, extRepliedMessage?: AnyObj } = m0
      m.client = undefined
      m._client = undefined
      await resolveMessagePeerInplace(m as telegram.Api.Message)

      const message = event?.message
      let repliedMessage: telegram.Api.Message | AnyObj | undefined = undefined
      let repliedMessageLv2: telegram.Api.Message | AnyObj | undefined = undefined
      let repliedMessageLv3: telegram.Api.Message | AnyObj | undefined = undefined

      if (event?.message?.replyTo?.replyToMsgId) {
        repliedMessage = await resolveRepliedMessage(event.message)
        m.extRepliedMessage = {
          ...repliedMessage
        }
        m.extRepliedMessage!._eventBuilders = undefined
        m.extRepliedMessage!.client = undefined
        m.extRepliedMessage!._client = undefined
        // to make gramjs toJSON() happy
        m.originalArgs.extRepliedMessage = m.extRepliedMessage
        if (repliedMessage?.replyTo?.replyToMsgId) {
          await new Promise(r => setTimeout(r, 1500))
          repliedMessageLv2 = await resolveRepliedMessage(repliedMessage as telegram.Api.Message)
          const mm = {
            ...repliedMessageLv2
          }
          m.extRepliedMessage!.extRepliedMessage = mm
          m.extRepliedMessage!.originalArgs.extRepliedMessage = mm
          ;(mm as AnyObj)._eventBuilders = undefined
          ;(mm as AnyObj).client = undefined
          mm._client = undefined

          if (repliedMessageLv2?.replyTo?.replyToMsgId) {
            repliedMessageLv3 = await resolveRepliedMessage(repliedMessageLv2 as telegram.Api.Message)
            const mmm = {
              ...repliedMessageLv3
            }
            m.extRepliedMessage!.extRepliedMessage.extRepliedMessage = mmm
            m.extRepliedMessage!.extRepliedMessage.originalArgs.extRepliedMessage = mmm
            ;(mmm as AnyObj)._eventBuilders = undefined
            ;(mmm as AnyObj).client = undefined
            mmm._client = undefined
          }
        }
      }

      let currDest
      let currMediaDest
      let jsonMessageToBeSaved
      if (message.message === "pub" && m.extRepliedMessage) {
        currDest = tgPubDest
        currMediaDest = tgPubMediaDest
        jsonMessageToBeSaved = m.extRepliedMessage
      } else {
        currDest = tgDest
        currMediaDest = tgMediaDest
        jsonMessageToBeSaved = m
      }

      for (const currM of [message, repliedMessage, repliedMessageLv2, repliedMessageLv3]) {
        if (currM) {
          if (currM.media?.photo?.id) {
            await mediaDownQ.run(async () => {
              const p = datePath(currMediaDest, currM.media?.photo?.id + ".png", "[%y-%m]/m-", now)
              await ensureParentDirExists(p)
              await client.downloadMedia(currM as telegram.Api.Message, {
                outputFile: p,
              })
              l(`telegram: saved media photo in ${p}`)
            })
          }
          if (currM.media?.webpage?.photo?.id) {
            await mediaDownQ.run(async () => {
              const p = datePath(currMediaDest, currM.media?.webpage?.photo?.id + ".png", "[%y-%m]/m-", now)
              await ensureParentDirExists(p)
              await client.downloadMedia(currM as telegram.Api.Message, {
                outputFile: p,
              })
              l(`telegram: saved webpage photo in ${p}`)
            })
          }
          // if (currM.media?.document?.id) {

          // }
        }
      }

      const jsonString = JSON.stringify(jsonMessageToBeSaved, function (key, value) {
        if (key === '_client' || key === 'client' || key === 'originalArgs') {
          return undefined
        }
        if (key.length > 0 && key[0] === '_') {
          return undefined
        }
        return value 
      }, 0)

      const tgFilePath = datePath(currDest, ".md", "[%y-%m]/%y-%m-%d", now)

      await appendFileQ.run(async () => {
        await ensureParentDirExists(tgFilePath)

        if (!(await pathExists(tgFilePath))) {
          l("telegram: " + tgFilePath + " not exists, creating")
          await fs.promises.writeFile(tgFilePath, `---\nisDerivableIntoChildren: true\nisTelegramRecord: true\n---\n`, {
            encoding: 'utf-8',
            flush: true,
          })
        }
        await fs.promises.appendFile(tgFilePath, `\n${genTimestampString(now)} tg=${jsonString}\n\n`, {
          encoding: 'utf-8',
          flush: true,
        })
      })

      await new Promise(r => setTimeout(r, 300))
      throttledSendHoarded()
      l(`telegram: saved message ${messageTextForLog} in ${tgFilePath}`)
    }

    client.addEventHandler(msgEvent, new tgEvents.NewMessage({}))
  })()
}

main()
// await main()
