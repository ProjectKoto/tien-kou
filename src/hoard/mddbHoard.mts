import * as turso from "@libsql/client"
import crypto from "crypto"
import { MarkdownDB } from "mddb"
import { FileInfo } from "mddb/dist/src/lib/process"
import pRetry from 'p-retry'
import { allKnownAssetExtNames, AnyObj, dedicatedAssetExtNames, isInExtensionList, l, le, markdownExtNames, stripExtensionList, strippedInLocatorExtNames } from '../lib/common.mts'
import { ensurePathDirExists, nodeResolvePath, TkContextHoard } from "../lib/nodeCommon.mts"
import path from 'node:path'
import escapeStringRegexp from 'escape-string-regexp'
import fs from 'node:fs'
import os from 'node:os'
import { buildGitProcessRunner, gitSyncScriptPath } from "../lib/nodeGitUtil.mts"
import replaceAll from 'string.prototype.replaceall'
import { defaultMarkdowndbDbPath } from "../serve/serveDef.mts"

// Only a very small subset of .gitignore and rclone filter match rules is supported
// here, and they still are not guaranteed to work as expected.

// Supported:
// **/*, **/a.txt, *.log, /dir1/file1, dir2/file2, /dir3/**

// Not supported:
// /**abc, **def**xyz (won't match /abc/def/ghi/xyz in git), !dir3/file3, path//to///some/file, //path/to/file, path/to/dir4, path/to/dir4/, /path/to/dir4, /path/to/dir4/, [0-9].txt, a?.txt, *.{jpg,png} ...


const convertIgnoreItemToMddbRegExp = (ignoreItem: string, baseDirPath: string) => {
  const doBreak = (delimiter: string, doBreakInner?: ((target2: string) => string[])) => (target: string): string[] => {
    const parts = target.split(delimiter)
    return parts.reduce((prev, curr) => {
      if (prev.length > 0) {
        prev.push(delimiter)
      }
      if (doBreakInner) {
        const breakInnerResult = doBreakInner(curr)
        prev.push(...breakInnerResult)
      } else {
        prev.push(curr)
      }
      return prev
    }, [] as string[])
  }

  let baseDirNonSlashLastI = baseDirPath.length - 1
  for (; baseDirNonSlashLastI >= 0; baseDirNonSlashLastI--) {
    if (baseDirPath[baseDirNonSlashLastI] !== '/') {
      break
    }
  }
  baseDirPath = baseDirNonSlashLastI < 0 ? '/' : baseDirPath.substring(0, baseDirNonSlashLastI + 1)

  const startFromBaseDir = ignoreItem.startsWith('/')
  // const ignoreItemStrippedStartingSlash = startFromBaseDir ? ignoreItem.substring(1) : ignoreItem
  const parsedParts = doBreak('**', doBreak('*'))(ignoreItem)
  let regexpStr = '^' + escapeStringRegexp(baseDirPath) + '(?=\\/)' + (startFromBaseDir ? '' : '(?:|.*/)')
  for (const part of parsedParts) {
    if (part === '**') {
      regexpStr += '.*'
    } else if (part === '*') {
      regexpStr += '[^/]*'
    } else {
      regexpStr += escapeStringRegexp(part)
    }
  }
  regexpStr += '$'

  // MUST NOT BE 'g' HERE.
  // RegExp.test(...) changes RegExp's INTERNAL STATE when it's /g !
  return new RegExp(regexpStr, 'i')
}

export const startMddbHoard = async (tkCtx: TkContextHoard, onUpdate: () => Promise<void>) => {
  const shouldRun = true
  if (!shouldRun) {
    return
  }
  const tkEnv = tkCtx.e

  // reused by the following ignore lists; do no assume a base path.
  const commonIgnoreList = [
    '**/*Excalidraw*',
    '**/*Excalidraw*/**',
    '**/*.obsidian*',
    '**/*.obsidian*/**',
    '**/*DS_Store*',
    '**/*DS_Store*/**',
    '**/.sync-conflict-*',
    '**/.syncthing.*.tmp',
    '**/.syncthing.tmp',
    '**/unscannedAsset/**',
    '/staticGen/**',
  ]

  if (!(
    tkEnv.SHOULD_INCLUDE_UNRELEASED_ASSETS &&
      ((tkEnv.SHOULD_INCLUDE_UNRELEASED_ASSETS).toString() === '1' || (tkEnv.SHOULD_INCLUDE_UNRELEASED_ASSETS).toString() === 'true')
  )) {
    commonIgnoreList.push(
      '**/Unreleased/**',
      '**/UnreleasedSlip/**',
      '**/Unreleased.md',
      '**/UnreleasedSlip.md',
      '**/TheUnreleased.md',
      '**/＊Unreleased.md',
      '**/＊TheUnreleased.md',
    )
  }

  // assume base path: HOARD_MULTI_TARGET_SYNC_BASE_DIR/
  const rcloneHeavyIgnoreList = [
    ...commonIgnoreList,
    '/liveAsset/backstage/**',
    ...(() => {
      const result: string[] = []
      dedicatedAssetExtNames.forEach(extName => {
        result.push(`/liveAsset/guarded/**/*.${extName}`)
      })
      return result
    })()
  ]

  // assume base path: HOARD_MULTI_TARGET_SYNC_BASE_DIR/liveAsset
  const mddbWatchIgnoreList = [
    ...commonIgnoreList,
  ]

  // assume base path: HOARD_MULTI_TARGET_SYNC_BASE_DIR/
  const gitIgnoreList = [
    ...commonIgnoreList,
  ]

  let tursoc: turso.Client | undefined = undefined

  const dbPath = tkCtx.tkEnv.MARKDOWNDB_DB_PATH || defaultMarkdowndbDbPath
  const liveAssetBaseSlashPath = nodeResolvePath(tkCtx.e.NODE_LOCAL_FS_LIVE_ASSET_BASE_PATH!).split(path.sep).join('/')
  const multiTargetSyncBaseSlashPath = nodeResolvePath(tkCtx.e.HOARD_MULTI_TARGET_SYNC_BASE_DIR!).split(path.sep).join('/')

  if (tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === '1' || tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === 'true') {
    tursoc = undefined
  } else {
    tursoc = turso.createClient({
      url: tkEnv.TURSO_DATABASE_URL!,
      authToken: tkEnv.TURSO_AUTH_TOKEN,
    })
  }

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
                  let endI = currBatchI + batchSize
                  endI = endI > currTableAllData.length ? currTableAllData.length : endI
                  l(`${t.tbl_name}_${newBufferIndexStr} table: [${endI}/${currTableAllData.length}]`)

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
        l('reimportSqlDdlList')
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
        l('reimportSqlDataList')
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
        l('reimportSql done')
      }, {
        onFailedAttempt: error => {
          le(error.message)
          le(`doSyncToTurso: attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`)
        },
        retries: 3 })
    }
  }

  const doSyncToTursoIncr = async () => {
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
        l(`syncToTursoLastTime`, syncToTursoLastTime)
        const currTableNewData = (await mddb.db.raw(`SELECT * FROM files WHERE update_time_by_hoard >= ?;`, syncToTursoLastTime ?? 0)) as AnyObj[]
        if (currTableNewData.length > 0) {
          const batchSize = 3
          for (let i = 0; i < currTableNewData.length; i += batchSize) {
            const currBatchI = i
            const currBatch = currTableNewData.slice(currBatchI, currBatchI + batchSize)

            syncSqlList
              .push({
                get sql() {
                  let endI = currBatchI + batchSize
                  endI = endI > currTableNewData.length ? currTableNewData.length : endI
                  l(`files_${newBufferIndex} table: [${endI}/${currTableNewData.length}]`)

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

  const gitLocalLiveAssetBareRepoPath = tkCtx.e.HOARD_GIT_LOCAL_LIVE_ASSET_BARE_REPO_PATH ? nodeResolvePath(tkCtx.e.HOARD_GIT_LOCAL_LIVE_ASSET_BARE_REPO_PATH).split(path.sep).join('/') : undefined
  const gitRemote = tkCtx.e.HOARD_GIT_LIVE_ASSET_REMOTE ? tkCtx.e.HOARD_GIT_LIVE_ASSET_REMOTE : undefined

  const gitSyncLiveAsset = async () => {
    const gitIgnoreFileTmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'tk-git-ignore-'))
    
    try {
      const gitIgnoreFilePath = path.join(gitIgnoreFileTmpDir, 'tkGitIgnore').split(path.sep).join('/')
      await fs.promises.writeFile(gitIgnoreFilePath, gitIgnoreList.join('\n'), 'utf-8')

      try {
        const fixedGitArgs = ['--git-dir=' + gitLocalLiveAssetBareRepoPath, '--work-tree=' + multiTargetSyncBaseSlashPath, '-c', 'alias.tk-sync=!bash ' + gitSyncScriptPath, '-c', 'branch.tk_asset_main.remote=origin', '-c', 'user.email=automation@localhost', '-c', 'user.name=automation', '-c', 'branch.tk_asset_main.syncNewFiles=true', '-c', 'branch.tk_asset_main.sync=true', '-c', 'core.excludesFile=' + gitIgnoreFilePath, '-c', 'branch.tk_asset_main.tkSyncRebaseMergeStrategyOption=theirs']

        const runGitProcess = buildGitProcessRunner('git-sync-live', fixedGitArgs)

        if (gitLocalLiveAssetBareRepoPath && gitRemote) {
          await ensurePathDirExists(gitLocalLiveAssetBareRepoPath)
          await buildGitProcessRunner('git-sync-live', [])(['init', '--bare', gitLocalLiveAssetBareRepoPath], 'init-bare', 4000, false)
          await runGitProcess(['remote', 'add', 'origin', gitRemote], 'add-remote', 4000, false)
          const ensureBranch1Result = await runGitProcess(['branch', '--force', 'tk_asset_main', 'HEAD'], 'ensure-branch1', 20000, false)
          if (ensureBranch1Result.exitCode !== 0 && ensureBranch1Result.stderr.indexOf('not a valid object name') < 0) {
            await runGitProcess(['checkout', '-b', 'tk_asset_main'], 'ensure-branch2', 4000, false)
          }
          await runGitProcess(['reset', 'tk_asset_main'], 'ensure-branch1', 20000, false)
          const rmCachedProcResult = await runGitProcess(['rm', '--cached', '-r', '.'], '', 20000, false)
          const rmCachedStdout = (rmCachedProcResult.stdout || '').split('\n')
          if (rmCachedStdout.length > 14) {
            rmCachedStdout.splice(5, rmCachedStdout.length - 10, '...')
          }
          for (const line of rmCachedStdout) {
            console.log(`git-sync-live: rm-cached stdout: ${line}`)
          }
          const rmCachedStderr = (rmCachedProcResult.stderr || '').split('\n')
          if (rmCachedStderr.length > 14) {
            rmCachedStderr.splice(5, rmCachedStderr.length - 10, '...')
          }
          for (const line of rmCachedStderr) {
            console.log(`git-sync-live: rm-cached stderr: ${line}`)
          }
          await runGitProcess(['tk-sync'], 'tk-sync', 120*1000, true)
        }
      } catch (e) {
        le('gitSyncLiveAsset err', e)
      }
    } finally {
      await fs.promises.rm(gitIgnoreFileTmpDir, {recursive: true, force: true})
    }
  }

  const rcloneAssetFilter = rcloneHeavyIgnoreList.map(x => '- ' + x)

  const rcloneHeavy = async () => {
    try {
      const [exitStatus, _exitSignal, stdout, stderr] = await tkCtx.rcloneW('sync', tkCtx.e.HOARD_MULTI_TARGET_SYNC_BASE_DIR!, 'TK_HOARD_HEAVY_SYNC:' + (tkCtx.e.HOARD_RCLONE_TK_HOARD_HEAVY_DEST_DIR || ''), {
        'verbose': true,
        'ignore-case': true,
        'ignore-errors': true,
        'delete-after': true,
        'delete-excluded': true,
        'max-duration': '10m',
        'copy-links': true,
        'filter': rcloneAssetFilter,
        abortSignal: AbortSignal.timeout(60 * 12 * 1000),
      }) as [number, number, Buffer, Buffer]

      l('rcloneHeavy result', exitStatus, stdout.toString('utf-8'), stderr.toString('utf-8'))
    } catch (e) {
      le('rcloneHeavy err', e)
    }
  }

  const mddb = await (async (origClient) => {
    const origSaveDataToDisk = origClient["saveDataToDisk"]
    origClient["saveDataToDisk"] = async function(...args: unknown[]) {
      try {
        l("initial indexing, start updating")
        const ret = await origSaveDataToDisk.apply(this, args)
        await doRefreshMetaMetaInfo()
        const tasksToWait: Promise<unknown>[] = []
        if ((tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnly' && (tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnlyOnce') {
          if (!(tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === '1' || tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === 'true')) {
            await doSyncToTurso()
            l('scheduling rcloneHeavy...')
            tasksToWait.push(rcloneHeavy())
            l('scheduling gitSyncLiveAsset...')
            tasksToWait.push(gitSyncLiveAsset())
          }
        }
        l('running update hook...')
        await onUpdate()
        l("updated")
        l("initial indexFolder done")
        if ((tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardOnce' && (tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnlyOnce') {
          l("[Watching changes]")
        } else {
          await Promise.all(tasksToWait)
          process.exit(0)
        }
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
        const ret = await origSaveDataToDiskIncr.apply(this, args as [number])
        await doRefreshMetaMetaInfo()
        if ((tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnly' && (tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnlyOnce') {
          if (!(tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === '1' || tkEnv.HOARD_FILE_SYNC_RELATED_DISABLE === 'true')) {
            await doSyncToTursoIncr()
            l('scheduling rcloneHeavy...')
            rcloneHeavy()
            l('scheduling gitSyncLiveAsset...')
            gitSyncLiveAsset()
          }
        }
        l('running update hook...')
        await onUpdate()
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
    const InOneChildAfterDirective = 2
    const InOneChildSource = 3
    const regexTimestampPrefix = /^\d\d\d\d-\d\d-\d\d \d\d[:.]\d\d[:.]\d\d/i

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

    const _modifiedFilePathToUrl = (filePath: string) => {
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
      let ongoingAssetGroup: string | undefined = undefined
      let ongoingAssetGroupPrimaryUpdaters: ((p: string) => void)[] | undefined = undefined

      let childMaxPubTime: number | undefined = undefined

      const endOneChild = () => {
        // further removed in process.js:processFile
        currentChildFileInfo._sourceWithoutMatter = currentChildFileAccumulatedSourceLines.join('\n')
        {
          let i = currentChildFileInfo._sourceWithoutMatter.length - 1
          while (i >= 0) {
            if (currentChildFileInfo._sourceWithoutMatter[i] === '\n') {
              i--
            } else {
              break
            }
          }
          currentChildFileInfo._sourceWithoutMatter = currentChildFileInfo._sourceWithoutMatter.substring(0, i + 1)
        }
        currentChildFileInfo.asset_raw_bytes = Buffer.from(currentChildFileInfo._sourceWithoutMatter, "utf-8")
        if (ongoingAssetGroup) {
          if (!currentChildFileInfo.metadata) {
            currentChildFileInfo.metadata = {}
          }
          currentChildFileInfo.metadata.groupPrimaryLocator = ongoingAssetGroup
          if (ongoingAssetGroupPrimaryUpdaters !== undefined) {
            {
              const currentChildFileInfoCaptured = currentChildFileInfo
              ongoingAssetGroupPrimaryUpdaters.push(p => {
                currentChildFileInfoCaptured.metadata!.groupPrimaryLocator = p
              })
            }
          }
        }
        currentChildFileInfo.asset_type = currentChildFileInfo.metadata?.type || null
        currentChildFileInfo.declaredTags = (currentChildFileInfo.metadata?.declaredTags || [])
        currentChildFileInfo.asset_size = currentChildFileInfo.asset_raw_bytes.byteLength

        if (childMaxPubTime === undefined || childMaxPubTime < currentChildFileInfo.publish_time_by_metadata) {
          childMaxPubTime = currentChildFileInfo.publish_time_by_metadata
        }
      }

      const childPathSep = fileInfo.metadata?.derivedChildrenPathSep || '/'
      const _childPathSepIsSlash = childPathSep === '/'

      const childPathDedup: Record<string, number> = {}
      for (const line of lines) {
        let i = 0
        const lineLength = line.length
        if (lineLength >= 19) {
          if (regexTimestampPrefix.test(line).valueOf()) {
            endOneChild()

            // new child
            state = InOneChildDirectivePossible
            // considering static file generation on Windows
            const childName = replaceAll(line.substring(0, 19), ':', '.')
            const validChildDateStr = replaceAll(line.substring(0, 19), '.', ':')
            const childPublishDate = new Date(validChildDateStr)
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
              referencedTags: [],
              declaredTags: [],
              links: [],
              tasks: [],
              asset_size: undefined,
              publish_time_by_metadata: childPublishDate.getTime(),
              has_derived_children: false,
              deriving_parent_id: fileInfo._id,
              // deriving_parent: fileInfo.asset_path,
              should_discard: false,
            }
            currentChildFileInfo.metadata!.isDerivableIntoChildren = false
            currentChildFileInfo.metadata!.derivedChildrenPathSep = undefined
            childFileInfoList.push(currentChildFileInfo)
            currentChildFileAccumulatedSourceLines = []
            i = lineLength >= 20 ? 20 : 19
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
                state = InOneChildAfterDirective
                break
              }
            }

            if (lineLength - i >= 2) {
              const char0 = line[i]
              const char1 = line[i + 1]
              let char2: string = undefined!
              if (lineLength - i >= 3) {
                char2 = line[i + 2]
              }
              if (char0 === '{') {
                const cjr = consumeJson(line, i)
                if (cjr === undefined) {
                  state = InOneChildAfterDirective
                  break
                } else {
                  if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                    state = InOneChildAfterDirective
                    break
                  } else {
                    Object.assign(currentChildFileInfo.metadata!, cjr.result)
                    i = cjr.newI
                    state = InOneChildAfterDirective
                    if (i < line.length) {
                      i++ // must be space " "
                    }
                    break
                  }
                }
              } else if (char0 === '[' && char1 == '[' && (char2 === undefined || char2 === ' ')) {
                ongoingAssetGroup = currentChildFileInfo.asset_locator
                ongoingAssetGroupPrimaryUpdaters = []
                
                i += 2
              } else if (char0 === '~' && char1 == '~' && (char2 === undefined || char2 === ' ')) {
                ongoingAssetGroup = currentChildFileInfo.asset_locator
                currentChildFileInfo.metadata!.isHoistedInGroup = true
                if (ongoingAssetGroupPrimaryUpdaters !== undefined) {
                  ongoingAssetGroupPrimaryUpdaters.forEach(u => {
                    u(currentChildFileInfo.asset_locator)
                  })
                }
                
                i += 2
              } else if (char0 === ']' && char1 == ']' && (char2 === undefined || char2 === ' ')) {
                ongoingAssetGroup = undefined
                ongoingAssetGroupPrimaryUpdaters = undefined
                currentChildFileInfo.should_discard = true
                i += 2
              } else if (char0 === '#') {
                // tag
                if (char1 === '"') {
                  const cjr = consumeJson(line, i + 1)
                  if (cjr === undefined) {
                    state = InOneChildAfterDirective
                    break
                  } else {
                    if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                      state = InOneChildAfterDirective
                      break
                    } else {
                      if (currentChildFileInfo.metadata!.declaredTags === undefined) {
                        currentChildFileInfo.metadata!.declaredTags = []
                      }
                      if (Array.isArray(currentChildFileInfo.metadata!.declaredTags)) {
                        currentChildFileInfo.metadata!.declaredTags.push(cjr.result.toString())
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
                    state = InOneChildAfterDirective
                    break
                  } else {
                    if (currentChildFileInfo.metadata!.declaredTags === undefined) {
                      currentChildFileInfo.metadata!.declaredTags = []
                    }
                    if (Array.isArray(currentChildFileInfo.metadata!.declaredTags)) {
                      currentChildFileInfo.metadata!.declaredTags.push(currTag)
                    }
                    i = i + 1 + currTag.length
                  }
                }
              } else {
                const lineSub = line.substring(i)
                if (/^[_a-zA-Z][_a-zA-Z0-9]{0,63}=/.test(lineSub)) {
                  const equalSignRelIndex = lineSub.indexOf("=")
                  if (equalSignRelIndex === -1) {
                    state = InOneChildAfterDirective
                    break
                  }
                  const keyName = lineSub.substring(0, equalSignRelIndex)
                  const charAfterEqualSign = (i + equalSignRelIndex + 1 < line.length) ? line[i + equalSignRelIndex + 1] : ''
                  if (charAfterEqualSign === '"' || charAfterEqualSign === '[' || charAfterEqualSign === '{') {
                    const cjr = consumeJson(line, i + equalSignRelIndex + 1)
                    if (cjr === undefined) {
                      state = InOneChildAfterDirective
                      break
                    } else {
                      if (!(cjr.newI === line.length || line[cjr.newI] === ' ')) {
                        state = InOneChildAfterDirective
                        break
                      } else {
                        currentChildFileInfo.metadata![keyName] = cjr.result
                        i = cjr.newI
                      }
                    }
                  } else {
                    const spaceIndex = line.indexOf(" ", i + equalSignRelIndex + 1)
                    let val
                    if (spaceIndex === -1) {
                      val = line.substring(i + equalSignRelIndex + 1)
                      i = line.length
                    } else {
                      val = line.substring(i + equalSignRelIndex + 1, spaceIndex)
                      i = spaceIndex
                    }
                    if (/^-?\d+$/.test(val)) {
                      val = Number.parseInt(val)
                    } else if (val === 'true') {
                      val = true
                    } else if (val === 'false') {
                      val = false
                    }
                    currentChildFileInfo.metadata![keyName] = val
                  }
                } else {
                  state = InOneChildAfterDirective
                  break
                }
              }
            } else {
              state = InOneChildAfterDirective
              break
            }
          }
        }

        if (state === InOneChildAfterDirective) {
          if (i < line.length) {
            currentChildFileAccumulatedSourceLines.push(line.substring(i))
          }
          state = InOneChildSource
        } else if (state === InOneChildSource) {
          if (i <= line.length) {
            currentChildFileAccumulatedSourceLines.push(line.substring(i))
          }
        }
        // l(line, state)
      }

      endOneChild()

      fileInfo._sourceWithoutMatter = parentSourceLines.join('\n') + '\n'
      fileInfo.asset_raw_bytes = Buffer.from(fileInfo._sourceWithoutMatter)
      fileInfo.asset_size = fileInfo.asset_raw_bytes.byteLength
      if (childMaxPubTime !== undefined && fileInfo.publish_time_by_metadata === undefined) {
        fileInfo.publish_time_by_metadata = childMaxPubTime
      }
      const filtered = childFileInfoList.filter(f => !f.should_discard)
      filtered.forEach(x => {
        delete x.should_discard
      })
      return filtered
    }
  })()

  const metadataFieldAliases = await (async () => {
    let aliasStr = tkCtx.e.HOARD_METADATA_FIELD_ALIAS_LIST
    if (!aliasStr) {
      aliasStr = 'ref:groupType=ref&groupPrimaryLocator;bottomPostRef:groupType=bottomPostRef&groupPrimaryLocator;bpref:groupType=bottomPostRef&groupPrimaryLocator;repo:privateOneSidedGroupType=noBorderRef&privateOneSidedGroupPrimaryLocator;thr:groupType=thread&groupPrimaryLocator;g:groupPrimaryLocator;sl:slugLocator&specifiedLocator;slug:slugLocator&specifiedLocator;tag:declaredTags;tags:declaredTags;tidbit=tidbits'
    }
    const aliasSplit = aliasStr.split(';').map(x => x.trim()).filter(x => x)
    const aliasMap: Record<string, {
      aliasName: string;
      defs: {
        targetFieldName: string;
        targetFieldFixedVal: string | undefined;
      }[];
    }> = {}
    for (const part of aliasSplit) {
      const partPart = part.split(':')
      if (partPart.length < 2) {
        continue
      }
      const aliasName = partPart[0]
      const aliasDefStr = partPart.slice(1).join(':')
      const aliasDefSplits = aliasDefStr.split('&').filter(x => x).map(x => x.split('='))
      const currAliasDefs: {
        targetFieldName: string;
        targetFieldFixedVal: string | undefined;
      }[] = []
      for (const aliasDefRaw of aliasDefSplits) {
        const targetFieldName = aliasDefRaw[0]
        let targetFieldFixedVal
        if (aliasDefRaw.length < 2) {
          targetFieldFixedVal = undefined
        } else {
          targetFieldFixedVal = aliasDefRaw.slice(1).join('=')
        }
        const def = {
          targetFieldName,
          targetFieldFixedVal
        }
        currAliasDefs.push(def)
      }
      const alias = {
        aliasName,
        defs: currAliasDefs,
      }
      aliasMap[aliasName] = alias
    }
    return aliasMap
  })()

  let watch = false
  if ((tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardOnce' && (tkEnv.PROCENV_TK_HOARD_SUB_MODE || '') !== 'hoardLocalOnlyOnce') {
    watch = true
  }

  // mddbWatchIgnoreList.map(x => convertIgnoreItemToMddbRegExp(x, liveAssetBaseSlashPath + '/')).forEach(x => {
  //   console.log(x.source)
  // })

  // have background parts
  await mddb.indexFolder({
    folderPath: liveAssetBaseSlashPath,
    ignorePatterns: mddbWatchIgnoreList.map(x => convertIgnoreItemToMddbRegExp(x, liveAssetBaseSlashPath)), 
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
        if (metadata) {
          for (const alias of Object.values(metadataFieldAliases)) {
            const aliasVal = metadata[alias.aliasName]
            if (aliasVal !== undefined) {
              for (const def of alias.defs) {
                if (def.targetFieldFixedVal === undefined) {
                  metadata[def.targetFieldName] = aliasVal
                } else {
                  metadata[def.targetFieldName] = def.targetFieldFixedVal
                }
              }
            }
          }
        }
      },
      otherHandlers: [
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        async (filePath, getSourceFunc, fileInfo) => {

        },
      ],
    },
    watch
  })
  
}

