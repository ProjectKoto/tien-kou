
import nanoSpawn, { SubprocessError } from 'nano-spawn'
import { ensurePathDirExists, nodeResolvePath } from './nodeCommon.mts'
import path from 'node:path'
import os from 'node:os'
import fs from 'node:fs'
import { le } from './common.mts'

export const buildGitProcessRunner = (commonLogPrefix: string, fixedGitArgs: string[]) => async (args: string[], logMark: string, timeoutMs: number, throwIfNonZero: boolean): Promise<{
  exitCode: number,
  stdout: string,
  stderr: string,
}> => {
  let gitProcExitCode: number | undefined = 0
  let gitProcStdout
  let gitProcStderr
  const stdoutMark = `${commonLogPrefix}: ${logMark} stdout:`
  const stderrMark = `${commonLogPrefix}: ${logMark} stderr:`
  const exitCodeMark = `${commonLogPrefix}: ${logMark} exit:`
  try {
    const gitProc = nanoSpawn('git', [...fixedGitArgs, ...args], {
      timeout: timeoutMs,
      env: {
        LC_ALL: 'en_US.UTF-8'
      }
    })
    ;(async () => {
      for await (const outLine of gitProc.stdout) {
        console.log(stdoutMark, outLine)
      }
    })().catch(() => {})
    ;(async () => {
      for await (const outLine of gitProc.stderr) {
        console.log(stderrMark, outLine)
      }
    })().catch(() => {})
    const gitProcResult = await gitProc
    gitProcStdout = gitProcResult.stdout
    gitProcStderr = gitProcResult.stderr
    return {
      exitCode: 0,
      stdout: gitProcStdout,
      stderr: gitProcStderr,
    }
  } catch (e: unknown) {
    if (e instanceof SubprocessError) {
      gitProcExitCode = e.exitCode
      gitProcStdout = e.stdout
      gitProcStderr = e.stderr
      if (throwIfNonZero) {
        throw e
      }
      return {
        exitCode: gitProcExitCode === undefined ? 256 : gitProcExitCode,
        stdout: gitProcStdout,
        stderr: gitProcStderr,
      }
    } else {
      throw e
    }
  } finally {
    console.log(exitCodeMark, gitProcExitCode)
  }
}

export const gitSyncScriptPath =  nodeResolvePath('./thirdparty/git-sync/git-sync').split(path.sep).join('/')

// assume base path: HOARD_MULTI_TARGET_SYNC_BASE_DIR/
const gitStaticGenIgnoreList = [
  '**/*DS_Store*',
  '**/*DS_Store*/**',
  '**/.sync-conflict-*',
  '**/.syncthing.*.tmp',
  '**/.syncthing.tmp',
  '**/Unpublished/**',
  '**/unscannedAsset/**',
]

export const gitSyncStaticGen = async ({
  gitLocalStaticGenBareRepoPath,
  staticGenBaseDir,
  gitRemote,
}: {
  gitLocalStaticGenBareRepoPath: string | undefined,
  staticGenBaseDir: string,
  gitRemote: string | undefined,
}) => {
  const gitIgnoreFileTmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'tk-git-ignore-'))
  
  try {
    const gitIgnoreFilePath = path.join(gitIgnoreFileTmpDir, 'tkGitIgnore').split(path.sep).join('/')
    await fs.promises.writeFile(gitIgnoreFilePath, gitStaticGenIgnoreList.join('\n'), 'utf-8')

    try {
      const fixedGitArgs = ['--git-dir=' + gitLocalStaticGenBareRepoPath, '--work-tree=' + staticGenBaseDir, '-c', 'alias.tk-sync=!bash ' + gitSyncScriptPath, '-c', 'branch.tk_static_gen.remote=origin', '-c', 'user.email=automation@localhost', '-c', 'user.name=automation', '-c', 'branch.tk_static_gen.syncNewFiles=true', '-c', 'branch.tk_static_gen.sync=true', '-c', 'core.excludesFile=' + gitIgnoreFilePath, '-c', 'branch.tk_static_gen.tkSyncRebaseMergeStrategyOption=theirs']

      const runGitProcess = buildGitProcessRunner('git-sync-staticg', fixedGitArgs)

      if (gitLocalStaticGenBareRepoPath && gitRemote) {
        await ensurePathDirExists(gitLocalStaticGenBareRepoPath)
        await buildGitProcessRunner('git-sync-staticg', [])(['init', '--bare', gitLocalStaticGenBareRepoPath], 'init-bare', 4000, false)
        await runGitProcess(['remote', 'add', 'origin', gitRemote], 'add-remote', 4000, false)
        const ensureBranch1Result = await runGitProcess(['branch', '--force', 'tk_static_gen', 'HEAD'], 'ensure-branch1', 20000, false)
        if (ensureBranch1Result.exitCode !== 0 && ensureBranch1Result.stderr.indexOf('not a valid object name') < 0) {
          await runGitProcess(['checkout', '-b', 'tk_static_gen'], 'ensure-branch2', 4000, false)
        }
        await runGitProcess(['reset', 'tk_static_gen'], 'ensure-branch3', 20000, false)
        await runGitProcess(['rm', '--cached', '-r', '.'], 'rm-cached', 20000, false)
        await runGitProcess(['tk-sync'], 'tk-sync', 20000, true)
      }
    } catch (e) {
      le('gitSyncStaticGen err', e)
    }
  } finally {
    await fs.promises.rm(gitIgnoreFileTmpDir, {recursive: true, force: true})
  }
}
