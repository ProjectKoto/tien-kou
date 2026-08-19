
2026-08: With the stance of a subtle resistance, the code of this engine, including all custom modifications of thirdparty dependencies, WILL NEVER rely on perceivable Large Language Models' (LLM) help in the future.

<span lang="zh-Hant-CN">2026-08: 作爲一個微小的反抗立場，本引擎的所有程式碼，包括所有對於第三方依賴項的自訂修改，在將來将不依賴於任何可感知的大語言模型（LLM）之協助。</span>

Declaration of past LLM usage:

- 2025 Asked DeepSeek V3 Web Chat (officially hosted) about JavaScript basic syntax, TypeScript type gymnastics, some simple generic impl of TypeScript methods, and some naming advice of some methods. That helped in some trivial and tedious work in the initial framework of tien-kou.
- 2025 Asked DeepSeek V3 Web Chat (officially hosted) about ETag-based caching logic. This helped building the ETag caching mechanism of Cloudflare Workers deployment.
- 2025 DeepSeek V3 Web Chat (officially hosted) was heavily used in writing senderApi, and the Style Customizer of tien-kou site instance. They both fall outside of the range of tien-kou engine.
- 2025 Asked DeepSeek V3 Web Chat (officially hosted) about a generic Telegram API question. That helped building the instance site, which falls outside of the range of tien-kou engine.
- 2025 Asked DeepSeek V3 Web Chat (officially hosted) about esbuild script writing. No tien-kou specific code/description fed into it; the question is a generic one. The response is heavily reviewed, inspected, tailored modified to fit the need of the project.
- 2025 Asked DeepSeek V3 Web Chat (officially hosted) about how to do Cloudflare KV cache manipulation in a llibrary callback without access to Cloudflare Workers ctx/env. No tien-kou specific code/description fed into it; the question is a generic one and the answer is more of a trivial solution, not rather helpful addressing the problem and is not used in actual engine code implementation.

# How to build

- git clone with submodules. / git pull all submodules. (shorthand: `npm run pull`)
- cd into each thirdparty/ directories, build them into dist files.
  - shorthand: `npm run b3`
  - for teleproto: run `npm install`, then `npm run buildForTk`. The all files suitable for npm/javascript import will entirely reside in `dist` directory, which is indicated in root package.json `file:` version string.
  - for libsql_isomorphic-ts/isomorphic-fetch: do nothing
  - for rclone.js: do nothing
  - for markdowndb: run `npm install`, then `npm run build`, then, done. (its package.json specifies `files`, which excludes everything than dist, package.json, LICENSE etc; `main` which points to the correct entrypoint.)
  - for cloudflare_workers-sdk/packages/miniflare (the fucking shit): first cd into `cloudflare_workers-sdk`, run `npx pnpm install` (will download and install playwright [with browser], but it recognizes proxy if set, so wait and should be ok); then `npx pnpm run build --filter miniflare` should succeed (will likely run into errors/frustration related to the fucking sucking npx/npm/pnpm/dlx, good luck). Then `npx pnpm --dir ./packages/miniflare pack`, go into `packages/miniflare` directory, do `tar zxvf miniflare-xxx.tgz`. Final package is in `packages/miniflare/package` directory, which is correctly referred by tien-kou package.json.
- cd into project root, delete (necessary?) `package-lock.json node_modules/package.json`, run `npm run u3` or `npm run upgradeBuiltThirdpartyForkDep`. (the `--install-links=true` in `.npmrc` is to workaround this bug: https://github.com/npm/cli/issues/6405 ; yes, `--install-links` DOESN'T create links; instead directories are created ) (Otherwise, specify file:tgz in package.json is another workaround.) (the thirdparty packages are always recommended to referred explicitly to make things refresh). 
- run the npm script you want.

# About why different esbuild config using different format (CJS/ESM)

https://dev.to/marcogrcr/nodejs-and-esbuild-beware-of-mixing-cjs-and-esm-493n

> # TL;DR
> When using esbuild to bundle code with --platform=node that depends on npm packages with a mixture of cjs and esm entry points, use the following rule of thumb:
> 
> 
> **When using --bundle, set --format to cjs**. This will work in all cases except for esm modules with top-level await.
> --format=esm can be used but requires a polyfill such as this one.
> **When using --packages=external, set --format to esm**.
> If you're wondering about the difference between cjs and esm, take a look at Node.js: A brief history of cjs, bundlers, and esm.

# Cloudflare Workers monitoring request CPU time

```

npx wrangler tail --format json | grep -vE '^Proxy' --line-buffered | jq -r '("   " + (.cpuTime | tostring) + "    " + (.event.response.status | tostring) + "     " + .event.request.url)'

npx wrangler tail --format json | grep --line-buffered -E '_parse|cpuTime|"url"' -C1

```
