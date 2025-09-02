# How to build

- git clone with submodules.
- cd into each thirdparty/ directories, build them into dist files.
  - for telegram: run `npm install`, then `node publish_npm.js`. (the `package.json` should remain unchanged except whitelines if everythng succeeds; else please revert it and gramjs/Version.ts) The all files suitable for npm/javascript import will entirely reside in `dist` directory, which is indicated in root package.json `file:` version string.
  - for libsql_isomorphic-ts/isomorphic-fetch: do nothing
  - for markdowndb: run `npm install`, then `npm run build`, then, done. (its package.json specifies `files`, which excludes everything than dist, package.json, LICENSE etc; `main` which points to the correct entrypoint.)
  - for cloudflare_workers-sdk/packages/miniflare (the fucking shit): `npx -g pnpm install` (will download and install playwright [with browser], but it recognizes proxy if set, so wait and should be ok) then `npm run build` should succeed. Then `npx -g pnpm pack`, `tar zxvf miniflare-xxx.tgz`. Final package is in `package` directory, which is correctly referred by tien-kou package.json.
- cd into project root, delete (necessary?) `package-lock.json node_modules/package.json`, run `npm run upgradeThirdpartyForkDep`. (the `--install-links=true` in `.npmrc` is to workaround this bug: https://github.com/npm/cli/issues/6405 ; yes, `--install-links` DOESN'T create links; instead directories are created ) (Otherwise, specify file:tgz in package.json is another workaround.) (the thirdparty packages are always recommended to referred explicitly to make things refresh). 
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

