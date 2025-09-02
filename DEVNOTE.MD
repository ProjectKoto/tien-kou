# Asset

categorized in the following dimensions

- liveLight/liveHeavy/static
- open(open) / guarded(content) / backstage(template, config etc)
- dedicated/generic

liveLight open dedicated: open markdown that may be stored in sqlite, everyone can view its source
liveLight open generic (same effect as liveLight guarded generic; only difference is their location): open small image that may be stored in sqlite, everyone can view in browser by url
liveLight guarded dedicated: markdown that may be stored in sqlite, but source unavailable for end user
liveLight guarded generic (same effect as liveLight open generic; only difference is their location): open small image that may be stored in sqlite, everyone can view in browser by url
liveLight backstage dedicated: template/config file that may be stored in sqlite, parsable, only for server usage
liveLight backstage generic: other resource file (like image/analytical dataset?) that may be stored in sqlite, only for server usage (not yet come up with a real-life usage)

liveHeavy open dedicated: not exists
liveHeavy open generic (same effect as liveHeavy guarded generic; only difference is their location): open big image that can't be stored in sqlite, but dynamically added/updated, so needed to be upload to github etc realtime. everyone can view in browser by url
liveHeavy guarded dedicated: not exists
liveHeavy guarded generic (same effect as liveHeavy open generic; only difference is their location): open big image that can't be stored in sqlite, but dynamically added/updated, so needed to be upload to github etc realtime. everyone can view in browser by url
liveHeavy backstage dedicated: not exists
liveHeavy backstage generic: other big resource file (like big analytical dataset?) that can't be stored in sqlite, but dynamically added/updated, so needed to be upload to github etc realtime. only for server usage (not yet come up with a real-life usage)

static open dedicated: not exists
static open generic: site static image file, stored in local file system/cloudflare worker assets, directly serve to user browser
static guarded dedicated: not exists
static guarded generic: not exists
static backstage dedicated: not exists
static backstage generic: site static other resource file, stored in local file system/cloudflare worker assets, not directly served to user browser

x.isDedicated = funcTellIfIsDedicated(x.virtPath)
isDedicated -> isLiveLight
!isLiveLight = (isStatic || isLiveHeavy) -> !isDedicated = isGeneric
isStatic -> !isGuarded
isGuarded -> !isStatic
isLocatable = isGuarded || isOpen
isGenericLocatable = isLocatable && isGeneric
                 = (isGuarded && isGeneric) || isOpen
isLocatableContent = isLiveLight && isLocatable && isDedicated
                   = (isGuarded || isOpen) && isDedicated


# TODO
template -> backstage?


node_modules/telegram/client/telegramBaseClient.js

node_modules/@libsql/isomorphic-ws/node.mjs
