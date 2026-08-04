import * as turso from "@libsql/client"
import { HC, KD, SqlDbHandler } from './serveDef.mts'
import { cutPrefix, SqlArgValue } from '../lib/common.mts'

export const TursoSqlDbHandler = HC<SqlDbHandler>()(async ({ TkFirstCtxProvideHandler }: KD<"TkFirstCtxProvideHandler">): Promise<SqlDbHandler> => {

  let tursoc: turso.Client | undefined = undefined

  let tursocReadyResolver: ((value: turso.Client) => void)
  const tursocReadyPromise = new Promise<turso.Client>(r => { tursocReadyResolver = r })

  TkFirstCtxProvideHandler.listenOnFirstCtxForInit(async ctx0 => {
    tursoc = turso.createClient({
      url: ctx0.e.TURSO_DATABASE_URL!,
      authToken: cutPrefix(ctx0.e.TURSO_AUTH_TOKEN!, 'basic:')[0],
      tls: !((ctx0.e.TURSO_DISABLE_TLS ?? '') === '1' || (ctx0.e.TURSO_DISABLE_TLS ?? '') === 'true'),
    })
    if (tursocReadyResolver) {
      tursocReadyResolver(tursoc)
    }
  })

  return {
    sql: async ({ sql, args }: { sql: string, args: SqlArgValue[] }) => {
      return (await (await tursocReadyPromise).execute({
        sql,
        args,
      })).rows
    },
  }
})

