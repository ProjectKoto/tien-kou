import { Env, ErrorHandler, Hono, Schema } from "hono"

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export type HonoWithErrorHandler<E extends Env = Env, S extends Schema = {}, BasePath extends string = "/"> = Hono<E, S, BasePath> & {
  errorHandler: ErrorHandler<E>
}
