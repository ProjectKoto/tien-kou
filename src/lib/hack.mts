import { Env, ErrorHandler, Hono, Schema } from "hono"

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export type HonoWithErrorHandler<E extends Env = Env, S extends Schema = {}, BasePath extends string = "/"> = Omit<Hono<E, S, BasePath>, "errroHandler"> & {
  errorHandler: ErrorHandler<E>
}
