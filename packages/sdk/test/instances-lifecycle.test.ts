import { expect } from "bun:test"
import { Instance } from "@opencode-ai/core/instance/service"
import { Session } from "@opencode-ai/core/session"
import { Location } from "@opencode-ai/schema/location"
import { AbsolutePath } from "@opencode-ai/schema/schema"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { testEffect } from "../../core/test/lib/effect"
import { EmbeddedHost } from "../src/internal/host"

const it = testEffect(Layer.empty)

it.live("a cancelled borrower cannot strand a later failed instance construction", () =>
  Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    const started = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>()
    const release = yield* Deferred.make<void>()
    const acquired: number[] = []
    const released: number[] = []
    const host = yield* Effect.acquireRelease(
      EmbeddedHost.create({
        config: { directory: directory.path, project: false, content: "{}" },
        models: { fetch: false },
        fs: { filewatcher: false },
        instances: {
          key: () => "shared",
          configure: () =>
            Effect.gen(function* () {
              const attempt = acquired.length + 1
              yield* Effect.acquireRelease(
                Effect.sync(() => acquired.push(attempt)),
                () => Effect.sync(() => released.push(attempt)),
              )
              if (attempt !== 1) return { plugins: [] }
              yield* Effect.withFiber((fiber) => Deferred.succeed(started, fiber))
              yield* Deferred.await(release)
              return yield* Effect.fail(new Error("Configuration unavailable"))
            }),
        },
      }),
      (host) => Effect.promise(host.close),
    )
    const services = yield* host.runtime.contextEffect
    const instances = Context.get(services, Instance.Service)
    const sessions = Context.get(services, Session.Service)
    const session = yield* sessions.create({
      location: Location.Ref.make({ directory: AbsolutePath.make(directory.path) }),
    })
    expect(yield* Effect.void.pipe(instances.provideIfLoaded(session))).toEqual(Option.none())
    expect(acquired).toEqual([])

    const borrower = yield* Effect.void.pipe(instances.provide(session), Effect.forkScoped)
    const lookup = yield* Deferred.await(started)
    yield* Fiber.interrupt(borrower)
    expect(released).toEqual([])
    yield* Deferred.succeed(release, undefined)
    expect(Exit.isFailure(yield* Fiber.await(lookup).pipe(Effect.timeout("1 second")))).toBe(true)
    expect(released).toEqual([1])

    yield* Effect.void.pipe(instances.provide(session))
    expect(acquired).toEqual([1, 2])
    expect(released).toEqual([1])
    const callerScope = yield* Effect.scope
    expect(Option.getOrThrow(yield* Effect.scope.pipe(instances.provideIfLoaded(session)))).toBe(callerScope)
    expect(yield* Effect.void.pipe(instances.provideIfLoaded(session))).toEqual(Option.some(undefined))
    const error = new Error("Operation failed")
    expect(yield* Effect.fail(error).pipe(instances.provideIfLoaded(session), Effect.flip)).toBe(error)

    yield* Effect.promise(host.close)
    expect(released).toEqual([1, 2])
    expect(yield* Effect.void.pipe(instances.provideIfLoaded(session))).toEqual(Option.none())
  }),
)
