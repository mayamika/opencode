export * as SdkInstances from "./instances"

import { Instance } from "@opencode-ai/core/instance"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Plugin } from "@opencode-ai/core/plugin"
import { PluginSupervisor } from "@opencode-ai/core/plugin/supervisor"
import type { InstancePlugins } from "@opencode-ai/core/plugin/instance"
import { Location } from "@opencode-ai/schema/location"
import type { Session } from "@opencode-ai/schema/session"
import type { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Duration, Effect, Layer, LayerMap, Option, Scope } from "effect"

export interface Configuration {
  readonly plugins: InstancePlugins.List
}

export interface Options {
  /** Select a sharing key within the Session's current Location. Must not initialize plugins. */
  readonly key: (session: Session.Info) => string
  /** Reconstruct configuration on a cache miss. Resources belong to the instance Scope. */
  readonly configure: (key: string) => Effect.Effect<Configuration, unknown, Scope.Scope>
}

export function layer(options: Options, replacements: LayerNode.Replacements) {
  return Layer.effect(
    Instance.Service,
    Effect.gen(function* () {
      const scope = yield* Effect.scope
      const key = (session: Session.Info) => ({
        key: options.key(session),
        ...LocationServiceMap.canonical(session.location),
      })
      const instances: LayerMap.LayerMap<ReturnType<typeof key>, Instance.Services> = yield* LayerMap.make(
        (input: ReturnType<typeof key>) =>
          Layer.unwrap(
            Effect.gen(function* () {
              const configuration = yield* options.configure(input.key).pipe(Effect.orDie)
              return Instance.layer(Location.Ref.make({ directory: input.directory, workspaceID: input.workspaceID }), {
                plugins: configuration.plugins,
                replacements: [
                  PluginSupervisor.node.replace(PluginSupervisor.configured({ failOnError: true })),
                  ...replacements,
                ],
              }).pipe(
                Layer.tap((context) =>
                  Effect.gen(function* () {
                    const supervisor = yield* PluginSupervisor.Service
                    const plugins = yield* Plugin.Service
                    yield* supervisor.awaitActivation
                    const failed = (yield* plugins.list()).filter(
                      (plugin) =>
                        plugin.state.status === "failed" &&
                        configuration.plugins.some((configured) => configured.id === plugin.id),
                    )
                    if (failed.length > 0)
                      yield* Effect.die(
                        new Error(`Instance plugin setup failed: ${failed.map((plugin) => plugin.id).join(", ")}`),
                      )
                  }).pipe(Effect.provide(context)),
                ),
              )
            }),
          ).pipe(
            // Eviction can close the lookup's scope; do not make that fiber wait on itself.
            Layer.tapCause(() =>
              instances.invalidate(input).pipe(Effect.forkIn(scope, { startImmediately: true }), Effect.asVoid),
            ),
          ),
        { idleTimeToLive: Duration.infinity },
      )
      return Instance.Service.of({
        provide: (session) => Effect.provide(instances.get(key(session))),
        provideIfLoaded: (session) => (effect) =>
          Effect.scopedWith((scope) =>
            Effect.gen(function* () {
              const context = yield* instances.contextEffectOption(key(session)).pipe(Scope.provide(scope))
              if (Option.isNone(context)) return Option.none()
              return Option.some(yield* effect.pipe(Effect.provide(context.value)))
            }),
          ),
      })
    }),
  )
}
