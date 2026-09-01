export * as State from "./state.js"

import { Clock, Context, Deferred, Effect, Exit, Scope } from "effect"

/**
 * A synchronous, replayable edit to the current domain state.
 *
 * Domain drafts expose readable and writable state while preserving concise
 * plugin/config code. Transforms synchronously rebuild derived state.
 */
type TransformCallback<DraftApi> = (draft: DraftApi) => void
export type MakeDraft<State, DraftApi> = (state: State) => DraftApi

export interface Registration {
  readonly dispose: Effect.Effect<void>
}

/**
 * Registers a scoped transform. Reads apply pending transforms in order.
 * Closing the owning Scope removes the transform and invalidates accumulated state.
 */
export type Transform<DraftApi> = (
  transform: TransformCallback<DraftApi>,
) => Effect.Effect<Registration, never, Scope.Scope>

/** Invalidates accumulated state after captured inputs change and coalesces notifications. */
export type Reload = () => Effect.Effect<void>

export interface Transformable<DraftApi> {
  readonly transform: Transform<DraftApi>
  readonly reload: Reload
}

type Batch = {
  active: boolean
  readonly flush: boolean
  readonly notifications: Set<Reload>
}

const CurrentBatch = Context.Reference<Batch | undefined>("@opencode/State/CurrentBatch", {
  defaultValue: () => undefined,
})
const reloadDebounce = 500

/** Batches notifications, not read visibility or rollback. flush: false is terminal teardown. */
export function batch<A, E, R>(effect: Effect.Effect<A, E, R>, options: { readonly flush?: boolean } = {}) {
  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const current = yield* CurrentBatch
      if (current?.active && options.flush !== false) return yield* restore(effect)
      const batch: Batch = { active: true, flush: options.flush !== false, notifications: new Set() }
      const exit = yield* restore(effect.pipe(Effect.provideService(CurrentBatch, batch))).pipe(Effect.exit)
      batch.active = false
      const notifications = batch.flush
        ? yield* Effect.forEach(batch.notifications, (notify) => restore(notify()).pipe(Effect.exit))
        : []
      // Aggregate ordinary failures across domains, while allowing cancellation to stop observer work.
      yield* Exit.asVoidAll([exit, ...notifications])
      return yield* exit
    }),
  )
}

export const inherit = Effect.fnUntraced(function* () {
  const batch = yield* CurrentBatch
  return <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provideService(effect, CurrentBatch, batch)
})

export interface Options<State, DraftApi> {
  readonly name?: string
  /** Creates the base value initially and after removal, reload, or failed replay. */
  readonly initial: () => State
  /** Wraps mutable state in a domain-specific draft API. */
  readonly draft: MakeDraft<State, DraftApi>
  /**
   * Observes current state outside the read path. Batched changes notify at
   * batch completion; reloads debounce notifications. Resource reconciliation
   * owns its execution scope and coordination.
   */
  readonly notify?: () => Effect.Effect<void>
}

export interface Interface<State, DraftApi> extends Transformable<DraftApi> {
  /** Applies pending edits synchronously. The returned value is a live view, not a retained snapshot. */
  readonly get: () => State
}

export function create<State, DraftApi>(options: Options<State, DraftApi>): Interface<State, DraftApi> {
  let state = options.initial()
  let transforms: { run: TransformCallback<DraftApi> }[] = []
  let prefix: { draft: DraftApi; applied: number } | undefined = { draft: options.draft(state), applied: 0 }
  let requestedAt = 0
  let closed = false
  let pending: Deferred.Deferred<void> | undefined

  const get = () => {
    if (closed || prefix?.applied === transforms.length) return state
    const cached = prefix
    // A callback can throw after mutating the accumulator. Retry from a fresh base, never that partial prefix.
    prefix = undefined
    const next = cached ? state : options.initial()
    const draft = cached ? cached.draft : options.draft(next)
    transforms.slice(cached?.applied ?? 0).forEach((transform) => transform.run(draft))
    state = next
    prefix = { draft, applied: transforms.length }
    return state
  }

  const notify = Effect.fn("State.notify")(function* () {
    if (closed) return
    get()
    if (options.notify) yield* options.notify()
  })

  const publish = Effect.fnUntraced(function* (done: Deferred.Deferred<void>): Effect.fn.Return<void> {
    const clock = yield* Clock.Clock
    const remaining = requestedAt + reloadDebounce - clock.currentTimeMillisUnsafe()
    if (remaining > 0) yield* Effect.sleep(remaining)
    if (clock.currentTimeMillisUnsafe() < requestedAt + reloadDebounce) return yield* publish(done)

    // Observers can request and await another reload without joining their own notification.
    pending = undefined
    return yield* notify().pipe(Deferred.into(done), Effect.asVoid)
  })

  const changed = (debounce: boolean) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (closed) return
        if (debounce) prefix = undefined
        const batch = yield* CurrentBatch
        if (batch?.active) {
          if (!batch.flush) {
            closed = true
            return
          }
          batch.notifications.add(notify)
          return
        }
        if (!debounce) {
          yield* restore(notify())
          return
        }

        const clock = yield* Clock.Clock
        requestedAt = clock.currentTimeMillisUnsafe()
        const done = pending ?? Deferred.makeUnsafe<void>()
        if (!pending) {
          pending = done
          yield* publish(done).pipe(Effect.forkDetach)
        }
        yield* restore(Deferred.await(done))
      }),
    )

  return {
    get,
    transform: Effect.fn("State.transform")(function* (update) {
      yield* Effect.annotateCurrentSpan("state", options.name ?? "anonymous")
      const scope = yield* Scope.Scope
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const transform = { run: update }
          const dispose = Effect.uninterruptible(
            Effect.suspend(() => {
              if (!transforms.includes(transform)) return Effect.void
              transforms = transforms.filter((item) => item !== transform)
              prefix = undefined
              return changed(false)
            }),
          )
          transforms.push(transform)
          yield* Scope.addFinalizer(scope, dispose)
          yield* changed(false)
          return { dispose }
        }),
      )
    }),
    reload: () => changed(true),
  }
}
