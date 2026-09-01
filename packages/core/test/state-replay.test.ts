import { describe, expect } from "bun:test"
import { State } from "@opencode-ai/core/state"
import { Effect, Layer } from "effect"
import { FastCheck } from "effect/testing"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)

type Operation = { multiply: number; add: number }
type Value = { value: number; order: number[] }

const operation = FastCheck.record({
  multiply: FastCheck.constantFrom(-3, -2, 2, 3),
  add: FastCheck.integer({ min: -9, max: 9 }),
})
const source = FastCheck.integer({ min: -100, max: 100 })
const target = FastCheck.integer({ min: 0, max: 1 })
const command = FastCheck.oneof(
  {
    weight: 4,
    arbitrary: FastCheck.record({ type: FastCheck.constant("append"), target, callback: FastCheck.nat(5) }),
  },
  { weight: 3, arbitrary: FastCheck.record({ type: FastCheck.constant("read"), target }) },
  {
    weight: 2,
    arbitrary: FastCheck.record({ type: FastCheck.constant("dispose"), target, registration: FastCheck.nat(100) }),
  },
  { weight: 2, arbitrary: FastCheck.record({ type: FastCheck.constant("reload"), target, source }) },
)

// Affine transforms do not generally commute; the modulus keeps long traces exact.
function apply(value: number, operation: Operation) {
  return (value * operation.multiply + operation.add) % 10_007
}

// Replay a reported counterexample with STATE_REPLAY_SEED, STATE_REPLAY_PATH,
// and --test-name-pattern. Raise STATE_REPLAY_RUNS for a larger corpus.
const seeds = process.env.STATE_REPLAY_SEED ? [Number(process.env.STATE_REPLAY_SEED)] : [0x5eed, 0xc0ffee, 0xdecaf]

describe("State replay properties", () => {
  seeds.forEach((seed) => {
    const parameters = {
      seed,
      numRuns: Number(process.env.STATE_REPLAY_RUNS ?? 200),
      path: process.env.STATE_REPLAY_PATH,
    }

    it.effect(
      `matches a full fold across reads, registrations, removals and batched reloads (seed ${seed})`,
      () =>
        Effect.sync(() => {
          FastCheck.assert(
            FastCheck.property(
              FastCheck.tuple(source, source),
              FastCheck.array(operation, { minLength: 1, maxLength: 6 }),
              FastCheck.array(FastCheck.array(command, { maxLength: 48 }), { minLength: 1, maxLength: 8 }),
              (initial, operations, batches) =>
                Effect.gen(function* () {
                  const sources = [...initial]
                  const notifications = [0, 0]
                  let calls = 0
                  const states = sources.map((_, index) =>
                    State.create({
                      initial: (): Value => ({ value: sources[index], order: [] }),
                      draft: (draft) => draft,
                      notify: () => Effect.sync(() => void notifications[index]++),
                    }),
                  )
                  const callbacks = operations.map((operation, index) => (draft: Value) => {
                    calls++
                    draft.value = apply(draft.value, operation)
                    draft.order.push(index)
                  })
                  const registrations: { handle: State.Registration; callback: number; active: boolean }[][] = [[], []]
                  const expected = (index: number): Value => {
                    const order = registrations[index].filter((entry) => entry.active).map((entry) => entry.callback)
                    return {
                      value: order.reduce((value, callback) => apply(value, operations[callback]), sources[index]),
                      order,
                    }
                  }

                  yield* Effect.forEach(
                    batches,
                    (commands) =>
                      Effect.gen(function* () {
                        const before = [...notifications]
                        const dirty = new Set<number>()
                        yield* State.batch(
                          Effect.gen(function* () {
                            yield* Effect.forEach(
                              commands,
                              (command) =>
                                Effect.gen(function* () {
                                  const state = states[command.target]
                                  switch (command.type) {
                                    case "append": {
                                      const callback = command.callback % callbacks.length
                                      const handle = yield* state.transform(callbacks[callback])
                                      registrations[command.target].push({ handle, callback, active: true })
                                      dirty.add(command.target)
                                      return
                                    }
                                    case "read":
                                      expect(state.get()).toEqual(expected(command.target))
                                      return
                                    case "dispose": {
                                      const entries = registrations[command.target]
                                      if (!entries.length) return
                                      const entry = entries[command.registration % entries.length]
                                      yield* entry.handle.dispose
                                      if (!entry.active) return
                                      entry.active = false
                                      dirty.add(command.target)
                                      return
                                    }
                                    case "reload":
                                      sources[command.target] = command.source
                                      yield* state.reload()
                                      dirty.add(command.target)
                                  }
                                }),
                              { discard: true },
                            )
                            expect(notifications).toEqual(before)
                          }),
                        )
                        expect(notifications).toEqual(before.map((count, index) => count + Number(dirty.has(index))))
                        const flushed = calls
                        states.forEach((state, index) => expect(state.get()).toEqual(expected(index)))
                        expect(calls).toBe(flushed)
                      }),
                    { discard: true },
                  )
                }).pipe(Effect.scoped, Effect.runSync),
            ),
            parameters,
          )
        }),
      60_000,
    )

    it.effect(
      `applies each appended suffix once and replays survivors only after invalidation (seed ${seed})`,
      () =>
        Effect.sync(() => {
          FastCheck.assert(
            FastCheck.property(
              FastCheck.array(
                FastCheck.record({
                  append: FastCheck.array(operation, { minLength: 1, maxLength: 8 }),
                  reads: FastCheck.integer({ min: 1, max: 5 }),
                }),
                { minLength: 1, maxLength: 8 },
              ),
              FastCheck.nat(100),
              (chunks, removal) =>
                Effect.gen(function* () {
                  let calls = 0
                  const state = State.create({
                    initial: () => ({ value: 1 }),
                    draft: (draft) => draft,
                  })
                  const registrations: State.Registration[] = []
                  yield* State.batch(
                    Effect.gen(function* () {
                      yield* Effect.forEach(
                        chunks,
                        (chunk) =>
                          Effect.gen(function* () {
                            const before = calls
                            const added = yield* Effect.forEach(chunk.append, (operation) =>
                              state.transform((draft) => {
                                calls++
                                draft.value = apply(draft.value, operation)
                              }),
                            )
                            registrations.push(...added)
                            expect(calls).toBe(before)
                            Array.from({ length: chunk.reads }).forEach(() => {
                              state.get()
                              expect(calls).toBe(registrations.length)
                            })
                          }),
                        { discard: true },
                      )

                      const removed = registrations[removal % registrations.length]
                      yield* removed.dispose
                      const before = calls
                      state.get()
                      expect(calls).toBe(before + registrations.length - 1)

                      const replayed = calls
                      yield* removed.dispose
                      state.get()
                      expect(calls).toBe(replayed)

                      yield* state.reload()
                      yield* state.reload()
                      expect(calls).toBe(replayed)
                    }),
                  )
                  const total = registrations.length + 2 * (registrations.length - 1)
                  expect(calls).toBe(total)
                  state.get()
                  expect(calls).toBe(total)
                }).pipe(Effect.scoped, Effect.runSync),
            ),
            parameters,
          )
        }),
      60_000,
    )

    it.effect(
      `keeps captured source changes out of an applied prefix until reload (seed ${seed})`,
      () =>
        Effect.sync(() => {
          FastCheck.assert(
            FastCheck.property(source, operation, operation, source, (initial, prefix, suffix, change) =>
              Effect.gen(function* () {
                let offset = prefix.add
                let calls = 0
                const state = State.create({
                  initial: () => ({ value: initial }),
                  draft: (draft) => draft,
                })
                yield* state.transform((draft) => {
                  calls++
                  draft.value = apply(draft.value, { multiply: prefix.multiply, add: offset })
                })
                const cached = apply(initial, prefix)
                expect(state.get().value).toBe(cached)

                offset += change || 1
                expect(state.get().value).toBe(cached)
                expect(calls).toBe(1)
                yield* state.transform((draft) => {
                  draft.value = apply(draft.value, suffix)
                })
                expect(state.get().value).toBe(apply(cached, suffix))
                expect(calls).toBe(1)

                yield* state.reload()
                expect(calls).toBe(1)
                expect(state.get().value).toBe(
                  [{ multiply: prefix.multiply, add: offset }, suffix].reduce(apply, initial),
                )
                expect(calls).toBe(2)
              }).pipe(State.batch, Effect.scoped, Effect.runSync),
            ),
            parameters,
          )
        }),
      60_000,
    )

    it.effect(
      `retries a throwing callback from a fresh full fold instead of a partial draft (seed ${seed})`,
      () =>
        Effect.sync(() => {
          FastCheck.assert(
            FastCheck.property(
              source,
              FastCheck.array(operation, { maxLength: 12 }),
              operation,
              FastCheck.array(operation, { maxLength: 12 }),
              FastCheck.boolean(),
              (initial, prefix, failing, suffix, invalidate) =>
                Effect.gen(function* () {
                  const operations = [...prefix, failing, ...suffix]
                  const calls = operations.map(() => 0)
                  let fresh = 0
                  let fail = true
                  const state = State.create({
                    initial: (): Value => {
                      fresh++
                      return { value: initial, order: [] }
                    },
                    draft: (draft) => draft,
                  })
                  yield* Effect.forEach(
                    prefix,
                    (operation, index) =>
                      state.transform((draft) => {
                        calls[index]++
                        draft.value = apply(draft.value, operation)
                        draft.order.push(index)
                      }),
                    { discard: true },
                  )
                  expect(state.get()).toEqual({ value: prefix.reduce(apply, initial), order: prefix.map((_, i) => i) })
                  yield* Effect.forEach(
                    [failing, ...suffix],
                    (operation, index) =>
                      state.transform((draft) => {
                        calls[prefix.length + index]++
                        draft.value = apply(draft.value, operation)
                        draft.order.push(prefix.length + index)
                        if (index === 0 && fail) {
                          fail = false
                          throw new Error("transient transform failure")
                        }
                      }),
                    { discard: true },
                  )
                  if (invalidate) yield* state.reload()
                  expect(() => state.get()).toThrow("transient transform failure")
                  const before = [...calls]
                  const failed = fresh
                  expect(state.get()).toEqual({
                    value: operations.reduce(apply, initial),
                    order: operations.map((_, index) => index),
                  })
                  expect(fresh).toBe(failed + 1)
                  expect(calls).toEqual(before.map((count) => count + 1))
                  state.get()
                  expect(calls).toEqual(before.map((count) => count + 1))
                }).pipe(State.batch, Effect.scoped, Effect.runSync),
            ),
            parameters,
          )
        }),
      60_000,
    )
  })
})
