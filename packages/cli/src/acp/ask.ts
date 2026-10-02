import { Cause, Effect } from "effect"

export function reply<A, F, E, R, E2, R2>(input: {
  readonly ask: Effect.Effect<A, E, R>
  readonly cancelled: Effect.Effect<void>
  readonly settled: Effect.Effect<void>
  readonly fallback: F
  readonly failure: string
  readonly respond: (outcome: A | F | "settled") => Effect.Effect<void, E2, R2>
}) {
  return Effect.uninterruptibleMask((restore) =>
    // The race starts racers in order and stops once one is done, so an earlier cancel never starts the ask.
    restore(
      input.cancelled.pipe(
        Effect.as(input.fallback),
        Effect.raceFirst(input.settled.pipe(Effect.as("settled" as const))),
        Effect.raceFirst(input.ask),
      ),
    ).pipe(
      Effect.tapCauseIf(Cause.hasDies, (cause) => Effect.logWarning(input.failure, cause)),
      Effect.catchCause(() => Effect.succeed(input.fallback)),
      Effect.flatMap(input.respond),
    ),
  )
}

export * as ACPAsk from "./ask"
