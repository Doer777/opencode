import type { CancelNotification, PromptRequest, PromptResponse, RequestError } from "@agentclientprotocol/sdk"
import type { OpenCodeClient, OpenCodeEvent } from "@opencode/client/effect"
import { TokenUsage } from "@opencode/schema/token-usage"
import {
  Cause,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Option,
  Queue,
  Ref,
  Scope,
  Stream,
} from "effect"
import type { Capabilities } from "./capabilities"
import type { ACPCatalog } from "./catalog"
import { ACPChild } from "./child"
import { ACPClient } from "./client"
import { currentModel } from "./config-option"
import type { ACPConnection } from "./connection"
import { ACPElicitation } from "./elicitation"
import { ACPError } from "./error"
import { ACPPermission } from "./permission"
import { ACPPrompt } from "./prompt"
import type { ACPSessions, Attached } from "./sessions"
import { ACPTranslate } from "./translate"

export interface Interface {
  readonly prompt: (input: PromptRequest, signal: AbortSignal) => Effect.Effect<PromptResponse, ACPError.Failure>
  readonly cancel: (input: CancelNotification) => Effect.Effect<void>
  /** Unlike `cancel`, interrupts an idle session too, since server work can outlive its turn. */
  readonly close: (sessionID: string) => Effect.Effect<void, ACPError.Error | RequestError>
}

/** Core acknowledges an interrupt before its cleanup settles, and its shell tool waits 3s before SIGKILL. */
export const CancelDrainTimeout = Context.Reference<Duration.Input>("@opencode/cli/acp/Turn/CancelDrainTimeout", {
  defaultValue: () => "5 seconds",
})

type PermissionAsk = Extract<ACPTranslate.Output, { readonly _tag: "PermissionAsk" }>

type Subscription = {
  readonly scope: Scope.Closeable
  readonly events: Queue.Dequeue<OpenCodeEvent, unknown>
  /** Asks run serially off the event stream. */
  readonly asks: Queue.Queue<Effect.Effect<void, ACPError.Error | RequestError>>
  readonly cancelled: Deferred.Deferred<void>
  readonly settled: Map<string, Deferred.Deferred<void>>
}

export const make = Effect.fnUntraced(function* (input: {
  readonly client: OpenCodeClient
  readonly connection: ACPConnection.Interface
  readonly sessions: ACPSessions.Interface
  readonly catalog: ACPCatalog.Interface
  readonly capabilities: Ref.Ref<Capabilities>
}) {
  const scope = yield* Effect.scope
  const drainTimeout = yield* CancelDrainTimeout
  const turns = yield* FiberMap.make<string, PromptResponse, ACPError.Failure>()

  const subscribe = Effect.fnUntraced(function* () {
    // Parented to the service scope; the session scope may already be closed.
    const subscriptionScope = yield* Scope.fork(scope)
    const subscription: Subscription = {
      scope: subscriptionScope,
      events: yield* input.client.event
        .subscribe()
        .pipe(Stream.toQueue({ capacity: "unbounded" }), Scope.provide(subscriptionScope)),
      asks: yield* Queue.unbounded<Effect.Effect<void, ACPError.Error | RequestError>>(),
      cancelled: yield* Deferred.make<void>(),
      settled: new Map(),
    }
    yield* Queue.take(subscription.asks).pipe(
      Effect.flatten,
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logWarning("ACP ask reply failed", cause),
      ),
      Effect.forever,
      Effect.forkIn(subscriptionScope),
    )
    return subscription
  })

  const take = (subscription: Subscription) =>
    Queue.take(subscription.events).pipe(
      Effect.catch((error) =>
        Cause.isDone(error) ? Effect.fail(new ACPError.ServerUnavailableError()) : ACPClient.classify(error),
      ),
    )

  const asksSettled = Effect.fnUntraced(function* (subscription: Subscription) {
    const settled = yield* Deferred.make<void>()
    yield* Queue.offer(subscription.asks, Deferred.succeed(settled, undefined).pipe(Effect.asVoid))
    yield* Deferred.await(settled)
  })

  const reply = (
    subscription: Subscription,
    ctx: ACPTranslate.Context,
    ask: PermissionAsk,
    settled: Deferred.Deferred<void>,
  ) =>
    ACPPermission.reply(
      {
        client: input.client,
        connection: input.connection,
        event: ask.event,
        sessionID: ask.event.data.sessionID,
        clientSessionID: ctx.sessionID,
        cwd: ctx.cwd,
        tool: ask.tool,
        child: ask.child,
        settled: Deferred.await(settled),
      },
      Deferred.await(subscription.cancelled),
    )

  const interpret = (subscription: Subscription, ctx: ACPTranslate.Context, output: ACPTranslate.Output) => {
    switch (output._tag) {
      case "SessionUpdate":
        return input.connection.sessionUpdate({ sessionId: ctx.sessionID, update: output.update })
      case "ChildUpdate":
        return input.connection
          .extNotification(ACPChild.UpdateMethod, output.update)
          .pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.void
                : Effect.logWarning("ACP child session update failed", cause),
            ),
          )
      case "PermissionAsk":
        return Effect.gen(function* () {
          const settled = yield* Deferred.make<void>()
          subscription.settled.set(output.event.data.id, settled)
          yield* Queue.offer(subscription.asks, reply(subscription, ctx, output, settled))
        })
      case "FormAsk":
        return Effect.gen(function* () {
          const capabilities = yield* Ref.get(input.capabilities)
          const requestedSchema = ACPElicitation.requestedSchema(output.form, capabilities)
          if (!requestedSchema) return yield* ACPElicitation.cancelUnshown(input.client, output.form)
          const settled = yield* Deferred.make<void>()
          subscription.settled.set(output.form.id, settled)
          yield* Queue.offer(
            subscription.asks,
            ACPElicitation.reply(
              {
                client: input.client,
                connection: input.connection,
                form: output.form,
                requestedSchema,
                clientSessionID: ctx.sessionID,
                child: output.child,
                toolCallSent: output.toolCallSent,
                settled: Deferred.await(settled),
              },
              Deferred.await(subscription.cancelled),
            ),
          )
        })
      case "AskSettled":
        return Effect.suspend(() => {
          const settled = subscription.settled.get(output.id)
          subscription.settled.delete(output.id)
          return settled ? Deferred.succeed(settled, undefined) : Effect.void
        })
    }
  }

  const consume = Effect.fnUntraced(function* (
    subscription: Subscription,
    ctx: ACPTranslate.Context,
    state: Ref.Ref<ACPTranslate.TurnState>,
  ) {
    while (true) {
      const event = yield* take(subscription)
      const next = yield* Ref.modify(state, (current) => {
        const step = ACPTranslate.step(current, event, ctx)
        return [step, step.state]
      })
      yield* Effect.forEach(next.outputs, (output) => interpret(subscription, ctx, output), { discard: true })
      if (next.terminal) {
        yield* asksSettled(subscription)
        return next.terminal
      }
    }
  })

  const submit = Effect.fnUntraced(function* (attached: Attached, prompt: ACPPrompt.Prepared) {
    const sessionID = attached.id
    if (prompt.synthetic.length > 0) {
      yield* input.client.session
        .synthetic({
          sessionID,
          text: prompt.synthetic.join("\n\n"),
          description: "ACP embedded context",
          delivery: "steer",
          resume: false,
        })
        .pipe(Effect.catch(ACPClient.classify))
    }
    if (prompt.start.type === "compaction") {
      yield* input.client.session.compact({ sessionID, id: prompt.start.id }).pipe(Effect.catch(ACPClient.classify))
      return
    }
    const command = prompt.command
    if (command) {
      yield* input.client.session
        .command({
          sessionID,
          name: command.name,
          text: prompt.slash?.args ?? "",
          files: prompt.files,
          delivery: "steer",
        })
        .pipe(Effect.catch(ACPClient.classify))
      return
    }
    yield* input.client.session
      .prompt({ sessionID, id: prompt.start.id, text: prompt.text, files: prompt.files, delivery: "steer" })
      .pipe(Effect.catch(ACPClient.classify))
  })

  const windDown = Effect.fnUntraced(function* (
    subscription: Subscription,
    ctx: ACPTranslate.Context,
    state: Ref.Ref<ACPTranslate.TurnState>,
    events: Fiber.Fiber<ACPTranslate.Terminal, ACPError.Failure>,
  ) {
    yield* Deferred.succeed(subscription.cancelled, undefined)
    yield* input.client.session
      .interrupt({ sessionID: ctx.sessionID })
      .pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logWarning("ACP server interrupt failed", cause),
        ),
      )
    if (!(yield* Ref.get(state)).started) return
    if (Option.exists(yield* Fiber.await(events).pipe(Effect.timeoutOption(drainTimeout)), Exit.isSuccess)) return
    yield* Fiber.interrupt(events)
    const abandoned = ACPTranslate.abandon(yield* Ref.get(state), ctx)
    yield* Ref.set(state, abandoned.state)
    yield* Effect.forEach(abandoned.outputs, (output) => interpret(subscription, ctx, output), { discard: true }).pipe(
      Effect.ignore,
    )
  })

  const execute = (
    attached: Attached,
    prompt: ACPPrompt.Prepared,
    ctx: ACPTranslate.Context,
    state: Ref.Ref<ACPTranslate.TurnState>,
  ) =>
    Effect.acquireUseRelease(
      subscribe(),
      (subscription) =>
        Effect.gen(function* () {
          // The feed opens with `server.connected`, so every event the submission causes comes after it.
          const connected = yield* take(subscription)
          if (connected.type !== "server.connected")
            return yield* Effect.die(new Error(`expected server.connected, got ${connected.type}`))
          const events = yield* consume(subscription, ctx, state).pipe(Effect.forkScoped)
          return yield* Effect.gen(function* () {
            yield* submit(attached, prompt)
            if (prompt.command) return "succeeded" as const
            return yield* Fiber.join(events)
          }).pipe(Effect.onInterrupt(() => windDown(subscription, ctx, state, events)))
        }).pipe(Effect.scoped),
      (subscription, exit) => handoff(attached, subscription, ctx, state, exit),
    )

  const handoff = Effect.fnUntraced(function* (
    attached: Attached,
    subscription: Subscription,
    ctx: ACPTranslate.Context,
    state: Ref.Ref<ACPTranslate.TurnState>,
    exit: Exit.Exit<ACPTranslate.Terminal, ACPError.Failure>,
  ) {
    const close = Scope.close(subscription.scope, Exit.void)
    if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)) return yield* close
    if ((yield* Ref.get(state)).openChildren.size === 0) return yield* close
    // Children outlive a cancelled turn, so their asks still reach the client.
    const cancelled = yield* Deferred.make<void>()
    const background = consume({ ...subscription, cancelled }, { ...ctx, mode: "background" }, state).pipe(
      Effect.ignore,
      Effect.ensuring(close),
      Effect.withSpan("cli.acp.turn.background"),
    )
    yield* input.sessions.fork(attached, background).pipe(Effect.catchTag("ACPSessionNotFoundError", () => close))
  })

  const settle = Effect.fnUntraced(function* (
    attached: Attached,
    state: Ref.Ref<ACPTranslate.TurnState>,
    exit: Exit.Exit<ACPTranslate.Terminal, ACPError.Failure>,
  ) {
    if (Exit.isFailure(exit) && !Cause.hasInterrupts(exit.cause)) return yield* Effect.failCause(exit.cause)
    const current = yield* Ref.get(state)
    const failure = ACPTranslate.failure(current)
    if (failure) return yield* failure
    yield* sendUsageUpdate(attached, current)
    return ACPTranslate.response(current, attached.id, Exit.isSuccess(exit) ? exit.value : "interrupted")
  })

  const sendUsageUpdate = Effect.fn("cli.acp.turn.usage")(
    function* (attached: Attached, state: ACPTranslate.TurnState) {
      const used = state.usage ? TokenUsage.total(state.usage.last) : 0
      if (!used) return
      const catalog = yield* input.catalog.get(attached.cwd)
      const current = currentModel(catalog, yield* Ref.get(attached.selection))
      const model = catalog.models.find((item) => item.providerID === current.providerID && item.id === current.id)
      if (!model?.limit.context) return
      const info = yield* input.client.session.get({ sessionID: attached.id }).pipe(Effect.catch(ACPClient.classify))
      yield* input.connection.sessionUpdate({
        sessionId: attached.id,
        update: {
          sessionUpdate: "usage_update",
          used,
          size: model.limit.context,
          cost: { amount: info.cost, currency: "USD" },
        },
      })
    },
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logWarning("ACP usage update failed", cause),
    ),
  )

  // Forked uninterruptible: interruption reaches only `execute`, so the fiber still settles with a response.
  const run = Effect.fn("cli.acp.turn.run")(function* (attached: Attached, prompt: ACPPrompt.Prepared) {
    const capabilities = yield* Ref.get(input.capabilities)
    const state = yield* Ref.make(ACPTranslate.initial)
    const ctx: ACPTranslate.Context = {
      sessionID: attached.id,
      cwd: attached.cwd,
      start: prompt.start,
      childUpdates: capabilities.childSessionUpdates,
      compaction: capabilities.compaction,
      mode: "turn",
    }
    const exit = yield* Effect.exit(Effect.interruptible(execute(attached, prompt, ctx, state)))
    return yield* settle(attached, state, exit)
  })

  return {
    prompt: Effect.fnUntraced(function* (params, signal) {
      const attached = yield* input.sessions.require(params.sessionId)
      const catalog = yield* input.catalog.get(attached.cwd)
      const prompt = yield* ACPPrompt.prepare(catalog, params.prompt)
      // Synchronous, so concurrent prompts for one session cannot both register.
      const turn = yield* Effect.withFiber((fiber) => {
        if (FiberMap.hasUnsafe(turns, attached.id)) {
          return Effect.fail(
            new ACPError.ServiceFailureError({
              safeMessage: `Session already has an active ACP prompt: ${attached.id}`,
              service: "session",
            }),
          )
        }
        const forked = Effect.runForkWith(fiber.context)(run(attached, prompt), { uninterruptible: true })
        FiberMap.setUnsafe(turns, attached.id, forked)
        return Effect.succeed(forked)
      })
      // A `$/cancel_request` for this prompt cancels its turn like `session/cancel`, rather than failing the request.
      yield* aborted(signal).pipe(Effect.andThen(Fiber.interrupt(turn)), Effect.forkChild)
      return yield* Fiber.join(turn)
    }),
    cancel: Effect.fnUntraced(function* (params) {
      yield* FiberMap.remove(turns, params.sessionId)
    }),
    close: Effect.fn("cli.acp.turn.close")(function* (sessionID) {
      if (FiberMap.hasUnsafe(turns, sessionID)) return yield* FiberMap.remove(turns, sessionID)
      yield* ACPClient.decodeSessionID(sessionID).pipe(
        Effect.flatMap((id) => input.client.session.interrupt({ sessionID: id })),
        Effect.catchTag(["ACPInvalidRequestError", "SessionNotFoundError"], () => Effect.void),
        Effect.catch(ACPClient.classify),
      )
    }),
  } satisfies Interface
})

function aborted(signal: AbortSignal) {
  return Effect.callback<void>((resume) => {
    if (signal.aborted) return resume(Effect.void)
    const abort = () => resume(Effect.void)
    signal.addEventListener("abort", abort, { once: true })
    return Effect.sync(() => signal.removeEventListener("abort", abort))
  })
}

export * as ACPTurn from "./turn"
