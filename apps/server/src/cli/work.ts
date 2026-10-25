import { CommandId, OrchestrationV2WorkControlResult, ThreadId } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/cli";
import { projectLocationFlags } from "./config.ts";
import { ThreadUsageError, reportThreadErrors, withThreadTools } from "./thread.ts";

const encodeWorkResult = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2WorkControlResult),
);

const control = (action: "pause" | "resume" | "status") =>
  Command.make(action, {
    ...projectLocationFlags,
    all: Flag.Boolean("all").pipe(
      Flag.withDescription("Control all work on this environment."),
      Flag.withDefault(false),
    ),
    thread: Flag.String("thread").pipe(Flag.optional),
    immediate: Flag.Boolean("immediate").pipe(
      Flag.withDescription("Interrupt without a fresh checkpoint (pause only)."),
      Flag.withDefault(false),
    ),
    json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
  }).pipe(
    Command.withDescription(
      action === "pause"
        ? "Save checkpoints and stay paused across app or machine restarts."
        : action === "resume"
          ? "Continue paused conversations and their T3-owned children."
          : "Show checkpoint and pause status. Only paused work has finished stopping.",
    ),
    Command.withHandler((flags) =>
      Effect.gen(function* () {
        if (flags.all === Option.isSome(flags.thread))
          return yield* new ThreadUsageError({
            detail: "Choose --all or --thread <thread-id>.",
          });
        if (flags.immediate && action !== "pause")
          return yield* new ThreadUsageError({
            detail: "--immediate is only available for pause.",
          });
        const crypto = yield* Crypto.Crypto;
        const commandId = CommandId.make(
          `command:cli:work:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
        );
        const result = yield* withThreadTools(flags, (tools) =>
          tools.call(
            "t3_work_control",
            {
              commandId,
              action,
              all: flags.all,
              ...(Option.isSome(flags.thread)
                ? { threadId: ThreadId.make(flags.thread.value) }
                : {}),
              ...(flags.immediate ? { immediate: true } : {}),
            },
            OrchestrationV2WorkControlResult,
          ),
        );
        if (flags.json) return yield* encodeWorkResult(result).pipe(Effect.flatMap(Console.log));
        yield* Console.log(
          result.allPaused
            ? "Environment paused; explicit resume required after restart."
            : "Environment accepting work.",
        );
        for (const thread of result.threads)
          yield* Console.log(
            `${thread.status}\t${thread.threadId}\t${thread.title}${thread.checkpointSaved ? " (checkpoint saved)" : ""}`,
          );
        if (action === "pause" && result.threads.some((thread) => thread.status !== "paused")) {
          yield* Console.log(
            "Pause requested. Check t3 work status with the same target before restarting. Graceful pause waits for the current turn; --immediate stops without a fresh checkpoint.",
          );
        }
      }).pipe(reportThreadErrors),
    ),
  );

export const workCommand = Command.make("work").pipe(
  Command.withDescription("Pause and resume durable agent work."),
  Command.withSubcommands([control("pause"), control("status"), control("resume")]),
);
