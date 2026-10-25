import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ServerSettings as ServerSettingsSchema } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type RunId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const encodeSettings = Schema.encodeSync(Schema.fromJsonString(ServerSettingsSchema));
const decodeSettings = Schema.decodeUnknownEffect(Schema.fromJsonString(ServerSettingsSchema));
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("These tests do not start real providers"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
type ReplayDatabase = NonNullable<
  NonNullable<Parameters<typeof ProviderReplayHarness.layerWithRegistry>[2]>["databaseLayer"]
>;
const makeLayer = (
  database: ReplayDatabase = SqlitePersistence.layerMemory,
  settings = ServerSettings.layerTest().pipe(Layer.orDie),
) =>
  ThreadManagementService.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        database,
        ProjectionStore.layer.pipe(Layer.provide(database)),
        EventStore.layer.pipe(Layer.provide(database)),
        EffectOutbox.layer.pipe(Layer.provide(database)),
        ProviderReplayHarness.layerWithRegistry(
          { name: "work-pause" },
          ProviderAdapterRegistry.layerFromAdapters([adapter]),
          {
            databaseLayer: database,
            runEffectWorker: false,
            serverSettingsLayer: settings,
          },
        ),
      ),
    ),
    Layer.provideMerge(settings),
    Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(database))),
  );
const layer = makeLayer();
const create = (threadId: ThreadId) =>
  Orchestrator.OrchestratorV2.use((orchestrator) =>
    orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:work-pause"),
      title: threadId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    }),
  );
const send = (threadId: ThreadId, text: string) =>
  Orchestrator.OrchestratorV2.use((orchestrator) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${threadId}:${text}`),
      threadId,
      messageId: MessageId.make(`message:${threadId}:${text}`),
      text,
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    }),
  );
const control = (
  action: "pause" | "resume" | "status",
  id: string,
  threadId?: ThreadId,
  immediate = false,
) =>
  ThreadManagementService.ThreadManagementService.use((service) =>
    service.controlWork({
      action,
      commandId: CommandId.make(id),
      ...(threadId === undefined ? { all: true } : { threadId }),
      immediate,
    }),
  );
const settle = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const thread = (yield* orchestrator.getThreadProjection(threadId)).thread;
    yield* (yield* ThreadManagementService.ThreadManagementService).settleWorkPause({
      threadId,
      requestId: thread.workPause!.requestId,
    });
  });
const complete = (
  threadId: ThreadId,
  runId: RunId,
  status: "completed" | "failed" = "completed",
  text = "Saved objective, worktree, child task IDs, and remaining steps.",
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const run = projection.runs.find((candidate) => candidate.id === runId)!;
    const now = yield* DateTime.now;
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: EventId.make(`event:result:${runId}`),
          type: "message.updated",
          threadId,
          runId,
          occurredAt: now,
          payload: {
            id: MessageId.make(`message:result:${runId}`),
            threadId,
            runId,
            nodeId: null,
            role: "assistant",
            text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "server",
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:complete:${runId}`),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: { ...run, status, completedAt: now },
        },
      ],
    });
  });

it.effect("retires a delayed provider-start effect when pausing before it executes", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:delayed-start");
    yield* create(threadId);
    yield* send(threadId, "original work");
    yield* control("pause", "pause:delayed-start", undefined, true);
    yield* settle(threadId);
    yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
    assert.equal(
      (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status,
      "interrupted",
    );
    yield* control("resume", "resume:delayed-start");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.lengthOf(projection.runs, 2);
    assert.equal(projection.runs[1]?.status, "starting");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "recovers a partly enrolled pause using its saved run identity without reviving older interrupted work",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const settings = yield* ServerSettings.ServerSettingsService;
      const activeId = ThreadId.make("thread:journal-active");
      const olderId = ThreadId.make("thread:journal-older");
      for (const threadId of [activeId, olderId]) {
        yield* create(threadId);
        yield* send(threadId, "original work");
        const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          threadId,
          runId: run.id,
          commandId: CommandId.make(`interrupt:${threadId}`),
          reason: "Simulated restart",
        });
      }
      const activeRun = (yield* orchestrator.getThreadProjection(activeId)).runs[0]!;
      yield* settings.updateSettings({
        workPaused: true,
        workPauseTargets: {
          requestId: CommandId.make("pause:partly-enrolled"),
          threadIds: [activeId, olderId],
          resumeRunIds: { [activeId]: activeRun.id },
        },
      });
      yield* send(olderId, "queued while paused");
      yield* control("resume", "resume:partly-enrolled");
      const active = yield* orchestrator.getThreadProjection(activeId);
      const older = yield* orchestrator.getThreadProjection(olderId);
      assert.lengthOf(active.runs, 2);
      assert.equal(active.runs.at(-1)?.status, "starting");
      assert.lengthOf(older.runs, 2);
      assert.equal(
        older.runs.at(-1)?.userMessageId,
        MessageId.make(`message:${olderId}:queued while paused`),
      );
      assert.equal(older.runs.at(-1)?.status, "starting");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "holds new inputs and their queue across recovery, and resumes once without releasing an existing held queue",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:pause-inputs");
      yield* create(threadId);
      yield* send(threadId, "original work");
      yield* send(threadId, "intentionally held");
      let projection = yield* orchestrator.getThreadProjection(threadId);
      const queued = projection.runs[1]!;
      yield* projections.apply({
        id: EventId.make("event:hold-existing"),
        type: "run.updated",
        threadId,
        occurredAt: yield* DateTime.now,
        payload: { ...queued, queueHeld: true },
      });
      yield* control("pause", "pause:inputs", threadId, true);
      yield* settle(threadId);
      yield* send(threadId, "arrived while paused");
      yield* orchestrator.resumeQueuedRuns;
      yield* orchestrator.recoverDelegatedTasks;
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.workPause?.phase, "paused");
      assert.equal(projection.runs.filter((run) => run.status === "starting").length, 0);
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["interrupted", "queued", "queued"],
      );
      assert.equal((yield* projections.getThreadShell(threadId))?.workPause?.phase, "paused");
      yield* control("resume", "resume:inputs", threadId);
      yield* control("resume", "resume:inputs-again", threadId);
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.workPause, null);
      assert.equal(projection.runs.length, 4);
      assert.equal(projection.runs[1]?.queueHeld, true);
      assert.equal(projection.runs.at(-1)?.status, "starting");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "pauses the parent and child without cancelling their result cohort and retains the delegated task identity on resume",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parentId = ThreadId.make("thread:pause-parent");
      yield* create(parentId);
      yield* send(parentId, "parent work");
      const run = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("delegate:pause-child"),
        parentThreadId: parentId,
        parentRunId: run.id,
        parentNodeId: run.rootNodeId!,
        task: "child work",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        completionWake: "always",
        createdBy: "agent",
        creationSource: "mcp",
      });
      const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
      const childId = task.childThreadId!;
      const paused = yield* control("pause", "pause:tree", parentId, true);
      assert.lengthOf(paused.threads, 2);
      yield* settle(parentId);
      yield* settle(childId);
      yield* orchestrator.recoverDelegatedTasks;
      const heldTask = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
      assert.isNull(heldTask.result);
      assert.notEqual(heldTask.completionDelivery?.state, "disposed");
      assert.isTrue(yield* orchestrator.delegatedTaskResultPending(childId, task.id));
      yield* control("resume", "resume:tree", parentId);
      const child = yield* orchestrator.getThreadProjection(childId);
      assert.equal(child.runs.at(-1)?.delegatedTaskId, task.id);
      assert.equal((yield* orchestrator.getThreadProjection(parentId)).subagents.length, 1);
      yield* complete(childId, child.runs.at(-1)!.id);
      yield* orchestrator.recoverDelegatedTasks;
      yield* orchestrator.recoverDelegatedTasks;
      const parent = yield* orchestrator.getThreadProjection(parentId);
      assert.isNotNull(parent.subagents[0]!.result);
      assert.notEqual(parent.subagents[0]!.completionDelivery?.state, "disposed");
      assert.lengthOf(
        parent.contextTransfers.filter((transfer) => transfer.type === "subagent_result"),
        1,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["failed", "empty"] as const)(
  "keeps the global barrier after an early resume and recovers a %s checkpoint",
  (failure) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:failed-checkpoint");
      yield* create(threadId);
      yield* send(threadId, "original work");
      yield* control("pause", "pause:failed-checkpoint");
      const early = yield* control("resume", "resume:too-early").pipe(Effect.result);
      assert.equal(early._tag, "Failure");
      assert.isTrue((yield* (yield* ServerSettings.ServerSettingsService).getSettings).workPaused);
      let projection = yield* orchestrator.getThreadProjection(threadId);
      yield* complete(threadId, projection.runs[0]!.id);
      yield* orchestrator.resumeQueuedRuns;
      projection = yield* orchestrator.getThreadProjection(threadId);
      const checkpoint = projection.runs.find(
        (run) => run.userMessageId === projection.thread.workPause!.checkpointMessageId,
      )!;
      yield* complete(threadId, checkpoint.id, failure === "failed" ? "failed" : "completed", "");
      yield* settle(threadId);
      const failed = yield* control("status", "status:failed-checkpoint");
      assert.equal(failed.threads[0]?.status, "needs_attention");
      assert.isFalse(failed.threads[0]!.checkpointSaved);
      yield* control("pause", "pause:failed-immediate", undefined, true);
      yield* settle(threadId);
      const resumed = yield* control("resume", "resume:failed-immediate");
      assert.isFalse(resumed.allPaused);
      assert.equal(resumed.threads[0]?.status, "running");
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.runs.at(-1)?.status, "starting");
      assert.isNull(projection.thread.workPause);
    }).pipe(Effect.provide(layer)),
);

it.effect("cancels an unstarted checkpoint when escalating to immediate pause", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:escalate-checkpoint");
    yield* create(threadId);
    yield* send(threadId, "original work");
    yield* control("pause", "pause:escalate-graceful", threadId);
    yield* control("pause", "pause:escalate-immediate", threadId, true);
    yield* settle(threadId);
    yield* orchestrator.resumeQueuedRuns;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(projection.thread.workPause?.phase, "paused");
    assert.deepEqual(
      projection.runs.map((run) => run.status),
      ["interrupted", "cancelled"],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "allows only the checkpoint through the pause barrier, including ahead of an intentionally held message",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:graceful-pause");
      yield* create(threadId);
      yield* send(threadId, "original work");
      yield* send(threadId, "later input");
      let projection = yield* orchestrator.getThreadProjection(threadId);
      const original = projection.runs[0]!;
      const queued = projection.runs[1]!;
      yield* (yield* ProjectionStore.ProjectionStoreV2).apply({
        id: EventId.make("event:hold-graceful"),
        type: "run.updated",
        threadId,
        occurredAt: yield* DateTime.now,
        payload: { ...queued, queueHeld: true },
      });
      yield* control("pause", "pause:graceful");
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.workPause?.phase, "checkpointing");
      assert.equal(projection.runs.at(-1)?.status, "queued");
      yield* complete(threadId, original.id);
      yield* orchestrator.resumeQueuedRuns;
      projection = yield* orchestrator.getThreadProjection(threadId);
      const checkpoint = projection.runs.find(
        (run) => run.userMessageId === projection.thread.workPause!.checkpointMessageId,
      )!;
      assert.equal(checkpoint.status, "starting");
      assert.equal(projection.runs.find((run) => run.id === queued.id)?.status, "queued");
      yield* complete(threadId, checkpoint.id);
      yield* settle(threadId);
      const status = yield* control("status", "status:graceful");
      assert.equal(status.threads[0]?.status, "paused");
      assert.equal(status.threads[0]?.checkpointSaved, true);
      yield* orchestrator.resumeQueuedRuns;
      projection = yield* orchestrator.getThreadProjection(threadId);
      assert.isFalse(projection.runs.some((run) => run.status === "starting"));
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "holds newly created conversations while the environment is paused and releases them on explicit resume",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* control("pause", "pause:empty-environment");
      const threadId = ThreadId.make("thread:created-while-paused");
      yield* create(threadId);
      yield* send(threadId, "new work");
      yield* orchestrator.resumeQueuedRuns;
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status, "queued");
      yield* control("resume", "resume:empty-environment");
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status, "starting");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "rebuilds a paused task tree from disk without starting it, then resumes the same child once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-work-pause-restart-" });
        const dbPath = path.join(directory, "statev2.sqlite");
        const settingsPath = path.join(directory, "settings.json");
        const parentId = ThreadId.make("thread:disk-parent");
        const saved = yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            yield* create(parentId);
            yield* send(parentId, "parent work");
            const run = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
            yield* orchestrator.dispatch({
              type: "delegated_task.request",
              commandId: CommandId.make("delegate:disk-child"),
              parentThreadId: parentId,
              parentRunId: run.id,
              parentNodeId: run.rootNodeId!,
              task: "child work",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              completionWake: "always",
              createdBy: "agent",
              creationSource: "mcp",
            });
            const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
            yield* control("pause", "pause:disk", undefined, true);
            yield* settle(parentId);
            yield* settle(task.childThreadId!);
            yield* send(parentId, "queued before restart");
            const settings = yield* (yield* ServerSettings.ServerSettingsService).getSettings;
            yield* fs.writeFileString(settingsPath, encodeSettings(settings));
            return { childId: task.childThreadId!, taskId: task.id };
          }).pipe(
            Effect.provide(
              Layer.fresh(
                makeLayer(
                  SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer)),
                ),
              ),
            ),
          ),
        );
        const reloadedSettings = yield* decodeSettings(yield* fs.readFileString(settingsPath));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            yield* orchestrator.recoverDelegatedTasks;
            yield* orchestrator.resumeQueuedRuns;
            const status = yield* control("status", "status:disk");
            assert.isTrue(status.allPaused);
            assert.deepEqual(
              status.threads.map((thread) => thread.status),
              ["paused", "paused"],
            );
            assert.isNull((yield* orchestrator.getThreadProjection(parentId)).subagents[0]!.result);
            assert.isFalse(
              (yield* orchestrator.getThreadProjection(saved.childId)).runs.some(
                (run) => run.status === "starting",
              ),
            );
            yield* control("resume", "resume:disk");
            yield* control("resume", "resume:disk");
            yield* control("pause", "pause:disk", undefined, true);
            assert.isFalse(
              (yield* (yield* ServerSettings.ServerSettingsService).getSettings).workPaused,
            );
            const child = yield* orchestrator.getThreadProjection(saved.childId);
            assert.equal(child.runs.at(-1)?.delegatedTaskId, saved.taskId);
            assert.lengthOf(child.runs, 2);
            assert.equal(child.runs.at(-1)?.status, "starting");
            assert.isNull(child.thread.workPause);
          }).pipe(
            Effect.provide(
              Layer.fresh(
                makeLayer(
                  SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer)),
                  ServerSettings.layerTest(reloadedSettings).pipe(Layer.orDie),
                ),
              ),
            ),
          ),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
