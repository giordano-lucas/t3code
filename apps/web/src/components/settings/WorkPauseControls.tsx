import { useAtomValue } from "@effect/atom-react";
import {
  CommandId,
  type EnvironmentId,
  type OrchestrationV2WorkControlResult,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import { useEnvironmentSettings } from "../../hooks/useSettings";
import { randomUUID } from "../../lib/utils";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { Button } from "../ui/button";
import { SettingsRow } from "./settingsLayout";
import { useSettingsScope } from "./SettingsScopeContext";

function EnvironmentWorkPause({ environmentId }: { environmentId: EnvironmentId }) {
  const paused = useEnvironmentSettings(environmentId, (settings) => settings.workPaused);
  const allowed = useAtomValue(threadEnvironment.controlWork.permissionAtom(environmentId));
  const control = useOrchestrationCommand(threadEnvironment.controlWork);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<OrchestrationV2WorkControlResult | null>(null);
  async function run(action: "pause" | "resume" | "status", immediate = false) {
    if (busy) return;
    setBusy(true);
    try {
      const response = await control({
        environmentId,
        input: {
          commandId: CommandId.make(randomUUID()),
          action,
          all: true,
          immediate,
        },
      });
      if (AsyncResult.isSuccess(response)) setResult(response.value);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          variant="outline"
          disabled={busy || !allowed}
          onClick={() => void run(paused ? "resume" : "pause")}
        >
          {paused ? "Resume all work" : "Pause all work"}
        </Button>
        {paused ? (
          <Button
            size="sm"
            variant="outline"
            disabled={busy || !allowed}
            onClick={() => void run("pause", true)}
          >
            Stop immediately
          </Button>
        ) : null}
        <Button
          size="sm"
          variant="ghost"
          disabled={busy || !allowed}
          onClick={() => void run("status")}
        >
          Check pause status
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        {paused
          ? "Work stays paused across restarts until you resume."
          : "Work is accepting new runs."}
      </p>
      {result ? (
        <ul className="text-xs text-muted-foreground">
          {result.threads.map((thread) => (
            <li key={thread.threadId}>
              {thread.title}: {thread.status.replaceAll("_", " ")}
              {thread.checkpointSaved ? " (checkpoint saved)" : ""}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

export function WorkPauseControls() {
  const { scope, connectedEnvironments } = useSettingsScope();
  if (scope.kind === "project" || scope.kind === "checkout") return null;
  return (
    <SettingsRow
      id="pause-agent-work"
      title="Pause agent work"
      description="Waits for current turns, saves recovery checkpoints, and pauses T3-owned child agents. Check that every listed thread is paused before restarting. Stop immediately skips the fresh checkpoint. Native subagents may need recovery from their saved notes."
    >
      <div className="flex flex-col gap-3">
        {connectedEnvironments
          .filter(
            (environment) => environment.serverConfig?.environment.capabilities.workPause === true,
          )
          .map((environment) => (
            <div key={environment.environmentId}>
              <p className="text-sm">
                {environment.serverConfig?.environment.label ?? environment.environmentId}
              </p>
              <EnvironmentWorkPause environmentId={environment.environmentId} />
            </div>
          ))}
      </div>
    </SettingsRow>
  );
}
