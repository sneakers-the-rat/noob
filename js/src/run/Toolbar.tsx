/**
 * Run controls + state pill at the top of the viewer.
 */

import { useRunStatus, useRunStore, useRunTelemetry } from "./RunContext.tsx";

export function Toolbar() {
  const store = useRunStore();
  const status = useRunStatus();
  const telemetry = useRunTelemetry();
  const { state, error } = status;

  const canInit = state === "uninitialized";
  const canDeinit =
    state === "initialized" || state === "stopped" || state === "error";
  const canStart = state === "initialized" || state === "stopped";
  const canStop = state === "running";

  return (
    <div className="runner-toolbar">
      <div className="runner-toolbar-buttons">
        <button
          type="button"
          disabled={!canInit}
          onClick={() => void store.runInit()}
        >
          init
        </button>
        <button
          type="button"
          disabled={!canStart}
          onClick={() => void store.runStart()}
        >
          start
        </button>
        <button
          type="button"
          disabled={!canStop}
          onClick={() => void store.runStop()}
        >
          stop
        </button>
        <button
          type="button"
          disabled={!canDeinit}
          onClick={() => void store.runDeinit()}
        >
          deinit
        </button>
      </div>
      <div className={`runner-state runner-state-${state}`}>
        <span className="runner-state-pill" />
        <span className="runner-state-label">{state}</span>
        {error ? <span className="runner-error" title={error}>!</span> : null}
      </div>
      <div className="runner-telemetry" title="events received by the GUI over the events WS">
        <span className="runner-telemetry-count">{telemetry.count}</span>
        <span className="runner-telemetry-sep">·</span>
        <span className="runner-telemetry-last">
          {telemetry.last
            ? `${telemetry.last.node_id}.${telemetry.last.signal}`
            : ""}
        </span>
      </div>
    </div>
  );
}
