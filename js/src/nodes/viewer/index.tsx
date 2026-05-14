/**
 * Viewer node — a user-added overlay node that subscribes to whichever
 * source handles are wired into its target handles, then dispatches to a
 * display child component based on the chosen mode.
 *
 * The mode picker lives in the header and is always available (even before
 * any signal is connected) so the user can configure the viewer first and
 * connect matching signals after — e.g. switch to LINE mode to expose the
 * `x` handle, then drag two sources onto it for a Lissajous.
 *
 * Handle topology is owned by each display module (see
 * `./displays/index.ts`). When the mode flips, edges targeting handles
 * the new display doesn't expose are dropped so we don't carry orphans.
 */

import { useEffect, useMemo } from "react";
import {
  NodeResizer,
  type Edge,
  type NodeProps,
  Position,
  useEdges,
  useReactFlow,
  useUpdateNodeInternals,
} from "@xyflow/react";

import {
  type ViewerNode as ViewerNodeType,
  type ViewerMode,
  parseSignalHandle,
} from "../../types.ts";
import { LabeledHandle } from "../../handle.tsx";
import { useOptionalSignal } from "../../run/RunContext.tsx";
import type { ValueRecord } from "../../run/protocol.ts";
import { ALL_MODES, DISPLAYS } from "./displays/index.ts";
import NoSignal from "./displays/NoSignal.tsx";
import Header from "./Header.tsx";
import Brand from "./Brand.tsx";

function srcFromEdge(edge: Edge | undefined) {
  if (!edge?.sourceHandle) return null;
  return parseSignalHandle(edge.sourceHandle);
}

function compatibleModes(latest: ValueRecord | undefined): ViewerMode[] {
  const out: ViewerMode[] = [];
  for (const m of ALL_MODES) {
    if (DISPLAYS[m].canRender(latest)) out.push(m);
  }
  return out.length ? out : ["raw"];
}

export default function ViewerNode({
  id,
  data,
}: NodeProps<ViewerNodeType>) {
  const edges = useEdges();
  const { setNodes, setEdges } = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();

  // xyflow caches each handle's offset from the node origin. Re-measure when
  // the mode flips (handles set changes) or the node is reparented.
  useEffect(() => {
    updateNodeInternals(id);
  }, [id, data.mode, updateNodeInternals]);

  const display = DISPLAYS[data.mode];

  // Fixed-position hook calls — declared per known handle id so we can call
  // `useOptionalSignal` unconditionally. Currently the only handles in use
  // are "in" (always) and "x" (line mode); adding more is a one-line addition
  // here and in the relevant display module.
  const incomingByHandle = useMemo(() => {
    const m: Record<string, Edge> = {};
    for (const e of edges) {
      if (e.target === id && e.targetHandle) m[e.targetHandle] = e;
    }
    return m;
  }, [edges, id]);

  const inSource = srcFromEdge(incomingByHandle["in"]);
  const xSource = srcFromEdge(incomingByHandle["x"]);

  const inRecords = useOptionalSignal(inSource);
  const xRecords = useOptionalSignal(xSource);

  // Filter recordsByHandle to only the handles the active display cares about.
  const recordsByHandle = useMemo<Record<string, ValueRecord[]>>(() => {
    const r: Record<string, ValueRecord[]> = {};
    for (const h of display.handles) {
      r[h.id] = h.id === "in" ? inRecords : h.id === "x" ? xRecords : [];
    }
    return r;
  }, [display, inRecords, xRecords]);

  const latestIn = inRecords.length ? inRecords[inRecords.length - 1] : undefined;
  const compatible: ViewerMode[] = inSource
    ? compatibleModes(latestIn)
    : ALL_MODES;

  const sources = useMemo<
    Record<string, { node_id: string; signal: string } | null>
  >(() => {
    const r: Record<string, { node_id: string; signal: string } | null> = {};
    for (const h of display.handles) {
      r[h.id] = h.id === "in" ? inSource : h.id === "x" ? xSource : null;
    }
    return r;
  }, [display, inSource, xSource]);

  const closeViewer = () => {
    setEdges((eds) => eds.filter((e) => e.target !== id));
    setNodes((ns) => ns.filter((n) => n.id !== id));
  };

  const changeMode = (next: ViewerMode) => {
    const validHandles = new Set(DISPLAYS[next].handles.map((h) => h.id));
    setEdges((eds) =>
      eds.filter(
        (e) => e.target !== id || validHandles.has(e.targetHandle ?? ""),
      ),
    );
    setNodes((ns) =>
      ns.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, mode: next } } : n,
      ),
    );
  };

  const DisplayComponent = display.Component;
  const hasRequiredSignal = inSource !== null;

  return (
    <>
      <NodeResizer
        minWidth={120}
        minHeight={80}
        lineClassName="viewer-resize-line"
        handleClassName="viewer-resize-handle"
      />
      <div className="handles targets">
        {display.handles.map((h) => (
          <LabeledHandle
            key={h.id}
            id={h.id}
            type="target"
            position={Position.Left}
            label={h.label}
          />
        ))}
      </div>
      <button
        type="button"
        className="viewer-close"
        aria-label="close viewer"
        data-testid="viewer-close"
        onClick={closeViewer}
        onMouseDown={(e) => e.stopPropagation()}
      >
        ×
      </button>
      <span className="viewer-led" />
      <div className="viewer-screen">
        <Header
          mode={data.mode}
          compatible={compatible}
          onChange={changeMode}
          sources={sources}
        />
        <div className="viewer-display">
          {hasRequiredSignal ? (
            <DisplayComponent records={recordsByHandle} />
          ) : (
            <NoSignal />
          )}
        </div>
      </div>
      <Brand />
    </>
  );
}
