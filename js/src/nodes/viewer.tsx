/**
 * Viewer node — a user-added overlay node that subscribes to whichever
 * source handles are wired into its left target handles.
 *
 * Most display modes use a single ``in`` handle, but the ``line`` mode
 * exposes a second ``x`` handle so the user can plot one signal against
 * another (Lissajous-style). When the mode flips, edges targeting handles
 * that no longer exist are dropped so we don't carry orphan connections.
 *
 * The mode picker lives in the screen header and is always available —
 * including before any signal is connected — so the user can configure the
 * viewer first, then draw the edges that match (e.g. switch to line plot
 * to expose the ``x`` handle, then drag two sources onto it).
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
} from "../types.ts";
import { LabeledHandle } from "../handle.tsx";
import { useOptionalSignal } from "../run/RunContext.tsx";
import { RawDisplay } from "../run/displays/RawDisplay.tsx";
import {
  ImageDisplay,
  canRenderImage,
} from "../run/displays/ImageDisplay.tsx";
import { LinePlot, canRenderLine } from "../run/displays/LinePlot.tsx";
import type { ValueRecord } from "../run/protocol.ts";

const ALL_MODES: ViewerMode[] = ["raw", "line", "image"];
const MODE_SHORT: Record<ViewerMode, string> = {
  raw: "RAW",
  line: "LINE",
  image: "IMG",
};
const MODE_TITLE: Record<ViewerMode, string> = {
  raw: "raw value",
  line: "line plot",
  image: "image",
};

function modeHandles(mode: ViewerMode): { id: string; label: string }[] {
  if (mode === "line")
    return [
      { id: "in", label: "y" },
      { id: "x", label: "x" },
    ];
  return [{ id: "in", label: "" }];
}

function compatibleModes(latest: ValueRecord | undefined): ViewerMode[] {
  const out: ViewerMode[] = ["raw"];
  if (canRenderLine(latest)) out.push("line");
  if (canRenderImage(latest)) out.push("image");
  return out;
}

function srcFromEdge(edge: Edge | undefined) {
  if (!edge?.sourceHandle) return null;
  return parseSignalHandle(edge.sourceHandle);
}

export default function ViewerNode({
  id,
  data,
  selected,
}: NodeProps<ViewerNodeType>) {
  const edges = useEdges();
  const { setNodes, setEdges } = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();

  // xyflow caches each handle's offset from the node origin. If we don't
  // re-measure when handles change (e.g. line mode exposes the second `x`
  // handle), edges stick to the stale offsets and snap to the wrong place.
  useEffect(() => {
    updateNodeInternals(id);
  }, [id, data.mode, updateNodeInternals]);

  const incomingByHandle = useMemo(() => {
    const m: Record<string, Edge> = {};
    for (const e of edges) {
      if (e.target === id && e.targetHandle) m[e.targetHandle] = e;
    }
    return m;
  }, [edges, id]);

  const ySource = srcFromEdge(incomingByHandle["in"]);
  const xSource =
    data.mode === "line" ? srcFromEdge(incomingByHandle["x"]) : null;

  const yRecords = useOptionalSignal(ySource);
  const xRecords = useOptionalSignal(xSource);
  const latestY = yRecords.length ? yRecords[yRecords.length - 1] : undefined;

  // Compatibility is only a *hint* — buttons stay clickable so the user can
  // pre-select a mode before any signal arrives. If a mode can't actually
  // render the current value, the display itself shows its own fallback.
  const compatible: ViewerMode[] = ySource ? compatibleModes(latestY) : ALL_MODES;

  const closeViewer = () => {
    setEdges((eds) => eds.filter((e) => e.target !== id));
    setNodes((ns) => ns.filter((n) => n.id !== id));
  };

  const changeMode = (next: ViewerMode) => {
    const validHandles = new Set(modeHandles(next).map((h) => h.id));
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

  const handles = modeHandles(data.mode);

  return (
    <>
      <NodeResizer
        minWidth={120}
        minHeight={80}
        isVisible={selected}
        lineClassName="viewer-resize-line"
        handleClassName="viewer-resize-handle"
      />
      <div className="handles targets">
        {handles.map((h) => (
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
        onClick={closeViewer}
        onMouseDown={(e) => e.stopPropagation()}
      >
        ×
      </button>
      <span className="viewer-led" />
      <div className="viewer-screen">
        <ViewerHeader
          mode={data.mode}
          compatible={compatible}
          onChange={changeMode}
          ySource={ySource}
          xSource={xSource}
        />
        <div className="viewer-display">
          {ySource ? (
            <ViewerDisplay
              mode={data.mode}
              yRecords={yRecords}
              xRecords={xRecords}
            />
          ) : (
            <div className="viewer-no-signal">
              <span>NO SIGNAL</span>
            </div>
          )}
        </div>
      </div>
      <ViewerBrand />
    </>
  );
}

function ViewerHeader({
  mode,
  compatible,
  onChange,
  ySource,
  xSource,
}: {
  mode: ViewerMode;
  compatible: ViewerMode[];
  onChange: (m: ViewerMode) => void;
  ySource: { node_id: string; signal: string } | null;
  xSource: { node_id: string; signal: string } | null;
}) {
  const sourceLabel = ySource
    ? xSource
      ? `${xSource.node_id}.${xSource.signal} → ${ySource.node_id}.${ySource.signal}`
      : `${ySource.node_id}.${ySource.signal}`
    : "—";
  const sourceTitle = ySource
    ? xSource
      ? `y: ${ySource.node_id}.${ySource.signal} · x: ${xSource.node_id}.${xSource.signal}`
      : `${ySource.node_id}.${ySource.signal}`
    : "no signal connected";

  return (
    <div className="viewer-header">
      <span className="viewer-source" title={sourceTitle}>
        {sourceLabel}
      </span>
      <div className="viewer-modes" role="group" aria-label="display mode">
        {ALL_MODES.map((m) => {
          const active = mode === m;
          const na = !compatible.includes(m);
          return (
            <button
              key={m}
              type="button"
              className={
                "viewer-mode-btn" +
                (active ? " active" : "") +
                (na ? " na" : "")
              }
              title={MODE_TITLE[m] + (na ? " (no matching value)" : "")}
              onClick={(e) => {
                e.stopPropagation();
                onChange(m);
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              {MODE_SHORT[m]}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function ViewerDisplay({
  mode,
  yRecords,
  xRecords,
}: {
  mode: ViewerMode;
  yRecords: ValueRecord[];
  xRecords: ValueRecord[];
}) {
  switch (mode) {
    case "raw":
      return <RawDisplay records={yRecords} />;
    case "line":
      return <LinePlot yRecords={yRecords} xRecords={xRecords} />;
    case "image":
      return <ImageDisplay records={yRecords} />;
  }
}

/**
 * 90s toy / cassette futurism maker's mark.
 * Chunky white text with a magenta drop-shadow + yellow accent star.
 */
function ViewerBrand() {
  return (
    <div className="viewer-brand" aria-hidden>
      <span className="viewer-logo-text">VIEWER</span>
      <span className="viewer-logo-star">★</span>
    </div>
  );
}
