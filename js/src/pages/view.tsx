import {
  isViewerNode,
  type NodeUnion,
  type TubeSpecification,
  type ViewerNode as ViewerNodeType,
} from "../types.ts";
import {
  addEdge,
  Background,
  type Connection,
  ConnectionMode,
  Controls,
  type Edge,
  ReactFlow,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "@xyflow/react";
import { useCallback, useEffect, useState } from "react";
import { tubeToFlow } from "../tube.tsx";
import useLayoutNodes from "../useLayoutNodes.tsx";
import ElkNode from "../nodes/elk.tsx";
import TitleNode from "../nodes/title.tsx";
import ViewerNode from "../nodes/viewer.tsx";
import { InputEdge, ReturnEdge } from "../edge.tsx";
import { RunProvider } from "../run/RunContext.tsx";
import { Toolbar } from "../run/Toolbar.tsx";
import { ContextMenu } from "../run/ContextMenu.tsx";

interface ViewProps {
  tube_id: string;
  color: "dark" | "light";
}

const nodeTypes = {
  elk: ElkNode,
  title: TitleNode,
  group: ElkNode,
  viewer: ViewerNode,
};

const edgeTypes = {
  inputEdge: InputEdge,
  returnEdge: ReturnEdge,
};

function makeViewerNode(position: { x: number; y: number }): ViewerNodeType {
  // Date.now() + random keeps ids stable across re-renders without needing crypto.
  const id = `viewer-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 6)}`;
  return {
    id,
    type: "viewer",
    position,
    data: {
      label: "viewer",
      mode: "raw",
      sourceHandles: [],
      targetHandles: [],
    },
  };
}

/**
 * Live viewer that refreshes a tube definition from a websocket.
 *
 * Two layers of node state live in the same xyflow tree:
 *  - "structural" nodes/edges, derived from the tube spec; replaced on every
 *    spec update.
 *  - "viewer" nodes and the edges that connect them to signal handles, owned
 *    by the GUI and preserved across spec updates.
 */
function ViewInner(props: ViewProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<NodeUnion>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [menu, setMenu] = useState<{ x: number; y: number } | null>(null);
  const rf = useReactFlow();

  useEffect(() => {
    const socket = new WebSocket(`/spec/${props.tube_id}`);
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      const next = JSON.parse(event.data) as TubeSpecification;
      const [structuralEdges, structuralNodes] = tubeToFlow(next);
      let viewerIds: Set<string> = new Set();
      setNodes((prev) => {
        const viewers = prev.filter(isViewerNode);
        viewerIds = new Set(viewers.map((v) => v.id));
        return [...structuralNodes, ...viewers];
      });
      setEdges((prev) => {
        const kept = prev.filter((e) => viewerIds.has(e.target));
        return [...structuralEdges, ...kept];
      });
    });
    return () => socket.close();
  }, [props.tube_id, setNodes, setEdges]);

  useLayoutNodes();

  const onConnect = useCallback(
    (conn: Connection) => {
      if (!conn.target) return;
      // Only accept connections terminating in a viewer node — guards against
      // accidentally wiring up tube nodes to each other through the GUI.
      const target = nodes.find((n) => n.id === conn.target);
      if (!target || !isViewerNode(target)) return;
      setEdges((eds) =>
        addEdge(
          {
            ...conn,
            id: `viewer-edge-${conn.source}-${conn.sourceHandle}-${conn.target}`,
          },
          eds,
        ),
      );
    },
    [nodes, setEdges],
  );

  const onPaneContextMenu = useCallback(
    (event: React.MouseEvent | MouseEvent) => {
      event.preventDefault();
      setMenu({ x: event.clientX, y: event.clientY });
    },
    [],
  );

  const addViewerAtMenu = useCallback(() => {
    if (!menu) return;
    const flowPos = rf.screenToFlowPosition({ x: menu.x, y: menu.y });
    setNodes((ns) => [...ns, makeViewerNode(flowPos)]);
    setMenu(null);
  }, [menu, rf, setNodes]);

  return (
    <>
      <Toolbar />
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onPaneContextMenu={onPaneContextMenu}
        onPaneClick={() => setMenu(null)}
        colorMode={props.color}
        connectionMode={ConnectionMode.Loose}
        minZoom={0.1}
        maxZoom={10}
        fitView
      >
        <Background />
        <Controls />
      </ReactFlow>
      {menu ? (
        <ContextMenu x={menu.x} y={menu.y} onClose={() => setMenu(null)}>
          <button type="button" onClick={addViewerAtMenu}>
            add viewer node
          </button>
        </ContextMenu>
      ) : null}
    </>
  );
}

export default function View(props: ViewProps) {
  return (
    <RunProvider tube_id={props.tube_id}>
      <ViewInner {...props} />
    </RunProvider>
  );
}
