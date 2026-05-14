/**
 * React bindings for the RunStore.
 *
 * - `<RunProvider tube_id>` creates a store, opens the WS, and exposes it via context.
 * - Hooks let components read narrow slices without re-rendering on every event.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
  type ReactNode,
} from "react";

import { RunStore, type SignalBuffer } from "./RunStore.ts";
import type { StatusSnapshot } from "./RunStore.ts";
import type { ValueRecord } from "./protocol.ts";

const RunStoreContext = createContext<RunStore | null>(null);

export function RunProvider({
  tube_id,
  children,
}: {
  tube_id: string;
  children: ReactNode;
}) {
  const store = useMemo(() => new RunStore(tube_id), [tube_id]);

  useEffect(() => {
    store.connect();
    return () => store.disconnect();
  }, [store]);

  return (
    <RunStoreContext.Provider value={store}>
      {children}
    </RunStoreContext.Provider>
  );
}

export function useRunStore(): RunStore {
  const store = useContext(RunStoreContext);
  if (!store) throw new Error("useRunStore outside RunProvider");
  return store;
}

export function useRunStatus(): StatusSnapshot {
  const store = useRunStore();
  return useSyncExternalStore(
    (cb) => store.subscribeState(cb),
    () => store.status,
  );
}

export function useRunTelemetry(): {
  count: number;
  last: { node_id: string; signal: string; ts: number } | null;
} {
  const store = useRunStore();
  return useSyncExternalStore(
    (cb) => store.subscribeTelemetry(cb),
    () => store.telemetry,
  );
}

/**
 * Returns ms-since-epoch timestamp of the last event from this node, or 0.
 * Subscribes only to this node; other nodes' events don't trigger re-render.
 */
export function useNodeActivity(node_id: string): number {
  const store = useRunStore();
  return useSyncExternalStore(
    (cb) => store.subscribeNode(node_id, cb),
    () => store.getNodeActivity(node_id),
  );
}

/**
 * Subscribe to a signal's value stream. Returns the buffered records.
 *
 * Server-side subscription is reference-counted: opening this hook tells the
 * server to start streaming values for `(node_id, signal)`; closing it stops.
 */
export function useSignal(
  node_id: string,
  signal: string,
): { records: ValueRecord[]; buffer: SignalBuffer | undefined } {
  const store = useRunStore();
  const records = useSyncExternalStore(
    (cb) => store.subscribeSignal(node_id, signal, cb),
    () => store.getSignalBuffer(node_id, signal)?.records ?? EMPTY_RECORDS,
  );
  const buffer = store.getSignalBuffer(node_id, signal);
  return { records, buffer };
}

/**
 * Like {@link useSignal} but accepts a nullable source so callers don't have
 * to wrap conditional subscriptions in extra components. When ``src`` is null
 * the hook returns ``EMPTY_RECORDS`` and doesn't open a server subscription.
 */
export function useOptionalSignal(
  src: { node_id: string; signal: string } | null,
): ValueRecord[] {
  const store = useRunStore();
  const node_id = src?.node_id ?? "";
  const signal = src?.signal ?? "";
  const subscribe = useCallback(
    (cb: () => void) => {
      if (!node_id || !signal) return () => {};
      return store.subscribeSignal(node_id, signal, cb);
    },
    [store, node_id, signal],
  );
  const getSnapshot = useCallback(
    () =>
      !node_id || !signal
        ? EMPTY_RECORDS
        : store.getSignalBuffer(node_id, signal)?.records ?? EMPTY_RECORDS,
    [store, node_id, signal],
  );
  return useSyncExternalStore(subscribe, getSnapshot);
}

const EMPTY_RECORDS: ValueRecord[] = [];

/**
 * One-shot history pull. Returns a function that fetches an older batch
 * before the given event id, useful for "load more" scroll-up.
 */
export function useHistoryLoader(
  node_id: string,
  signal: string,
): (before_id: number | null, limit?: number) => Promise<ValueRecord[]> {
  const store = useRunStore();
  const ref = useRef({ store, node_id, signal });
  ref.current = { store, node_id, signal };
  return useMemo(
    () =>
      (before_id: number | null, limit = 50) =>
        ref.current.store.requestHistory(
          ref.current.node_id,
          ref.current.signal,
          before_id,
          limit,
        ),
    [],
  );
}
