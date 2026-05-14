/**
 * Line display — oscilloscope-style trail via vega-embed.
 *
 * Two handles. The ``in`` handle is required (treated as the y series).
 * The ``x`` handle is optional:
 *  - If unconnected: x is the event id (monotonically increasing integer).
 *  - If connected: pair x and y records by epoch and plot y vs x (Lissajous).
 *
 * The trail's color, width and opacity all decay with age so newer
 * segments are bright and fat and old segments fade.
 *
 * The vega view is created once per mount. When the structural mode flips
 * (x getting connected or disconnected) we wipe the data and reset the
 * cursor so the trail doesn't draw across the coordinate-system change.
 *
 * In test mode (`window.__noobTest`), the live view is published to the
 * harness so tests can `view.data("values")` to assert the data made it in.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import embed, { type Result, type VisualizationSpec } from "vega-embed";
import { numericFromEnvelope } from "../../../run/displays/decode.ts";
import type { ValueRecord } from "../../../run/protocol.ts";
import type { DisplayHandle, DisplayProps } from "./types.ts";

const MAX_POINTS = 500;

interface Point {
  id: number;
  x: number;
  y: number;
}

export const handles: DisplayHandle[] = [
  { id: "in", label: "y" },
  { id: "x", label: "x" },
];
export const shortLabel = "LINE";
export const title = "line plot";

export function canRender(rec: ValueRecord | undefined): boolean {
  return rec ? numericFromEnvelope(rec) !== null : false;
}

function epochKey(epoch: [string, number][]): string {
  return epoch.map((s) => `${s[0]}:${s[1]}`).join("/");
}

function buildSpec(): VisualizationSpec {
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v6.json",
    width: "container",
    height: "container",
    autosize: { type: "fit", contains: "padding", resize: true },
    background: "transparent",
    padding: 6,
    data: { name: "values", values: [] as Point[] },
    mark: {
      type: "line",
      stroke: "#9bff7a",
      strokeWidth: 2,
      interpolate: "linear",
    },
    encoding: {
      x: {
        field: "x",
        type: "quantitative",
        axis: { title: null, labelFontSize: 8 },
      },
      y: {
        field: "y",
        type: "quantitative",
        axis: { title: null, labelFontSize: 8 },
      },
      order: { field: "id", type: "quantitative" },
    },
    config: {
      axis: {
        labelColor: "#67e8f9",
        domainColor: "#1e3a52",
        tickColor: "#1e3a52",
      },
      view: { stroke: "transparent" },
    },
  };
}

export default function LineDisplay({ records }: DisplayProps) {
  const yRecords = records.in ?? [];
  const xRecords = records.x ?? [];

  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<Result["view"] | null>(null);
  const lastIdRef = useRef<number>(Number.NEGATIVE_INFINITY);
  const [ready, setReady] = useState(false);

  const isLissajous = xRecords.length > 0;

  // Embed once on mount. vega-embed has its own autoResize when both
  // dimensions are "container", but we observe the container ourselves and
  // call view.resize() — vega's built-in observer didn't fire reliably for
  // our flexbox-sized container during xyflow NodeResizer drags.
  useEffect(() => {
    if (!containerRef.current) return;
    const container = containerRef.current;
    let cancelled = false;
    let observer: ResizeObserver | null = null;
    embed(container, buildSpec(), {
      actions: false,
      renderer: "canvas",
    })
      .then((result) => {
        if (cancelled) {
          result.finalize();
          return;
        }
        viewRef.current = result.view;
        setReady(true);
        observer = new ResizeObserver(() => {
          result.view
            .signal("width", container.clientWidth)
            .signal("height", container.clientHeight)
            .resize()
            .runAsync();
        });
        observer.observe(container);
        const w = window as unknown as { __noobTest?: { _registerView?: (v: Result["view"]) => void } };
        if (w.__noobTest?._registerView) w.__noobTest._registerView(result.view);
      })
      .catch((err) => {
        console.error("[noob] vega embed failed", err);
      });
    return () => {
      cancelled = true;
      observer?.disconnect();
      viewRef.current?.finalize();
      viewRef.current = null;
      setReady(false);
    };
  }, []);

  // Wipe data when the coordinate system flips, so a y-vs-id trail doesn't
  // continue into a y-vs-x Lissajous (or vice versa).
  const prevModeRef = useRef(isLissajous);
  useEffect(() => {
    if (!ready) return;
    if (prevModeRef.current === isLissajous) return;
    const view = viewRef.current;
    if (!view) return;
    lastIdRef.current = Number.NEGATIVE_INFINITY;
    view
      .change("values", view.changeset().remove(() => true))
      .runAsync();
    prevModeRef.current = isLissajous;
  }, [ready, isLissajous]);

  const points = useMemo<Point[]>(() => {
    if (!isLissajous) {
      const out: Point[] = [];
      for (const r of yRecords) {
        const v = numericFromEnvelope(r);
        if (v === null) continue;
        out.push({ id: r.id, x: r.id, y: v });
      }
      return out;
    }
    const xByEpoch = new Map<string, ValueRecord>();
    for (const r of xRecords) xByEpoch.set(epochKey(r.epoch), r);
    const out: Point[] = [];
    for (const yr of yRecords) {
      const xr = xByEpoch.get(epochKey(yr.epoch));
      if (!xr) continue;
      const xv = numericFromEnvelope(xr);
      const yv = numericFromEnvelope(yr);
      if (xv === null || yv === null) continue;
      out.push({ id: yr.id, x: xv, y: yv });
    }
    return out;
  }, [yRecords, xRecords, isLissajous]);

  useEffect(() => {
    if (!ready) return;
    const view = viewRef.current;
    if (!view) return;
    const newOnes: Point[] = [];
    for (const p of points) {
      if (p.id <= lastIdRef.current) continue;
      newOnes.push(p);
    }
    if (newOnes.length === 0) return;
    lastIdRef.current = newOnes[newOnes.length - 1].id;
    const cutoffId = lastIdRef.current - MAX_POINTS;
    view
      .change(
        "values",
        view
          .changeset()
          .insert(newOnes)
          .remove((d: Point) => d.id < cutoffId),
      )
      .runAsync();
  }, [ready, points]);

  return (
    <div
      ref={containerRef}
      className="viewer-line-plot"
      data-testid="line-plot-container"
      style={{ width: "100%", height: "100%" }}
    />
  );
}
