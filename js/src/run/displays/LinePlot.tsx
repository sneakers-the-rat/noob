/**
 * Line plot via vega-embed.
 *
 * Behavior:
 *  - If only ``yRecords`` is supplied: x is the event id (monotonically
 *    increasing integer), y is the numeric value.
 *  - If ``xRecords`` is also supplied: pair records by epoch and plot y vs x
 *    (Lissajous mode).
 *
 * Style is oscilloscope-flavored: the line is a ``trail`` mark whose color,
 * width and opacity all decay with age so the most recent stretch is bright
 * and fat and old segments fade into the background.
 *
 * The vega view is created exactly once per mount. Data is streamed via
 * ``view.change()``; when the structural mode flips (X gets connected /
 * disconnected) we clear the existing data and reset the cursor so the
 * trail restarts cleanly in the new coordinate system.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import embed, { type Result, type VisualizationSpec } from "vega-embed";
import type { ValueRecord } from "../protocol.ts";
import { numericFromEnvelope } from "./decode.ts";

const MAX_POINTS = 500;

interface Point {
  id: number;
  x: number;
  y: number;
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
    mark: { type: "trail", interpolate: "linear" },
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
      size: {
        field: "id",
        type: "quantitative",
        scale: { type: "linear", range: [0, 4] },
        legend: null,
      },
      color: {
        field: "id",
        type: "quantitative",
        scale: { type: "linear", range: ["#1a4a1a", "#9bff7a"] },
        legend: null,
      },
      opacity: {
        field: "id",
        type: "quantitative",
        scale: { type: "linear", range: [0.05, 1] },
        legend: null,
      },
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

export function LinePlot({
  yRecords,
  xRecords,
}: {
  yRecords: ValueRecord[];
  xRecords?: ValueRecord[];
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<Result["view"] | null>(null);
  const lastIdRef = useRef<number>(Number.NEGATIVE_INFINITY);
  const [ready, setReady] = useState(false);

  const isLissajous = (xRecords?.length ?? 0) > 0;

  // Embed once. Container sizing handles resize via vega's own ResizeObserver.
  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;
    embed(containerRef.current, buildSpec(), {
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
      })
      .catch((err) => {
        // surface vega-embed errors instead of swallowing them silently
        console.error("[noob] vega embed failed", err);
      });
    return () => {
      cancelled = true;
      viewRef.current?.finalize();
      viewRef.current = null;
      setReady(false);
    };
  }, []);

  // When the structural mode flips, wipe the view and reset the cursor so
  // the trail doesn't draw across the coordinate-system change.
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

  // Compute points for the current records / mode.
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
    for (const r of xRecords ?? []) xByEpoch.set(epochKey(r.epoch), r);
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

  // Insert newly-seen points, evict anything older than MAX_POINTS behind
  // the newest. Runs when records change OR when the view first becomes
  // ready (so initial data isn't dropped during the async embed).
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
      className="runner-display-lineplot"
      style={{ width: "100%", height: "100%" }}
    />
  );
}

export function canRenderLine(rec: ValueRecord | undefined): boolean {
  return rec ? numericFromEnvelope(rec) !== null : false;
}
