/**
 * Unit tests for the LineDisplay component in isolation.
 *
 * Mounted directly (no RunProvider needed — it takes records as props) so
 * the tests focus on the data path: synthetic ValueRecords → vega view.
 *
 * We hook the registered-view callback the component exposes when
 * `window.__noobTest._registerView` is set; production-side that callback
 * is a no-op (only invoked if the global is present).
 */

import { afterEach, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { page, commands } from "vitest/browser";
import { render, cleanup } from "vitest-browser-react";
import { createElement } from "react";
import type { Result } from "vega-embed";
import LineDisplay from "../../src/nodes/viewer/displays/Line.tsx";
import type { ValueRecord } from "../../src/run/protocol.ts";

declare global {
  // eslint-disable-next-line no-var
  var __noobTest:
    | { _registerView?: (v: Result["view"]) => void; lastView?: Result["view"] }
    | undefined;
}

function makeNumericRec(id: number, value: number, node = "sine_x"): ValueRecord {
  return {
    id,
    node_id: node,
    signal: "value",
    epoch: [["tube", id]],
    timestamp: new Date(id * 1000).toISOString(),
    value: { kind: "json", data: value },
  };
}

function mountLine(yRecs: ValueRecord[], xRecs: ValueRecord[] = []) {
  return render(
    createElement(
      "div",
      { style: { width: "320px", height: "200px", position: "relative" } },
      createElement(LineDisplay, {
        records: { in: yRecs, x: xRecs },
      }),
    ),
  );
}

async function waitForView(timeout = 4000): Promise<Result["view"]> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (globalThis.__noobTest?.lastView) return globalThis.__noobTest.lastView;
    await new Promise((r) => setTimeout(r, 30));
  }
  throw new Error("vega view never registered");
}

let SCREENSHOT = false;
beforeAll(async () => {
  SCREENSHOT = await commands.screenshotEnabled();
});

beforeEach(() => {
  globalThis.__noobTest = {
    _registerView(view: Result["view"]) {
      globalThis.__noobTest!.lastView = view;
    },
  };
});

async function snap(name: string): Promise<void> {
  if (!SCREENSHOT) return;
  await page.screenshot({ path: `./screenshots/${name}.png` });
}

afterEach(async () => {
  await cleanup();
  delete globalThis.__noobTest;
});

describe("LineDisplay — vega data path", () => {
  test("renders all y-only records into the trail data", async () => {
    const recs = Array.from({ length: 30 }, (_, i) =>
      makeNumericRec(i + 1, Math.sin(i / 3)),
    );
    mountLine(recs);
    const view = await waitForView();
    // give vega a moment to process the inserted points
    await new Promise((r) => setTimeout(r, 200));
    const data = view.data("values") as unknown[];
    expect(data.length).toBe(30);
  });

  test("trail leaves visible pixels on the canvas", async () => {
    const recs = Array.from({ length: 30 }, (_, i) =>
      makeNumericRec(i + 1, Math.sin(i / 3)),
    );
    mountLine(recs);
    await waitForView();
    await new Promise((r) => setTimeout(r, 400));

    // Sample the canvas: count pixels that aren't pure background. If vega
    // renders the trail successfully there will be a handful of phosphor
    // pixels; if the trail collapsed to 0 width / hidden opacity, the
    // canvas will be entirely transparent or background-only.
    const canvas = document.querySelector(".viewer-line-plot canvas") as
      | HTMLCanvasElement
      | null;
    expect(canvas).toBeTruthy();
    const ctx = canvas!.getContext("2d");
    expect(ctx).toBeTruthy();
    const img = ctx!.getImageData(0, 0, canvas!.width, canvas!.height);
    let visiblePixels = 0;
    for (let i = 0; i < img.data.length; i += 4) {
      const a = img.data[i + 3];
      if (a > 8) visiblePixels++;
    }
    expect(visiblePixels).toBeGreaterThan(50);
    await snap("unit-trail-y-only");
  });

  test("plot is empty when no records arrive", async () => {
    mountLine([]);
    const view = await waitForView();
    await new Promise((r) => setTimeout(r, 200));
    expect((view.data("values") as unknown[]).length).toBe(0);
  });

  test("Lissajous mode pairs x/y records by epoch", async () => {
    const ys = Array.from({ length: 10 }, (_, i) =>
      makeNumericRec(i + 1, Math.sin(i / 2), "sine_y"),
    );
    const xs = Array.from({ length: 10 }, (_, i) =>
      makeNumericRec(i + 1, Math.cos(i / 2), "sine_x"),
    );
    mountLine(ys, xs);
    const view = await waitForView();
    await new Promise((r) => setTimeout(r, 200));
    const data = view.data("values") as { id: number; x: number; y: number }[];
    expect(data.length).toBe(10);
    // x came from xs, y from ys, paired by id (epoch) — verify a sample point
    expect(data[0].x).toBeCloseTo(Math.cos(0));
    expect(data[0].y).toBeCloseTo(Math.sin(0));
  });

  test("skips records without numeric values", async () => {
    const recs: ValueRecord[] = [
      makeNumericRec(1, 0.5),
      // non-numeric — should be filtered
      {
        ...makeNumericRec(2, 0),
        value: { kind: "repr", data: "<not numeric>" },
      },
      makeNumericRec(3, 0.7),
    ];
    mountLine(recs);
    const view = await waitForView();
    await new Promise((r) => setTimeout(r, 200));
    expect((view.data("values") as unknown[]).length).toBe(2);
  });
});
