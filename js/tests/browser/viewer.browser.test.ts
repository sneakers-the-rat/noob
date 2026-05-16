/**
 * Browser tests for the viewer node — drives the production View component
 * through real UI gestures (right-click, click, drag) so we exercise the
 * full xyflow + RunStore + WebSocket path.
 *
 * Mock noob server runs as a Vite plugin in the same Node process, so
 * `commands.mockPushNumeric(...)` reaches the browser over a real WebSocket.
 *
 * Set `SCREENSHOT=1` to dump a PNG per test for visual review.
 */

import { afterEach, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { page, commands, userEvent } from "vitest/browser";
import { render, cleanup } from "vitest-browser-react";
import { createElement } from "react";
import { ReactFlowProvider } from "@xyflow/react";
import type { Result } from "vega-embed";
import View from "../../src/pages/view.tsx";
// CSS load order MUST match production (main.tsx): xyflow's default styles
// first, then our overrides. Reversing the order lets xyflow's CSS win the
// cascade and hides the real bug that production users see.
import "@xyflow/react/dist/style.css";
import "../../src/css/index.css";

import { SAMPLE_TUBE_SPEC } from "./sample-spec.ts";

declare global {
   
  var __noobTest:
    | { _registerView?: (v: Result["view"]) => void; lastView?: Result["view"] }
    | undefined;
}

// Enabled via `npm run test:browser:screenshots` (sets SCREENSHOT=1). The
// env var lives in Node, so we read it through a vitest browser command.
let SCREENSHOT = false;
const TUBE_ID = "test-tube";

async function snap(name: string): Promise<void> {
  if (!SCREENSHOT) return;
  // page.screenshot resolves paths relative to the test file; ./screenshots
  // lands in tests/browser/screenshots/ (which global-setup pre-creates).
  await page.screenshot({ path: `./screenshots/${name}.png` });
}

/** Drive the production "init" + "start" buttons so the mock-server status
 * transitions through the same states the real GUI would see. The mock
 * pushes values regardless, but the test mirrors the real interactive flow
 * to catch regressions in the toolbar wiring. */
async function initAndStart(): Promise<void> {
  await page.getByRole("button", { name: /^init$/i }).click();
  await page.getByRole("button", { name: /^start$/i }).click();
}

// Big enough that the auto-layouted tube nodes (left side) don't overlap a
// viewer dropped on the right side. Vitest's iframe doesn't auto-scroll so
// everything we need to interact with has to fit on screen.
const SCENE_WIDTH = 1600;
const SCENE_HEIGHT = 900;

function mountView() {
  return render(
    createElement(
      "div",
      {
        style: {
          width: `${SCENE_WIDTH}px`,
          height: `${SCENE_HEIGHT}px`,
          position: "relative",
        },
      },
      createElement(
        ReactFlowProvider,
        null,
        createElement(View, { tube_id: TUBE_ID, color: "dark" }),
      ),
    ),
  );
}

async function waitForLayout(): Promise<void> {
  // We're done when (a) the structural nodes from the /spec WS have rendered
  // (so sine_x has a DOM element with measured bounds) AND (b) ELK has run +
  // fitView has applied a non-identity transform on the xyflow viewport.
  const start = Date.now();
  while (Date.now() - start < 8000) {
    const sineX = document.querySelector(
      '.react-flow__node[data-id="sine_x"]',
    );
    const viewport = document.querySelector(
      ".react-flow__viewport",
    );
    if (sineX && viewport) {
      const r = sineX.getBoundingClientRect();
      const t = viewport.style.transform || "";
      const scaleMatch = /scale\(([^)]+)\)/.exec(t);
      const scale = scaleMatch ? parseFloat(scaleMatch[1]) : 1;
      // post-fitView state: a non-zero scale (typically < 1 with several
      // nodes) and a non-pathological screen position for sine_x.
      if (
        r.width > 10 &&
        r.width < 400 &&
        scale > 0 &&
        scale !== 1 &&
        r.x >= 0 &&
        r.x < 2000
      ) {
        // small settle so fitView's commit has fully flushed
        await new Promise((r) => setTimeout(r, 100));
        return;
      }
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

async function addViewerAt(x: number, y: number): Promise<void> {
  await waitForLayout();
  await new Promise((r) => setTimeout(r, 300));
  const pane = page.bySelector("body");
  await pane.click({ button: "right", position: { x, y } });
  await new Promise((r) => setTimeout(r, 150));
  // Click directly via DOM rather than through playwright's stability-
  // retry pipeline — the menu vanishes the instant we click it, which
  // playwright reads as "element not stable" and retries forever.
  const start = Date.now();
  while (Date.now() - start < 5000) {
    const btn = Array.from(document.querySelectorAll("button")).find((b) =>
      /add viewer node/i.test(b.textContent ?? ""),
    );
    if (btn) {
      btn.click();
      return;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("add viewer node button never appeared");
}

async function waitFor<T>(
  pred: () => Promise<T>,
  predicate: (v: T) => boolean,
  { timeout = 4000, every = 50 } = {},
): Promise<T> {
  const deadline = Date.now() + timeout;
  let last: T = await pred();
  while (!predicate(last) && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, every));
    last = await pred();
  }
  return last;
}

beforeAll(async () => {
  SCREENSHOT = await commands.screenshotEnabled();
});

beforeEach(async () => {
  await commands.mockReset();
  await commands.mockSetSpec(SAMPLE_TUBE_SPEC);
  await page.viewport(SCENE_WIDTH + 60, SCENE_HEIGHT + 80);
  // LineDisplay calls window.__noobTest?._registerView when vega embeds.
  // Set up the hook before mounting so tests can grab the live view.
  globalThis.__noobTest = {
    _registerView(view: Result["view"]) {
      globalThis.__noobTest!.lastView = view;
    },
  };
});

afterEach(async () => {
  await cleanup();
  delete globalThis.__noobTest;
});

async function waitForView(timeout = 4000): Promise<Result["view"]> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (globalThis.__noobTest?.lastView) return globalThis.__noobTest.lastView;
    await new Promise((r) => setTimeout(r, 30));
  }
  throw new Error("vega view never registered");
}

// Drop the viewer in the right half of the canvas, but with enough room on
// the right that the bottom-right resize handle stays in the viewport.
const VIEWER_X = 1100;
const VIEWER_Y = 350;

describe("viewer node — rendering + mode selection", () => {
  test("renders the tube graph from the /spec WS", async () => {
    mountView();

    await expect.element(page.getByText("test-tube").first()).toBeVisible();

    await snap("page-loaded");
  });

  test("right-click → add viewer node shows NO SIGNAL placeholder", async () => {
    mountView();

    await expect.element(page.getByText("test-tube").first()).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);

    await expect.element(page.getByText("NO SIGNAL")).toBeVisible();

    await snap("viewer-no-signal");
  });

  test("lINE mode exposes labeled x and y handles", async () => {
    mountView();

    await expect.element(page.getByText("test-tube").first()).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    const viewer = page.bySelector(".react-flow__node-viewer");

    await expect.element(viewer.getByText("y", { exact: true })).toBeVisible();
    await expect.element(viewer.getByText("x", { exact: true })).toBeVisible();

    await snap("viewer-line-handles");
  });
});

describe("viewer node — edge connection", () => {
  test("dragging a source handle to viewer.in subscribes to that signal", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    const source = page.bySelector(
      '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
    );
    const target = page.bySelector(
      '.react-flow__node-viewer [data-handleid$=".in"]',
    );
    await userEvent.dragAndDrop(source, target);

    // The mock-server records subscribe_values; subscribing to sine_x.value
    // implies the edge connected, which implies handle positions were
    // accurate enough to land the drop on the right handle.
    const n = await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );

    expect(n).toBe(1);

    await snap("viewer-edge-connected");
  });

  test("right-clicking an edge offers a delete option that unwires it", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    await userEvent.dragAndDrop(
      page.bySelector(
        '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
      ),
      page.bySelector('.react-flow__node-viewer [data-handleid$=".in"]'),
    );
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );

    // Right-click the new viewer edge — there's exactly one viewer edge
    // (`viewer-edge-*` id) in the graph at this point.
    const edge = page.bySelector(
      ".react-flow__edge[data-id^='viewer-edge-']",
    );
    await edge.click({ button: "right" });
    await page.getByRole("button", { name: /delete edge/i }).click();

    // After delete, the subscription should also disappear: useOptionalSignal
    // unsubscribes when the edge providing the source vanishes.
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 0,
    );

    await expect(commands.mockSubscriberCount("sine_x", "value")).resolves.toBe(0);

    // The viewer is back to NO SIGNAL.
    await expect.element(page.getByText("NO SIGNAL")).toBeVisible();

    await snap("viewer-edge-deleted");
  });

  test("subscriber count survives moving the viewer node after connecting", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    const source = page.bySelector(
      '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
    );
    const target = page.bySelector(
      '.react-flow__node-viewer [data-handleid$=".in"]',
    );
    await userEvent.dragAndDrop(source, target);
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );

    // Move the viewer by dragging its header (or any non-handle area)
    const viewer = page.bySelector(".react-flow__node-viewer");
    await userEvent.dragAndDrop(
      viewer,
      page.bySelector(".react-flow__pane"),
      { targetPosition: { x: 1000, y: 500 } },
    );

    // Subscription should still be intact — moving the node doesn't unwire.
    await expect(commands.mockSubscriberCount("sine_x", "value")).resolves.toBe(1);

    await snap("viewer-after-move");
  });
});

describe("viewer node — line plot data flow", () => {
  test("linePlot renders points when y-only signal arrives", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    await userEvent.dragAndDrop(
      page.bySelector(
        '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
      ),
      page.bySelector('.react-flow__node-viewer [data-handleid$=".in"]'),
    );
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );

    // Mirror the real interactive flow — the user has to init + start the
    // tube for events to flow.
    await initAndStart();

    const view = await waitForView();
    for (let i = 0; i < 60; i++) {
      await commands.mockPushNumeric("sine_x", "value", Math.sin(i / 4));
    }
    // give React + vega a beat to flush
    await new Promise((r) => setTimeout(r, 400));

    // Integration assertion: the values pushed via the mock should make it
    // through the WS → store → LineDisplay → vega view pipeline.
    const data = view.data("values") as unknown[];

    expect(data.length).toBeGreaterThan(0);

    await snap("viewer-line-y-only");
  });

  test(
    "each newly-added viewer lands at the click position (not below it)",
    async () => {
      mountView();

      await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

      await waitForLayout();
      await new Promise((r) => setTimeout(r, 400));

      // Click at three distinct screen positions, well clear of any tube
      // node, and record where each new viewer actually lands.
      // Five distinct, non-overlapping click positions so we can clearly
       // detect cumulative drift of N*x where x might be small. Each
       // viewer is in a different quadrant.
      const clicks = [
        { x: 1380, y: 180 },
        { x: 180, y: 180 },
        { x: 1380, y: 700 },
        { x: 180, y: 700 },
        { x: 780, y: 440 },
      ];
      const deltas: { x: number; click: number; viewer: number; dy: number }[] = [];

      for (let i = 0; i < clicks.length; i++) {
        const before = document.querySelectorAll(
          ".react-flow__node-viewer",
        ).length;
        await addViewerAt(clicks[i].x, clicks[i].y);
        const start = Date.now();
        while (
          Date.now() - start < 2000 &&
          document.querySelectorAll(".react-flow__node-viewer").length === before
        ) {
          await new Promise((r) => setTimeout(r, 25));
        }
        await new Promise((r) => setTimeout(r, 200));

        const viewers = Array.from(
          document.querySelectorAll(".react-flow__node-viewer"),
        );

        expect(viewers).toHaveLength(i + 1);

        const newest = viewers[viewers.length - 1];
        const rect = newest.getBoundingClientRect();
        // xyflow renders a node at its `position` (top-left of the node), so
        // the chassis's top-left should land on the click position.
        deltas.push({
          x: clicks[i].x,
          click: clicks[i].y,
          viewer: Math.round(rect.top),
          dy: Math.round(rect.top - clicks[i].y),
        });
      }

      console.log("[placement] deltas:", JSON.stringify(deltas));
      // Cumulative-offset regression: each new viewer should land at the
      // click, no matter how many viewers are already on the canvas. A
      // constant delta is fine (sub-pixel rounding from screen ↔ flow
      // round-trip), but `dy_n - dy_0` must NOT grow with n.
      const dy0 = deltas[0].dy;
      for (let i = 1; i < deltas.length; i++) {
        expect(
          Math.abs(deltas[i].dy - dy0),
          `viewer #${i + 1} drifted relative to #1: dy0=${dy0}, dy${i}=${
            deltas[i].dy
          } (all: ${JSON.stringify(deltas)})`,
        ).toBeLessThan(8);
      }

      // First viewer should land approximately where we clicked.
      expect(
        Math.abs(dy0),
        `first viewer should land near click; dy0=${dy0}`,
      ).toBeLessThan(30);

      await snap("three-viewers-placement");
    },
  );

  // Flaky in the test harness (layout/viewport sync) but the cumulative-
  // offset behavior it was meant to catch is covered by the placement test
  // above. Skipping until we can stabilize the second-add-then-drag path.
  test(
    "a second viewer's handle still receives edges at the right position",
    async () => {
      mountView();

      await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();
      await expect.element(page.bySelector('[data-id="gradient"]')).toBeVisible();

      // 1st viewer: line plot from sine_x.value
      await addViewerAt(VIEWER_X, VIEWER_Y);
      await page.getByTestId("mode-line").click();
      // Wait for layout / xyflow to settle before dragging — playwright's
      // "stable element" check is sensitive to small position changes.
      await new Promise((r) => setTimeout(r, 250));
      await userEvent.dragAndDrop(
        page.bySelector(
          '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
        ),
        page.bySelector('.react-flow__node-viewer [data-handleid$=".in"]'),
      );
      await waitFor(
        () => commands.mockSubscriberCount("sine_x", "value"),
        (v) => v === 1,
      );

      // 2nd viewer: image from gradient.frame
      // Drop the 2nd viewer well below the 1st but inside the visible
       // viewport. The first viewer in LINE mode is ~200px tall at fitView
       // zoom; +250px from VIEWER_Y clears it without falling off-screen.
      await addViewerAt(VIEWER_X + 100, VIEWER_Y + 250);
      await new Promise((r) => setTimeout(r, 200));

      const viewers = Array.from(
        document.querySelectorAll(".react-flow__node-viewer"),
      );

      expect(viewers, "expected two viewers on the canvas").toHaveLength(2);

      const secondViewerEl = viewers[1];
      // xyflow's data-id on the wrapping node element is what we'll match.
      const secondId = secondViewerEl
        .closest("[data-id]")!
        .getAttribute("data-id")!;
      // Switch only THAT viewer to IMG mode (scoping the click to its DOM).
      (secondViewerEl.querySelector(
        '[data-testid="mode-image"]',
      ) as HTMLElement).click();
      await new Promise((r) => setTimeout(r, 100));

      await userEvent.dragAndDrop(
        page.bySelector(
          '.react-flow__node[data-id="gradient"] [data-handleid="gradient.signals.frame"]',
        ),
        page.bySelector(
          `.react-flow__node[data-id="${secondId}"] [data-handleid="${secondId}.in"]`,
        ),
      );

      const count = await waitFor(
        () => commands.mockSubscriberCount("gradient", "frame"),
        (v) => v === 1,
      );

      expect(
        count,
        "second viewer's edge should land on its in handle",
      ).toBe(1);

      // Now the visual check — the rendered edge endpoint should be at the
      // 2nd viewer's "in" handle, not floating somewhere above it. Reading
      // the SVG path's end point and comparing to the handle's screen
      // position catches the "edge points to a random place above the
      // handle" symptom even when the React state has the connection right.
      await new Promise((r) => setTimeout(r, 200));
      const handleEl = secondViewerEl.querySelector(
        '[data-handleid$=".in"]',
      );

      expect(handleEl, "target handle should exist in DOM").toBe(true);

      const handleRect = handleEl!.getBoundingClientRect();
      const handleCx = handleRect.x + handleRect.width / 2;
      const handleCy = handleRect.y + handleRect.height / 2;

      // Find the gradient → second-viewer edge in the SVG layer.
      const edgePath = document.querySelector(
        `.react-flow__edge[data-id*="${secondId}"] path.react-flow__edge-path`,
      );

      expect(edgePath, "edge SVG path should be rendered").toBe(true);

      const total = edgePath!.getTotalLength();
      const endLocal = edgePath!.getPointAtLength(total);
      // Apply the SVG's cumulative on-screen transform (pan + zoom from
      // xyflow's viewport group) to convert local SVG → screen coords.
      const ctm = edgePath!.getScreenCTM();

      expect(ctm, "edge path should have a screen CTM").toBe(true);

      const screenPt = endLocal.matrixTransform(ctm);
      const endX = screenPt.x;
      const endY = screenPt.y;

      // Allow a small tolerance for sub-pixel rendering / rounding.
      expect(
        Math.abs(endX - handleCx),
        `edge endpoint x (${endX}) should match handle center x (${handleCx})`,
      ).toBeLessThan(8);
      expect(
        Math.abs(endY - handleCy),
        `edge endpoint y (${endY}) should match handle center y (${handleCy})`,
      ).toBeLessThan(8);

      await snap("two-viewers-edge-connected");
    },
  );

  test("linePlot canvas resizes when the viewer node is resized", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();
    await userEvent.dragAndDrop(
      page.bySelector(
        '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
      ),
      page.bySelector('.react-flow__node-viewer [data-handleid$=".in"]'),
    );
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );
    await initAndStart();

    const view = await waitForView();
    for (let i = 0; i < 30; i++) {
      await commands.mockPushNumeric("sine_x", "value", Math.sin(i / 4));
    }
    await new Promise((r) => setTimeout(r, 300));

    function canvasDims(): { w: number; h: number } | null {
      const c = document.querySelector(
        ".react-flow__node-viewer canvas",
      );
      if (!c) return null;
      // clientWidth/Height reflect the CSS-rendered size, which is what
      // vega-embed's autoResize updates when the container changes.
      return { w: c.clientWidth, h: c.clientHeight };
    }

    const before = canvasDims();

    expect(before).not.toBeNull();

    const handle = page.bySelector(
      ".react-flow__node-viewer .react-flow__resize-control.handle.bottom.right",
    );

    await expect.element(handle).toBeVisible();

    // Drag the corner handle ~250x200 px down-right to grow the chassis.
    // userEvent.dragAndDrop with a sourcePosition + a target locator at a
    // far-away location: the dragger interpolates a real pointer path.
    const handleEl = document.querySelector(
      ".react-flow__node-viewer .react-flow__resize-control.handle.bottom.right",
    ) as HTMLElement;

    expect(handleEl).toBe(true);

    const hBox = handleEl.getBoundingClientRect();
    const startX = hBox.left + hBox.width / 2;
    const startY = hBox.top + hBox.height / 2;
    await userEvent.dragAndDrop(
      handle,
      page.bySelector(".react-flow__pane"),
      {
        sourcePosition: { x: hBox.width / 2, y: hBox.height / 2 },
        targetPosition: { x: startX + 300, y: startY + 220 },
      },
    );
    await new Promise((r) => setTimeout(r, 800));

    // Sanity check: the chassis itself grew
    const chassisAfter = document
      .querySelector(".react-flow__node-viewer")!
      .getBoundingClientRect();

    expect(chassisAfter.width).toBeGreaterThan(250);

    const after = canvasDims();

    expect(after).not.toBeNull();
    expect(after!.w, "canvas width should track chassis").toBeGreaterThan(
      before!.w,
    );
    expect(after!.h, "canvas height should track chassis").toBeGreaterThan(
      before!.h,
    );

    // vega keeps the same data after resize
    expect((view.data("values") as unknown[]).length).toBeGreaterThan(0);

    await snap("viewer-resized");
  });

  test("linePlot pairs x/y when both handles are connected", async () => {
    mountView();

    await expect.element(page.bySelector('[data-id="sine_x"]')).toBeVisible();

    await addViewerAt(VIEWER_X, VIEWER_Y);
    await page.getByTestId("mode-line").click();

    await userEvent.dragAndDrop(
      page.bySelector(
        '.react-flow__node[data-id="sine_y"] [data-handleid="sine_y.signals.value"]',
      ),
      page.bySelector('.react-flow__node-viewer [data-handleid$=".in"]'),
    );
    await userEvent.dragAndDrop(
      page.bySelector(
        '.react-flow__node[data-id="sine_x"] [data-handleid="sine_x.signals.value"]',
      ),
      page.bySelector('.react-flow__node-viewer [data-handleid$=".x"]'),
    );
    await waitFor(
      () => commands.mockSubscriberCount("sine_x", "value"),
      (v) => v === 1,
    );
    await waitFor(
      () => commands.mockSubscriberCount("sine_y", "value"),
      (v) => v === 1,
    );

    await initAndStart();

    const view = await waitForView();
    for (let i = 0; i < 80; i++) {
      await commands.mockPushNumeric("sine_x", "value", Math.cos(i / 5), i);
      await commands.mockPushNumeric("sine_y", "value", Math.sin(i / 3), i);
    }
    await new Promise((r) => setTimeout(r, 400));

    const data = view.data("values") as unknown[];

    expect(data.length).toBeGreaterThan(0);

    await snap("viewer-lissajous");
  });
});
