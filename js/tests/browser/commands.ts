/**
 * Vitest browser-mode commands — run in the Node process and let the
 * browser-side test body drive the in-process MockServer that the page
 * is connected to.
 *
 * Register these in `vitest.config.ts` under `test.browser.commands`,
 * then import-type them with module augmentation so the test body can
 * call `commands.mockSetSpec(...)` etc. with proper typing.
 */

import type { BrowserCommand } from "vitest/node";
import { getMockServer, type ValueEnvelope } from "./mock-server.ts";

export const mockReset: BrowserCommand<[]> = () => {
  getMockServer().reset();
};

export const mockSetSpec: BrowserCommand<[unknown]> = (_ctx, spec) => {
  getMockServer().setSpec(spec);
};

export const mockSetStatus: BrowserCommand<
  [Partial<{ state: string; tube_id: string | null; error: string | null }>]
> = (_ctx, status) => {
  getMockServer().setStatus(
    status as Partial<{
      state:
        | "uninitialized"
        | "initialized"
        | "running"
        | "stopped"
        | "error";
      tube_id: string | null;
      error: string | null;
    }>,
  );
};

export const mockPushEvent: BrowserCommand<
  [string, string, number | undefined]
> = (_ctx, node_id, signal, epoch) => {
  return getMockServer().pushEvent(node_id, signal, { epoch });
};

export const mockPushValue: BrowserCommand<
  [string, string, ValueEnvelope, { epoch?: number; eventId?: number }]
> = (_ctx, node_id, signal, value, opts) => {
  return getMockServer().pushValue(node_id, signal, value, opts);
};

export const mockPushNumeric: BrowserCommand<
  [string, string, number, number | undefined]
> = (_ctx, node_id, signal, value, epoch) => {
  return getMockServer().pushNumeric(node_id, signal, value, { epoch });
};

export const mockSubscriberCount: BrowserCommand<[string, string]> = (
  _ctx,
  node_id,
  signal,
) => {
  return getMockServer().signalSubscriberCount(node_id, signal);
};

export const mockEventClientCount: BrowserCommand<[]> = () => {
  return getMockServer().eventClientCount();
};

export const sleep: BrowserCommand<[number]> = (_ctx, ms) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Server-side check: was SCREENSHOT=1 passed to the vitest CLI? */
export const screenshotEnabled: BrowserCommand<[]> = () => {
  return !!process.env.SCREENSHOT;
};

export const commands = {
  mockReset,
  mockSetSpec,
  mockSetStatus,
  mockPushEvent,
  mockPushValue,
  mockPushNumeric,
  mockSubscriberCount,
  mockEventClientCount,
  sleep,
  screenshotEnabled,
};

declare module "vitest/browser" {
  interface BrowserCommands {
    mockReset: () => Promise<void>;
    mockSetSpec: (spec: unknown) => Promise<void>;
    mockSetStatus: (status: {
      state?: string;
      tube_id?: string | null;
      error?: string | null;
    }) => Promise<void>;
    mockPushEvent: (
      node_id: string,
      signal: string,
      epoch?: number,
    ) => Promise<unknown>;
    mockPushValue: (
      node_id: string,
      signal: string,
      value: ValueEnvelope,
      opts?: { epoch?: number; eventId?: number },
    ) => Promise<unknown>;
    mockPushNumeric: (
      node_id: string,
      signal: string,
      value: number,
      epoch?: number,
    ) => Promise<void>;
    mockSubscriberCount: (
      node_id: string,
      signal: string,
    ) => Promise<number>;
    mockEventClientCount: () => Promise<number>;
    sleep: (ms: number) => Promise<void>;
    screenshotEnabled: () => Promise<boolean>;
  }
}
