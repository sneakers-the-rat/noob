/**
 * Test setup — register a custom locator that takes a raw CSS selector,
 * so tests can target xyflow's internal classes (`.react-flow__pane`,
 * `.react-flow__node-viewer`, etc.) without us having to plumb testids
 * through framework internals.
 */

import { locators } from "vitest/browser";

locators.extend({
  bySelector(selector: string) {
    return selector;
  },
});

declare module "vitest/browser" {
  interface LocatorAPI {
    bySelector(selector: string): Locator;
  }
  interface Page {
    bySelector(selector: string): Locator;
  }
}
