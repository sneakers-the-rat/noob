/**
 * Vitest config — two projects:
 *
 *  - "unit": node-mode tests in `tests/*.test.ts` (pure logic, e.g. tube.test.ts)
 *  - "browser": browser-mode tests in `tests/browser/*.test.ts`, driven via
 *    Playwright. The mock noob server runs as a Vite plugin alongside, and
 *    browser tests drive it via Vitest commands (which run in the Node
 *    process where the mock server lives).
 */

import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { noobMockServerPlugin } from "./tests/browser/mock-server.ts";
import { commands } from "./tests/browser/commands.ts";

export default defineConfig({
  plugins: [react()],
  test: {
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["tests/**/*.test.ts"],
          exclude: ["tests/browser/**"],
          environment: "node",
        },
      },
      {
        extends: true,
        plugins: [react(), noobMockServerPlugin()],
        test: {
          name: "browser",
          include: ["tests/browser/**/*.test.ts"],
          setupFiles: ["./tests/browser/setup.ts"],
          globalSetup: ["./tests/browser/global-setup.ts"],
          browser: {
            enabled: true,
            provider: playwright({}),
            headless: true,
            instances: [{ browser: "chromium" }],
            commands,
          },
        },
      },
    ],
  },
});
