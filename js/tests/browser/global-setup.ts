/**
 * Global vitest setup — clears the screenshot directory at the start of
 * each test run so stale screenshots from a previous run don't get
 * mistaken for the current one.
 */

import { mkdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SCREENSHOT_DIR = resolve(__dirname, "screenshots");
const VITEST_INTERNAL_DIR = resolve(__dirname, "__screenshots__");

export default async function globalSetup(): Promise<void> {
  // Snapshots we explicitly save (via snap()) live in tests/browser/screenshots/.
  // Vitest also drops failure snapshots into tests/browser/__screenshots__/;
  // we clear both at the start of each run so stale images don't get mixed
  // with the current run's output.
  await rm(SCREENSHOT_DIR, { recursive: true, force: true });
  await rm(VITEST_INTERNAL_DIR, { recursive: true, force: true });
  await mkdir(SCREENSHOT_DIR, { recursive: true });
}
