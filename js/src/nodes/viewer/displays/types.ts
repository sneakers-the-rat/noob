/**
 * Shared types for viewer display modules.
 *
 * Each display module exports:
 *  - default React component (rendered inside the screen)
 *  - `handles`: which target handles the viewer exposes on the left side
 *    when this display is active
 *  - `canRender(rec)`: whether a given latest value is renderable in this mode
 *  - `shortLabel` / `title`: shown in the mode-picker buttons
 */

import type { ValueRecord } from "../../../run/protocol.ts";

export interface DisplayHandle {
  id: string;
  label: string;
}

export interface DisplayProps {
  /** Records keyed by handle id — e.g. {in: [...], x: [...]}. */
  records: Record<string, ValueRecord[]>;
}
