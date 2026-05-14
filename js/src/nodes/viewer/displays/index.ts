/**
 * Registry of display modules, keyed by ViewerMode.
 *
 * Each module owns:
 *  - the React component rendered in the screen
 *  - the handle topology (`handles`) the viewer exposes when this mode is active
 *  - `canRender(record)` predicate used to dim incompatible modes in the picker
 *  - short label + title for the mode-picker buttons
 */

import type { ComponentType } from "react";
import type { ViewerMode } from "../../../types.ts";
import type { ValueRecord } from "../../../run/protocol.ts";
import type { DisplayHandle, DisplayProps } from "./types.ts";

import RawDisplay, * as RawMod from "./Raw.tsx";
import LineDisplay, * as LineMod from "./Line.tsx";
import ImageDisplay, * as ImageMod from "./Image.tsx";

export interface DisplayModule {
  Component: ComponentType<DisplayProps>;
  handles: DisplayHandle[];
  shortLabel: string;
  title: string;
  canRender: (rec: ValueRecord | undefined) => boolean;
}

export const DISPLAYS: Record<ViewerMode, DisplayModule> = {
  raw: {
    Component: RawDisplay,
    handles: RawMod.handles,
    shortLabel: RawMod.shortLabel,
    title: RawMod.title,
    canRender: RawMod.canRender,
  },
  line: {
    Component: LineDisplay,
    handles: LineMod.handles,
    shortLabel: LineMod.shortLabel,
    title: LineMod.title,
    canRender: LineMod.canRender,
  },
  image: {
    Component: ImageDisplay,
    handles: ImageMod.handles,
    shortLabel: ImageMod.shortLabel,
    title: ImageMod.title,
    canRender: ImageMod.canRender,
  },
};

export const ALL_MODES: ViewerMode[] = ["raw", "line", "image"];
