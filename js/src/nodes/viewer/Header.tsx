/**
 * Header strip across the top of the viewer screen — source label on the
 * left, mode-picker buttons on the right. Always visible (even before any
 * signal is connected) so the user can choose a display first and connect
 * matching signals after.
 */

import type { ViewerMode } from "../../types.ts";
import { ALL_MODES, DISPLAYS } from "./displays/index.ts";

export interface HeaderProps {
  mode: ViewerMode;
  compatible: ViewerMode[];
  onChange: (m: ViewerMode) => void;
  sources: Record<string, { node_id: string; signal: string } | null>;
}

export default function Header({
  mode,
  compatible,
  onChange,
  sources,
}: HeaderProps) {
  const sourceLabel = formatSources(sources);
  return (
    <div className="viewer-header">
      <span className="viewer-source" title={sourceLabel}>
        {sourceLabel}
      </span>
      <div className="viewer-modes" role="group" aria-label="display mode">
        {ALL_MODES.map((m) => {
          const active = mode === m;
          const na = !compatible.includes(m);
          const cls =
            "viewer-mode-btn" +
            (active ? " active" : "") +
            (na ? " na" : "");
          return (
            <button
              key={m}
              type="button"
              className={cls}
              data-testid={`mode-${m}`}
              title={DISPLAYS[m].title + (na ? " (no matching value)" : "")}
              onClick={(e) => {
                e.stopPropagation();
                onChange(m);
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              {DISPLAYS[m].shortLabel}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function formatSources(
  sources: Record<string, { node_id: string; signal: string } | null>,
): string {
  const entries = Object.entries(sources).filter(([, v]) => v !== null) as [
    string,
    { node_id: string; signal: string },
  ][];
  if (entries.length === 0) return "—";
  if (entries.length === 1) {
    const [, src] = entries[0];
    return `${src.node_id}.${src.signal}`;
  }
  return entries
    .map(([handle, src]) => `${handle}=${src.node_id}.${src.signal}`)
    .join(" · ");
}
