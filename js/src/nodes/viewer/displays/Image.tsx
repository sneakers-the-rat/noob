/**
 * Image display — paint the latest ndarray onto a canvas.
 */

import { useEffect, useRef } from "react";
import { decodeImage } from "../../../run/displays/decode.ts";
import type { ValueRecord, ValueEnvelope } from "../../../run/protocol.ts";
import type { DisplayHandle, DisplayProps } from "./types.ts";

export const handles: DisplayHandle[] = [{ id: "in", label: "" }];
export const shortLabel = "IMG";
export const title = "image";

export function canRender(rec: ValueRecord | undefined): boolean {
  if (!rec) return false;
  return decodeImage(rec) !== null;
}

export default function ImageDisplay({ records }: DisplayProps) {
  const recs = records.in ?? [];
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const latest = recs.length ? recs[recs.length - 1] : undefined;

  useEffect(() => {
    if (!latest) return;
    const decoded = decodeImage(latest);
    if (!decoded) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    if (canvas.width !== decoded.width || canvas.height !== decoded.height) {
      canvas.width = decoded.width;
      canvas.height = decoded.height;
    }
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.putImageData(decoded.imageData, 0, 0);
  }, [latest]);

  if (!latest) {
    return <div className="runner-display-info">(no events yet)</div>;
  }

  const decodable = latest.value.kind === "ndarray" && latest.payload;
  return (
    <div className="runner-display-image">
      <canvas ref={canvasRef} />
      <div className="runner-display-info">
        {decodable
          ? `id=${latest.id} · ${describe(latest.value)}`
          : "value is not a renderable ndarray"}
      </div>
    </div>
  );
}

function describe(v: ValueEnvelope): string {
  if (v.kind === "ndarray")
    return `shape=${JSON.stringify(v.shape)} dtype=${v.dtype}`;
  return v.kind;
}
