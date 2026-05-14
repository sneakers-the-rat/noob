/**
 * Render the latest ndarray-as-image on a canvas.
 *
 * Same component handles both "image" and "video" modes — the only
 * difference is how often it repaints. With xyflow already re-rendering
 * the component each time `records` grows, we just always repaint the
 * latest record's image.
 */

import { useEffect, useRef } from "react";
import { decodeImage } from "./decode.ts";
import type { ValueRecord, ValueEnvelope } from "../protocol.ts";

export function ImageDisplay({ records }: { records: ValueRecord[] }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const latest = records[records.length - 1];

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

export function canRenderImage(rec: ValueRecord | undefined): boolean {
  if (!rec) return false;
  return decodeImage(rec) !== null;
}
