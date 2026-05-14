/**
 * Decode ndarray-style payloads into ImageData for canvas rendering.
 *
 * Supported shapes (HWC order):
 *  - (H, W)         → grayscale
 *  - (H, W, 3)      → RGB
 *  - (H, W, 4)      → RGBA
 *
 * Supported dtypes (v1):
 *  - uint8 / int8  (int8 reinterpreted as unsigned)
 *
 * Other shapes/dtypes return null. The display layer surfaces an "unsupported"
 * message and you can swap to the raw display.
 */

import type { ValueRecord } from "../protocol.ts";

export interface DecodedImage {
  imageData: ImageData;
  width: number;
  height: number;
}

export function decodeImage(rec: ValueRecord): DecodedImage | null {
  if (rec.value.kind !== "ndarray" || !rec.payload) return null;
  const { shape, dtype } = rec.value;
  if (!isUint8Compatible(dtype)) return null;

  let h: number, w: number, c: number;
  if (shape.length === 2) {
    [h, w] = shape;
    c = 1;
  } else if (shape.length === 3) {
    [h, w, c] = shape;
    if (c !== 1 && c !== 3 && c !== 4) return null;
  } else {
    return null;
  }
  if (h <= 0 || w <= 0) return null;
  const expected = h * w * c;
  if (rec.payload.byteLength < expected) return null;

  const src = rec.payload;
  const rgba = new Uint8ClampedArray(h * w * 4);
  if (c === 4) {
    rgba.set(src.subarray(0, expected));
  } else if (c === 3) {
    for (let i = 0, j = 0; i < expected; i += 3, j += 4) {
      rgba[j] = src[i];
      rgba[j + 1] = src[i + 1];
      rgba[j + 2] = src[i + 2];
      rgba[j + 3] = 255;
    }
  } else {
    for (let i = 0, j = 0; i < expected; i += 1, j += 4) {
      const v = src[i];
      rgba[j] = v;
      rgba[j + 1] = v;
      rgba[j + 2] = v;
      rgba[j + 3] = 255;
    }
  }
  return {
    imageData: new ImageData(rgba, w, h),
    width: w,
    height: h,
  };
}

function isUint8Compatible(dtype: string): boolean {
  const d = dtype.toLowerCase();
  return d === "uint8" || d === "u1" || d === "|u1" || d === "int8" || d === "i1" || d === "|i1";
}

export function numericFromEnvelope(rec: ValueRecord): number | null {
  if (rec.value.kind !== "json") return null;
  const v = rec.value.data;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "boolean") return v ? 1 : 0;
  return null;
}
