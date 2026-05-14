/**
 * Sample TubeSpecification used by browser tests — three nodes (counter +
 * two sine waves) so test code has signal handles to drag onto viewer
 * targets. Mirrors the shape the real `/spec/{tube_id}` WS pushes.
 */

export const SAMPLE_TUBE_SPEC = {
  noob_id: "test-tube",
  noob_model: "noob.tube.TubeSpecification",
  noob_version: "0.0.0",
  description: "playwright fixture tube",
  nodes: {
    counter: {
      id: "counter",
      type: "noob.testing.counter",
      nodeinfo: {
        node_id: "counter",
        type: "noob.testing.counter",
        signals: { count: { name: "count", annotation: "int" } },
        slots: {},
      },
    },
    sine_x: {
      id: "sine_x",
      type: "noob.testing.sine",
      depends: [{ count: "counter.count" }],
      nodeinfo: {
        node_id: "sine_x",
        type: "noob.testing.sine",
        signals: { value: { name: "value", annotation: "float" } },
        slots: { count: { name: "count", annotation: "int", required: true } },
      },
    },
    sine_y: {
      id: "sine_y",
      type: "noob.testing.sine",
      depends: [{ count: "counter.count" }],
      nodeinfo: {
        node_id: "sine_y",
        type: "noob.testing.sine",
        signals: { value: { name: "value", annotation: "float" } },
        slots: { count: { name: "count", annotation: "int", required: true } },
      },
    },
    gradient: {
      id: "gradient",
      type: "noob.testing.gradient_image",
      depends: [{ count: "counter.count" }],
      nodeinfo: {
        node_id: "gradient",
        type: "noob.testing.gradient_image",
        signals: { frame: { name: "frame", annotation: "ndarray" } },
        slots: { count: { name: "count", annotation: "int", required: true } },
      },
    },
  },
};
