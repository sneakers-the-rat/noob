from noob.node.spec import NodeSpecification  # noqa: I001 - needs to be defined before Node is
from noob.node.base import Edge, Node, process_method, Signal, Slot
from noob.node.return_ import Return
from noob.node.gather import Gather, GatherResult
from noob.node.map import Map, MapResult
from noob.node.tube import TubeNode

SPECIAL_NODES = {"gather": Gather, "map": Map, "return": Return, "tube": TubeNode}
"""
Map from short names used in node ``type`` values to special node classes
"""


__all__ = [
    "Edge",
    "Gather",
    "GatherResult",
    "Map",
    "MapResult",
    "Node",
    "NodeSpecification",
    "Return",
    "Signal",
    "Slot",
    "process_method",
]
