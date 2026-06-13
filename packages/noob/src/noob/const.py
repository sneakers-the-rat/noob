RESERVED_IDS = (
    "assets",
    "input",
    "meta",
    "tube",
)
"""Reserved strings that can't be used as IDs for Nodes, etc."""

VIRTUAL_NODES = ("input", "assets")
"""
Virtual nodes that don't actually exist as nodes,
but can be depended on
(and can be present or absent, and so shouldn't be marked as trivially done)
"""

META_SIGNAL = "__META__"
"""Signal name used for meta events from a node"""
