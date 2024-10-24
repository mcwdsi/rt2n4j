from rt2_neo4j.client import Neo4jRtStore
from rt_core_v2.rttuple import (
    ANTuple,
    ARTuple,
    DITuple,
    DCTuple,
    FTuple,
    NtoNTuple,
    NtoRTuple,
    NtoCTuple,
    NtoDETuple,
    NtoLackRTuple,
    AttributesVisitor,
)
from rt_core_v2.ids_codes.rui import Rui, TempRef


# storage = Neo4jRtStore(uri="neo4j://localhost:7687", auth=("neo4j", "neo4j_pass"))

# storage.save_tuple(a)