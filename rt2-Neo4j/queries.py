from rt_core_v2.rttuple import RtTupleVisitor, RtTuple, ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, TupleType, TupleComponents, AttributesVisitor, TempRef
from rt_core_v2.ids_codes.rui import Rui, Relationship
import cypher
from cypher import CypherComponent, CypherNode, CypherRelationship, CypherOperation, AtomicEntityQuery, CompoundQuery
from enum import Enum
from datetime import datetime

"""
Enum for defining various node labels used in Cypher queries.
These labels correspond to different types of nodes in the graph database.
"""
class NodeLabels(Enum):
    AR = TupleType.AR.value
    AN = TupleType.AN.value
    DC = TupleType.DC.value
    DI = TupleType.DI.value
    F = TupleType.F.value
    NtoDE = TupleType.NtoDE.value
    NtoN = TupleType.NtoN.value
    NtoR = TupleType.NtoR.value
    NtoC = TupleType.NtoC.value
    NtoLackR = TupleType.NtoLackR.value
    NPoR = "N"
    RPoR = "R"
    Data = "data"
    Temporal = "temp"
    Relation = "rel"
    Concept = "con"

"""
Enum for defining various relationship labels used in Cypher queries.
These labels correspond to different types of relationships in the graph database.
"""
class RelationshipLabels(Enum):
    ruia = TupleComponents.ruia.value
    ruin = TupleComponents.ruin.value
    ruio = TupleComponents.ruio.value
    ruir = TupleComponents.ruir.value
    ruid = TupleComponents.ruid.value
    ruit = TupleComponents.ruit.value
    ta = TupleComponents.ta.value
    replacement = TupleComponents.replacements.value
    ruitn = TupleComponents.ruitn.value
    r = TupleComponents.r.value
    p_list = TupleComponents.p_list.value
    tr = TupleComponents.tr.value
    ruidt = TupleComponents.ruidt.value
    data = TupleComponents.data.value

class TupleInsertionVisitor(RtTupleVisitor):
    """
    Visitor class for handling different types of tuples and generating corresponding Cypher queries for insertion.

    Attributes:
    get_attr -- A static visitor that retrieves a tuple's attributes
    """
    get_attr = AttributesVisitor()

    def visit(self, host: RtTuple):
        """
        Visits a tuple and generates a Cypher query based on the tuple's type.
        
        Args:
            host (RtTuple): The tuple to be visited.

        Returns:
            CompoundQuery: A compound Cypher query for the given tuple.
        """
        attributes = host.accept(self.get_attr)
        pop_key(attributes, TupleComponents.type.value)
        match host.tuple_type:
            case TupleType.AN:
                query = self.visit_an(host, attributes)
            case TupleType.AR:
                query = self.visit_ar(host, attributes)
            case TupleType.DI:
                query = self.visit_di(host, attributes)
            case TupleType.DC:
                query = self.visit_dc(host, attributes)
            case TupleType.F:
                query = self.visit_f(host, attributes)
            case TupleType.NtoN:
                query = self.visit_nton(host, attributes)
            case TupleType.NtoR:
                query = self.visit_ntor(host, attributes)
            case TupleType.NtoC:
                query = self.visit_ntoc(host, attributes)
            case TupleType.NtoDE:
                query = self.visit_ntode(host, attributes)
            case TupleType.NtoLackR:
                query = self.visit_ntolackr(host, attributes)
        return query

    def visit_an(self, host: ANTuple, attributes: dict):
        """
        Generates a Cypher query for an ANTuple.

        Args:
            host (ANTuple): The ANTuple instance.
            attributes (dict): Attributes of the ANTuple.

        Returns:
            CompoundQuery: A Cypher query to create an AN node and its relationships.
        """
        labels = [NodeLabels.AN.value]

        ruin_query = query_rui_node("ruin", pop_key(attributes, TupleComponents.ruin.value), operation=CypherOperation.CREATE)
        ruia_query = query_rui_node("ruia", pop_key(attributes, TupleComponents.ruia.value))
        an_query = cypher.query_node(CypherOperation.CREATE, "an", labels, attributes)
        
        ruin_rel = cypher.create_relationship(an_query.component, ruin_query.component, [RelationshipLabels.ruin.value])
        ruia_rel = cypher.create_relationship(ruia_query.component, an_query.component, [RelationshipLabels.ruia.value])
        total_query = CompoundQuery([ruin_query, ruia_query, an_query, ruin_rel, ruia_rel])

        return total_query
    
    def visit_ar(self, host: ARTuple, attributes: dict):
        """
        Generates a Cypher query for an ARTuple.

        Args:
            host (ARTuple): The ARTuple instance.
            attributes (dict): Attributes of the ARTuple.

        Returns:
            CompoundQuery: A Cypher query to create an AR node and its relationships.
        """
        labels = [NodeLabels.AR.value]

        ruir_query = query_rui_node("ruir", pop_key(attributes, TupleComponents.ruir.value), operation=CypherOperation.CREATE)
        ruia_query = query_rui_node("ruia", pop_key(attributes, TupleComponents.ruia.value))
        ruio_query = query_rui_node("ruio", pop_key(attributes, TupleComponents.ruio.value))
        ar_query = cypher.query_node(CypherOperation.CREATE, "ar", labels, attributes)
        
        ruir_rel = cypher.create_relationship(ar_query.component, ruir_query.component, [RelationshipLabels.ruir.value])
        ruia_rel = cypher.create_relationship(ruia_query.component, ar_query.component, [RelationshipLabels.ruia.value])
        ruio_rel = cypher.create_relationship(ruir_query.component, ruio_query.component, [RelationshipLabels.ruio.value])

        total_query = CompoundQuery([ruir_query, ruia_query, ruio_query, ar_query, ruir_rel, ruia_rel, ruio_rel])
        return total_query

    def visit_di(self, host: DITuple, attributes: dict):
        """
        Generates a Cypher query for a DITuple.

        Args:
            host (DITuple): The DITuple instance.
            attributes (dict): Attributes of the DITuple.

        Returns:
            CompoundQuery: A Cypher query to create a DI node and its relationships.
        """
        labels = [NodeLabels.DI.value]

        ruid_query = query_rui_node("ruid", pop_key(attributes, TupleComponents.ruid.value))
        ruia_query = query_rui_node("ruia", pop_key(attributes, TupleComponents.ruia.value))
        ruit_query = query_rui_node("ruit", pop_key(attributes, TupleComponents.ruit.value))
        ta_query = create_temp_ref(host.ta, pop_key(attributes, TupleComponents.ta.value), name="ta")
        di_query = cypher.query_node(CypherOperation.CREATE, "di", labels, attributes)
        
        ruid_rel = cypher.create_relationship(di_query.component, ruid_query.component, [RelationshipLabels.ruid.value])
        ruia_rel = cypher.create_relationship(di_query.component, ruia_query.component, [RelationshipLabels.ruia.value])
        ruit_rel = cypher.create_relationship(di_query.component, ruit_query.component, [RelationshipLabels.ruit.value])
        ta_rel = cypher.create_relationship(di_query.component, ta_query.component, [RelationshipLabels.ta.value])

        total_query = CompoundQuery([ruid_query, ruia_query, ruit_query, ta_query, di_query, ruid_rel, ruia_rel, ruit_rel, ta_rel])
        return total_query

    def visit_dc(self, host: DCTuple, attributes: dict):
        """
        Generates a Cypher query for a DCTuple.

        Args:
            host (DCTuple): The DCTuple instance.
            attributes (dict): Attributes of the DCTuple.

        Returns:
            CompoundQuery: A Cypher query to create a DC node and its relationships.
        """
        labels = [NodeLabels.DC.value]

        ruid_query = query_rui_node("ruid", pop_key(attributes, TupleComponents.ruid.value))
        ruit_query = query_rui_node("ruit", pop_key(attributes, TupleComponents.ruit.value))
        pop_key(attributes, TupleComponents.replacements.value)
        replacements = host.replacements
        dc_query = cypher.query_node(CypherOperation.CREATE, "dc", labels, attributes)
        
        ruid_rel = cypher.create_relationship(dc_query.component, ruid_query.component, [RelationshipLabels.ruid.value])
        ruit_rel = cypher.create_relationship(dc_query.component, ruit_query.component, [RelationshipLabels.ruit.value])
        replacements_rel = create_ordered_relationships(dc_query.component, replacements, [RelationshipLabels.replacement.value])

        total_query = CompoundQuery([ruid_query, ruit_query, dc_query, replacements_rel, ruid_rel, ruit_rel])
        return total_query

    def visit_f(self, host: FTuple, attributes: dict):
        """
        Generates a Cypher query for an FTuple.

        Args:
            host (FTuple): The FTuple instance.
            attributes (dict): Attributes of the FTuple.

        Returns:
            CompoundQuery: A Cypher query to create an F node and its relationships.
        """
        labels = [NodeLabels.F.value]

        ruitn_query = cypher.query_node(CypherOperation.MATCH, "ruitn", [], {TupleComponents.rui.value: pop_key(attributes, TupleComponents.ruitn.value)})
        f_query = cypher.query_node(CypherOperation.CREATE, "f", labels, attributes)

        ruitn_rel = cypher.create_relationship(f_query.component, ruitn_query.component, [RelationshipLabels.ruitn.value])

        total_query = CompoundQuery([ruitn_query, f_query, ruitn_rel])
        return total_query

    # TODO Figure out how to implement relationship nodes
    def visit_nton(self, host: NtoNTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoNTuple.

        Args:
            host (NtoNTuple): The NtoNTuple instance.
            attributes (dict): Attributes of the NtoNTuple.

        Returns:
            CompoundQuery: A Cypher query to create an NtoN node and its relationships.
        """
        labels = [NodeLabels.NtoN.value]

        tr_query = create_temp_ref(host.tr, pop_key(attributes, TupleComponents.tr.value), name="tr")
        r_query = get_relation_node(host.r)
        pop_key(attributes, TupleComponents.r.value)
        pop_key(attributes, TupleComponents.p_list.value)
        nton_query = cypher.query_node(CypherOperation.CREATE, "nton", labels, attributes)

        p_query = create_ordered_relationships(nton_query.component, host.p, [RelationshipLabels.p_list.value])
        r_rel = cypher.create_relationship(nton_query.component, r_query.component, [RelationshipLabels.r.value])
        tr_rel = cypher.create_relationship(nton_query.component, tr_query.component, [RelationshipLabels.tr.value])

        total_query = CompoundQuery([tr_query, r_query, nton_query, p_query, tr_rel, r_rel])
        return total_query
    
    def visit_ntor(self, host: NtoRTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoRTuple.

        Args:
            host (NtoRTuple): The NtoRTuple instance.
            attributes (dict): Attributes of the NtoRTuple.

        Returns:
            CompoundQuery: A Cypher query to create an NtoR node and its relationships.
        """
        labels = [NodeLabels.NtoR.value]

        ruin_query = query_rui_node("ruin", pop_key(attributes, TupleComponents.ruin.value))
        ruir_query = query_rui_node("ruir", pop_key(attributes, TupleComponents.ruir.value))
        tr_query = create_temp_ref(host.tr, pop_key(attributes, TupleComponents.tr.value), name="tr")
        r_query = get_relation_node(host.r)
        pop_key(attributes, TupleComponents.r.value)
        ntor_query = cypher.query_node(CypherOperation.CREATE, "ntor", labels, attributes)

        ruin_rel = cypher.create_relationship(ntor_query.component, ruin_query.component, [RelationshipLabels.ruin.value])
        ruir_rel = cypher.create_relationship(ntor_query.component, ruir_query.component, [RelationshipLabels.ruir.value])
        tr_rel = cypher.create_relationship(ntor_query.component, tr_query.component, [RelationshipLabels.tr.value])
        r_rel = cypher.create_relationship(ntor_query.component, r_query.component, [RelationshipLabels.r.value])

        total_query = CompoundQuery([ruin_query, ruir_query, tr_query, r_query, ntor_query, ruin_rel, ruir_rel, tr_rel, r_rel])
        return total_query

    # TODO Figure out how to create concept nodes and implement
    def visit_ntoc(self, host: NtoCTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoCTuple.

        Args:
            host (NtoCTuple): The NtoCTuple instance.
            attributes (dict): Attributes of the NtoCTuple.

        Returns:
            CompoundQuery: A Cypher query to create an NtoC node and its relationships.
        """
        labels = [NodeLabels.NtoC.value]

    #TODO Implement
    def visit_ntode(self, host: NtoDETuple, attributes: dict):
        """
        Generates a Cypher query for an NtoDETuple.

        Args:
            host (NtoDETuple): The NtoDETuple instance.
            attributes (dict): Attributes of the NtoDETuple.

        Returns:
            CompoundQuery: A Cypher query to create an NtoDE node and its relationships.
        """
        labels = [NodeLabels.NtoDE.value]

        # ruin_query = query_rui_node("ruin", pop_key(attributes, TupleComponents.ruin.value))
        # ruidt_query = query_rui_node("ruidt", pop_key(attributes, TupleComponents.ruidt.value))
        # data_query = get_data_node(host.data)
        # ntode_query = cypher.query_node(CypherOperation.CREATE, "ntode", labels, attributes)

        # data_rel = cypher.create_relationship(ntode_query.component, data_query.component, [RelationshipLabels.data.value])
        # ruin_rel = cypher.create_relationship(ntode_query.component, ruin_query.component, [RelationshipLabels.ruin.value])
        # ruidt_rel = cypher.create_relationship(ntode_query.component, ruidt_query.component, [RelationshipLabels.ruidt.value])
        # total_query = CompoundQuery([ruin_query, ruidt_query, data_query, ntode_query, ruin_rel, ruidt_rel, data_rel])
        # return total_query
    
    def visit_ntolackr(self, host: NtoLackRTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoLackRTuple.

        Args:
            host (NtoLackRTuple): The NtoLackRTuple instance.
            attributes (dict): Attributes of the NtoLackRTuple.

        Returns:
            CompoundQuery: A Cypher query to create an NtoLackR node and its relationships.
        """
        labels = [NodeLabels.NtoLackR.value]

        ruin_query = query_rui_node("ruin", pop_key(attributes, TupleComponents.ruin.value))
        ruir_query = query_rui_node("ruir", pop_key(attributes, TupleComponents.ruir.value))
        tr_query = create_temp_ref(host.tr, pop_key(attributes, TupleComponents.tr.value), name="tr")
        r_query = get_relation_node(host.r)
        pop_key(attributes, TupleComponents.r.value)
        ntor_query = cypher.query_node(CypherOperation.CREATE, "ntor", labels, attributes)

        ruin_rel = cypher.create_relationship(ntor_query.component, ruin_query.component, [RelationshipLabels.ruin.value])
        ruir_rel = cypher.create_relationship(ntor_query.component, ruir_query.component, [RelationshipLabels.ruir.value])
        tr_rel = cypher.create_relationship(ntor_query.component, tr_query.component, [RelationshipLabels.tr.value])
        r_rel = cypher.create_relationship(ntor_query.component, r_query.component, [RelationshipLabels.r.value])

        total_query = CompoundQuery([ruin_query, ruir_query, tr_query, r_query, ntor_query, ruin_rel, ruir_rel, tr_rel, r_rel])
        return total_query

"""Removes a key from a dictionary and returns the value"""
def pop_key(dict, key):
    """
    Removes a key from a dictionary and returns its associated value.
    
    Args:
        dict (dict): The dictionary to remove the key from.
        key: The key to be removed.

    Returns:
        The value associated with the removed key.
    """
    value = dict[key]
    del dict[key]
    return value

# TODO Figure out how to do relation insertion with an ontology
def get_relation_node(rel: Relationship, name="r") -> AtomicEntityQuery:
    """
    Creates a Cypher query to retrieve a relationship node.

    Args:
        rel (Relationship): The relationship instance to query.
        name (str): The name of the node in the query.

    Returns:
        AtomicEntityQuery: A Cypher query for the relationship node.
    """
    return cypher.query_node(CypherOperation.MATCH, name, [NodeLabels.Relation.value], {
        TupleComponents.r.value: str(rel)
    })

"""Creates a query for a node with a specified rui"""
def query_rui_node(name, rui, labels: list[NodeLabels] = [NodeLabels.NPoR], operation: CypherOperation = CypherOperation.MATCH) -> AtomicEntityQuery:
    """
    Creates a Cypher query to retrieve or create a node with a specified rui.

    Args:
        name (str): The name of the node in the query.
        rui: The rui value for the node.
        labels (list[NodeLabels]): Labels for the node.
        operation (CypherOperation): The type of Cypher operation (MATCH or CREATE).

    Returns:
        AtomicEntityQuery: A Cypher query for the node with the specified rui.
    """
    labels_str = [label.value for label in labels]
    return cypher.query_node(operation, name, labels_str, {TupleComponents.rui.value: rui})

# TODO Figure out how to represent TempRef nodes
"""Creates a query to create a temporal reference node"""
def create_temp_ref(temp_ref: TempRef, entry, name: str = "tr") -> AtomicEntityQuery:
    """
    Creates a Cypher query for a temporal reference node.

    Args:
        temp_ref (TempRef): The temporal reference instance.
        entry: The entry value for the temporal reference.
        name (str): The name of the node in the query.

    Returns:
        AtomicEntityQuery: A Cypher query for the temporal reference node.
    """
    if isinstance(temp_ref.ref, Rui):
        return cypher.query_node(CypherOperation.MATCH, name, [NodeLabels.Temporal.value], {TupleComponents.rui.value: entry})
    # TODO How should temp_ref attributes be named?
    return cypher.query_node(CypherOperation.MERGE, name, [NodeLabels.Temporal.value], {"date": entry})

"""Takes a sequence of ruis to create numbered relations between the source and nodes found in the sequence"""
def create_ordered_relationships(source: CypherNode, ruis: list[Rui], labels: list[str]) -> CompoundQuery:
    """
    Creates Cypher queries for a series of numbered relationships between the source node and the nodes found in the sequence of ruis.

    Args:
        source (CypherNode): The source node.
        ruis (list[Rui]): The list of ruis.
        labels (list[str]): Labels for the relationships.

    Returns:
        CompoundQuery: A Cypher query to create the ordered relationships.
    """
    output = []
    for idx, rui in enumerate(ruis):
        sink_query = cypher.query_node(CypherOperation.MATCH, str(idx), [], {TupleComponents.rui.value: rui})
        output.append(sink_query)
        output.append(cypher.create_relationship(source, sink_query.component, labels, {"order": idx}))

    return CompoundQuery(output)

# Sample usage
get_attr = AttributesVisitor()
get_query = TupleInsertionVisitor()

tuple_an = ANTuple()
tuple_ar = ARTuple()
tuple_di = DITuple()
tuple_dc = DCTuple(replacements=[Rui(), Rui(), Rui()])
tuple_f = FTuple()
tuple_nton = NtoNTuple()
tuple_ntor = NtoRTuple()
# tuple_ntoc = NtoCTuple()
# tuple_ntode = NtoDETuple()
tuple_ntolackr = NtoLackRTuple()

an_query = tuple_an.accept(get_query).get_query()
ar_query = tuple_ar.accept(get_query).get_query()
di_query = tuple_di.accept(get_query).get_query()
dc_query = tuple_dc.accept(get_query).get_query()
f_query = tuple_f.accept(get_query).get_query()
nton_query = tuple_nton.accept(get_query).get_query()
ntor_query = tuple_ntor.accept(get_query).get_query()
# ntoc_query = tuple_ntoc.accept(get_query).get_query()
# ntode_query = tuple_ntode.accept(get_query).get_query()
ntolackr_query = tuple_ntolackr.accept(get_query).get_query()

print(f'an: {an_query}\n')
print(f'ar: {ar_query}\n')
print(f'di: {di_query}\n')
print(f'dc: {dc_query}\n')
print(f'f: {f_query}\n') 
print(f'nton: {nton_query}\n')
print(f'ntor: {ntor_query}\n')
# print(f'ntoc: {ntoc_query}\n')
# print(f'ntode: {ntode_query}\n')
print(f'ntolackr: {ntolackr_query}\n')