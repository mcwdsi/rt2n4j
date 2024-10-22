from rt_core_v2.rttuple import RtTupleVisitor, RtTuple, ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, TupleType, TupleComponents, AttributesVisitor, RuiStatus, PorType, TempRef, ISO_Rui, ID_Rui
from rt_core_v2.ids_codes.rui import Rui, Relationship
from rt_core_v2.metadata import TupleEventType, RtChangeReason
from enum import Enum
from datetime import datetime
import uuid
import base64
import re

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


class Neo4jEntryConverter:
    """Contains functions for converting neo4j representation to and from tuple representation"""
    @staticmethod
    def str_to_rui(x: str) -> Rui:
        if ':' in x:
            return Neo4jEntryConverter.str_to_isorui(x)
        else:
            return Neo4jEntryConverter.str_to_idrui(x)
    
    @staticmethod
    def lst_to_ruis(x: list[str]) -> list[Rui]:
        # return [Rui(uuid.UUID(entry)) for entry in x]
        return [Neo4jEntryConverter.str_to_rui(entry) for entry in x]


    @staticmethod
    def str_to_idrui(x: str) -> ID_Rui:
        val = uuid.UUID(x)
        return ID_Rui(val)
    
    @staticmethod
    def str_to_isorui(x: str) -> ISO_Rui:
        return ISO_Rui(datetime.strptime(x, Neo4jEntryConverter.format))

    @staticmethod 
    def str_to_uui(x: str) -> uuid.UUI:
        return uuid.UUI(x)
    
    @staticmethod
    def str_to_relationship(x: str) -> Relationship:
        return Relationship(x)

    @staticmethod
    def lst_to_ruis(x: list[str]) -> list[Rui]:
        return [Neo4jEntryConverter.str_to_rui(entry) for entry in x]

    @staticmethod
    def str_to_str(x: str):
        return x
    
    @staticmethod
    def process_datetime(x: str):
        return datetime.strptime(x, Neo4jEntryConverter.format)
    
    @staticmethod
    def process_temp_ref(x: str):
        if ':' in x:
            time_data = Neo4jEntryConverter.str_to_isorui(x)
        else:
            time_data = Neo4jEntryConverter.str_to_idrui(x)
        return TempRef(time_data)
    
    @staticmethod
    def str_to_relation(relation_str: str) -> Relationship:
        return Relationship(relation_str)
    
    @staticmethod
    def str_to_bytes(x: str):
        return base64.b64decode(x)

neo4j_entry_converter = {
    TupleComponents.rui: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruin: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruia: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruid: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruin: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruir: Neo4jEntryConverter.str_to_uui,
    TupleComponents.ruics: Neo4jEntryConverter.str_to_uui,
    TupleComponents.ruidt: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruit: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruitn: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.ruio: Neo4jEntryConverter.str_to_idrui,
    TupleComponents.t: Neo4jEntryConverter.process_datetime,
    TupleComponents.ta: Neo4jEntryConverter.process_temp_ref,
    TupleComponents.tr: Neo4jEntryConverter.process_temp_ref,
    TupleComponents.ar: lambda x: RuiStatus(x),
    TupleComponents.unique: lambda x: PorType(x),
    TupleComponents.event: lambda x: TupleEventType(x),
    TupleComponents.event_reason: lambda x: RtChangeReason(x),
    TupleComponents.replacements: Neo4jEntryConverter.lst_to_ruis,
    TupleComponents.p_list: Neo4jEntryConverter.lst_to_ruis,
    TupleComponents.C: lambda x: float(x),
    TupleComponents.polarity: lambda x: bool(x),
    TupleComponents.r: Neo4jEntryConverter.str_to_relationship,
    TupleComponents.code: Neo4jEntryConverter.str_to_str,
    TupleComponents.data: Neo4jEntryConverter.str_to_bytes,
    TupleComponents.type: lambda x: TupleType(x),
}

def neo4j_to_rttuple(record) -> RtTuple:
    """Map a dictionary containing neo4j tuple components to a tuple"""
    output = {}
    for key, value in record.items():
        try:
            entry = TupleComponents(key)
            output[key] = neo4j_entry_converter[entry](value)
        except ValueError:
            # TODO Log error
            print(
                f"Invalid neo4j-rttuple processed due to key: {key} with entry: {value}. The processing of this tuple has been skipped."
            )
    return output

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
    ruics = TupleComponents.ruics.value
    code = TupleComponents.code.value

class TupleInsertionVisitor(RtTupleVisitor):
    """
    Visitor class for handling different types of tuples and generating corresponding Cypher queries for insertion.

    Attributes:
    get_attr -- A static visitor that retrieves a tuple's attributes
    """

    def __init__(self):
        self.tx = None
    

    def convert_att_neo4j(self, attribute):
        att_type = attribute
        if att_type == int or att_type == float or att_type == datetime or att_type == bool:
            return attribute
        elif isinstance(att_type, Enum):
            return attribute.value
        return str(attribute)
    
    def set_transaction(self, tx):
        self.tx = tx  # Set the transaction when it starts
    
    get_attr = AttributesVisitor()

    def visit(self, host: RtTuple):
        """
        Visits a tuple and generates a Cypher query based on the tuple's type.
        
        Args:
            host (RtTuple): The tuple to be visited.

        """
        if self.tx is None:
            raise RuntimeError("Transaction has not been set!")
        attributes = host.accept(self.get_attr)
        pop_key(attributes, TupleComponents.type.value)
        query = None
        attributes = {key: self.convert_att_neo4j(value) for key, value in attributes.items()}
        #TODO Move session control out of insertion and into RtStore
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

        """
        return self.tx.run(f"""
               CREATE (an:{NodeLabels.AN.value} {{rui: $rui, ar: $ar, unique: $unique}}) 
               CREATE (npor:{NodeLabels.NPoR.value} {{rui:$ruin}})
               CREATE (an)-[:{RelationshipLabels.ruin.value}]->(npor)
               """, **attributes)
        
    
    def visit_ar(self, host: ARTuple, attributes: dict):
        """
        Generates a Cypher query for an ARTuple.

        Args:
            host (ARTuple): The ARTuple instance.
            attributes (dict): Attributes of the ARTuple.

        """
        return self.tx.run(f"""
               CREATE (ar:{NodeLabels.AR.value} {{rui: $rui, ar: $ar, unique: $unique, ruio: $ruio}}) 
               CREATE (rpor:{NodeLabels.RPoR.value} {{rui:$ruir}})
               CREATE (ar)-[:{RelationshipLabels.ruir.value}]->(rpor)
               """, **attributes)

    def visit_di(self, host: DITuple, attributes: dict):
        """
        Generates a Cypher query for a DITuple.

        Args:
            host (DITuple): The DITuple instance.
            attributes (dict): Attributes of the DITuple.

        """
        return self.tx.run(f"""
            CREATE (di:{NodeLabels.DI.value} {{rui: $rui, t: $t, event_reason: $event_reason}})

            WITH di
            MATCH (ruit {{rui: $ruit}})
            CREATE (di)-[:{RelationshipLabels.ruit.value}]->(ruit)

            WITH di
            MATCH (ruid {{rui: $ruid}})
            CREATE (di)-[:{RelationshipLabels.ruid.value}]->(ruid)

            WITH di
            MATCH (ruia {{rui: $ruia}})
            CREATE (di)-[:{RelationshipLabels.ruia.value}]->(ruia)

            WITH di
            MERGE (ta:{NodeLabels.Temporal.value} {{rui: $ta}})
            CREATE (di)-[:{RelationshipLabels.ta.value}]->(ta)
            """, **attributes)

    def visit_dc(self, host: DCTuple, attributes: dict):
        """
        Generates a Cypher query for a DCTuple.

        Args:
            host (DCTuple): The DCTuple instance.
            attributes (dict): Attributes of the DCTuple.

        """

        query = f"""
            CREATE (dc:{NodeLabels.DC.value} {{rui: $rui, t: $t, event_reason: $event_reason, event: $event}})

            WITH dc
            MATCH (ruit {{rui: $ruit}})
            CREATE (dc)-[:{RelationshipLabels.ruit.value}]->(ruit)

            WITH dc
            MATCH (ruid {{rui: $ruid}})
            CREATE (dc)-[:{RelationshipLabels.ruid.value}]->(ruid)"""
        
        for idx, repl in enumerate(host.replacements):
            repl_id = TupleComponents.replacements.value + str(idx)
            attributes[repl_id] = str(repl)
            query += f"""
                    WITH dc
                    MATCH ({TupleComponents.replacements.value} {{rui: ${repl_id}}})
                    CREATE (dc)-[:{RelationshipLabels.replacement.value} {{replacements:{str(idx)}}}]->({TupleComponents.replacements.value})"""
            
        pop_key(attributes, TupleComponents.replacements.value)
        return self.tx.run(query, **attributes)

    def visit_f(self, host: FTuple, attributes: dict):
        """
        Generates a Cypher query for an FTuple.

        Args:
            host (FTuple): The FTuple instance.
            attributes (dict): Attributes of the FTuple.

        """
        return self.tx.run(f"""
               MATCH (tup {{rui:$ruitn}})
               CREATE (f:{NodeLabels.F.value} {{rui: $rui, C: $C}}) 
               CREATE (f)-[:{RelationshipLabels.ruitn.value}]->(tup)
               """, **attributes)

    # TODO Figure out how to implement relationship nodes
    def visit_nton(self, host: NtoNTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoNTuple.

        Args:
            host (NtoNTuple): The NtoNTuple instance.
            attributes (dict): Attributes of the NtoNTuple.

        """
        query = f"""
            CREATE (nton:{NodeLabels.NtoN.value} {{rui: $rui, polarity: $polarity}})

            WITH nton
            MATCH (r {{rui: $r}})
            CREATE (nton)-[:{RelationshipLabels.r.value}]->(r)

            WITH nton
            MERGE (tr:{NodeLabels.Temporal.value} {{rui: $tr}})
            CREATE (nton)-[:{RelationshipLabels.tr.value}]->(tr)"""
        
        for idx, ruin in enumerate(host.p):
            ruin_id = TupleComponents.p_list.value + str(idx)
            attributes[ruin_id] = str(ruin)
            query += f"""
                    WITH nton
                    MATCH ({TupleComponents.p_list.value} {{rui: ${ruin_id}}})
                    CREATE (nton)-[:{RelationshipLabels.p_list.value} {{p:{str(idx)}}}]->({TupleComponents.p_list.value})"""
            
        pop_key(attributes, TupleComponents.p_list.value)
        return self.tx.run(query, **attributes)
    
    def visit_ntor(self, host: NtoRTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoRTuple.

        Args:
            host (NtoRTuple): The NtoRTuple instance.
            attributes (dict): Attributes of the NtoRTuple.

        """
        return self.tx.run(f"""
            CREATE (ntor:{NodeLabels.NtoR.value} {{rui: $rui, polarity: $polarity}})

            WITH ntor
            MATCH (ruin {{rui: $ruin}})
            CREATE (ntor)-[:{RelationshipLabels.ruin.value}]->(ruin)

            WITH ntor
            MATCH (ruir {{rui: $ruir}})
            CREATE (ntor)-[:{RelationshipLabels.ruir.value}]->(ruir)

            WITH ntor
            MATCH (r {{rui: $r}})
            CREATE (ntor)-[:{RelationshipLabels.r.value}]->(r)

            WITH ntor
            MERGE (tr:{NodeLabels.Temporal.value} {{rui: $tr}})
            CREATE (ntor)-[:{RelationshipLabels.tr.value}]->(tr)
            """, **attributes)

    def visit_ntoc(self, host: NtoCTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoCTuple, ensuring that the `code` node has a unique relationship to `ruics`.

        Args:
            host (NtoCTuple): The NtoCTuple instance.
            attributes (dict): Attributes of the NtoCTuple.
        """
        return self.tx.run(f"""
            CREATE (ntoc:{NodeLabels.NtoC.value} {{rui: $rui, polarity: $polarity}})

            WITH ntoc
            MATCH (r {{rui: $r}})
            CREATE (ntoc)-[:{RelationshipLabels.r.value}]->(r)

            WITH ntoc
            MATCH (ruin {{rui: $ruin}})
            CREATE (ntoc)-[:{RelationshipLabels.ruin.value}]->(ruin)

            WITH ntoc
            MATCH (ruics {{rui: $ruics}})
            OPTIONAL MATCH (code_node:Code {{code: $code}})-[:{RelationshipLabels.ruics.value}]->(ruics {{rui: $ruics}})
            
            WITH ntoc, code_node, ruics
            CALL (code_node, ruics){{
                WITH * WHERE code_node IS NULL
                CREATE (new_code_node:Code {{code: $code}})
                CREATE (new_code_node)-[:{RelationshipLabels.ruics.value}]->(ruics)
                RETURN new_code_node
            }}
            WITH ntoc, COALESCE(new_code_node, code_node) AS final_code_node
            CREATE (ntoc)-[:{RelationshipLabels.code.value}]->(final_code_node)

            WITH ntoc
            MERGE (tr:{NodeLabels.Temporal.value} {{rui: $tr}})
            CREATE (ntoc)-[:{RelationshipLabels.tr.value}]->(tr)
        """, **attributes)

    def visit_ntode(self, host: NtoDETuple, attributes: dict):
        """
        Generates a Cypher query for an NtoDETuple, ensuring the `data` is stored in a separate node.

        Args:
            host (NtoDETuple): The NtoDETuple instance.
            attributes (dict): Attributes of the NtoDETuple.
        """
        attributes[TupleComponents.data.value] = base64.b64encode(host.data).decode('utf-8')

        return self.tx.run(f"""
            CREATE (ntode:{NodeLabels.NtoDE.value} {{rui: $rui, polarity: $polarity}})

            WITH ntode
            MATCH (ruin {{rui: $ruin}})
            CREATE (ntode)-[:{RelationshipLabels.ruin.value}]->(ruin)

            WITH ntode
            MATCH (ruidt {{rui: $ruidt}})
            OPTIONAL MATCH (data_node:{NodeLabels.Data.value} {{data: $data}})-[:{RelationshipLabels.ruidt.value}]->(ruidt)

            WITH ntode, data_node, ruidt
            CALL (data_node, ruidt){{
                WITH * WHERE data_node IS NULL
                CREATE (new_data_node:{NodeLabels.Data.value} {{data: $data}})
                CREATE (new_data_node)-[:{RelationshipLabels.ruidt.value}]->(ruidt)
                RETURN new_data_node
            }}
            
            WITH ntode, COALESCE(new_data_node, data_node) AS final_data_node
            CREATE (ntode)-[:{RelationshipLabels.data.value}]->(final_data_node)
        """, **attributes)

    
    def visit_ntolackr(self, host: NtoLackRTuple, attributes: dict):
        """
        Generates a Cypher query for an NtoLackRTuple.

        Args:
            host (NtoLackRTuple): The NtoLackRTuple instance.
            attributes (dict): Attributes of the NtoLackRTuple.

        """
        return self.tx.run(f"""
            CREATE (ntolackr:{NodeLabels.NtoLackR.value} {{rui: $rui}})

            WITH ntolackr
            MATCH (ruin {{rui: $ruin}})
            CREATE (ntolackr)-[:{RelationshipLabels.ruin.value}]->(ruin)

            WITH ntolackr
            MATCH (ruir {{rui: $ruir}})
            CREATE (ntolackr)-[:{RelationshipLabels.ruir.value}]->(ruir)

            WITH ntolackr
            MATCH (r {{rui: $r}})
            CREATE (ntolackr)-[:{RelationshipLabels.r.value}]->(r)

            WITH ntolackr
            MERGE (tr:{NodeLabels.Temporal.value} {{rui: $tr}})
            CREATE (ntolackr)-[:{RelationshipLabels.tr.value}]->(tr)
            """, **attributes)

        

def tuple_query(tuple_rui: Rui, tx):
    """
    Visits a tuple and generates a Cypher query based on the tuple's type.
    Retrieves the data and recreates the corresponding RtTuple object.
    
    Args:
        tuple_rui (Rui): The Rui of the tuple to be queried.
        driver: The Neo4j database driver.

    Returns:
        RtTuple: The recreated tuple based on the retrieved data.
    """
    retrieved_tuple = None

    # First, determine the label of the node by matching the rui
    result = tx.run(f"""
        MATCH (node {{rui: $rui}})
        RETURN labels(node) AS labels
    """, rui=str(tuple_rui))

    record = result.single()
    if not record:
        raise ValueError(f"No node found for Rui: {tuple_rui}")

    labels = record["labels"]

    # Match the node label to the correct tuple type
    match labels:
        case [NodeLabels.AN.value]:
            retrieved_tuple = query_an(tuple_rui, tx)
        case [NodeLabels.AR.value]:
            retrieved_tuple = query_ar(tuple_rui, tx)
        case [NodeLabels.DI.value]:
            retrieved_tuple = query_di(tuple_rui, tx)
        case [NodeLabels.DC.value]:
            retrieved_tuple = query_dc(tuple_rui, tx)
        case [NodeLabels.F.value]:
            retrieved_tuple = query_f(tuple_rui, tx)
        case [NodeLabels.NtoN.value]:
            retrieved_tuple = query_nton(tuple_rui, tx)
        case [NodeLabels.NtoR.value]:
            retrieved_tuple = query_ntor(tuple_rui, tx)
        case [NodeLabels.NtoC.value]:
            retrieved_tuple = query_ntoc(tuple_rui, tx)
        case [NodeLabels.NtoDE.value]:
            retrieved_tuple = query_ntode(tuple_rui, tx)
        case [NodeLabels.NtoLackR.value]:
            retrieved_tuple = query_ntolackr(tuple_rui, tx)
        case _:
            raise ValueError(f"Unknown tuple type for labels: {labels}")
    return retrieved_tuple

def query_an(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (an:{NodeLabels.AN.value} {{rui: $rui}})
        OPTIONAL MATCH (an)-[:{RelationshipLabels.ruin.value}]->(npor:{NodeLabels.NPoR.value})
        RETURN an.ar AS ar, an.unique AS unique, an.rui AS rui, npor.rui AS ruin
    """, rui=str(rui))
    
    record = result.single()
    if record:
        return ANTuple(**neo4j_to_rttuple(record))
    return None

def query_ar(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (ar:{NodeLabels.AR.value} {{rui: $rui}})
        OPTIONAL MATCH (ar)-[:{RelationshipLabels.ruir.value}]->(rpor:{NodeLabels.RPoR.value})
        RETURN ar.ar AS ar, ar.unique AS unique, ar.ruio AS ruio, ar.rui AS rui, rpor.rui AS ruir
    """, rui=str(rui))
    
    record = result.single()
    if record:
        return ARTuple(**neo4j_to_rttuple(record))
    return None

def query_di(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (di:{NodeLabels.DI.value} {{rui: $rui}})
        OPTIONAL MATCH (di)-[:{RelationshipLabels.ruit.value}]->(ruit)
        OPTIONAL MATCH (di)-[:{RelationshipLabels.ruid.value}]->(ruid)
        OPTIONAL MATCH (di)-[:{RelationshipLabels.ruia.value}]->(ruia)
        OPTIONAL MATCH (di)-[:{RelationshipLabels.ta.value}]->(ta)
        RETURN di.t AS t, di.event_reason AS event_reason, di.rui AS rui, ta.rui AS ta,
               ruit.rui AS ruit, ruid.rui AS ruid, ruia.rui AS ruia
    """, rui=str(rui))
    
    record = result.single()
    if record:
        return DITuple(**neo4j_to_rttuple(record))
    return None

def query_dc(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (dc:{NodeLabels.DC.value} {{rui: $rui}})
        OPTIONAL MATCH (dc)-[:{RelationshipLabels.ruit.value}]->(ruit)
        OPTIONAL MATCH (dc)-[:{RelationshipLabels.ruid.value}]->(ruid)
        OPTIONAL MATCH (dc)-[rel:{RelationshipLabels.replacement.value}]->(replacement)
        RETURN dc.t AS t, dc.event_reason AS event_reason, dc.event AS event, dc.rui AS rui,
               ruit.rui AS ruit, ruid.rui AS ruid, replacement.rui AS replacement_rui, rel.replacements AS replacements
        ORDER BY rel.replacements
    """, rui=str(rui))

    records = result.data()

    if records:
        first_record = records[0]  
        replacements = []
        for record in records:
            if record["replacement_rui"]:
                replacements.append((record["replacement_rui"], record["replacements"]))

        replacements = sorted(replacements, key=lambda x: x[1])
        ordered_replacements = [rui for rui, _ in replacements]
        attributes = {
            "t": first_record["t"],
            "event_reason": first_record["event_reason"],
            "event": first_record["event"],
            "rui": first_record["rui"],
            "ruit": first_record["ruit"],
            "ruid": first_record["ruid"],
            "replacements": ordered_replacements  
        }

        return DCTuple(**neo4j_to_rttuple(attributes))

    return None


def query_f(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (f:{NodeLabels.F.value} {{rui: $rui}})
        OPTIONAL MATCH (f)-[:{RelationshipLabels.ruitn.value}]->(ruitn)
        RETURN f.C AS C, f.rui AS rui, ruitn.rui as ruitn
    """, rui=str(rui))
    
    record = result.single()
    if record:
        return FTuple(**neo4j_to_rttuple(record))
    return None

def query_nton(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (nton:{NodeLabels.NtoN.value} {{rui: $rui}})
        OPTIONAL MATCH (nton)-[:{RelationshipLabels.r.value}]->(r)
        OPTIONAL MATCH (nton)-[:{RelationshipLabels.tr.value}]->(tr)
        OPTIONAL MATCH (nton)-[rel:{RelationshipLabels.p_list.value}]->(p)
        RETURN nton.polarity AS polarity, nton.rui AS rui, r.rui AS r, tr.rui AS tr, p.rui AS p_rui, rel.p AS p
        ORDER BY p
    """, rui=str(rui))

    records = result.data()

    if records:
        first_record = records[0]

        p_list = []
        for record in records:
            if record["p_rui"]:
                p_list.append((record["p_rui"], record["p"]))

        p_list = sorted(p_list, key=lambda x: x[1])

        ordered_p_list = [rui for rui, _ in p_list]

        attributes = {
            "polarity": first_record["polarity"],
            "rui": first_record["rui"],
            "r": first_record["r"],
            "tr": first_record["tr"],
            "p": ordered_p_list
        }

        return NtoNTuple(**neo4j_to_rttuple(attributes))
    return None


def query_ntor(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (ntor:{NodeLabels.NtoR.value} {{rui: $rui}})
        OPTIONAL MATCH (ntor)-[:{RelationshipLabels.ruin.value}]->(ruin)
        OPTIONAL MATCH (ntor)-[:{RelationshipLabels.ruir.value}]->(ruir)
        OPTIONAL MATCH (ntor)-[:{RelationshipLabels.r.value}]->(r)
        OPTIONAL MATCH (ntor)-[:{RelationshipLabels.tr.value}]->(tr)
        RETURN ntor.polarity AS polarity, ntor.rui AS rui, ruin.rui AS ruin, ruir.rui AS ruir, tr.rui AS tr, r.rui AS r
    """, rui=str(rui))

    record = result.single()
    if record:
        return NtoRTuple(**neo4j_to_rttuple(record))
    return None


def query_ntolackr(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (ntolackr:{NodeLabels.NtoLackR.value} {{rui: $rui}})
        OPTIONAL MATCH (ntolackr)-[:{RelationshipLabels.ruin.value}]->(ruin)
        OPTIONAL MATCH (ntolackr)-[:{RelationshipLabels.ruir.value}]->(ruir)
        OPTIONAL MATCH (ntolackr)-[:{RelationshipLabels.r.value}]->(r)
        OPTIONAL MATCH (ntolackr)-[:{RelationshipLabels.tr.value}]->(tr)
        RETURN ntolackr.rui AS rui, ruin.rui AS ruin, ruir.rui AS ruir, tr.rui AS tr, r.rui AS r
    """, rui=str(rui))

    record = result.single()
    if record:
        return NtoLackRTuple(**neo4j_to_rttuple(record))
    return None

def query_ntoc(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (ntoc:{NodeLabels.NtoC.value} {{rui: $rui}})
        OPTIONAL MATCH (ntoc)-[:{RelationshipLabels.r.value}]->(r)
        OPTIONAL MATCH (ntoc)-[:{RelationshipLabels.ruin.value}]->(ruin)
        OPTIONAL MATCH (ntoc)-[:{RelationshipLabels.code.value}]->(code_node)-[:{RelationshipLabels.ruics.value}]->(ruics)
        OPTIONAL MATCH (ntoc)-[:{RelationshipLabels.tr.value}]->(tr)
        RETURN ntoc.polarity AS polarity, ntoc.rui AS rui, r.rui AS r, ruin.rui AS ruin, 
               code_node.code AS code, ruics.rui AS ruics, tr.rui AS tr
    """, rui=str(rui))

    record = result.single()

    if record:
        return NtoCTuple(**neo4j_to_rttuple(record))

    return None

def query_ntode(rui: Rui, tx):
    result = tx.run(f"""
        MATCH (ntode:{NodeLabels.NtoDE.value} {{rui: $rui}})
        OPTIONAL MATCH (ntode)-[:{RelationshipLabels.ruin.value}]->(ruin)
        OPTIONAL MATCH (ntode)-[:{RelationshipLabels.data.value}]->(data_node)-[:{RelationshipLabels.ruidt.value}]->(ruidt)
        RETURN ntode.polarity AS polarity, ntode.rui AS rui, ruin.rui AS ruin, 
               data_node.data AS data, ruidt.rui AS ruidt
    """, rui=str(rui))

    record = result.single()

    if record:
        record_dict = dict(record)

        return NtoDETuple(**neo4j_to_rttuple(record_dict))

    return None


"""Removes a key from a dictionary and returns the value"""
def pop_key(dict, key):
    value = dict[key]
    del dict[key]
    return value

