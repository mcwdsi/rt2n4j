from rt_core_v2.rttuple import RtTupleVisitor, RtTuple, ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, TupleType, TupleComponents, AttributesVisitor, TempRef
from rt_core_v2.ids_codes.rui import Rui, Relationship
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
    def __init__(self, driver):
        self.driver = driver

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

        """
        attributes = host.accept(self.get_attr)
        pop_key(attributes, TupleComponents.type.value)
        query = None
        attributes = {key: str(value) for key, value in attributes.items()}
        #TODO Move session control out of insertion and into RtStore
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                match host.tuple_type:
                    case TupleType.AN:
                        query = self.visit_an(host, attributes, tx)
                    case TupleType.AR:
                        query = self.visit_ar(host, attributes, tx)
                    case TupleType.DI:
                        query = self.visit_di(host, attributes, tx)
                    case TupleType.DC:
                        query = self.visit_dc(host, attributes, tx)
                    case TupleType.F:
                        query = self.visit_f(host, attributes, tx)
                    case TupleType.NtoN:
                        query = self.visit_nton(host, attributes, tx)
                    case TupleType.NtoR:
                        query = self.visit_ntor(host, attributes, tx)
                    case TupleType.NtoC:
                        query = self.visit_ntoc(host, attributes, tx)
                    case TupleType.NtoDE:
                        query = self.visit_ntode(host, attributes, tx)
                    case TupleType.NtoLackR:
                        query = self.visit_ntolackr(host, attributes, tx)
        return query

    def visit_an(self, host: ANTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an ANTuple.

        Args:
            host (ANTuple): The ANTuple instance.
            attributes (dict): Attributes of the ANTuple.

        """
        return tx.run(f"""
               CREATE (an:{NodeLabels.AN.value} {{rui: $rui, ar: $ar, unique: $unique}}) 
               CREATE (npor:{NodeLabels.NPoR.value} {{rui:$ruin}})
               CREATE (an)-[:{RelationshipLabels.ruin.value}]->(npor)
               """, **attributes)
        
    
    def visit_ar(self, host: ARTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an ARTuple.

        Args:
            host (ARTuple): The ARTuple instance.
            attributes (dict): Attributes of the ARTuple.

        """
        return tx.run(f"""
               CREATE (ar:{NodeLabels.AR.value} {{rui: $rui, ar: $ar, unique: $unique, ruio: $ruio}}) 
               CREATE (rpor:{NodeLabels.RPoR.value} {{rui:$ruir}})
               CREATE (ar)-[:{RelationshipLabels.ruir.value}]->(rpor)
               """, **attributes)

    def visit_di(self, host: DITuple, attributes: dict, tx):
        """
        Generates a Cypher query for a DITuple.

        Args:
            host (DITuple): The DITuple instance.
            attributes (dict): Attributes of the DITuple.

        """
        return tx.run(f"""
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

    def visit_dc(self, host: DCTuple, attributes: dict, tx):
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
        return tx.run(query, **attributes)

    def visit_f(self, host: FTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an FTuple.

        Args:
            host (FTuple): The FTuple instance.
            attributes (dict): Attributes of the FTuple.

        """
        return tx.run(f"""
               MATCH (tup {{rui:$ruitn}})
               CREATE (f:{NodeLabels.F.value} {{rui: $rui, C: $C}}) 
               CREATE (f)-[:{RelationshipLabels.ruitn.value}]->(tup)
               """, **attributes)

    # TODO Figure out how to implement relationship nodes
    def visit_nton(self, host: NtoNTuple, attributes: dict, tx):
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
        return tx.run(query, **attributes)
    
    def visit_ntor(self, host: NtoRTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an NtoRTuple.

        Args:
            host (NtoRTuple): The NtoRTuple instance.
            attributes (dict): Attributes of the NtoRTuple.

        """
        return tx.run(f"""
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

    # TODO Implement
    def visit_ntoc(self, host: NtoCTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an NtoCTuple.

        Args:
            host (NtoCTuple): The NtoCTuple instance.
            attributes (dict): Attributes of the NtoCTuple.

        """
        labels = [NodeLabels.NtoC.value]

    #TODO Implement
    def visit_ntode(self, host: NtoDETuple, attributes: dict, tx):
        """
        Generates a Cypher query for an NtoDETuple.

        Args:
            host (NtoDETuple): The NtoDETuple instance.
            attributes (dict): Attributes of the NtoDETuple.

        """
        labels = [NodeLabels.NtoDE.value]

    
    def visit_ntolackr(self, host: NtoLackRTuple, attributes: dict, tx):
        """
        Generates a Cypher query for an NtoLackRTuple.

        Args:
            host (NtoLackRTuple): The NtoLackRTuple instance.
            attributes (dict): Attributes of the NtoLackRTuple.

        """
        return tx.run(f"""
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

"""Removes a key from a dictionary and returns the value"""
def pop_key(dict, key):
    value = dict[key]
    del dict[key]
    return value

