from rt_core_v2.ids_codes.rui import Rui, ID_Rui, ISO_Rui
from rt_core_v2.rttuple import RtTuple, RtTupleVisitor, TupleType, TupleComponents
from rt_core_v2.persist.rts_store import RtStore, TupleQuery
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query, Neo4jEntryConverter, NodeLabels, RelationshipLabels
import base64

class Neo4jRtStore(RtStore):

    def __init__(self, uri, auth, config={}):
        self.driver = GraphDatabase.driver(uri, auth=auth, **config)
        self.insertion_visitor = TupleInsertionVisitor()
        self.transaction_manager = TransactionManager(self.driver)

    def save_tuple(self, tup: RtTuple) -> bool:
        tx = self.transaction_manager.start_transaction()
        self.insertion_visitor.set_transaction(tx)
        tup.accept(self.insertion_visitor)

    def get_tuple(self, rui: Rui) -> RtTuple:
        tx = self.transaction_manager.start_transaction()
        return tuple_query(rui, tx)

    #TODO Decide if this is worth keeping
    def get_by_referent(self, rui: Rui) -> set[RtTuple]:
        pass

    def get_by_author(self, rui: Rui) -> list[RtTuple]:
        tx = self.transaction_manager.start_transaction()
        result = tx.run(f"""
            MATCH (d_tuple:{NodeLabels.DI.value})-[:ruia]->(author {{rui: $ruia}})
            MATCH (d_tuple)-[:ruit]->(ruit_tuple)
            RETURN ruit_tuple.rui AS rui
        """, ruia=str(rui))
        return [self.get_tuple(Neo4jEntryConverter.str_to_rui(record["rui"])) for record in result]


    def get_available_rui(self) -> list[Rui]:
        tx = self.transaction_manager.start_transaction()
        result = tx.run("""
            MATCH (n) WHERE n.rui IS NOT NULL
            RETURN n.rui AS rui
        """)
        return Neo4jEntryConverter.lst_to_ruis([record["rui"] for record in result])


    def get_referents_by_type_and_designator_type(self, referent_type: Rui, designator_type: Rui, designator_txt: bytes) -> set[RtTuple]:
        """
        Retrieve referents based on a designator type and concretized string (designator_txt).
        
        Args:
            referent_type (Rui): The Rui of the referent type (e.g., type of entity).
            designator_type (Rui): The Rui of the designator type (e.g., name, identifier).
            designator_txt (str): The exact string by which the designator is concretized.

        Returns:
            Set[RtTuple]: A set of RtTuples representing the referents.
        """
        designator_str = base64.b64encode(designator_txt).decode('utf-8')
        query = f"""
        MATCH (n:{NodeLabels.NPoR.value})-[r1:{RelationshipLabels.ruin.value}]-(o:{NodeLabels.NtoR.value})-[:{RelationshipLabels.ruir.value}]->(q:{NodeLabels.RPoR.value}), 
              (n)-[p1:{RelationshipLabels.p_list.value}]-(n2:{NodeLabels.NtoN.value})-[r:{RelationshipLabels.r.value}]->(n3:{NodeLabels.Relation.value}), 
              (n2)-[p2:{RelationshipLabels.p_list.value}]->(n4), 
              (n)-[r2:{RelationshipLabels.ruin.value}]-(n5:{NodeLabels.NtoDE.value})-[:{RelationshipLabels.data.value}]->(n6:{NodeLabels.Data.value}) 
        WHERE q.uui = $designator_type 
          AND n3.uri = "http://purl.obolibrary.org/obo/IAO_0000219" 
          AND n6.data = $designator_str
        RETURN n4
        """
        parameters = {
            "designator_type": str(designator_type),
            "designator_str": designator_str
        }

        result_set = []
        tx = self.transaction_manager.start_transaction()  # Start the transaction
        result = tx.run(query, parameters)

        for record in result:
            node = record["n4"]
            if NodeLabels.NPoR.value in node.labels:
                result_set.append(Neo4jEntryConverter.str_to_rui(node.get("rui")))
        return result_set

    def build_condition(self, match_conditions, rel, node_name, node_label):
        match_conditions.append(f",(n)-[:{rel}]-({node_name}:{node_label})")

    def build_where(self, match_where, node_name, property_name, value):
        if match_where:
            match_where.append(" AND ")
        match_where.append(f"{node_name}.{property_name}='{value}'")

    def run_query(self, tuple_query: TupleQuery):
        match_conditions = []
        match_where = []

        if tuple_query.rui:
            self.build_where(match_where, "n", "rui", str(tuple_query.rui))

        if tuple_query.repeatable_rui:
            self.build_condition(match_conditions, RelationshipLabels.ruir.value, "nrui", NodeLabels.RPoR.value)
            self.build_where(match_where, "nrui", "uui", str(tuple_query.repeatable_rui))

        if tuple_query.nonrepeatable_rui:
            self.build_condition(match_conditions, RelationshipLabels.ruin.value, "nnrui", NodeLabels.NPoR.value)
            self.build_where(match_where, "nnrui", "rui", str(tuple_query.nonrepeatable_rui))

        #TODO Figure out how if querying over timestamps should query only over d-tuples or their referents too
        # if tuple_query.begin_timestamp:
        #     self.build_where(match_where, "n", "beginTimestamp", str(tuple_query.begin_timestamp))

        # if tuple_query.end_timestamp:
        #     self.build_where(match_where, "n", "endTimestamp", str(tuple_query.end_timestamp))

        if tuple_query.ta:
            self.build_condition(match_conditions, RelationshipLabels.ta.value, "nta", NodeLabels.Temporal.value)
            self.build_where(match_where, "nta", "rui", str(tuple_query.ta))

        if tuple_query.tr:
            self.build_condition(match_conditions, RelationshipLabels.tr.value, "ntr", NodeLabels.Temporal.value)
            self.build_where(match_where, "ntr", "rui", str(tuple_query.tr))

        #TODO Check if this has to be lower
        if tuple_query.polarity is not None:
            self.build_where(match_where, "n", "polarity", str(tuple_query.polarity).lower())

        if tuple_query.change_code:
            self.build_where(match_where, "n", "event_reason", str(tuple_query.change_code))

        if tuple_query.change_reason:
            self.build_where(match_where, "n", "c", str(tuple_query.change_reason))

        if tuple_query.concept_code:
            self.build_condition(match_conditions, RelationshipLabels.code, "ncode", NodeLabels.Concept)
            self.build_where(match_where, "ncode", "code", tuple_query.concept_code)

        if tuple_query.confidence is not None:
            self.build_where(match_where, "n", "C", str(tuple_query.confidence))

        if tuple_query.data:
            data_as_str = base64.b64encode(tuple_query.data).decode('utf-8')
            self.build_condition(match_conditions, "dr", "ndr", NodeLabels.Data.value)
            self.build_where(match_where, "ndr", "dr", data_as_str)

        # if tuple_query.datatype:
        #     self.build_condition(match_conditions, "uui", "nuui", "TYPE")
        #     self.build_where(match_where, "nuui", "uui", str(tuple_query.datatype))

        if tuple_query.relationship:
            self.build_condition(match_conditions, "r", "nr", NodeLabels.Relation.value)
            self.build_where(match_where, "nr", "uri", str(tuple_query.relationship))

        #TODO Ensure that order is enforced for p_list and replacements
        if tuple_query.p_list:
            for idx, ref in enumerate(tuple_query.p_list):
                node_name = f"p{idx}"
                self.build_condition(match_conditions, "p", node_name, "")
                self.build_where(match_where, node_name, "rui", str(ref))

        if tuple_query.replacements:
            for idx, replacement in enumerate(tuple_query.replacements):
                node_name = f"replacement{idx}"
                self.build_condition(match_conditions, "replacements", node_name, "")
                self.build_where(match_where, node_name, "rui", str(replacement))

        types = tuple_query.types
        query_match = []
        
        if types:
            for tup_type in types:
                query_match.append(f"OPTIONAL MATCH (n:{tup_type})")
                query_match.extend(match_conditions)
                if match_where:
                    query_match.append(" WHERE ")
                    query_match.extend(match_where)
                query_match.append(" RETURN n as aResult")
                if tup_type != types[-1]:
                    query_match.append(" UNION ")
        else:
            query_match.append(f"MATCH (n:)")
            query_match.extend(match_conditions)
            if match_where:
                query_match.append(" WHERE ")
                query_match.extend(match_where)
            query_match.append(" RETURN n as aResult")

        query = "".join(query_match)
        tx = self.transaction_manager.start_transaction()
        result_set = []
        result = tx.run(query)
        #TODO Make sure that each result is just one tuple and not a list of tuples
        for record in result:
            node = record["aResult"]
            if node:
                result_set.append(node)  

        return result_set


    def commit(self):
        self.transaction_manager.commit_transaction()

    def rollback(self):
        self.transaction_manager.rollback_transaction()
    
    def shut_down(self):
        self.transaction_manager.close()
        self.driver.close()

class TransactionManager:
    def __init__(self, driver):
        self.driver = driver
        self.current_tx = None
        self.session = None

    def start_transaction(self):
        if self.current_tx is None:
            self.session = self.driver.session()
            self.current_tx = self.session.begin_transaction()
        return self.current_tx

    def commit_transaction(self):
        if self.current_tx is not None:
            self.current_tx.commit()
            self.current_tx = None
            self.session.close()

    def rollback_transaction(self):
        if self.current_tx is not None:
            self.current_tx.rollback()
            self.current_tx = None
            self.session.close()

    def close(self):
        if self.current_tx is not None:
            self.rollback_transaction()

