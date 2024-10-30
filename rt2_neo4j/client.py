from rt_core_v2.ids_codes.rui import Rui
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

    def run_query(self, query: TupleQuery) -> set:
        match_conditions = []
        match_where = []

        # Authoring time
        if query.ta:
            node_name = "nta"
            match_conditions.append(f"MATCH (n)-[:ta]->({node_name}:{NodeLabels.Temporal.value})")
            match_where.append(f"{node_name}.tref = '{query.ta}'")

        # Author Rui
        if query.author_rui:
            node_name = "niuia"
            match_conditions.append(f"MATCH (n)-[:iuia]->({node_name}:{NodeLabels.NPoR.value})")
            match_where.append(f"{node_name}.iui = '{query.author_rui}'")

        # Change reason
        if query.change_reason:
            match_where.append(f"n.c = '{query.change_reason}'")

        # Tuple Rui
        if query.rui:
            match_where.append(f"n.rui = '{query.rui}'")

        # Data field
        if query.data:
            node_name = "ndr"
            data_as_string = base64.b64encode(query.data).decode('utf-8')
            match_conditions.append(f"MATCH (n)-[:dr]->({node_name}:{NodeLabels.Data.value})")
            match_where.append(f"{node_name}.dr = '{data_as_string}'")

        # Data type Rui
        if query.datatype:
            node_name = "nuui"
            match_conditions.append(f"MATCH (n)-[:uui]->({node_name}:Type)")
            match_where.append(f"{node_name}.uui = '{query.datatype}'")

        # Error code
        if query.change_code:
            match_where.append(f"n.e = '{query.change_code}'")

        # Naming system Rui
        if query.universal_rui:
            node_name = "niuins"
            match_conditions.append(f"MATCH (n)-[:iuins]->({node_name}:Instance)")
            match_where.append(f"{node_name}.iui = '{query.universal_rui}'")

        # Referent Rui
        if query.relationship_rui:
            node_name = "niuip"
            match_conditions.append(f"MATCH (n)-[:iuip]->({node_name}:Instance)")
            match_where.append(f"{node_name}.iui = '{query.relationship_rui}'")

        # Relationship Uui
        if query.relationship_rui:
            node_name = "nr"
            match_conditions.append(f"MATCH (n)-[:r]->({node_name}:Relation)")
            match_where.append(f"{node_name}.rui = '{query.relationship_rui}'")

        # Temporal reference
        if query.tr:
            match_conditions.append(f"MATCH (n)-[:tr]->(tr:TemporalRegion)")
            match_where.append(f"tr.tref = '{query.tr}'")

        # Particular reference list (p_list)
        if query.p_list:
            p_seq = 1
            for pr in query.p_list:
                node_name = f"p{p_seq}"
                label = "Instance" if isinstance(pr, Rui) else "TemporalRegion"
                prop_name = "iui" if isinstance(pr, Rui) else "tref"
                match_conditions.append(f"MATCH (n)-[:p]->({node_name}:{label})")
                match_where.append(f"{node_name}.{prop_name} = '{pr}'")
                p_seq += 1

        # Tuple types
        query_match = []
        if query.types:
            for tup_type in query.types:
                query_match.append(f"MATCH (n:{tup_type})")
                query_match.extend(match_conditions)
                if match_where:
                    query_match.append(f"WHERE {' AND '.join(match_where)}")
                query_match.append("RETURN n AS aResult")
                if tup_type != list(query.types)[-1]:
                    query_match.append("UNION")
        else:
            query_match.append("MATCH (n:tuple)")
            query_match.extend(match_conditions)
            if match_where:
                query_match.append(f"WHERE {' AND '.join(match_where)}")
            query_match.append("RETURN n AS aResult")

        # Build the final query string
        query_string = "\n".join(query_match)

        result_set = set()
        tx = self.transaction_manager.start_transaction
        result = tx.run(query_string)
        for record in result:
            node = record["aResult"]
            rui = node["rui"]
            result_set.add(self.get_tuple(rui))
        tx.commit()

        return result_set

    def reconstitute_tuple(self, node, label, rui):
        # Recreate the RtsTuple
        # This function is a placeholder and should be implemented to properly convert the node to an RtsTuple
        pass


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

