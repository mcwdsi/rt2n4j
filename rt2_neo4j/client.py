from rt_core_v2.ids_codes.rui import Rui
from rt_core_v2.rttuple import RtTuple, RtTupleVisitor, TupleType, TupleComponents
from rt_core_v2.persist.rts_store import RtStore
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query, Neo4jEntryConverter

class Neo4jRtStore(RtStore):

    def __init__(self, uri, auth, config={}):
        self.driver = GraphDatabase.driver(uri, auth=auth, **config)
        self.insertion_visitor = TupleInsertionVisitor(self.driver)
        self.transaction_manager = TransactionManager(self.driver)

    def save_tuple(self, tup: RtTuple) -> bool:
        tx = self.transaction_manager.start_transaction()
        tup.accept(self.insertion_visitor, tx)

    def get_tuple(self, rui: Rui) -> RtTuple:
        tx = self.transaction_manager.start_transaction()
        return tuple_query(rui, tx)

    #TODO Decide if this is worth keeping
    def get_by_referent(self, rui: Rui) -> set[RtTuple]:
        pass

    def get_by_author(self, rui: Rui) -> set[RtTuple]:
        tx = self.transaction_manager.start_transaction()
        result = tx.run(f"""
            MATCH (d_tuple:D)-[:ruia]->(author {{rui: $ruia}})
            MATCH (d_tuple)-[:ruit]->(ruit_tuple)
            RETURN ruit_tuple
        """, ruia=str(rui))
        return set([tuple_query(Neo4jEntryConverter.str_to_rui(record["ruit_tuple"]["rui"]), tx) for record in result])


    def get_available_rui(self) -> Rui:
        tx = self.transaction_manager.start_transaction()
        result = tx.run("""
            MATCH (n) WHERE EXISTS(n.rui)
            RETURN n.rui AS rui
        """)
        return set(Neo4jEntryConverter.lst_to_ruis([record["rui"] for record in result]))

    def get_referents_by_type_and_designator_type(self, referent_type: Rui, designator_type: Rui, designator_txt: str) -> Set[RtTuple]:
        """
        Retrieve referents based on a designator type and concretized string (designator_txt).
        
        Args:
            referent_type (Rui): The Rui of the referent type (e.g., type of entity).
            designator_type (Rui): The Rui of the designator type (e.g., name, identifier).
            designator_txt (str): The exact string by which the designator is concretized.

        Returns:
            Set[RtTuple]: A set of RtTuples representing the referents.
        """

        #TODO Look into changing this query rui
        query = """
        MATCH (n:instance)-[r1:iuip]-(o:U)-[:uui]->(q:universal), 
              (n)-[p1:p]-(n2:NtoP)-[r:relation]->(n3:R), 
              (n2)-[p2:p]->(n4), 
              (n)-[r2:iuip]-(n5:NtoDE)-[:dr]->(n6:data) 
        WHERE q.rui = $designatorType 
          AND n3.rui = "http://purl.obolibrary.org/obo/IAO_0000219" 
          AND n6.dr = $designatorTxt 
        RETURN n4
        """

        parameters = {
            "designatorType": str(designator_type),
            "designatorTxt": designator_txt
        }

        result_set = set()
        tx = self.transaction_manager.start_transaction()  # Start the transaction
        result = tx.run(query, parameters)

        for record in result:
            node = record["n4"]
            if "instance" in node.labels:
                # Handling "instance" label (assuming it's similar to IUI)
                rui_txt = Rui(node.get("iui"))
                result_set.add(iui)
            elif "temporal_region" in node.labels:
                # Handling "temporal_region"
                temporal_ref = self.get_temporal_reference_from_db(node)
                result_set.add(temporal_ref)
        return result_set

    def run_query(self, query) -> set[RtTuple]:
        pass

    def commit(self):
        self.transaction_manager.commit_transaction()

    def rollback(self):
        self.transaction_manager.rollback_transaction()
    
    def shut_down(self):
        self.transaction_manager.close()
        self.driver.close()


#TODO Merge this with the RtStore implementation in order to manage the transactions
class TransactionManager:
    def __init__(self, driver):
        self.driver = driver
        self.current_tx = None

    def start_transaction(self):
        if self.current_tx is None:
            session = self.driver.session()
            self.current_tx = session.begin_transaction()
        return self.current_tx

    def commit_transaction(self):
        if self.current_tx is not None:
            self.current_tx.commit()
            self.current_tx = None

    def rollback_transaction(self):
        if self.current_tx is not None:
            self.current_tx.rollback()
            self.current_tx = None

    def close(self):
        if self.current_tx is not None:
            self.rollback_transaction()

