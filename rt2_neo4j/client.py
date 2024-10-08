from rt_core_v2.ids_codes.rui import Rui
from rt_core_v2.rttuple import RtTuple, RtTupleVisitor, TupleType, TupleComponents
from rt_core_v2.persist.rts_store import RtStore
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query

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

        #TODO Convert the ruis to Rui objects
        tuples = set([tuple_query(record["ruit_tuple"]["rui"], self.driver) for record in result])
        return tuples


    def get_available_rui(self) -> Rui:
        tx = self.transaction_manager.start_transaction()
        result = tx.run("""
            MATCH (n) WHERE EXISTS(n.rui)
            RETURN n.rui AS rui
        """)

        #TODO Convert the ruis from strings to uuids
        ruis = set([Rui(record["rui"]) for record in result])
        return ruis

    def get_by_type(self, referentType, designatorType, designatorText) -> set:
        pass

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

