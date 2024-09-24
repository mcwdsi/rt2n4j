from rt_core_v2.ids_codes.rui import Rui
from rt_core_v2.rttuple import RtTuple, RtTupleVisitor, TupleType, TupleComponents
from rt_core_v2.persist.rts_store import RtStore
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query

class Neo4jRtStore(RtStore):

    def __init__(self, uri, auth, config={}):
        self.driver = GraphDatabase.driver(uri, auth=auth, **config)
        self.insertion_visitor = TupleInsertionVisitor(self.driver)

    def save_tuple(self, tup: RtTuple) -> bool:
        tup.accept(self.insertion_visitor)

    def get_tuple(self, rui: Rui) -> RtTuple:
        return tuple_query(rui, self.driver)

    def get_by_referent(self, rui: Rui) -> set[RtTuple]:
        pass

    def get_by_author(self, rui: Rui) -> Rui:
        pass

    def get_available_rui(self) -> Rui:
        pass

    def get_by_type(self, referentType, designatorType, designatorText) -> set:
        pass

    def run_query(self, query) -> set[RtTuple]:
        pass

    def shut_down(self):
        self.driver.close()

    def commit(self):
        #TODO Still figure out if I want transactions to be on a per commit or per function call basis
        pass
    
    #TODO Remove this function from superclass
    def save_rts_declaration(self, declaration) -> bool:
        pass
