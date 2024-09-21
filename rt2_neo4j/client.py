from rt_core_v2.ids_codes.rui import Rui
from rt_core_v2.rttuple import RtTuple
from rt_core_v2.persist.rts_store import RtStore
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor

class Neo4jRtStore(RtStore):

    def __init__(self, uri, auth, config={}):
        self.driver = GraphDatabase.driver(uri, auth=auth, **config)
        self.insertion_visitor = TupleInsertionVisitor(self.driver)

    def save_tuple(self, tup: RtTuple) -> bool:
        tup.accept(self.insertion_visitor)

    def get_tuple(self, rui: Rui) -> RtTuple:
        name = "n"
        # query = query_rui_node()
        # query += f'RETURN {name}'
        # with self.driver.session() as session:
        #     result = session.run(query, value=rui.lower())
            
        #     node = None
        #     label = None
            
        #     for record in result:
        #         n = record.get("n")
        #         labels = record.get("labels(n)")
        #         if n:
        #             node = n
        #         if labels:
        #             for lbl in labels:
        #                 if lbl != "tuple":
        #                     label = lbl
        #                     break
        #         if node and label:
        #             break
            
        #     # Assuming `reconstitute_tuple` is a method you have to construct RtsTuple
        #     tuple = self.reconstitute_tuple(node, label, rui) if node and label else None
        #     return tuple

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
        pass

    def save_rts_declaration(self, declaration) -> bool:
        pass

