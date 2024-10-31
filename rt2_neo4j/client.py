from rt_core_v2.ids_codes.rui import Rui, ID_Rui, ISO_Rui
from rt_core_v2.rttuple import RtTuple, RtTupleVisitor, TupleType, TupleComponents
from rt_core_v2.persist.rts_store import RtStore, TupleQuery
from neo4j import GraphDatabase
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query, Neo4jEntryConverter, NodeLabels, RelationshipLabels
import base64

class Neo4jRtStore(RtStore):
    """Handles storage and retrieval operations in a Neo4j database for RtTuple instances."""

    def __init__(self, uri, auth, config={}):
        """Initializes the Neo4j driver, insertion visitor, and transaction manager.
        
        Args:
            uri (str): URI for connecting to the Neo4j database.
            auth (tuple): Authentication credentials for the Neo4j database.
            config (dict): Additional configuration settings for the Neo4j driver.
        """
        self.driver = GraphDatabase.driver(uri, auth=auth, **config)
        self.insertion_visitor = TupleInsertionVisitor()
        self.transaction_manager = TransactionManager(self.driver)

    def save_tuple(self, tup: RtTuple) -> bool:
        """Saves a tuple to the Neo4j database using the insertion visitor.
        
        Args:
            tup (RtTuple): Tuple to be saved in the database.

        Returns:
            bool: True if the tuple was saved successfully, False otherwise.
        """
        tx = self.transaction_manager.start_transaction()
        self.insertion_visitor.set_transaction(tx)
        tup.accept(self.insertion_visitor)

    def get_tuple(self, rui: Rui) -> RtTuple:
        """Retrieves a tuple from the Neo4j database based on a unique identifier.
        
        Args:
            rui (Rui): Unique identifier of the tuple to be retrieved.

        Returns:
            RtTuple: The tuple instance retrieved from the database.
        """
        tx = self.transaction_manager.start_transaction()
        return tuple_query(rui, tx)

    def get_by_referent(self, rui: Rui) -> set[RtTuple]:
        """Retrieves all tuples in the database that reference a specific entity.
        
        Args:
            rui (Rui): Unique identifier of the referent.

        Returns:
            set[RtTuple]: Set of tuples that reference the specified entity.
        """
        pass

    def get_by_author(self, rui: Rui) -> list[RtTuple]:
        """Retrieves all tuples created by a specific author.
        
        Args:
            rui (Rui): Unique identifier of the author.

        Returns:
            list[RtTuple]: List of tuples authored by the specified entity.
        """
        tx = self.transaction_manager.start_transaction()
        result = tx.run(f"""
            MATCH (d_tuple:{NodeLabels.DI.value})-[:ruia]->(author {{rui: $ruia}})
            MATCH (d_tuple)-[:ruit]->(ruit_tuple)
            RETURN ruit_tuple.rui AS rui
        """, ruia=str(rui))
        return [self.get_tuple(Neo4jEntryConverter.str_to_rui(record["rui"])) for record in result]

    def get_available_rui(self) -> list[Rui]:
        """Retrieves all RUIs available in the Neo4j database.
        
        Returns:
            list[Rui]: List of available RUIs in the database.
        """
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
        """Appends a Cypher match condition for a relationship between nodes.
        
        This method builds a segment of the Cypher query that matches a relationship
        between the main node 'n' and a secondary node, connected via a specified relationship type.

        Args:
            match_conditions (list): List that accumulates the Cypher match conditions.
            rel (str): The type of relationship (label) between nodes.
            node_name (str): Name of the secondary node in the relationship.
            node_label (str): Label of the secondary node type in the relationship.
        """
        match_conditions.append(f",(n)-[:{rel}]-({node_name}:{node_label})")

    def build_where(self, match_where, node_name, property_name, value):
        """Appends a Cypher where clause condition to filter nodes by property values.
        
        Builds a segment of the Cypher query that specifies a filtering condition based on 
        node properties, ensuring the query only matches nodes with the given property and value.

        Args:
            match_where (list): List that accumulates the Cypher where clause conditions.
            node_name (str): Name of the node being filtered.
            property_name (str): Property of the node to filter by.
            value (str): Value the node property should match.
        """
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
        """Commits the current transaction.
        
        Ensures that any changes made within the transaction are saved to the database.
        """
        self.transaction_manager.commit_transaction()

    def rollback(self):
        """Rolls back the current transaction.
        
        Reverts any changes made within the transaction, restoring the database to its previous state.
        """
        self.transaction_manager.rollback_transaction()
    
    def shut_down(self):
        """Closes the transaction manager and the Neo4j driver connection.
        
        Releases resources associated with the database connection, finalizing any pending actions.
        """
        self.transaction_manager.close()
        self.driver.close()


class TransactionManager:
    """Manages transactions for Neo4j database interactions.
    
    This class ensures safe and consistent transaction handling by starting,
    committing, and rolling back transactions, while managing the associated session.
    """
    
    def __init__(self, driver):
        """Initializes the TransactionManager with the given Neo4j driver.
        
        Args:
            driver: Neo4j driver instance used to create sessions and transactions.
        """
        self.driver = driver
        self.current_tx = None
        self.session = None

    def start_transaction(self):
        """Begins a new transaction if no transaction is active.
        
        Returns:
            The active transaction, either a newly started one or an existing transaction.
        """
        if self.current_tx is None:
            self.session = self.driver.session()
            self.current_tx = self.session.begin_transaction()
        return self.current_tx

    def commit_transaction(self):
        """Commits the current transaction if it exists and closes the session.
        
        Ensures that any changes made within the transaction are saved to the database.
        """
        if self.current_tx is not None:
            self.current_tx.commit()
            self.current_tx = None
            self.session.close()

    def rollback_transaction(self):
        """Rolls back the current transaction if it exists and closes the session.
        
        Reverts any changes made within the transaction, restoring the database to its previous state.
        """
        if self.current_tx is not None:
            self.current_tx.rollback()
            self.current_tx = None
            self.session.close()

    def close(self):
        """Closes the current transaction manager session safely.
        
        Rolls back any ongoing transaction to ensure that no partial changes remain uncommitted.
        """
        if self.current_tx is not None:
            self.rollback_transaction()


