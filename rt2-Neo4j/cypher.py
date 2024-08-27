from enum import Enum
from abc import ABC, abstractmethod
from functools import reduce

class CypherComponent(ABC):
    """
    Abstract base class representing a Cypher component.
    """

    def get_component(self):
        """
        Returns the string representation of the Cypher component.

        Returns:
            str: String representation of the Cypher component.
        """
        return str(self)

class CypherQuery(ABC):
    """
    Abstract base class representing a Cypher query.
    """

    @abstractmethod
    def get_query(self):
        """
        Abstract method to get the Cypher query string.

        Returns:
            str: The Cypher query string.
        """
        pass

    def __str__(self):
        """
        Returns the string representation of the Cypher query.

        Returns:
            str: The Cypher query string.
        """
        return self.get_query()

class CypherNode(CypherComponent):
    """
    Represents a Cypher node.

    Attributes:
        name (str): The name of the node.
        labels (list): List of labels associated with the node.
        attributes (dict): Dictionary of attributes for the node.
    """

    def __init__(self, name, labels, attributes):
        """
        Initializes a CypherNode instance.

        Args:
            name (str): The name of the node.
            labels (list): List of labels for the node.
            attributes (dict): Attributes of the node.
        """
        self.name = name
        self.labels = labels
        self.attributes = attributes

    def __str__(self) -> str:
        """
        Returns the string representation of the Cypher node.

        Returns:
            str: The Cypher node representation in Cypher query format.
        """
        node_attributes = dict_to_attributes(self.attributes)
        return f'({self.name}:{" : ".join(self.labels)} {node_attributes})'

class CypherRelationship(CypherComponent):
    """
    Represents a Cypher relationship.

    Attributes:
        start_node (CypherNode): The start node of the relationship.
        end_node (CypherNode): The end node of the relationship.
        labels (list): List of labels for the relationship.
        attributes (dict): Dictionary of attributes for the relationship.
    """

    def __init__(self, start_node: CypherNode, end_node: CypherNode, labels, attributes):
        """
        Initializes a CypherRelationship instance.

        Args:
            start_node (CypherNode): The start node of the relationship.
            end_node (CypherNode): The end node of the relationship.
            labels (list): Labels for the relationship.
            attributes (dict): Attributes of the relationship.
        """
        self.start_node = start_node
        self.end_node = end_node
        self.labels = labels
        self.attributes = attributes

    def __str__(self) -> str:
        """
        Returns the string representation of the Cypher relationship.

        Returns:
            str: The Cypher relationship representation in Cypher query format.
        """
        edge_attributes = dict_to_attributes(self.attributes)
        return f'({self.start_node.name})-[:{":".join(self.labels)} {edge_attributes}]->({self.end_node.name})'

class CypherOperation(Enum):
    """
    Enum representing different Cypher operations.
    """
    MATCH = "MATCH"
    MERGE = "MERGE"
    CREATE = "CREATE"

class CompoundQuery(CypherQuery):
    """
    Represents a compound query consisting of multiple Cypher queries.

    Attributes:
        queries (list[CypherQuery]): List of CypherQuery objects to be combined.
    """

    def __init__(self, queries: list[CypherQuery]):
        """
        Initializes a CompoundQuery instance.

        Args:
            queries (list[CypherQuery]): List of queries to combine.
        """
        self.queries = queries.copy()

    def get_query(self):
        """
        Combines multiple queries into a single Cypher query string.

        Returns:
            str: Combined Cypher query string.
        """
        return reduce(lambda x, y: f"{x}\n{y}", self.queries, "")

class AtomicEntityQuery(CypherQuery):
    """
    Represents a single atomic Cypher query for a node or relationship.

    Attributes:
        operation (str): The Cypher operation (MATCH, MERGE, CREATE).
        component (CypherComponent): The Cypher component (node or relationship).
    """

    def __init__(self, operation: CypherOperation, component: CypherComponent):
        """
        Initializes an AtomicEntityQuery instance.

        Args:
            operation (CypherOperation): The operation to perform.
            component (CypherComponent): The component to query.
        """
        self.operation = operation.value
        self.component = component

    def get_query(self):
        """
        Returns the Cypher query string for the atomic query.

        Returns:
            str: The Cypher query string.
        """
        return f"{self.operation} {self.component}"

def dict_to_attributes(dct):
    """
    Converts a dictionary of attributes to a Cypher attributes string.

    Args:
        dct (dict): Dictionary of attributes.

    Returns:
        str: The Cypher attributes string.
    """
    return f'{{{", ".join([f"{key}:{value}" for key, value in dct.items()])}}}'

def query_node(operation, name, labels, attributes):
    """
    Creates a Cypher node query.

    Args:
        operation (CypherOperation): The operation to perform.
        name (str): The name of the node.
        labels (list): The labels for the node.
        attributes (dict): The attributes of the node.

    Returns:
        AtomicEntityQuery: The Cypher query for the node.
    """
    node = CypherNode(name, labels, attributes)
    return AtomicEntityQuery(operation, node)

def create_relationship(source: CypherNode, sink: CypherNode, labels, attributes={}):
    """
    Creates a Cypher relationship query between two nodes.

    Args:
        source (CypherNode): The start node of the relationship.
        sink (CypherNode): The end node of the relationship.
        labels (list): The labels for the relationship.
        attributes (dict): The attributes of the relationship.

    Returns:
        AtomicEntityQuery: The Cypher query for the relationship.
    """
    relation = CypherRelationship(source, sink, labels, attributes)
    return AtomicEntityQuery(CypherOperation.CREATE, relation)
