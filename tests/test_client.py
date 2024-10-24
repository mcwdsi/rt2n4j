import pytest
from rt2_neo4j.client import Neo4jRtStore
from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple
from rt_core_v2.ids_codes.rui import Rui, ID_Rui
from neo4j import GraphDatabase

# Setup the Neo4j connection
uri = "neo4j://localhost:7687/"
auth = ("neo4j", "neo4jneo4j")
store = Neo4jRtStore(uri, auth)
with store.driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")

def test_save_and_get_tuple():
    # Create a sample ANTuple
    tuple_an = ANTuple()
    # Save the tuple to the store
    store.save_tuple(tuple_an)
    store.commit()
    # Retrieve the saved tuple using its rui
    retrieved_tuple = store.get_tuple(tuple_an.rui)
    
    # Assert that the retrieved tuple matches the original
    assert retrieved_tuple == tuple_an, "The retrieved tuple should match the saved tuple."

def test_get_by_author():
    # Create two tuples with different authors
    author_rui = ID_Rui()
    tuple_an = ANTuple(ruin=author_rui)
    tuple_di = DITuple(ruit=tuple_an.rui, ruia=author_rui, ruid=author_rui)
    
    # Save the tuples
    store.save_tuple(tuple_an)
    store.save_tuple(tuple_di)
    store.commit()
    # Retrieve tuples by author
    retrieved_tuples = store.get_by_author(author_rui)
    assert tuple_an == retrieved_tuples[0]

def test_get_available_rui():
    # Create tuples
    tuple_an = ANTuple()
    tuple_ar = ARTuple()
    
    # Save the tuples
    store.save_tuple(tuple_an)
    store.save_tuple(tuple_ar)
    
    # Get all available ruis
    available_rui = store.get_available_rui()
    
    # Verify that the ruis of the inserted tuples are in the returned set
    assert tuple_an.rui in available_rui
    assert tuple_ar.rui in available_rui
