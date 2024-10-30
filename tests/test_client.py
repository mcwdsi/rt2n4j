import pytest
from rt2_neo4j.client import Neo4jRtStore
from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple, NtoDETuple, NtoRTuple, NtoNTuple
from rt_core_v2.ids_codes.rui import Rui, ID_Rui
from neo4j import GraphDatabase
import base64

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
    tuple_an2 = ANTuple()
    
    # Save the tuples
    store.save_tuple(tuple_an)
    store.save_tuple(tuple_an2)
    
    # Get all available ruis
    available_rui = store.get_available_rui()
    
    # Verify that the ruis of the inserted tuples are in the returned set
    assert tuple_an.rui in available_rui
    assert tuple_an2.rui in available_rui

def setup_test_data():
    """
    Setup test data that will be used by get_referents_by_type_and_designator_type.
    """
    # Create and save an instance of the NtoNTuple and NtoDETuple with mock data
    designator_rui = ID_Rui()
    referent_rui = ID_Rui()
    designator_txt = b"test designator text"
    
    data_type_creator = ANTuple(ruin=designator_rui)
    named_creator = ANTuple(ruin=referent_rui)
    repeatable_creator = ARTuple()
    ntor_rel = NtoRTuple(ruin=data_type_creator.ruin, ruir=repeatable_creator.ruir)

    nton_rel = NtoNTuple(r="http://purl.obolibrary.org/obo/IAO_0000219", p=[designator_rui, referent_rui])

    ntode_tuple = NtoDETuple(
        ruin=data_type_creator.ruin,
        data=designator_txt,  
        ruidt=repeatable_creator.ruir 
    )
    # Insert data into Neo4j to set up query conditions
    store.save_tuple(named_creator)
    store.save_tuple(data_type_creator)
    store.save_tuple(repeatable_creator)
    store.save_tuple(ntor_rel)
    store.save_tuple(nton_rel)
    store.save_tuple(ntode_tuple)
    store.commit()  

    return referent_rui, repeatable_creator.ruir, designator_txt

def test_get_referents_by_type_and_designator_type():
    """
    Test retrieval of referents by type and designator type based on a specific designator text.
    """
    referent_rui, designator_rui, designator_txt = setup_test_data()

    result_set = store.get_referents_by_type_and_designator_type(referent_rui, designator_rui, designator_txt)

    assert len(result_set) > 0 
    assert referent_rui in result_set

    store.transaction_manager.rollback_transaction()
