from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, AttributesVisitor
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query
from rt_core_v2.ids_codes.rui import Rui
from neo4j import GraphDatabase



uri = "neo4j://localhost:7687/"
auth = ("neo4j", "neo4jneo4j")
config = {}

driver = GraphDatabase.driver(uri, auth=auth, **config)
# Sample usage


with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
session = driver.session()
tx = session.begin_transaction()
get_attr = AttributesVisitor()
insert_tuple = TupleInsertionVisitor(driver)

tuple_an = ANTuple()
tuple_ar = ARTuple()
tuple_di = DITuple(ruia=tuple_an.ruin, ruid=tuple_an.ruin, ruit=tuple_ar.rui)
replacement_one_an = ANTuple()
replacement_two_an = ANTuple()
tuple_dc = DCTuple(ruid=replacement_one_an.ruin, ruit=tuple_an.rui, replacements=[replacement_one_an.rui, replacement_two_an.rui])
tuple_nton = NtoNTuple(r=replacement_one_an.ruin, p=[replacement_one_an.ruin, replacement_two_an.ruin])
tuple_ntor = NtoRTuple(ruin=replacement_two_an.ruin, ruir=tuple_ar.ruir, r=replacement_one_an.ruin)
tuple_f = FTuple(C=0.32, ruitn=tuple_ntor.rui)
tuple_ntoc = NtoCTuple(code="Test_code", ruin=replacement_one_an.ruin, r=replacement_two_an.ruin, ruics=tuple_an.ruin)
tuple_ntode = NtoDETuple(ruin=replacement_one_an.ruin, ruidt=replacement_two_an.ruin)
tuple_ntolackr = NtoLackRTuple(ruin=replacement_two_an.ruin, ruir=tuple_ar.ruir, r=replacement_one_an.ruin)


def test_an():
    # Test inserting and retrieving an ANTuple
    an_query = tuple_an.accept(insert_tuple)
    print(f'an: {an_query}\n')
    retrieved_an = tuple_query(tuple_an.rui, driver)
    assert(retrieved_an == tuple_an)


def test_ar():
    # Test inserting and retrieving an ARTuple
    ar_query = tuple_ar.accept(insert_tuple)
    print(f'ar: {ar_query}\n')
    retrieved_ar = tuple_query(tuple_ar.rui, driver)
    assert(retrieved_ar == tuple_ar)


def test_di():
    # Test inserting and retrieving a DITuple
    di_query = tuple_di.accept(insert_tuple)
    print(f'di: {di_query}\n')
    retrieved_di = tuple_query(tuple_di.rui, driver)
    assert(retrieved_di == tuple_di)


def test_dc():
    # Test inserting and retrieving a DCTuple with replacement ANTuples
    replacement_one_an.accept(insert_tuple)
    replacement_two_an.accept(insert_tuple)
    dc_query = tuple_dc.accept(insert_tuple)
    print(f'dc: {dc_query}\n')
    retrieved_dc = tuple_query(tuple_dc.rui, driver)
    assert(retrieved_dc == tuple_dc)


def test_nton():
    # Test inserting and retrieving an NtoNTuple
    nton_query = tuple_nton.accept(insert_tuple)
    print(f'nton: {nton_query}\n')
    retrieved_nton = tuple_query(tuple_nton.rui, driver)
    assert(retrieved_nton == tuple_nton)


def test_ntor():
    # Test inserting and retrieving an NtoRTuple
    ntor_query = tuple_ntor.accept(insert_tuple)
    print(f'ntor: {ntor_query}\n')
    retrieved_ntor = tuple_query(tuple_ntor.rui, driver)
    assert(retrieved_ntor == tuple_ntor)


def test_f():
    # Test inserting and retrieving an FTuple
    f_query = tuple_f.accept(insert_tuple)
    print(f'f: {f_query}\n')
    retrieved_f = tuple_query(tuple_f.rui, driver)
    assert(retrieved_f == tuple_f)


def test_ntoc():
    # Test inserting and retrieving an NtoCTuple
    ntoc_query = tuple_ntoc.accept(insert_tuple)
    print(f'ntoc: {ntoc_query}\n')
    retrieved_ntoc = tuple_query(tuple_ntoc.rui, driver)
    assert(retrieved_ntoc == tuple_ntoc)


def test_ntode():
    # Test inserting and retrieving an NtoDETuple
    ntode_query = tuple_ntode.accept(insert_tuple)
    print(f'ntode: {ntode_query}\n')
    retrieved_ntode = tuple_query(tuple_ntode.rui, driver)
    assert(retrieved_ntode == tuple_ntode)


def ntolackr():
    # Test inserting and retrieving an NtoLackRTuple
    ntolackr_query = tuple_ntolackr.accept(insert_tuple)
    print(f'ntolackr: {ntolackr_query}\n')
    retrieved_ntolackr = tuple_query(tuple_ntolackr.rui, driver)
    assert(retrieved_ntolackr == tuple_ntolackr)
