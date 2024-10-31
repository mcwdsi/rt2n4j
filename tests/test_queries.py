from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, AttributesVisitor
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query
from rt_core_v2.ids_codes.rui import Rui, Relationship
from neo4j import GraphDatabase



uri = "neo4j://localhost:7687/"
auth = ("neo4j", "neo4jneo4j")
config = {}

driver = GraphDatabase.driver(uri, auth=auth, **config)


with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
session = driver.session()
tx = session.begin_transaction()
get_attr = AttributesVisitor()
insert_tuple = TupleInsertionVisitor()
insert_tuple.set_transaction(tx)

tuple_an = ANTuple()
tuple_ar = ARTuple()
tuple_di = DITuple(ruia=tuple_an.ruin, ruid=tuple_an.ruin, ruit=tuple_ar.rui)
replacement_one_an = ANTuple()
replacement_two_an = ANTuple()
r = Relationship("http://purl.obolibrary.org/obo/IAO_0000219")
tuple_dc = DCTuple(ruid=replacement_one_an.ruin, ruit=tuple_an.rui, replacements=[replacement_one_an.rui, replacement_two_an.rui])
tuple_nton = NtoNTuple(r=r, p=[replacement_one_an.ruin, replacement_two_an.ruin])
tuple_ntor = NtoRTuple(ruin=replacement_two_an.ruin, ruir=tuple_ar.ruir, r=r)
tuple_f = FTuple(C=0.32, ruitn=tuple_ntor.rui)
tuple_ntoc = NtoCTuple(code="Test_code", ruin=replacement_one_an.ruin, r=r, ruics=replacement_two_an.ruin)
tuple_ntode = NtoDETuple(ruin=replacement_one_an.ruin, ruidt=tuple_ar.ruir, data=b'\x01\x02\x03\x04\x05')
tuple_ntolackr = NtoLackRTuple(ruin=replacement_two_an.ruin, ruir=tuple_ar.ruir, r=r)


def test_init():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def test_an():
    # Test inserting and retrieving an ANTuple
    an_query = tuple_an.accept(insert_tuple)
    print(f'an query: {an_query}\n')
    print(f'an expected: {tuple_an}')
    retrieved_an = tuple_query(tuple_an.rui, tx)
    print(f'an retrieved: {retrieved_an}\n')
    assert(retrieved_an == tuple_an)


def test_ar():
    # Test inserting and retrieving an ARTuple
    ar_query = tuple_ar.accept(insert_tuple)
    print(f'ar query: {ar_query}\n')
    print(f'ar expected: {tuple_ar}')
    retrieved_ar = tuple_query(tuple_ar.rui, tx)
    print(f'ar retrieved: {retrieved_ar}\n')
    assert(retrieved_ar == tuple_ar)


def test_di():
    # Test inserting and retrieving a DITuple
    di_query = tuple_di.accept(insert_tuple)
    print(f'di query: {di_query}\n')
    print(f'di expected: {tuple_di}')
    retrieved_di = tuple_query(tuple_di.rui, tx)
    print(f'di retrieved: {retrieved_di}\n')
    assert(retrieved_di == tuple_di)


def test_dc():
    # Test inserting and retrieving a DCTuple with replacement ANTuples
    replacement_one_an.accept(insert_tuple)
    replacement_two_an.accept(insert_tuple)
    dc_query = tuple_dc.accept(insert_tuple)
    print(f'dc query: {dc_query}\n')
    print(f'dc expected: {tuple_dc}')
    retrieved_dc = tuple_query(tuple_dc.rui, tx)
    print(f'dc retrieved: {retrieved_dc}\n')
    assert(retrieved_dc == tuple_dc)


def test_nton():
    # Test inserting and retrieving an NtoNTuple
    nton_query = tuple_nton.accept(insert_tuple)
    print(f'nton query: {nton_query}\n')
    print(f'nton expected: {tuple_nton}')
    retrieved_nton = tuple_query(tuple_nton.rui, tx)
    print(f'nton retrieved: {retrieved_nton}\n')
    assert(retrieved_nton == tuple_nton)


def test_ntor():
    # Test inserting and retrieving an NtoRTuple
    ntor_query = tuple_ntor.accept(insert_tuple)
    print(f'ntor query: {ntor_query}\n')
    print(f'ntor expected: {tuple_ntor}')
    retrieved_ntor = tuple_query(tuple_ntor.rui, tx)
    print(f'ntor retrieved: {retrieved_ntor}\n')
    assert(retrieved_ntor == tuple_ntor)


def test_f():
    # Test inserting and retrieving an FTuple
    f_query = tuple_f.accept(insert_tuple)
    print(f'f query: {f_query}\n')
    print(f'f expected: {tuple_f}')
    retrieved_f = tuple_query(tuple_f.rui, tx)
    print(f'f retrieved: {retrieved_f}\n')
    assert(retrieved_f == tuple_f)



def test_ntoc():
    # Test inserting and retrieving an NtoCTuple
    ntoc_query = tuple_ntoc.accept(insert_tuple)
    print(f'ntoc query: {ntoc_query}\n')
    print(f'ntoc expected: {tuple_ntoc}')
    retrieved_ntoc = tuple_query(tuple_ntoc.rui, tx)
    print(f'ntoc retrieved: {retrieved_ntoc}\n')
    assert(retrieved_ntoc == tuple_ntoc)


def test_ntode():
    # Test inserting and retrieving an NtoDETuple
    ntode_query = tuple_ntode.accept(insert_tuple)
    print(f'ntode query: {ntode_query}\n')
    print(f'ntode expected: {tuple_ntode}')
    retrieved_ntode = tuple_query(tuple_ntode.rui, tx)
    print(f'ntode retrieved: {retrieved_ntode}\n')
    print(f'ntode {tuple_ntode}')
    assert(retrieved_ntode == tuple_ntode)


def test_ntolackr():
    # Test inserting and retrieving an NtoLackRTuple
    ntolackr_query = tuple_ntolackr.accept(insert_tuple)
    print(f'ntolackr query: {ntolackr_query}\n')
    print(f'ntolackr expected: {tuple_ntolackr}')
    retrieved_ntolackr = tuple_query(tuple_ntolackr.rui, tx)
    print(f'ntolackr retrieved: {retrieved_ntolackr}\n')
    assert(retrieved_ntolackr == tuple_ntolackr)
    tx.commit()
    tx.close()
    session.close()
