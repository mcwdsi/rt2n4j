from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, AttributesVisitor
from rt2_neo4j.queries import TupleInsertionVisitor, tuple_query
from rt_core_v2.ids_codes.rui import Rui
from neo4j import GraphDatabase
from time import sleep



uri = "neo4j://localhost:7687/"
auth = ("neo4j", "neo4jneo4j")
config = {}

driver = GraphDatabase.driver(uri, auth=auth, **config)
# Sample usage


with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
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
# # tuple_ntoc = NtoCTuple()
# # tuple_ntode = NtoDETuple()
tuple_ntolackr = NtoLackRTuple(ruin=replacement_two_an.ruin, ruir=tuple_ar.ruir, r=replacement_one_an.ruin)

an_query = tuple_an.accept(insert_tuple)
ar_query = tuple_ar.accept(insert_tuple)
di_query = tuple_di.accept(insert_tuple)

replacement_one_an.accept(insert_tuple)
replacement_two_an.accept(insert_tuple)
dc_query = tuple_dc.accept(insert_tuple)
nton_query = tuple_nton.accept(insert_tuple)
ntor_query = tuple_ntor.accept(insert_tuple)
f_query = tuple_f.accept(insert_tuple)
# # ntoc_query = tuple_ntoc.accept(get_query)
# # ntode_query = tuple_ntode.accept(get_query)
ntolackr_query = tuple_ntolackr.accept(insert_tuple)

# print(f'an: {an_query}\n')
# print(f'ar: {ar_query}\n')
# print(f'di: {di_query}\n')
# print(f'dc: {dc_query}\n')
# print(f'f: {f_query}\n') 
# print(f'nton: {nton_query}\n')
# print(f'ntor: {ntor_query}\n')
# # # print(f'ntoc: {ntoc_query}\n')
# # # print(f'ntode: {ntode_query}\n')
# print(f'ntolackr: {ntolackr_query}\n')

retrieved_an = tuple_query(tuple_an.rui, driver)
retrieved_ar = tuple_query(tuple_ar.rui, driver)
retrieved_di = tuple_query(tuple_di.rui, driver)
retrieved_dc = tuple_query(tuple_dc.rui, driver)
retrieved_nton = tuple_query(tuple_nton.rui, driver)
retrieved_ntor = tuple_query(tuple_ntor.rui, driver)
retrieved_f = tuple_query(tuple_f.rui, driver)
retrieved_ntolackr = tuple_query(tuple_ntolackr.rui, driver)

print(tuple_ntolackr)
print(retrieved_ntolackr)


assert(retrieved_an == tuple_an)
assert(retrieved_ar == tuple_ar)
assert(retrieved_di == tuple_di)
assert(retrieved_dc == tuple_dc)
assert(retrieved_nton == tuple_nton)
assert(retrieved_ntor == tuple_ntor)
assert(retrieved_f == tuple_f)
assert(retrieved_ntolackr == tuple_ntolackr)