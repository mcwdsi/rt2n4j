from rt_core_v2.rttuple import ANTuple, ARTuple, DITuple, DCTuple, FTuple, NtoNTuple, NtoRTuple, NtoCTuple, NtoDETuple, NtoLackRTuple, AttributesVisitor
from rt2_neo4j.queries import TupleInsertionVisitor
from rt_core_v2.ids_codes.rui import Rui
from neo4j import GraphDatabase



uri = "neo4j://localhost:7687/"
auth = ("neo4j", "neo4jneo4j")
config = {}

driver = GraphDatabase.driver(uri, auth=auth, **config)
# Sample usage


with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
get_attr = AttributesVisitor()
get_query = TupleInsertionVisitor(driver)

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

an_query = tuple_an.accept(get_query)
ar_query = tuple_ar.accept(get_query)
di_query = tuple_di.accept(get_query)

replacement_one_an.accept(get_query)
replacement_two_an.accept(get_query)
dc_query = tuple_dc.accept(get_query)
nton_query = tuple_nton.accept(get_query)
ntor_query = tuple_ntor.accept(get_query)
f_query = tuple_f.accept(get_query)
# # ntoc_query = tuple_ntoc.accept(get_query)
# # ntode_query = tuple_ntode.accept(get_query)
ntolackr_query = tuple_ntolackr.accept(get_query)

print(f'an: {an_query}\n')
print(f'ar: {ar_query}\n')
print(f'di: {di_query}\n')
print(f'dc: {dc_query}\n')
print(f'f: {f_query}\n') 
print(f'nton: {nton_query}\n')
print(f'ntor: {ntor_query}\n')
# # print(f'ntoc: {ntoc_query}\n')
# # print(f'ntode: {ntode_query}\n')
print(f'ntolackr: {ntolackr_query}\n')