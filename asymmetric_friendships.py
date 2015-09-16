import MapReduce
import sys
import pdb
"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    # pdb.set_trace()
    myself = record[0]
    friend = record[1]
    mr.emit_intermediate(myself, friend)
    mr.emit_intermediate(friend, myself)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    # pdb.set_trace()
    #dup_set = [i for i in list_of_values if list_of_values.count(i)>1]
    single_set = [i for i in list_of_values if list_of_values.count(i)==1]
    #iden = [i[0] for i in list_of_values]
    #unique_set = [i[0] for i in list_of_values if iden.count(i[0])==1 and i[1]==1]
    for i in single_set:    
	mr.emit((key,i))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
