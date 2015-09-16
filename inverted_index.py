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
    #pdb.set_trace()
    iden = record[0]
    text = record[1]
    words = text.split()
    words = [item.lower() for item in words]
    for w in list(set(words)):
      mr.emit_intermediate(w, iden)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #pdb.set_trace()
    
    mr.emit((key, list_of_values))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
