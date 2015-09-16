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
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    order_list = []
    line_list = []
    for i in list_of_values:
	if i[0]=='order':
	    order_list.append(i)
	elif i[0]=='line_item':
	    line_list.append(i)
	else:
	    print "error!"   
 
    for i in order_list:
	for j in line_list:
	    mr.emit(i+j)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
