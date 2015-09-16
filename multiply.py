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
    if record[0]=='a':
        key = record[2]
    elif record[0]=='b':
	key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    A_index = [i[1] for i in list_of_values if i[0]=='a']
    A_value = [i[3] for i in list_of_values if i[0]=='a']
    B_index = [i[2] for i in list_of_values if i[0]=='b']
    B_value = [i[3] for i in list_of_values if i[0]=='b']
    #pdb.set_trace()
    for i in range(len(A_index)):
	for jj in range(len(B_index)): 
 	    if [A_index[i],B_index[jj]] in [[ii[0],ii[1]] for ii in mr.result]:
		temp_index = [[ii[0],ii[1]] for ii in mr.result].index([A_index[i],B_index[jj]])
		mr.result[temp_index][2]+=A_value[i]*B_value[jj]
	    else:
		#pdb.set_trace()
		mr.emit([A_index[i],B_index[jj],A_value[i]*B_value[jj]])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
