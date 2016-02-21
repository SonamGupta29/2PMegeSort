How to run ?
java -jar Generator.jar <no_of_records> <typeofcol1> <typeofcol2> .... <typeofcoln>

e.g. java -jar Generator.jar 100 'char(11)' date int

Output:
metadata.txt and the input.txt
metadata.txt will contains:
col1,typeofcol1
col2,typeofcol2
...
...
coln,typeofcoln

input.txt will contain:
column data separated by a comma.

Credits:
DBA TAs
