# Python-Mysql
Python codes to create Mysql Tables, Split CSV files and upload CSV with minimal customization

Add files via upload

To understand the usage of each file you may run the file and it will display the usage.
===================================================================================
createtb.py | Requires 2 files. 1) Database credentials file 2) Table info file.
This script will create a table from a file that contains the column name and data type of the column. Sample file is ProductAttributes. This script also require a file containing db credentials. Sample file is dbconnector.cf
ProductAttributes| The first column is the column name in the table and the second column is the data type.
dbconnnector.cf| The file which will contain the IP, user name, password and database name of the database.

===================================================================================

splitfiles.py| Specify the filename to split, the number of files [with headers intact] to produce and the number of rows to be present in each row. This is useful if we would like to test smaller chunks of big files.
===================================================================================
UploadData.py|This script will upload the data from .csv file to a specified and existing table name and DB name. The script will not upload and will alert with error message if the headers of the table and csv doesnt match. Specify the full path of the csv file.
