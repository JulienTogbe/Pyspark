import pyspark
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark=SparkSession.builder.appName('Dataframe').getOrCreate()

# Specify the path to your CSV file
csv_file_path = 'C:/Users/NB/Documents/PythonProjects/dec17pub.dat'

column_locations = {
    'HRHHID': (1, 14), 
    'HRMMYYYY': (15, 21),
    'HUFINAL': (24, 26),
    'HEHOUSUT': (31, 32),
    'HRHTYPE': (61, 62),
    'HETELHHD': (33, 34),
    'HETELAVL': (35, 36),
    'HEPHONEO': (37, 38),
    'HUINTTYP':(61, 66),
    'HEFAMINC': (39, 40),
    'GEDIV': (91, 91),
    'PTDTRACE': (139, 140)
    
}

# Read the fixed-width file using pandas read_fwf
extracted_data = pd.read_fwf(csv_file_path, colspecs=list(column_locations.values()), header=None, names=column_locations.keys())
 
# Export the extracted data to a CSV file
extracted_data.to_csv('C:/Users/NB/Documents/PythonProjects/output.csv', index=False)

# Print table data using `to_string` with formatting options
print("Table data:\n", extracted_data.head(10).to_string(index=False))

# Print extracted data as a list
print("\nList data:", list(extracted_data.head(10).itertuples(index=False)))

# Trying to view data in a table
if __name__ == "__main__":
    html_table = extracted_data.head(10).to_html()
    with open("C:/Users/NB/Documents/PythonProjects/output.html", "w") as f:
        f.write(html_table)
    print("\nHTML table generated!\n", html_table)

df_pyspark=spark.read.csv('output.csv',header=True,inferSchema=True)
df_pyspark.show()

###========SECTION 4============
##create a temporary view and give it a name
df_pyspark.createOrReplaceTempView('vw_pubdec')

##1. What is the count of responders per family income range (show all)?
df_ = spark.sql('select count(HEFAMINC)family_income_range from vw_pubdec')
df_.show()

#3. How many responders do not have telephone in their house, but can access
df_ = spark.sql("select count(*)telephone_access from vw_pubdec where HETELAVL = '1'")
df_.show()

#4. How many responders can access a telephone, but telephone interview is not accepted?
df_ = spark.sql("select count(*)notelephone_interview from vw_pubdec where HEPHONEO = '0'")
df_.show()


