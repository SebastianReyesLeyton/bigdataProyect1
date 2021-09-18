from config import APPNAME, COLUMNS, DATAPATH
from pyspark.sql import SparkSession


def loadCreateSparkData(appname, datapath):
    """
    Description: This function create the Spark-app and create an dataframe with the data 
                 obtained in the path stored in DATAPATH, and return both.
    Input:
        - appname: App name
        - datapaht: Path where data is located

    Output: The Spark-app and DataFrame with the data of path file.
    """

    # Create an application with the name pass by appname parameter
    spark = SparkSession.builder.appName(appname).getOrCreate()

    # Makes the output look more like pandas and less like command-line SQL
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    # Load and create the DataFrame with the data obtained in the route datapath
    df = spark.read.csv(datapath, inferSchema=True, header=True, sep=';')

    return spark, df

def obtaintUniqueDataByColumn(df):

    dic = {}
    
    for col in COLUMNS:
        dic[col] = [ row[0] for row in df.select(col).distinct().collect() ]
        dic[col].sort()
        print(col, len(dic[col]))
    
    return dic

def obtainRegisterByYear(df, anios):

    dic = {}
    
    for row in anios:
        
        dic[row] = df.where(df.año == row)
        #print(row, dic[row].count())

    return dic

def obtainRegisterByInstitution(df, institutions):

    dic = {}

    for row in institutions:
        dic[row] = df.where(df.nombre_institucion == row)
        if (dic[row].count() != 0):
            print(row, dic[row].count())

def main():

    spark, df = loadCreateSparkData(APPNAME, DATAPATH)
    
    # Change the columns name by the constant values COLUMNS
    df = df.toDF(*COLUMNS)

    # Show DataFrame
    #df.show()

    # Get number of row and columns in the dataframe
    #print(df.count(), len(df.columns))
    
    print("Number of unique registers by column: \n")
    dic = obtaintUniqueDataByColumn(df)

    print("Registers by year: \n")
    dicAnio = obtainRegisterByYear(df, dic['año'])

    for row in dicAnio:
        obtainRegisterByInstitution(dicAnio[row], dic['nombre_institucion'])
    
    # for row in dic['año']:
    #     for inst in dic['nombre_institucion']:
            
    #         print(row, inst, df.where( (df.año == row) & (df.nombre_institucion == inst)).count())
        

    spark.stop()

main()