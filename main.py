from config import APPNAME, COLUMNS, DATAPATH, RESULTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, isnan, count, col
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
import seaborn as sns
class Project1:

    def __init__(self, result='result.md'):

        self.appname = APPNAME          # Name of spark app
        self.datapath = DATAPATH        # Data path
        self.columns = COLUMNS          # Name of dataset columns
        self.spark = None               # Store the spark app
        self.df = None                  # Store the spark dataset
        self.size = None                # Store a tuple as form: (number of rows, number of columns)
        self.results = RESULTS + result # Route of the file where the report was stored
        self.variables = { 'Nominal': [], 
                           'Ordinal': [],
                           'Interval': [], 
                           'Ratio': []}

        self.cleanFile()                # Clean data into the result file

    def createSparkApp(self):
        
        # Create an application with the name pass by appname parameter
        self.spark =  SparkSession.builder.appName(self.appname).getOrCreate()

        # Makes the output look more like pandas and less like command-line SQL        
        self.spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    def loadData(self):

        # Load and create the DataFrame with the data obtained in the route datapath
        self.df = self.spark.read.csv(self.datapath, inferSchema=True, header=True, sep=';')

    def loadSize(self):

        # Obtain the number of rows and columns by our own dataframe df
        self.size = (self.df.count(), len(self.df.columns))

    def initialization(self):
        """
        Description: This function initializate the proyect1 elements
        """
        
        self.storeResults({ 'title': 'Proyecto 1: Urgencias' })
        self.createSparkApp()

        self.storeResults({ 'content': f'Direccion: {self.datapath}'})
        self.loadData()

        self.storeResults({ 'subtitle': 'Descripcion', 'content': 'Descripcion de la base de datos con la que se va a trabajar' })

        self.loadSize()
        self.storeResults({ 'subsubtitle': 'Dimensiones', 'content': f'La base de datos cuenta con {self.size[0]} filas y {self.size[1]} columnas.' })

        self.changeColumnsNames(self.columns)
        self.storeResults({ 'subsubtitle': 'Atributos del dataset', 'enumeration': self.columns, 'content': 'Los atributos de base de datos para urgencias son los siguientes:' })

    def undertandingDataSet(self):

        # self.storeResults({ 'subtitle': 'Entendimiento de los datos', 'subsubtitle': 'Clasificacion de los atributos' })

        # # CLASIFICATION OF STATISTICIAN VARIABLES BY EACH FEATURE 

        # self.storeResults({ 'content': 'En primer lugar podemos evidenciar que el dataset maneja desde datos con variable cualitativa y cuantitativa, los cuales podríamos clasificar de la siguiente manera:' })

        # self.variables['Nominal'] = [ 'cod_eas', 'nombre_eas', 'sexo', 'zona', 'cod_ips', 'nombre_institucion', 'cod_dx_salida', 'nombre_dx', 'servicio' ]
        # self.variables['Ordinal'] = [ 'consecutivo', 'año', 'tipo_usuario', 'tipo_edad', 'cod_departamento', 'cod_municio', 'causa_externa' ]
        # self.variables['Interval'] = [ 'edad' ]
        # self.variables['Ratio'] = [ 'total_atenciones' ]

        # self.storeResults({ 'subsubsubtitle': 'Cualitativa - Nominal', 'list': self.variables['Nominal'] })
        # self.storeResults({ 'subsubsubtitle': 'Cualitativa - Ordinal', 'list': self.variables['Ordinal'] })
        # self.storeResults({ 'subsubsubtitle': 'Cuantitativa - Intervalo', 'list': self.variables['Interval'] })
        # self.storeResults({ 'subsubsubtitle': 'Cuantitativa - Razon', 'list': self.variables['Ratio'] })

        # # UNIQUE VALUES

        # self.storeResults({ 'subsubtitle': 'Valores Unicos', 'content': 'Se realizará es ver cuales son los valores que tiene cada atributo (columna).' })

        # uniques = self.obtainUniqueDataByColumn()
        # file = str(self.results)
        # for col in uniques:
        #     self.results = f'{RESULTS}{col}.md'
        #     self.cleanFile()
        #     elements = list(map(str, uniques[col]))
        #     self.storeResults({ 'subsubsubtitle': col, 'enumeration': elements })

        # self.results = str(file)

        # self.storeResults({ 'content': 'Como podemos observar se tiene que los atributos ' +
        #                                 'como servicio, cod_municipio y cod_departamento solo presentan 1 elemento dentro de su rango de opciones. ' +
        #                                 'Por otro lado, se tiene que el atributo zona, presenta un problema en la distinción entre mayusculas y minusculas, ' +
        #                                 'especificamente con la letra u. Además, se puede evidenciar que los datos de edad presentan algunos valores anormales (outliers)' })

        # # SUMMARY OF QUANTITY VARIABLES

        # self.storeResults({ 'subsubtitle': 'Medidas de centralidad' ,'content': str(self.df.select(self.variables['Interval'] + self.variables['Ratio']).summary()) })

        # Graphics

        #self.graphics()

        # OTHER 

        df_pandas = self.df.toPandas()

        le = preprocessing.LabelEncoder()
        df_pandas = df_pandas.apply(le.fit_transform)
        
        self.df = self.spark.createDataFrame(df_pandas)

        f, ax = plt.subplots(figsize=(20, 15))
        heatmap = sns.heatmap(df_pandas.corr(), square=True, annot=True, linewidths=.5, ax=ax)
        fig = heatmap.get_figure()
        fig.savefig('graphics/correlation_before.png', bbox_inches='tight')

    def graphics(self):


        uniques = self.obtainUniqueDataByColumn()

        for column in ['año', 'edad', 'sexo']:

            plt.bar( [ row[0] for row in uniques[column]] ,[ row[1] for row in uniques[column]])
            plt.xlabel( column )
            plt.ylabel( 'Frecuencia' )
            
            plt.title(f'Diagrama de {column}')
            plt.savefig(f'graphics/{column}.png', bbox_inches='tight')

    def dataCleaning(self):

        # Remove the servicio, cod_municipio, and cod_departamento columns of dataset
        ans = self.df.drop('servicio').drop('cod_municipio').drop('cod_departamento')
        self.loadSize()

        # Correct the zona column
        ans = ans.withColumn("zona",
                       when(ans.zona == 'u', 'U')
                       .otherwise(ans.zona))

        # ans.select('zona').distinct().show()

        # Remove register with age > 99
        ans = ans.filter((ans.edad <= 99))

        # # Remove F of sexo column
        # ans = ans.filter(ans.sexo != 'F')

        ans_pandas = ans.toPandas()

        ans_pandas.drop_duplicates()

        f, ax = plt.subplots(figsize=(20, 15))
        heatmap = sns.heatmap(ans_pandas.corr(), square=True, annot=True, linewidths=.5, ax=ax)
        fig = heatmap.get_figure()
        fig.savefig('graphics/correlation.png', bbox_inches='tight')

        print(ans_pandas.shape)

        #convertir edad a tipo numero

        self.df = self.spark.createDataFrame(ans_pandas)

    def datasetTransformation(self):

        self.dataCleaning()

        self.df = self.df.filter(self.df.nombre_dx == 'OTROS DOLORES ABDOMINALES Y LOS NO ESPECIFICADOS')

        self.df = self.df.drop('nombre_dx')

        self.loadSize()

        self.columns = list(self.df.columns)

        print(self.columns)
        print(self.size)

        self.df.groupby('año').count().show()

    def prediction(self):

        pass

    def obtainUniqueDataByColumn(self):

        ans = {}

        for col in self.df.columns:

            # Obtain all unique values of one columns
            ans[col] = [ [row[col], row[1]] for row in self.df.groupby(col).count().collect() ]

            # Sort the array of unique values
            ans[col].sort(key=lambda x: -1*x[1])
    
        return ans

    def changeColumnsNames(self, columns):

        # Change the columns name by the constant values COLUMNS
        self.df = self.df.toDF(*columns)

    def start(self):

        # Initializate the app
        self.initialization()

        # Understanding dataset
        self.undertandingDataSet()

        # Action Plan
        self.datasetTransformation()

        # Prediction
        self.prediction()

        # Stop the Spark app
        self.spark.stop()

    def storeResults(self, data):

        fs = open(self.results, 'a', encoding="utf-8")

        if ('title' in data): fs.write('# ' + data['title'] + '\n\n')
        if ('subtitle' in data): fs.write('## ' + data['subtitle'] + '\n\n')
        if ('subsubtitle' in data): fs.write('### ' + data['subsubtitle'] + '\n\n')
        if ('subsubsubtitle' in data): fs.write('#### ' + data['subsubsubtitle'] + '\n\n')

        if ('content' in data): 
            fs.write(data['content'])
            fs.write('\n\n')
        
        if ('list' in data):
            for line in data['list']:
                fs.write('- ' + str(line) + '\n')

            fs.write('\n')

        if ('enumeration' in data):
            for e, line in enumerate(data['enumeration']):
                fs.write(f'{e+1}. ' + str(line) + '\n')

            fs.write('\n')

        fs.close()
    
    def cleanFile(self):

        fs = open(self.results, 'w')
        fs.write('')
        fs.close()


def main():
    app = Project1()
    app.start()

main()