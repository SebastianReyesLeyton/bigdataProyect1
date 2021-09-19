from config import APPNAME, COLUMNS, DATAPATH, RESULTS
from pyspark.sql import SparkSession

class Project1:

    def __init__(self, result='result.md'):

        self.appname = APPNAME
        self.datapath = DATAPATH
        self.columns = COLUMNS
        self.spark = None
        self.df = None
        self.size = None
        self.results = RESULTS + result
        self.variables = { 'Nominal': [], 
                           'Ordinal': [],
                           'Interval': [], 
                           'Ratio': []}

        self.cleanFile()

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

        self.storeResults({ 'subtitle': 'Entendimiento de los datos', 'subsubtitle': 'Clasificacion de los atributos' })

        # CLASIFICATION OF STATISTICIAN VARIABLES BY EACH FEATURE 

        self.storeResults({ 'content': 'En primer lugar podemos evidenciar que el dataset maneja desde datos con variable cualitativa y cuantitativa, los cuales podríamos clasificar de la siguiente manera:' })

        self.variables['Nominal'] = [ 'cod_eas', 'nombre_eas', 'sexo', 'zona', 'cod_ips', 'nombre_institucion', 'cod_dx_salida', 'nombre_dx', 'servicio' ]
        self.variables['Ordinal'] = [ 'consecutivo', 'año', 'tipo_usuario', 'tipo_edad', 'cod_departamento', 'cod_municio', 'causa_externa' ]
        self.variables['Interval'] = [ 'edad' ]
        self.variables['Ratio'] = [ 'total_atenciones' ]

        self.storeResults({ 'subsubsubtitle': 'Cualitativa - Nominal', 'list': self.variables['Nominal'] })
        self.storeResults({ 'subsubsubtitle': 'Cualitativa - Ordinal', 'list': self.variables['Ordinal'] })
        self.storeResults({ 'subsubsubtitle': 'Cuantitativa - Intervalo', 'list': self.variables['Interval'] })
        self.storeResults({ 'subsubsubtitle': 'Cuantitativa - Razon', 'list': self.variables['Ratio'] })

        # UNIQUE VALUES

        self.storeResults({ 'subsubtitle': 'Valores Unicos', 'content': 'Se realizará es ver cuales son los valores que tiene cada atributo (columna).' })

        uniques = self.obtainUniqueDataByColumn()
        
        for col in uniques:
            elements = list(map(str, uniques[col]))
            self.storeResults({ 'subsubsubtitle': col, 'enumeration': elements })

        self.storeResults({ 'content': 'Como podemos observar se tiene que los atributos ' +
                                        'como servicio, cod_municipio y cod_departamento solo presentan 1 elemento dentro de su rango de opciones. ' +
                                        'Por otro lado, se tiene que el atributo zona, presenta un problema en la distinción entre mayusculas y minusculas, ' +
                                        'especificamente con la letra u. Además, se puede evidenciar que los datos de edad presentan algunos valores anormales (outliers)' })

        # MEASURE OF CENTRALITY

        self.df.summary(self.variables['Interval'] + self.variables['Ratio']).show()

    def obtainUniqueDataByColumn(self):

        ans = {}

        for col in self.df.columns:

            # Obtain all unique values of one columns
            ans[col] = [ row[0] for row in self.df.select(col).distinct().collect() ]

            # Sort the array of unique values
            ans[col].sort()
    
        return ans

    def changeColumnsNames(self, columns):

        # Change the columns name by the constant values COLUMNS
        self.df = self.df.toDF(*columns)

    def start(self):

        # Initializate the app
        self.initialization()

        # Understanding dataset
        self.undertandingDataSet()

        # Stop the Spark app
        self.spark.stop()

    def storeResults(self, data):

        fs = open(self.results, 'a')

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
    app = Project1('results1.md')
    app.start()

main()