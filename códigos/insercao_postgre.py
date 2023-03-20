from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType,IntegerType, DoubleType
from google.cloud import storage
from pyspark.sql.functions import *
from functools import reduce
from datetime import datetime
from os import environ
class GCP:
    def __init__(self):
       self.storage_client = storage.Client()
    def create_bucket(self,name_bucket,location='southamerica-east1'):
        bucket = self.storage_client.create_bucket(name_bucket,location=location)
        print(f"Bucket {bucket.name} created.")
    def delete_bucket(self,name_bucket):
        bucket = self.storage_client.get_bucket(name_bucket)
        bucket.delete()
        print(f"Bucket {bucket.name} deletado")
        
# [START funcoes]
def rename_column(df,tipo:str='lower'):
    """Função para transformar os nomes das colunas dos
    dataframes através da criação de um dicionário que
    implementa substituição de caracteres"""
    
    dicionario_caracteres = {'Á':'A','Ã':'A',
                            'Â':'A','À':'A',
                            'É':'E','Ê':'E',
                            'Í':'I',
                            'Ó':'O','Ô':'O',
                            'Ú':'U',' ':'_',
                            '/':'_','. ':'_',
                            '.':'','(':'',
                            ')':'','Ç':'C'}
    
    lista_colunas = [coluna[0].upper().strip() for coluna in df.dtypes]
    lista_colunas_renomeada = []
    for coluna in lista_colunas:
        for key,value in dicionario_caracteres.items():
            coluna = coluna.replace(key,value)
        if tipo == 'lower':
            lista_colunas_renomeada.append(coluna.lower())
        else:
            lista_colunas_renomeada.append(coluna)
    return lista_colunas_renomeada

def concat_csv(lista):
    """Função para ler e concatenar arquivos '.csv'
    para geração de dataframes"""
    
    df = reduce(DataFrame.unionAll,lista)
    return df

def inserir_dados(url:str,db:str,user:str,password:str,df:DataFrame,table:str):
    df.write\
    .format("jdbc")\
    .option("url", f"jdbc:postgresql://{url}:5432/{db}")\
    .option("dbtable", table)\
    .option("driver", "org.postgresql.Driver")\
    .option("user", user)\
    .option("password", password)\
    .save()

def marcar_tempo(descricao:str,ti:datetime,tf:datetime):
        descricao = f"{descricao} - {tf-ti}\n"
        print(descricao)
# [FINAL funcoes]


name_bucket = 'tbf-deloitte-processing-zone'
ip_postgre = '35.247.230.118'
environ['GOOGLE_APPLICATION_ARGS'] = '/path-json'

spark = SparkSession.builder\
.appName('PostgreSQL-lc')\
.config('spark.sql.caseSensitive',"True")\
.getOrCreate()

gcp = GCP()
print('Lendo os CSVS em tbf-deloitte-landing-zone')
acoes_bancarias = spark.read\
.format('csv')\
.option('header','True')\
.option('delimiter',',')\
.option('inferSchema','true')\
.load('gs://tbf-deloitte-landing-zone/acoes/acoes_bancarias.csv')

bndes = spark.read\
.format('csv')\
.option('header','True')\
.option('delimiter',';')\
.option('inferSchema','true')\
.load('gs://tbf-deloitte-landing-zone/bndes/bndes.csv')

reclamacoes_2019 = spark.read\
.format('csv')\
.option('header','True')\
.option('delimiter',',')\
.option('inferSchema','true')\
.load('gs://tbf-deloitte-landing-zone/reclamacoes/reclamacoes_2019.csv')

reclamacoes_2020 = spark.read\
.format('csv')\
.option('header','True')\
.option('delimiter',',')\
.option('inferSchema','true')\
.load('gs://tbf-deloitte-landing-zone/reclamacoes/reclamacoes_2020.csv')

reclamacoes_2021 = spark.read\
.format('csv')\
.option('header','True')\
.option('delimiter',',')\
.option('inferSchema','true')\
.load('gs://tbf-deloitte-landing-zone/reclamacoes/reclamacoes_2021.csv')
#[FINAL leitura csv's]

#concatenação dos csv's de reclamações
print('Concatenando os DataFrames de reclamacoes')
reclamacoes = concat_csv([reclamacoes_2019,reclamacoes_2020,reclamacoes_2021])

#[START renomeação de colunas]
print('Renomeando as colunas dos DataFrames')
lista_colunas_acoes_bancarias = rename_column(acoes_bancarias)
acoes_bancarias = acoes_bancarias.toDF(*lista_colunas_acoes_bancarias)

lista_colunas_bnds = rename_column(bndes)
bndes = bndes.toDF(*lista_colunas_bnds)

lista_colunas_reclamacoes = rename_column(reclamacoes)
reclamacoes = reclamacoes.toDF(*lista_colunas_reclamacoes)
#[FINAL renomeação de colunas]
print(f'Criando o Bucket {name_bucket}')
gcp.create_bucket(name_bucket)

#[START escrita e leitura de parquets]
print(f'Escrevendo os parquets brutos em {name_bucket} para ser utilizado pelo Pyspark e inserido no Cassandra')
acoes_bancarias.write.mode('overwrite').parquet(f'gs://{name_bucket}/AcoesBancarias')
acoes_bancarias = spark.read.parquet(f'gs://{name_bucket}/AcoesBancarias')

bndes.write.mode('overwrite').parquet(f'gs://{name_bucket}/Bndes')
bndes = spark.read.parquet(f'gs://{name_bucket}/Bndes')

reclamacoes.write.mode('overwrite').parquet(f'gs://{name_bucket}/Reclamacoes')
reclamacoes = spark.read.parquet(f'gs://{name_bucket}/Reclamacoes')
#[FINAL escrita e leitura de parquets]

#[START inserção de dados postgresql]
lista_nomedb_df = [[acoes_bancarias,'acoes'],[bndes,'bndes'],[reclamacoes,'reclamacoes']]
for i in lista_nomedb_df:
    print(f'Inserindo os dados de {i[1]}')
    inicio_insercao = datetime.now()
    inserir_dados(ip_postgre,'trabalho-final','postgres','root',i[0],i[1])
    final_insercao = datetime.now()
    marcar_tempo(f'insercao {i[1]}',inicio_insercao,final_insercao)
#[FINAL inserção de dados postgresql]
spark.stop()
