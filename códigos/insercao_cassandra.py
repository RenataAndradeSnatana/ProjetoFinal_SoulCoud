from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType,IntegerType, DoubleType
from pyspark.sql.functions import *
from functools import reduce
from datetime import datetime
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
from os import environ

class GCP:
    def __init__(self):
       self.storage_client = storage.Client()
    def delete_bucket(self,name_bucket):
        bucket = self.storage_client.get_bucket(name_bucket)
        bucket.delete(force=True)
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

def inserir_dados(df, table:str,keyspace:str):
    df.write\
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode('append') \
        .save()

def marcar_tempo(descricao:str,ti:datetime,tf:datetime):
        descricao = f"{descricao} - {tf-ti}\n"
        print(descricao)
# [FINAL funcoes]
environ['GOOGLE_APPLICATION_ARGS'] = '/path-json'
name_bucket, project_id, region = 'tbf-deloitte-processing-zone','crypto-argon-338802','us-central1'

gcp = GCP()

spark = SparkSession.builder\
.appName("ETL_DATAPROC")\
.config("spark.sql.caseSensitive", "True")\
.config("spark.cassandra.connection.host","35.247.252.230")\
.config("spark.cassandra.connection.port","9042")\
.config("spark.cassandra.output.batch.grouping.buffer.size", "3000")\
.config("spark.cassandra.output.concurrent.writes", "1500")\
.config("cassandra.output.throughput_mb_per_sec", "128")\
.config("spark.cassandra.output.batch.size.bytes", "2056")\
.config("cassandra.connection.keep_alive_ms", "30000")\
.getOrCreate()

#[START leitura parquet bruto]
print(f'Lendo os parquets brutos em {name_bucket}')
acoes_bancarias = spark.read.parquet(f'gs://{name_bucket}/AcoesBancarias')
bndes = spark.read.parquet(f'gs://{name_bucket}/Bndes')
reclamacoes = spark.read.parquet(f'gs://{name_bucket}/Reclamacoes')


#[START inserindo coluna id]
print('Adicionando colunas com uuid')
acoes_bancarias = acoes_bancarias.withColumn("id", expr("uuid()"))
bndes = bndes.withColumn("id", expr("uuid()"))
reclamacoes = reclamacoes.withColumn("id", expr("uuid()"))
#[FINAL inserindo coluna id]

#[START filtragem de dados]
print('Filtrando os dados')
reclamacoes.createGlobalTempView('reclamacoes')
bndes.createGlobalTempView('op_bndes')

reclamacoes = spark.sql("select * from global_temp.reclamacoes where nome_fantasia = 'Caixa Econômica Federal' or nome_fantasia = 'Banco do Brasil' or nome_fantasia = 'Banco Santander' or nome_fantasia = 'Banco Bradesco' or nome_fantasia = 'Banco Itaú Unibanco' or nome_fantasia = 'Nubank' or nome_fantasia = 'Banco Inter (Banco Intermedium)' or nome_fantasia = 'Banco Pan ' or nome_fantasia = 'Banco Modal' or nome_fantasia = 'Banco BMG'")
bndes = spark.sql("select * from global_temp.op_bndes where instituicao_financeira_credenciada = 'BANCO BMG SA' or instituicao_financeira_credenciada = 'BANCO DO BRASIL SA' or instituicao_financeira_credenciada = 'BANCO SANTANDER (BRASIL) S.A.' or instituicao_financeira_credenciada = 'ITAU UNIBANCO S.A.' or instituicao_financeira_credenciada = 'BANCO BRADESCO S.A.' or instituicao_financeira_credenciada = 'CAIXA ECONOMICA FEDERAL' or instituicao_financeira_credenciada = 'BANCO MODAL S.A.'")
#[FINAL filtragem de dados]
print('Limpando os dados')

bndes = bndes.withColumn('valor_da_operacao_em_reais', regexp_replace('valor_da_operacao_em_reais', ',', '.'))
bndes = bndes.withColumn('valor_desembolsado_reais', regexp_replace('valor_desembolsado_reais', ',', '.'))
bndes = bndes.withColumn('juros', regexp_replace('juros', ',', '.'))

acoes_bancarias = acoes_bancarias.withColumn("date", acoes_bancarias["date"].cast(DateType()))
drop_acoes = ['volume', 'dividends', 'stock_splits']
acoes_bancarias = acoes_bancarias.drop(*drop_acoes)

drop_rec = ['uf', 'cidade', 'sexo', 'data_finalizacao', 'segmento_de_mercado', 'area', 'problema']
reclamacoes = reclamacoes.drop(*drop_rec)
reclamacoes = reclamacoes.withColumn("tempo_resposta", reclamacoes["tempo_resposta"].cast(IntegerType()))
reclamacoes = reclamacoes.withColumn("nota_do_consumidor", reclamacoes["nota_do_consumidor"].cast(IntegerType()))

drop_bndes = ['cliente', 'cpf_cnpj', 'municipio', 'municipio_codigo', 'fonte_de_recurso_desembolsos', 'custo_financeiro', 'prazo_amortizacao_meses', 'modalidade_de_apoio', 'forma_de_apoio', 'produto', 'instrumento_financeiro', 'area_operacional', 'setor_cnae', 'subsetor_cnae_agrupado', 'subsetor_cnae_codigo', 'subsetor_cnae_nome', 'setor_bndes', 'situacao_da_operacao', 'cnpj_do_agente_financeiro']
bndes = bndes.drop(*drop_bndes)
bndes = bndes.withColumn("valor_desembolsado_reais", bndes["valor_desembolsado_reais"].cast(IntegerType()))
bndes = bndes.withColumn("juros", bndes["juros"].cast(DoubleType()))

#[START escrita e leitura de parquets]
print('Gravando e lendo os parquets limpos')
acoes_bancarias.write.mode('overwrite').parquet('gs://tbf-deloitte-curated-zone/AcoesBancarias')
acoes_bancarias = spark.read.parquet('gs://tbf-deloitte-curated-zone/AcoesBancarias')

bndes.write.mode('overwrite').parquet('gs://tbf-deloitte-curated-zone/BNDES')
bndes = spark.read.parquet('gs://tbf-deloitte-curated-zone/BNDES')

reclamacoes.write.mode('overwrite').parquet('gs://tbf-deloitte-curated-zone/Reclamacoes')
reclamacoes = spark.read.parquet('gs://tbf-deloitte-curated-zone/Reclamacoes')
#[FINAL escrita e leitura de parquets]

acoes_bancarias.show(3)
bndes.show(3)
reclamacoes.show(3)
lista_nomes_df = [[acoes_bancarias,'acoes'],[reclamacoes,'reclamacoes'],[bndes,'bndes']]
for i in lista_nomes_df:
    print(f'Inserindo {i[1]}')
    inicio_insercao = datetime.now()
    inserir_dados(i[0],i[1],'trabalho_final')
    final_insercao = datetime.now()
    marcar_tempo(f'insercao {i[1]} em ',inicio_insercao,final_insercao)
    
spark.stop()
gcp.delete_bucket(name_bucket)

    

