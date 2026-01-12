# 1. Informando o Catalog
multiple_run_parameters = dbutils.notebook.entry_point.getCurrentBindings()
catalog = multiple_run_parameters["catalog"] or "develop"
schema = "planejamento"

# 2. Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import current_timestamp, date_format, col, lit, expr, row_number, max, date_sub
from pyspark.sql.window import Window

# 3. Inicializando a sessão Spark
spark = SparkSession.builder.appName("Snapshot_Semanal").getOrCreate()

# 4. Definindo as tabelas de origem e destino
table_origem = f"{catalog}.{schema}.fcontas"
table_destino = f"{catalog}.{schema}.fcontas_congelamento_semanal"

# 5. Gerando a data exata do snapshot
data_ingestao = current_timestamp()
data_ingestao_str = date_format(data_ingestao, 'yyyy-MM-dd HH:mm:ss')

# 6. Carregando os dados da tabela de origem
df_origem = spark.table(table_origem)

# 7. Criando um identificador único para cada snapshot
df_congelado = df_origem.withColumn("Data_Ingestao_Congelamento", lit(data_ingestao)) \
                           .withColumn("ID_Snapshot", expr("uuid()")) \
                           .withColumn("Execucao_Snapshot", lit(data_ingestao_str))

# 8. Verificando a versão mais recente do snapshot
tables = spark.catalog.listTables(f"{catalog}.{schema}")
table_destino_exists = any(t.name == "fcontas_congelamento_semanal" for t in tables)

if table_destino_exists:
    df_destino = spark.table(table_destino)
    count_before = df_destino.count()
    
    # 9. Verificando se a coluna Versao_Snapshot existe na tabela
    if "Versao_Snapshot" in df_destino.columns:
        max_version = df_destino.select(max(col("Versao_Snapshot"))).collect()[0][0]
    else:
        max_version = None
    
    versao_atual = (max_version if max_version is not None else 0) + 1
else:
    count_before = 0
    versao_atual = 1

# 10. Adicionando a versão do snapshot
df_congelado = df_congelado.withColumn("Versao_Snapshot", lit(versao_atual).cast(IntegerType()))

# 11. Escrevendo na tabela principal
if table_destino_exists:
    df_congelado.write.mode("append").option("mergeSchema", "true").saveAsTable(table_destino)
else:
    df_congelado.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_destino)

# 12. Removendo registros com mais de 12 meses
cutoff_date = date_sub(current_timestamp(), 365)
df_destino_filtered = spark.table(table_destino).filter(col("Data_Ingestao_Congelamento") >= cutoff_date)

df_destino_filtered.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_destino)

# 13. Contando depois da inserção
df_destino_updated = spark.table(table_destino)
count_after = df_destino_updated.count()

print(f"Tabela 'fcontas_congelamento_semanal' atualizada com sucesso!")
print(f"Quantidade de registros antes: {count_before} | Depois: {count_after}")
if count_after > count_before:
    print("✅ Novos snapshots foram adicionados corretamente.")
else:
    print("⚠ ALERTA: Nenhum novo snapshot foi adicionado! Verifique os registros na tabela final.")
