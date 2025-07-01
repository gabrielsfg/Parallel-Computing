#!/usr/bin/env python3
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, avg, when, regexp_extract, to_date
from pyspark.sql.window import Window

def process_sample_spark(df, fraction, executors):
    """
    Processa uma amostra fraction do DataFrame Spark e mede
    tempos de cada etapa, forçando execução com actions.
    """
    # amostragem e cache
    sampled_df = df.sample(fraction=fraction, seed=42).cache()
    # força cache materialização
    sampled_df.count()

    # --- Rolling‐window 7 dias ---
    t0 = time.time()
    # 1) extrai data e conta acidentes por dia
    df_daily = (
        sampled_df
        .withColumn("date", to_date("Start_Time"))
        .groupBy("date")
        .agg(count("ID").alias("daily_count"))
    )
    # define janela de 6 dias anteriores + dia atual
    windowSpec = Window.orderBy("date").rowsBetween(-6, 0)
    # 2) calcula média móvel de 7 dias
    df_rolling = df_daily.withColumn(
        "rolling_avg_7d",
        avg("daily_count").over(windowSpec)
    )
    # força execução
    df_rolling.count()
    metrics = {
        "t_moving_avg_7d": time.time() - t0
    }
    # --- Fim rolling‐window ---

    # continua contando linhas e colunas
    n_rows = sampled_df.count()
    n_cols = len(df.columns)
    metrics.update({
        "threads": executors,
        "fraction": fraction,
        "n_rows": n_rows,
        "n_cols": n_cols
    })

    total_start = time.time()

    # 1) Acidentes por estado
    t0 = time.time()
    df_states = sampled_df.groupBy("State") \
                         .agg(count("ID").alias("Qtd_Acidentes"))
    df_states.count()
    metrics["t_acidentes_estado"] = time.time() - t0

    # 2) Clima grave (Severity >= 4)
    t0 = time.time()
    df_clima = sampled_df.filter(col("Severity") >= 4) \
                         .groupBy("Weather_Condition") \
                         .agg(count("ID").alias("Qtd_Grave"))
    df_clima.count()
    metrics["t_clima_grave"] = time.time() - t0

    # 3) Severidade média por hora
    t0 = time.time()
    df_hora = sampled_df.withColumn("hora", hour("Start_Time")) \
                        .groupBy("hora") \
                        .agg(avg("Severity").alias("Media_Severidade"))
    df_hora.count()
    metrics["t_severidade_hora"] = time.time() - t0

    # 4) Condições da via (Crossing e Traffic_Signal)
    t0 = time.time()
    df_cond = sampled_df.agg(
        count(when(col("Crossing") == True, True)).alias("Qtd_Cruzamentos"),
        count(when(col("Traffic_Signal") == True, True)).alias("Qtd_Sinais")
    )
    df_cond.collect()
    metrics["t_condicoes_via"] = time.time() - t0

    # 5) Tipo de acidente via regex
    t0 = time.time()
    df_tipo = sampled_df.withColumn(
        "tipo_acidente",
        regexp_extract("Description", "(colisão|capotamento|atropelamento|batida)", 1)
    ).groupBy("tipo_acidente") \
     .count()
    df_tipo.count()
    metrics["t_tipo_acidente"] = time.time() - t0

    # Tempo total
    metrics["t_total"] = time.time() - total_start

    return metrics

if __name__ == "__main__":
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("Parallel Accidents Analysis") \
        .getOrCreate()

    # Caminho de entrada corrigido
    input_path = "s3://us-accidents-dataset/US_Accidents_March23_reduzido.csv"
    df = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(input_path))

    # Parâmetros de experimento
    sample_fractions = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0]
    executors_list   = [2, 4, 8, 12, 16]

    all_metrics = []
    for frac in sample_fractions:
        for ex in executors_list:
            # Ajusta dinamicamente o paralelismo
            spark.conf.set("spark.sql.shuffle.partitions", ex)
            spark.conf.set("spark.default.parallelism",   ex)

            m = process_sample_spark(df, frac, ex)
            all_metrics.append(m)
            print(f"[frac={frac:.2f} threads={ex}] "
                  f"t_total={m['t_total']:.2f}s, "
                  f"t_moving_avg_7d={m['t_moving_avg_7d']:.2f}s")

    # Converte para DataFrame e salva em CSV
    result_df = spark.createDataFrame(all_metrics)
    (result_df.coalesce(1)
              .write
              .mode("overwrite")
              .option("header", "true")
              .csv("s3://us-accidents-dataset/parallel_metrics_actions"))

    spark.stop()
