import math
from datetime import date, timedelta

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession, Window


@pytest.fixture(scope="session")
def spark():
    """Instancia uma sessão Spark local para testes unitários."""
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("MacroFlow-Transform-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_daily_return_and_zero_division_guard(spark):
    """Verifica se o retorno diário respeita a fórmula e ignora bases <= 0."""
    data = [
        ("USDBRL=X", date(2026, 1, 1), 5.0),
        ("USDBRL=X", date(2026, 1, 2), 5.5),  # Retorno esperado: +10% (0.10)
        ("USDBRL=X", date(2026, 1, 3), 0.0),  # Fechamento anômalo
        ("USDBRL=X", date(2026, 1, 4), 5.5),  # Base anterior = 0, deve resultar em NULL
    ]
    df = spark.createDataFrame(data, ["ticker", "reference_date", "close_price"])

    w_lag = Window.partitionBy("ticker").orderBy("reference_date")
    df_calc = (
        df.withColumn("prev_close", F.lag("close_price", 1).over(w_lag)).withColumn(
            "daily_return",
            F.when(F.col("prev_close") > 0, (F.col("close_price") - F.col("prev_close")) / F.col("prev_close")),
        )
    ).collect()

    # Linha 0: sem dia anterior -> NULL
    assert df_calc[0]["daily_return"] is None

    # Linha 1: (5.5 - 5.0) / 5.0 = 0.10
    assert pytest.approx(df_calc[1]["daily_return"], 1e-4) == 0.10

    # Linha 3: dia anterior era 0.0 -> proteção prev_close > 0 ativada -> NULL
    assert df_calc[3]["daily_return"] is None


def test_cold_start_sma_and_volatility(spark):
    """Garante que janelas com menos observações que o tamanho exigido retornem NULL."""
    # Cria uma série temporal com 35 dias consecutivos
    start_dt = date(2026, 1, 1)
    data = [("USDBRL=X", start_dt + timedelta(days=i), 5.0 + (i * 0.05)) for i in range(35)]
    df = spark.createDataFrame(data, ["ticker", "reference_date", "close_price"])

    w_lag = Window.partitionBy("ticker").orderBy("reference_date")
    w_7d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-6, 0)
    w_vol_30d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-29, 0)

    df_analytics = (
        df.withColumn("prev_close", F.lag("close_price", 1).over(w_lag))
        .withColumn(
            "daily_return",
            F.when(F.col("prev_close") > 0, (F.col("close_price") - F.col("prev_close")) / F.col("prev_close")),
        )
        .withColumn("count_7d", F.count("close_price").over(w_7d))
        .withColumn("count_vol_30d", F.count("daily_return").over(w_vol_30d))
        .withColumn("sma_7", F.when(F.col("count_7d") == 7, F.avg("close_price").over(w_7d)))
        .withColumn("rolling_vol_30d", F.when(F.col("count_vol_30d") >= 30, F.stddev("daily_return").over(w_vol_30d)))
    ).collect()

    # 1. Validação de Cold Start da SMA-7
    # Primeiros 6 dias: count < 7 -> sma_7 deve ser NULL
    for idx in range(6):
        assert df_analytics[idx]["sma_7"] is None
    # No 7º dia (idx=6): janela atinge 7 elementos completos
    assert df_analytics[6]["sma_7"] is not None

    # 2. Validação de Cold Start e Correção da Volatilidade de 30 dias
    # Como o primeiro dia não tem retorno (idx=0 tem daily_return=NULL),
    # a contagem atinge 30 retornos válidos no 31º registro da série (idx=30)
    assert df_analytics[29]["rolling_vol_30d"] is None
    assert df_analytics[30]["rolling_vol_30d"] is not None

    # No dia seguinte (idx=31), com count >= 30, a métrica continua calculando e não zera
    assert df_analytics[31]["rolling_vol_30d"] is not None
    assert not math.isnan(df_analytics[31]["rolling_vol_30d"])
