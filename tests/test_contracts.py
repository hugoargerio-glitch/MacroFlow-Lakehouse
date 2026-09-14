from datetime import date

import pytest
from pydantic import ValidationError

from src.schemas.macro_models import BCBRecord, MarketRecord

# =====================================================================
# 1. TESTES DE CONTRATOS DO BANCO CENTRAL DO BRASIL (BCB)
# =====================================================================


def test_bcb_record_valid():
    """Valida a criação de um registro consistente do BCB."""
    record = BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=10.75)
    assert record.reference_date == date(2026, 3, 10)
    assert record.ticker_or_code == "SELIC"
    assert record.close_price == 10.75


def test_bcb_record_invalid_price():
    """Garante rejeição de valores nulos ou abaixo do limiar estrutural."""
    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=None)

    with pytest.raises(ValidationError):
        BCBRecord(
            reference_date="2026-03-10",
            ticker_or_code="IPCA",
            close_price=-150.0,  # Limiar gt=-100.0 violado
        )


def test_bcb_record_invalid_date_format():
    """Rejeita datas fora do formato ISO 8601."""
    with pytest.raises(ValidationError):
        BCBRecord(reference_date="10/03/2026", ticker_or_code="USD_PTAX", close_price=5.1250)


# =====================================================================
# 2. TESTES DE CONTRATOS DO YAHOO FINANCE (MERCADO)
# =====================================================================


def test_market_record_valid():
    """Valida a estrutura de ativos financeiros regulares."""
    record = MarketRecord(
        reference_date="2026-03-10",
        ticker="USDBRL=X",
        open_price=5.1000,
        high_price=5.1500,
        low_price=5.0900,
        close_price=5.1420,
        volume=1500000.0,
    )
    assert record.ticker == "USDBRL=X"
    assert record.close_price == 5.1420


def test_market_record_rejects_negative_or_zero_price():
    """Garante que preços de fechamento menores ou iguais a zero disparem exceção."""
    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="^BVSP", close_price=0.0)

    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="BTC-BRL", close_price=-100.50)


def test_market_record_missing_mandatory_field():
    """Garante que ausência de ticker ou de close_price bloqueie a ingestão."""
    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", close_price=5.20)
