from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class BCBRecord(BaseModel):
    """Contrato de validação para os dados das séries do Banco Central (SGS)."""
    data: str = Field(description="Data da observação no formato DD/MM/YYYY")
    valor: float = Field(description="Valor numérico do indicador")

    @field_validator("valor", mode="before")
    def parse_float(cls, v):
        if v is None or v == "":
            raise ValueError("Valor não pode ser nulo ou vazio")
        return float(v)


class MarketAssetRecord(BaseModel):
    """Contrato de validação para ativos financeiros do Yahoo Finance."""
    reference_date: date = Field(description="Data de fechamento do mercado")
    ticker: str = Field(description="Código do ativo (ex: USDBRL=X, ^BVSP)")
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: float = Field(description="Preço de fechamento ajustado")
    volume: Optional[float] = 0.0
