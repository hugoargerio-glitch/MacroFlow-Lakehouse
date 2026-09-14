from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class BCBRecord(BaseModel):
    reference_date: date
    ticker_or_code: str
    close_price: float = Field(..., gt=-100.0, description="Taxa ou índice macroeconômico")

    @field_validator("close_price")
    def validate_positive(cls, v: float) -> float:
        if v is None:
            raise ValueError("O valor de fechamento não pode ser nulo.")
        return float(v)


class MarketRecord(BaseModel):
    reference_date: date
    ticker: str
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: float = Field(..., gt=0.0, description="Preço de fechamento estritamente positivo")
    volume: Optional[float] = 0.0
