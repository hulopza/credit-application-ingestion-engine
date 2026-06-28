import re
from datetime import datetime, date
from pydantic import BaseModel, Field, field_validator

class CreditApplicationEvent(BaseModel):
    application_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    requested_amount: float
    declared_income: float
    customer_age: int
    timestamp: datetime

    @field_validator('requested_amount')
    @classmethod
    def validate_requested_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("requested_amount must be strictly positive")
        return v

    @field_validator('declared_income')
    @classmethod
    def validate_declared_income(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("declared_income must be strictly positive")
        return v

    @field_validator('customer_age')
    @classmethod
    def validate_customer_age(cls, v: int) -> int:
        if not (18 <= v <= 65):
            raise ValueError("customer_age must be between 18 and 65 inclusive")
        return v


class PartnerTransactionRow(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    account_id: str = Field(..., min_length=1)
    transaction_date: date
    amount: float
    reference_code: str = Field(..., min_length=1)

    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be strictly positive")
        return v

    @field_validator('reference_code')
    @classmethod
    def validate_reference_code(cls, v: str) -> str:
        if not re.match(r"^REF-PARTNER_[A-Z]+-\d+$", v):
            raise ValueError("reference_code must match format REF-PARTNER_<NAME>-<ID>")
        return v

