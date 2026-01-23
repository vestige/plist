from pydantic import BaseModel
from typing import Optional, Literal
from datetime import datetime

Status = Literal["available", "loaned", "retired"]

class AssetIn(BaseModel):
    name: str
    asset_tag: str
    category: Optional[str] = None
    location: Optional[str] = None
    note: Optional[str] = None

class AssetUpdate(BaseModel):
    name: Optional[str] = None
    asset_tag: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    note: Optional[str] = None
    status: Optional[Status] = None

class Asset(AssetIn):
    id: str
    status: Status = "available"
    created_at: datetime
    updated_at: datetime

class AssetsMeta(BaseModel):
    total: int
    limit: int
    offset: int
    total_pages: int

class LoanIn(BaseModel):
    borrower: str
    due_at: Optional[datetime] = None
    note: Optional[str] = None

class Loan(BaseModel):
    id: str
    asset_id: str
    borrower: str
    loaned_at: datetime
    due_at: Optional[datetime] = None
    returned_at: Optional[datetime] = None
    note: Optional[str] = None
