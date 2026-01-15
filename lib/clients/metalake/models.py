from __future__ import annotations

from typing import Any, List, Optional
from pydantic import BaseModel, Field



class TrinoTypeSignature(BaseModel):
    rawType: str
    arguments: List[Any] = Field(default_factory=list)


class TrinoColumn(BaseModel):
    name: str
    type: str
    typeSignature: Optional[TrinoTypeSignature] = None


class TrinoQueryPage(BaseModel):
    id: Optional[str] = None
    infoUri: Optional[str] = None
    nextUri: Optional[str] = None
    columns: Optional[List[TrinoColumn]] = None
    data: Optional[List[List[Any]]] = None
    stats: Optional[dict] = None
    error: Optional[dict] = None
    warnings: Optional[List[dict]] = None


class TrinoQueryResult(BaseModel):
    id: Optional[str] = None
    infoUri: Optional[str] = None
    columns: List[TrinoColumn] = Field(default_factory=list)
    data: List[List[Any]] = Field(default_factory=list)
    stats: Optional[dict] = None
    warnings: List[dict] = Field(default_factory=list)


class DescribeRow(BaseModel):
    Column: str
    Type: str
    Extra: Optional[str] = None
    Comment: Optional[str] = None

