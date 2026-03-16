from pydantic import BaseModel
from typing import Any

class ParameterSpecSchema(BaseModel):
    name: str
    type: str
    default: Any
    min_val: Any = None
    max_val: Any = None
    description: str = ""

class StrategySchema(BaseModel):
    id: str
    name: str
    description: str = ""
    parameters: list[ParameterSpecSchema] = []

    model_config = {"from_attributes": True}
