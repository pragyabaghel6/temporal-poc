from enum import Enum

from pydantic import BaseModel, Field


class OperationType(str, Enum):
    freeze = "freeze"
    unfreeze = "unfreeze"


class StartWorkflowRequest(BaseModel):
    file_path: str = Field(..., min_length=1)
    operation_type: OperationType

class FineRecord(BaseModel):
    first_name: str
    last_name: str
    dob: str
    amount: float
    address: str
    cif_id: str
