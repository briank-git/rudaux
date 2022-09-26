import pendulum as plm
from typing import Optional, List, Dict
from pydantic import BaseModel
from .override import Override


class Assignment(BaseModel):
    lms_id: str
    name: str
    due_at: plm.DateTime
    lock_at: plm.DateTime
    unlock_at: plm.DateTime
    overrides: Dict[str, Override]
    published: bool
