from typing import Optional, List, Dict
from dataclasses import dataclass

from uuid import uuid4


@dataclass
class Pin:
    user_id: str
    board_id: str
    id: str
    created_at: str
    image_url: str
    from_pinterest: bool
    board_name: Optional[str] = None
    title: Optional[str] = None

    def to_dict(self) -> Dict:
        return self.__dict__


@dataclass
class Vector: 
    values: List[float]
    metadata: Optional[Dict] = None

    def __post_init__(self):
        self.id = str(uuid4())

    def to_dict(self) -> Dict:
        return self.__dict__


@dataclass
class PinVector:
    user_id: str
    pin_id: str
    point_id: str
    
    def to_dict(self) -> Dict:
        return self.__dict__
