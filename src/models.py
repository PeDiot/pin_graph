from typing import Optional, List, Dict
from dataclasses import dataclass

from uuid import uuid4

from .enums.supabase import (
    DEFAULT_RECOMMEND_BOARD_NAME,
    DEFAULT_RECOMMEND_BOARD_DESCRIPTION,
)


@dataclass
class Board:
    user_id: str
    name: str = DEFAULT_RECOMMEND_BOARD_NAME
    description: str = DEFAULT_RECOMMEND_BOARD_DESCRIPTION
    id: Optional[str] = None

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid4())

    def to_dict(self) -> Dict:
        return self.__dict__


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

    @classmethod
    def from_dict(cls, data: Dict) -> "PinVector":
        return cls(
            user_id=data["user_id"],
            pin_id=data["pin_id"],
            point_id=data["point_id"],
        )
