from typing import Optional, List, Dict
from dataclasses import dataclass

from uuid import uuid4
from datetime import datetime

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
    created_at: Optional[str] = None
    from_pinterest: bool = False

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid4())

        if not self.created_at:
            self.created_at = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return self.__dict__

    def reset_created_at(self):
        self.created_at = datetime.now().isoformat()


@dataclass
class Pin:
    id: str
    user_id: str
    board_id: str
    created_at: str
    image_url: str
    from_pinterest: bool = True
    board_name: Optional[str] = None
    title: Optional[str] = None
    point_id: Optional[str] = None

    def to_dict(self) -> Dict:
        return self.__dict__

    def set_board_id(self, board_id: str):
        self.board_id = board_id

    def set_point_id(self, point_id: str):
        self.point_id = point_id

    def reset_created_at(self):
        self.created_at = datetime.now().isoformat()

    def reset_id(self):
        self.id = str(uuid4())

    def to_bigquery(self) -> Dict:
        return {
            "id": self.id,
            "created_at": datetime.now().isoformat(),
            "board_id": self.board_id,
            "image_url": self.image_url,
            "title": self.title,
            "point_id": self.point_id,
            "from_pinterest": self.from_pinterest,
        }


@dataclass
class Vector:
    values: List[float]
    metadata: Optional[Dict] = None

    def __post_init__(self):
        self.id = str(uuid4())
        self.process_metadata()

    def to_dict(self) -> Dict:
        return self.__dict__

    def process_metadata(self):
        metadata_copy = self.metadata.copy()

        for key, value in metadata_copy.items():
            if value is None:
                del self.metadata[key]


@dataclass
class PinVector:
    user_id: str
    pin_id: str
    point_id: str
    created_at: Optional[str] = None

    def __post_init__(self):
        self.id = f"{self.user_id}{self.pin_id}"

        if not self.created_at:
            self.created_at = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return self.__dict__

    @classmethod
    def from_dict(cls, data: Dict) -> "PinVector":
        return cls(
            user_id=data["user_id"],
            pin_id=data["pin_id"],
            point_id=data["point_id"],
            created_at=data.get("created_at"),
        )
