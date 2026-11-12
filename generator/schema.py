"""
Event schema matching Apache Beam's Nexmark reference implementation
(Person, Auction, Bid field lists) so results stay comparable to
published Nexmark/Beam numbers. Channel/url fields from some other
Nexmark ports are intentionally omitted — not part of Beam's model.
"""
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Person:
    id: int
    name: str
    email: str
    credit_card: str
    city: str
    state: str
    date_time: int  # epoch millis
    extra: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Auction:
    id: int
    item_name: str
    description: str
    initial_bid: int
    reserve_price: int
    date_time: int  # epoch millis
    expires: int  # epoch millis
    seller: int  # references Person.id
    category: int
    extra: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Bid:
    auction: int  # references Auction.id
    bidder: int  # references Person.id
    price: int
    date_time: int  # epoch millis
    extra: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)
