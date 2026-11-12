"""
Stateful Nexmark-style event generator.

Maintains a bounded sliding window of recently-generated person and
auction ids so that foreign-key-style references (Bid.bidder,
Bid.auction, Auction.seller) point at entities that actually exist,
without holding unbounded history in memory.

Event-type mix (1 person : 3 auctions : 46 bids) matches the original
Nexmark generator's documented defaults (confirmed against the Rust
`nexmark` crate's config.rs: person_proportion=1, auction_proportion=3,
bid_proportion=46). The "hot seller / hot auction / hot bidder" skew
model from the original generator is NOT implemented here — references
are chosen uniformly at random from the recent window. That's a
deliberate simplification, documented so results aren't mistaken for
a faithful full Nexmark generator.
"""
import random
import time
from collections import deque
from typing import Optional

from generator.schema import Auction, Bid, Person

PERSON_PROPORTION = 1
AUCTION_PROPORTION = 3
BID_PROPORTION = 46

FIRST_NAMES = ["Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Riley", "Jamie"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Brown", "Garcia", "Patel", "Kim", "Nguyen"]
STATES = ["CA", "OR", "WA", "NY", "TX", "FL", "IL", "CO"]
CITIES = ["Springfield", "Riverside", "Fairview", "Centerville", "Georgetown"]
ITEM_ADJECTIVES = ["Vintage", "Rare", "Handmade", "Refurbished", "Limited-Edition"]
ITEM_NOUNS = ["Watch", "Camera", "Guitar", "Painting", "Bicycle", "Chair", "Lamp"]

PERSON_WINDOW_SIZE = 2000
AUCTION_WINDOW_SIZE = 2000
AUCTION_DURATION_MS = 10 * 60 * 1000  # auctions "expire" 10 minutes after creation


def _now_ms() -> int:
    return int(time.time() * 1000)


class NexmarkGenerator:
    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)
        self._next_person_id = 1
        self._next_auction_id = 1
        self._recent_person_ids: deque = deque(maxlen=PERSON_WINDOW_SIZE)
        self._recent_auction_ids: deque = deque(maxlen=AUCTION_WINDOW_SIZE)

    def _random_person_id(self) -> int:
        if not self._recent_person_ids:
            return self.generate_person().id
        return self._rng.choice(self._recent_person_ids)

    def _random_auction_id(self) -> int:
        if not self._recent_auction_ids:
            return self.generate_auction().id
        return self._rng.choice(self._recent_auction_ids)

    def generate_person(self) -> Person:
        pid = self._next_person_id
        self._next_person_id += 1
        first = self._rng.choice(FIRST_NAMES)
        last = self._rng.choice(LAST_NAMES)
        person = Person(
            id=pid,
            name=f"{first} {last}",
            email=f"{first.lower()}.{last.lower()}{pid}@example.com",
            credit_card=" ".join(f"{self._rng.randint(0, 9999):04d}" for _ in range(4)),
            city=self._rng.choice(CITIES),
            state=self._rng.choice(STATES),
            date_time=_now_ms(),
        )
        self._recent_person_ids.append(pid)
        return person

    def generate_auction(self) -> Auction:
        aid = self._next_auction_id
        self._next_auction_id += 1
        seller_id = self._random_person_id()
        now = _now_ms()
        initial_bid = self._rng.randint(100, 5000)
        auction = Auction(
            id=aid,
            item_name=f"{self._rng.choice(ITEM_ADJECTIVES)} {self._rng.choice(ITEM_NOUNS)}",
            description="Nexmark-style auction listing (generated).",
            initial_bid=initial_bid,
            reserve_price=initial_bid + self._rng.randint(0, 2000),
            date_time=now,
            expires=now + AUCTION_DURATION_MS,
            seller=seller_id,
            category=self._rng.randint(1, 10),
        )
        self._recent_auction_ids.append(aid)
        return auction

    def generate_bid(self) -> Bid:
        return Bid(
            auction=self._random_auction_id(),
            bidder=self._random_person_id(),
            price=self._rng.randint(100, 10000),
            date_time=_now_ms(),
        )

    def next_event(self):
        """Pick an event type by the 1:3:46 mix and generate it.

        Returns (kafka_topic_name, event_as_dict).
        """
        total = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION
        roll = self._rng.randint(1, total)
        if roll <= PERSON_PROPORTION:
            return "person", self.generate_person().to_dict()
        if roll <= PERSON_PROPORTION + AUCTION_PROPORTION:
            return "auction", self.generate_auction().to_dict()
        return "bid", self.generate_bid().to_dict()
