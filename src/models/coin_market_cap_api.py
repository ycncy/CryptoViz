from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class Quote:
    name: str
    price: float
    volume_24h: float
    market_cap: float
    percent_change_1h: float
    percent_change_24h: float
    percent_change_7d: float
    last_updated: str
    percent_change_30d: Optional[float] = None
    percent_change_60d: Optional[float] = None
    percent_change_90d: Optional[float] = None
    fully_diluted_market_cap: Optional[float] = None
    market_cap_by_total_supply: Optional[float] = None
    dominance: Optional[float] = None
    turnover: Optional[float] = None
    ytd_price_change_percentage: Optional[float] = None
    percent_change_1y: Optional[float] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class CryptoCurrencyData:
    id: int
    name: str
    symbol: str
    cmc_rank: int
    market_pair_count: int
    circulating_supply: float
    total_supply: float
    max_supply: Optional[float]
    is_active: int
    last_updated: str
    date_added: str
    quotes: List[Quote] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: dict) -> "CryptoCurrencyData":
        quotes = [
            Quote(
                name=quote["name"],
                price=quote["price"],
                volume_24h=quote["volume24h"],
                market_cap=quote["marketCap"],
                percent_change_1h=quote["percentChange1h"],
                percent_change_24h=quote["percentChange24h"],
                percent_change_7d=quote["percentChange7d"],
                last_updated=quote["lastUpdated"],
                percent_change_30d=quote.get("percentChange30d"),
                percent_change_60d=quote.get("percentChange60d"),
                percent_change_90d=quote.get("percentChange90d"),
                fully_diluted_market_cap=quote.get("fullyDilluttedMarketCap"),
                market_cap_by_total_supply=quote.get("marketCapByTotalSupply"),
                dominance=quote.get("dominance"),
                turnover=quote.get("turnover"),
                ytd_price_change_percentage=quote.get("ytdPriceChangePercentage"),
                percent_change_1y=quote.get("percentChange1y"),
            )
            for quote in data["quotes"]
        ]

        return cls(
            id=data["id"],
            name=data["name"],
            symbol=data["symbol"],
            cmc_rank=data["cmcRank"],
            market_pair_count=data["marketPairCount"],
            circulating_supply=data["circulatingSupply"],
            total_supply=data["totalSupply"],
            max_supply=data.get("maxSupply"),
            is_active=data["isActive"],
            last_updated=data["lastUpdated"],
            date_added=data["dateAdded"],
            quotes=quotes,
        )

    def to_dict(self):
        return asdict(self)
