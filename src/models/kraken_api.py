from datetime import datetime
from dataclasses import dataclass, asdict


@dataclass
class CryptoCurrencyOHLC:
    name: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    record_datetime: str

    @classmethod
    def from_json(cls, data: dict, name: str, symbol: str) -> "CryptoCurrencyOHLC":
        pair_key = next(iter(data.keys() - {"last"}), None)

        if not pair_key:
            raise ValueError("No valid pair key found in the provided data")

        last_timestamp = data["last"]

        last_entry = next((entry for entry in data[pair_key] if entry[0] == last_timestamp), None)

        if not last_entry:
            raise ValueError(f"No data found for timestamp {last_timestamp}")

        return cls(
            name=name,
            symbol=symbol,
            record_datetime=datetime.fromtimestamp(last_entry[0]).isoformat(),
            open=float(last_entry[1]),
            high=float(last_entry[2]),
            low=float(last_entry[3]),
            close=float(last_entry[4]),
        )

    def to_dict(self):
        return asdict(self)