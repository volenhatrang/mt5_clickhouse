from esmerald import Esmerald, Gateway, get
from clickhouse_driver import Client
from msgspec import Struct
from typing import List, Optional

client = Client(host="clickhouse", port=9000)

class MT5Data(Struct):
    symbol: str
    time: str
    bid: float
    ask: float
    volume: int

@get(path="/data")
async def get_mt5_data(
    symbol: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> List[MT5Data]:
    """
    Lấy dữ liệu từ bảng mt5_data.
    """
    query = "SELECT symbol, time, bid, ask, volume FROM mt5.mt5_data WHERE 1=1"
    params = {}

    if symbol:
        query += " AND symbol = %(symbol)s"
        params["symbol"] = symbol
    if start_time:
        query += " AND time >= %(start_time)s"
        params["start_time"] = start_time
    if end_time:
        query += " AND time <= %(end_time)s"
        params["end_time"] = end_time

    query += " ORDER BY time DESC LIMIT 100"

    result = client.execute(query, params)

    return [
        MT5Data(
            symbol=row[0],
            time=row[1].strftime("%Y-%m-%d %H:%M:%S"),
            bid=row[2],
            ask=row[3],
            volume=row[4],
        )
        for row in result
    ]

app = Esmerald(routes=[Gateway(handler=get_mt5_data)])
