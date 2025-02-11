import asyncio

from trading.gate import gate_trade
from trading.bybit import bybit_trade
from facades.redis import redis
from config import config



class Trade:
    def __init__(self):
        self._max_volume_for_trade = config.MAX_VOLUME_FOR_TRADE
        self._gate = gate_trade
        self._bybit = bybit_trade

    async def enter_position(self, pair):
        symbol = pair.get("symbol")
        qty_in_usdt = await self.get_max_volume_for_enter_position(pair)
        if qty_in_usdt == 0:
            return False
        price_bybit, price_gate, contract_size = await asyncio.gather(
            redis.get(f"{symbol}@BYBIT"),
            redis.get(f"{symbol}@GATE"),
            redis.get(f"{symbol}@contract_size"),
        )
        price_bybit = (
            price_bybit.get("asks")[0].get("p")
            if pair.get("long") == "bybit"
            else price_bybit.get("bids")[0].get("p")
        )
        price_gate = (
            price_gate.get("asks")[0].get("p")
            if pair.get("long") == "gate"
            else price_gate.get("bids")[0].get("p")
        )
        qty_bybit = qty_in_usdt / price_bybit
        qty_gate = qty_in_usdt / price_gate
        if contract_size > 1:
            qty_bybit, qty_gate = qty_bybit / contract_size, qty_gate / contract_size
        qty = min(qty_bybit, qty_gate)
        qty = int(qty)
        if qty == 0:
            return False
        if contract_size > 1:
            qty_bybit = qty * contract_size
            qty_gate = qty
        else:
            qty_bybit = qty
            qty_gate = qty / contract_size

        side_bybit = "long" if pair.get("long") == "bybit" else "short"
        side_gate = "long" if pair.get("long") == "gate" else "short"
        await asyncio.gather(
            self._bybit.place_order(symbol, side_bybit, qty_bybit),
            self._gate.place_order(symbol, side_gate, qty_gate),
        )
        return True

    async def exit_position(self, pair):
        symbol = pair.get("symbol")
        position_size_bybit, position_size_gate = await self._get_position_sizes(pair)
        if position_size_bybit == 0 or position_size_gate == 0:
            return False
        price_bybit, price_gate, contract_size = await asyncio.gather(
            redis.get(f"{symbol}@BYBIT"),
            redis.get(f"{symbol}@GATE"),
            redis.get(f"{symbol}@contract_size"),
        )
        if pair.get("long") == "gate":
            price_bybit, price_gate, side_bybit, side_gate = (
                price_bybit.get("asks")[0],
                price_gate.get("bids")[0],
                "long",
                "short",
            )
        else:
            price_bybit, price_gate, side_bybit, side_gate = (
                price_bybit.get("bids")[0],
                price_gate.get("asks")[0],
                "short",
                "long",
            )
        price_bybit_size, price_gate_size = price_bybit.get("s"), price_gate.get("s")

        if contract_size > 1:
            price_bybit_size, position_size_bybit = price_bybit_size / contract_size, position_size_bybit / contract_size
        qty = min(
            position_size_bybit,
            position_size_gate,
            price_bybit_size,
            price_gate_size,   
        )
        qty = int(qty)
        if contract_size > 1:
            qty_bybit = qty * contract_size
            qty_gate = qty
        else:
            qty_bybit = qty
            qty_gate = qty / contract_size
        await asyncio.gather(
            self._bybit.place_order(symbol, side_bybit, qty_bybit, reduce_only=True),
            self._gate.place_order(symbol, side_gate, qty_gate, reduce_only=True),
        )
        return True

    async def _get_min_balance(self):
        bybit_balance, gate_balance = await asyncio.gather(
            self._bybit.get_balance(), self._gate.get_balance()
        )

        return min(bybit_balance, gate_balance)

    async def get_max_volume_for_enter_position(self, pair):
        balance = await self._get_min_balance()
        min_vol_for_trade_in_usdt = pair.get("min_vol_for_trade_in_usdt")
        if min_vol_for_trade_in_usdt < 10 or balance < 10:
            return 0
        return min(pair.get("min_vol_for_trade_in_usdt"), balance, self._max_volume_for_trade)
    
    async def start_websockets_for_orders(self):
        await asyncio.gather(
            self._bybit.websocket_connection_for_orders(),
            self._gate.websocket_connection_for_orders(),
        )

    async def _get_position_sizes(self, pair):
        symbol = pair.get("symbol")
        bybit_position, gate_position, contract_size = await asyncio.gather(
            self._bybit.get_all_positions(),
            self._gate.get_all_positions(),
            redis.get(f"{symbol}@contract_size"),
        )
        bybit_position_volume, gate_position_volume = None, None
        for position in bybit_position:
            if position.get("symbol").replace("USDT", "_USDT") == symbol:
                bybit_position_volume = position.get("size")
        for position in gate_position:
            if position.get("contract") == symbol:
                gate_position_volume = position.get("size")
        if not bybit_position_volume or not gate_position_volume:
            return 0, 0
        gate_position_volume = (
            gate_position_volume
            if pair.get("long") == "gate"
            else gate_position_volume * -1
        )
        gate_position_volume *= contract_size
        return float(bybit_position_volume), gate_position_volume

real_trade = Trade()

# asyncio.run(real_trade.exit_position({"symbol": "GMT_USDT", "long": "bybit"}))