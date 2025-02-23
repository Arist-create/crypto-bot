import asyncio

from trading.gate import gate_trade
from trading.mexc import mexc_trade
from facades.redis import redis
from config import config


class Trade:
    def __init__(self):
        self._max_volume_for_trade = config.MAX_VOLUME_FOR_TRADE
        self._gate = gate_trade
        self._mexc = mexc_trade

    async def enter_position(self, pair):
        symbol = pair.get("symbol")
        qty_in_usdt = await self.get_max_volume_for_enter_position(pair)
        print(f"Qty in usdt: {qty_in_usdt}")
        if qty_in_usdt == 0:
            return False
        (
            price_mexc,
            price_gate,
            contract_size_gate,
            contract_size_mexc,
        ) = await asyncio.gather(
            redis.get(f"{symbol}@MEXC"),
            redis.get(f"{symbol}@GATE"),
            redis.get(f"{symbol}@contract_size@GATE"),
            redis.get(f"{symbol}@contract_size@MEXC"),
        )
        price_mexc = (
            price_mexc.get("asks")[0].get("p")
            if pair.get("long") == "mexc"
            else price_mexc.get("bids")[0].get("p")
        )
        price_gate = (
            price_gate.get("asks")[0].get("p")
            if pair.get("long") == "gate"
            else price_gate.get("bids")[0].get("p")
        )
        qty_mexc = qty_in_usdt / price_mexc
        qty_gate = qty_in_usdt / price_gate
        qty = min(qty_mexc, qty_gate)
        max_contract_size = max(contract_size_mexc, contract_size_gate)
        qty_mexc = qty / max_contract_size
        qty_gate = qty / max_contract_size
        qty = min(qty_mexc, qty_gate)
        qty = int(qty)
        qty_mexc = qty * max_contract_size / contract_size_mexc
        qty_gate = qty * max_contract_size / contract_size_gate

        print(f"Qty in mexc: {int(qty_mexc)}")
        print(f"Qty in gate: {int(qty_gate)}")

        side_mexc = "long" if pair.get("long") == "mexc" else "short"
        side_gate = "long" if pair.get("long") == "gate" else "short"
        await asyncio.gather(
            self._mexc.place_order(symbol, side_mexc, int(qty_mexc)),
            self._gate.place_order(symbol, side_gate, int(qty_gate)),
        )
        return True

    async def exit_position(self, pair):
        symbol = pair.get("symbol")
        position_size_mexc, position_size_gate = await self._get_position_sizes(pair)
        if position_size_mexc == 0 or position_size_gate == 0:
            return False
        (
            price_mexc,
            price_gate,
            contract_size_mexc,
            contract_size_gate,
        ) = await asyncio.gather(
            redis.get(f"{symbol}@MEXC"),
            redis.get(f"{symbol}@GATE"),
            redis.get(f"{symbol}@contract_size@MEXC"),
            redis.get(f"{symbol}@contract_size@GATE"),
        )
        if pair.get("long") == "gate":
            price_mexc, price_gate, side_mexc, side_gate = (
                price_mexc.get("asks")[0],
                price_gate.get("bids")[0],
                "long",
                "short",
            )
        else:
            price_mexc, price_gate, side_mexc, side_gate = (
                price_mexc.get("bids")[0],
                price_gate.get("asks")[0],
                "short",
                "long",
            )
        price_mexc_size, price_gate_size = price_mexc.get("s"), price_gate.get("s")
        max_contract_size = max(contract_size_mexc, contract_size_gate)
        price_mexc_size = price_mexc_size / max_contract_size
        price_gate_size = price_gate_size / max_contract_size
        
        position_size_gate = position_size_gate * contract_size_gate / max_contract_size
        position_size_mexc = position_size_mexc * contract_size_mexc / max_contract_size

        qty = min(
            position_size_mexc,
            position_size_gate,
            price_mexc_size,
            price_gate_size,
        )

        qty_mexc = qty * max_contract_size / contract_size_mexc
        qty_gate = qty * max_contract_size / contract_size_gate
        await asyncio.gather(
            self._mexc.place_order(symbol, side_mexc, int(qty_mexc), reduce_only=True),
            self._gate.place_order(symbol, side_gate, int(qty_gate), reduce_only=True),
        )
        return True

    async def _get_min_balance(self):
        bybit_balance, gate_balance = await asyncio.gather(
            self._mexc.get_balance(), self._gate.get_balance()
        )

        return min(bybit_balance, gate_balance)

    async def get_max_volume_for_enter_position(self, pair):
        # symbol = pair.get("symbol")
        # price_mexc, price_gate, contract_size = await asyncio.gather(
        #     redis.get(f"{symbol}@MEXC"),
        #     redis.get(f"{symbol}@GATE"),
        #     redis.get(f"{symbol}@contract_size"),
        # )
        # if not price_mexc or not price_gate or not contract_size:
        #     return 0
        balance = await self._get_min_balance()
        if balance < 10:
            return 0
        return min(balance, self._max_volume_for_trade)

    async def start_websockets_for_orders(self):
        await asyncio.gather(
            self._gate.websocket_connection_for_orders(),
        )

    async def _get_position_sizes(self, pair):
        symbol = pair.get("symbol")
        mexc_position, gate_position = await asyncio.gather(
            self._mexc.get_all_positions(),
            self._gate.get_all_positions(),
        )
        mexc_position_volume, gate_position_volume = None, None
        for position in mexc_position:
            if position.get("symbol") == symbol:
                mexc_position_volume = position.get("holdVol")
        for position in gate_position:
            if position.get("contract") == symbol:
                gate_position_volume = position.get("size")
        if not mexc_position_volume or not gate_position_volume:
            return 0, 0
        gate_position_volume = (
            gate_position_volume
            if pair.get("long") == "gate"
            else gate_position_volume * -1
        )
        return float(mexc_position_volume), gate_position_volume


real_trade = Trade()
