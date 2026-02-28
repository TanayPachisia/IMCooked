"""Emergency close - close ALL positions IMMEDIATELY.

Closes everything ASAP at market prices, regardless of profit/loss.
Keeps retrying until all positions are 0.
"""

import time
from bot import BaseBot, OrderBook, Trade, OrderRequest, Side

TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
USERNAME = "IMCooked"
PASSWORD = "imsocooked"


class PositionCloser(BaseBot):
    def on_orderbook(self, orderbook: OrderBook) -> None:
        pass

    def on_trades(self, trade: Trade) -> None:
        pass


print(f"\nConnecting to exchange as {USERNAME}...")
bot = PositionCloser(TEST_URL, USERNAME, PASSWORD)

print("\n" + "="*60)
print("EMERGENCY CLOSE - Closing ALL positions at market price")
print("="*60)

while True:
    # Cancel all orders first
    bot.cancel_all_orders()
    time.sleep(0.3)

    # Get fresh positions
    positions = bot.get_positions()
    open_positions = {p: pos for p, pos in positions.items() if pos != 0}

    if not open_positions:
        print("\n*** ALL POSITIONS CLOSED! ***")
        break

    print(f"\n--- {len(open_positions)} positions to close ---")

    for product, net_pos in sorted(open_positions.items()):
        try:
            ob = bot.get_orderbook(product)

            if net_pos > 0:
                # LONG - sell at best bid
                if ob.buy_orders:
                    price = ob.buy_orders[0].price
                    order = OrderRequest(product, price, Side.SELL, abs(net_pos))
                    print(f"  SELL {abs(net_pos)} {product} @ {price}")
                    resp = bot.send_order(order)
                    if resp:
                        print(f"    -> filled={resp.filled}/{resp.volume}")
                        if resp.filled < resp.volume:
                            bot.cancel_order(resp.id)
                else:
                    print(f"  {product}: No bids - cannot sell")

            else:
                # SHORT - buy at best ask
                if ob.sell_orders:
                    price = ob.sell_orders[0].price
                    order = OrderRequest(product, price, Side.BUY, abs(net_pos))
                    print(f"  BUY {abs(net_pos)} {product} @ {price}")
                    resp = bot.send_order(order)
                    if resp:
                        print(f"    -> filled={resp.filled}/{resp.volume}")
                        if resp.filled < resp.volume:
                            bot.cancel_order(resp.id)
                else:
                    print(f"  {product}: No asks - cannot buy")

            time.sleep(0.3)

        except Exception as e:
            print(f"  {product}: Error - {e}")

    time.sleep(1)

# Final state
print("\n--- Final State ---")
bot.cancel_all_orders()

positions = bot.get_positions()
open_pos = {p: pos for p, pos in positions.items() if pos != 0}
if open_pos:
    print("Remaining positions:")
    for product, pos in sorted(open_pos.items()):
        print(f"  {product}: {pos:+d}")
else:
    print("Completely flat!")

pnl = bot.get_pnl()
if pnl:
    print(f"\nTotal P&L: {pnl.get('totalProfit', 0)}")

print("\nDone.")
"""Emergency close - close ALL positions IMMEDIATELY.

Closes everything ASAP at market prices, regardless of profit/loss.
Keeps retrying until all positions are 0.
"""

import time
from bot import BaseBot, OrderBook, Trade, OrderRequest, Side

TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
USERNAME = "IMCooked"
PASSWORD = "imsocooked"


class PositionCloser(BaseBot):
    def on_orderbook(self, orderbook: OrderBook) -> None:
        pass

    def on_trades(self, trade: Trade) -> None:
        pass


print(f"\nConnecting to exchange as {USERNAME}...")
bot = PositionCloser(TEST_URL, USERNAME, PASSWORD)

print("\n" + "="*60)
print("EMERGENCY CLOSE - Closing ALL positions at market price")
print("="*60)

while True:
    # Cancel all orders first
    bot.cancel_all_orders()
    time.sleep(0.3)

    # Get fresh positions
    positions = bot.get_positions()
    open_positions = {p: pos for p, pos in positions.items() if pos != 0}

    if not open_positions:
        print("\n*** ALL POSITIONS CLOSED! ***")
        break

    print(f"\n--- {len(open_positions)} positions to close ---")

    for product, net_pos in sorted(open_positions.items()):
        try:
            ob = bot.get_orderbook(product)

            if net_pos > 0:
                # LONG - sell at best bid
                if ob.buy_orders:
                    price = ob.buy_orders[0].price
                    order = OrderRequest(product, price, Side.SELL, abs(net_pos))
                    print(f"  SELL {abs(net_pos)} {product} @ {price}")
                    resp = bot.send_order(order)
                    if resp:
                        print(f"    -> filled={resp.filled}/{resp.volume}")
                        if resp.filled < resp.volume:
                            bot.cancel_order(resp.id)
                else:
                    print(f"  {product}: No bids - cannot sell")

            else:
                # SHORT - buy at best ask
                if ob.sell_orders:
                    price = ob.sell_orders[0].price
                    order = OrderRequest(product, price, Side.BUY, abs(net_pos))
                    print(f"  BUY {abs(net_pos)} {product} @ {price}")
                    resp = bot.send_order(order)
                    if resp:
                        print(f"    -> filled={resp.filled}/{resp.volume}")
                        if resp.filled < resp.volume:
                            bot.cancel_order(resp.id)
                else:
                    print(f"  {product}: No asks - cannot buy")

            time.sleep(0.3)

        except Exception as e:
            print(f"  {product}: Error - {e}")

    time.sleep(1)

# Final state
print("\n--- Final State ---")
bot.cancel_all_orders()

positions = bot.get_positions()
open_pos = {p: pos for p, pos in positions.items() if pos != 0}
if open_pos:
    print("Remaining positions:")
    for product, pos in sorted(open_pos.items()):
        print(f"  {product}: {pos:+d}")
else:
    print("Completely flat!")

pnl = bot.get_pnl()
if pnl:
    print(f"\nTotal P&L: {pnl.get('totalProfit', 0)}")

print("\nDone.")
