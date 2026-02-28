"""Script to close ALL positions at a profit.

Keeps running until all positions are closed profitably.
- Cancels unfilled orders each loop
- Retries until position is actually 0
"""

import time
from bot import BaseBot, OrderBook, Trade, OrderRequest, Side

TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
USERNAME = "test1"
PASSWORD = "test1"

CHECK_INTERVAL = 3  # seconds between checks


class PositionCloser(BaseBot):
    def on_orderbook(self, orderbook: OrderBook) -> None:
        pass

    def on_trades(self, trade: Trade) -> None:
        pass


def calculate_entry_prices(trades: list[Trade], username: str) -> dict:
    """Calculate average entry price for longs and shorts separately."""
    positions = {}

    for trade in trades:
        product = trade.product
        if product not in positions:
            positions[product] = {
                'long_cost': 0,
                'long_qty': 0,
                'short_cost': 0,
                'short_qty': 0,
            }

        if trade.buyer == username:
            positions[product]['long_cost'] += trade.price * trade.volume
            positions[product]['long_qty'] += trade.volume
        elif trade.seller == username:
            positions[product]['short_cost'] += trade.price * trade.volume
            positions[product]['short_qty'] += trade.volume

    result = {}
    for product, data in positions.items():
        result[product] = {
            'long_avg': data['long_cost'] / data['long_qty'] if data['long_qty'] > 0 else None,
            'long_qty': data['long_qty'],
            'short_avg': data['short_cost'] / data['short_qty'] if data['short_qty'] > 0 else None,
            'short_qty': data['short_qty'],
        }

    return result


print(f"\nConnecting to exchange as {USERNAME}...")
bot = PositionCloser(TEST_URL, USERNAME, PASSWORD)

# Fetch trade history once at start
print("Fetching trade history...")
trades = bot.get_market_trades()
my_trades = [t for t in trades if t.buyer == USERNAME or t.seller == USERNAME]
print(f"Found {len(my_trades)} of your trades")

entry_data = calculate_entry_prices(my_trades, USERNAME)

print("\n" + "="*60)
print("RUNNING - Will close positions when profitable")
print("Press Ctrl+C to stop")
print("="*60)

try:
    while True:
        # ALWAYS cancel all orders first - this frees up positions for closing
        bot.cancel_all_orders()
        time.sleep(0.3)

        # Get FRESH positions after cancelling orders
        positions = bot.get_positions()

        # Filter to non-zero positions
        open_positions = {p: pos for p, pos in positions.items() if pos != 0}

        if not open_positions:
            print("\n*** ALL POSITIONS CLOSED! ***")
            break

        print(f"\n--- {time.strftime('%H:%M:%S')} - {len(open_positions)} open positions ---")

        for product, net_pos in sorted(open_positions.items()):
            try:
                ob = bot.get_orderbook(product)
                data = entry_data.get(product, {})

                if net_pos > 0:
                    # LONG position - need to sell at bid
                    entry_price = data.get('long_avg')
                    exit_price = ob.buy_orders[0].price if ob.buy_orders else None

                    if entry_price and exit_price:
                        pnl = (exit_price - entry_price) * net_pos
                        profitable = exit_price > entry_price

                        if profitable:
                            # CLOSE IT!
                            order = OrderRequest(product, exit_price, Side.SELL, abs(net_pos))
                            print(f"  {product}: LONG {net_pos} - CLOSING at {exit_price} (entry={entry_price:.1f}, P&L={pnl:+.0f})")
                            resp = bot.send_order(order)
                            if resp:
                                print(f"    -> filled={resp.filled}/{resp.volume}")
                                # If not fully filled, cancel the remainder
                                if resp.filled < resp.volume:
                                    bot.cancel_order(resp.id)
                                    print(f"    -> cancelled unfilled portion")

                            # Refresh entry data
                            time.sleep(0.3)
                            trades = bot.get_market_trades()
                            my_trades = [t for t in trades if t.buyer == USERNAME or t.seller == USERNAME]
                            entry_data = calculate_entry_prices(my_trades, USERNAME)
                        else:
                            print(f"  {product}: LONG {net_pos} @ {entry_price:.1f}, current={exit_price}, P&L={pnl:+.0f} (waiting...)")
                    else:
                        print(f"  {product}: LONG {net_pos} - no entry/exit data")

                else:
                    # SHORT position - need to buy at ask
                    entry_price = data.get('short_avg')
                    exit_price = ob.sell_orders[0].price if ob.sell_orders else None

                    if entry_price and exit_price:
                        pnl = (entry_price - exit_price) * abs(net_pos)
                        profitable = exit_price < entry_price

                        if profitable:
                            # CLOSE IT!
                            order = OrderRequest(product, exit_price, Side.BUY, abs(net_pos))
                            print(f"  {product}: SHORT {abs(net_pos)} - CLOSING at {exit_price} (entry={entry_price:.1f}, P&L={pnl:+.0f})")
                            resp = bot.send_order(order)
                            if resp:
                                print(f"    -> filled={resp.filled}/{resp.volume}")
                                # If not fully filled, cancel the remainder
                                if resp.filled < resp.volume:
                                    bot.cancel_order(resp.id)
                                    print(f"    -> cancelled unfilled portion")

                            # Refresh entry data
                            time.sleep(0.3)
                            trades = bot.get_market_trades()
                            my_trades = [t for t in trades if t.buyer == USERNAME or t.seller == USERNAME]
                            entry_data = calculate_entry_prices(my_trades, USERNAME)
                        else:
                            print(f"  {product}: SHORT {abs(net_pos)} @ {entry_price:.1f}, current={exit_price}, P&L={pnl:+.0f} (waiting...)")
                    else:
                        print(f"  {product}: SHORT {abs(net_pos)} - no entry/exit data")

            except Exception as e:
                print(f"  {product}: Error - {e}")

        time.sleep(CHECK_INTERVAL)

except KeyboardInterrupt:
    print("\n\nStopped by user.")

# Final state
print("\n--- Final State ---")
bot.cancel_all_orders()

positions = bot.get_positions()
if positions:
    open_pos = {p: pos for p, pos in positions.items() if pos != 0}
    if open_pos:
        print("Remaining positions:")
        for product, pos in sorted(open_pos.items()):
            print(f"  {product}: {pos:+d}")
    else:
        print("Completely flat!")
else:
    print("Completely flat!")

pnl = bot.get_pnl()
if pnl:
    print(f"\nTotal P&L: {pnl.get('totalProfit', 0)}")

print("\nDone.")
