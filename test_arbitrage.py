"""Test script for ETF arbitrage bot on the test exchange."""

import time
from bot import ETFArbitrageBot

# Test exchange credentials
# TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
TEST_URL =  "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com/"
# Your test exchange credentials
USERNAME = "IMCooked"
PASSWORD = "imsocooked"

print(f"\nConnecting to test exchange as {USERNAME}...")

# Create and configure the bot
bot = ETFArbitrageBot(TEST_URL, USERNAME, PASSWORD)

# Adjust parameters for testing - lower threshold to see more opportunities
bot.MIN_SPREAD = 5  # Lower threshold for more opportunities
bot.ORDER_SIZE = 2  # Smaller size for testing
bot.MAX_POSITION = 20  # Conservative position limits

print("\nBot configuration:")
print(f"  Min spread: {bot.MIN_SPREAD}")
print(f"  Order size: {bot.ORDER_SIZE}")
print(f"  Max position: {bot.MAX_POSITION}")
print(f"\nETF components:")
print(f"  {bot.MARKET_1} (Tide) + {bot.MARKET_3} (Weather) + {bot.MARKET_5} (Flights) = {bot.MARKET_7} (ETF)")

# First, show available products on the exchange
print("\n--- Available Products ---")
try:
    products = bot.get_products()
    for p in products:
        print(f"  {p.symbol} (tick={p.tickSize}, start={p.startingPrice})")
except Exception as e:
    print(f"  Error fetching products: {e}")

# Start the bot
bot.start()
print("\n✓ Bot started. Listening for arbitrage opportunities...")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        time.sleep(5)

        print("\n" + "="*60)

        # Show orderbook state for our markets
        print("--- Orderbook State ---")
        for market in [bot.MARKET_1, bot.MARKET_3, bot.MARKET_5, bot.MARKET_7]:
            if market in bot.orderbooks:
                ob = bot.orderbooks[market]
                bid = ob.buy_orders[0].price if ob.buy_orders else None
                ask = ob.sell_orders[0].price if ob.sell_orders else None
                spread = (ask - bid) if (bid and ask) else None
                print(f"  {market:12}: bid={bid}, ask={ask}, spread={spread}")
            else:
                print(f"  {market:12}: NO DATA (symbol may not exist or no orders)")

        # Calculate and show arbitrage spreads
        etf_bid = bot.get_best_bid(bot.MARKET_7)
        etf_ask = bot.get_best_ask(bot.MARKET_7)
        synth_bid = bot.get_synthetic_etf_bid()
        synth_ask = bot.get_synthetic_etf_ask()

        print("\n--- Arbitrage Analysis ---")
        if all(v is not None for v in [etf_bid, etf_ask, synth_bid, synth_ask]):
            spread_over = etf_bid - synth_ask   # ETF overpriced opportunity
            spread_under = synth_bid - etf_ask  # ETF underpriced opportunity

            print(f"  ETF:       bid={etf_bid:.0f}, ask={etf_ask:.0f}")
            print(f"  Synthetic: bid={synth_bid:.0f}, ask={synth_ask:.0f}")
            print(f"  Spread (ETF overpriced):  {spread_over:+.2f} {'<-- OPPORTUNITY!' if spread_over > bot.MIN_SPREAD else ''}")
            print(f"  Spread (ETF underpriced): {spread_under:+.2f} {'<-- OPPORTUNITY!' if spread_under > bot.MIN_SPREAD else ''}")
            print(f"  Min spread required: {bot.MIN_SPREAD}")
        else:
            print("  Cannot calculate - missing orderbook data")
            print(f"    ETF bid={etf_bid}, ask={etf_ask}")
            print(f"    Synth bid={synth_bid}, ask={synth_ask}")

        # Show current positions
        positions = bot.get_positions()
        print("\n--- Positions ---")
        if positions:
            for product, pos in sorted(positions.items()):
                print(f"  {product}: {pos:+d}")
        else:
            print("  (flat - no positions)")

        # Show PnL
        pnl = bot.get_pnl()
        if pnl:
            print(f"\nP&L: totalProfit={pnl.get('totalProfit', 0)}")

except KeyboardInterrupt:
    print("\n\nStopping bot...")
    bot.cancel_all_orders()
    bot.stop()

    # Final summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)

    positions = bot.get_positions()
    if positions:
        print("\nFinal positions:")
        for product, pos in sorted(positions.items()):
            print(f"  {product}: {pos:+d}")

    pnl = bot.get_pnl()
    if pnl:
        print(f"\nFinal P&L: {pnl}")

    print("\nBot stopped successfully.")
