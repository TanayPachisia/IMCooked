"""Test script for ETF arbitrage bot on the test exchange."""

import time
from bot import ETFArbitrageBot

# Test exchange credentials
TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"

# Your test exchange credentials
USERNAME = "IMCooked"
PASSWORD = "imsocooked"

print(f"\nConnecting to test exchange as {USERNAME}...")

# Create and configure the bot
bot = ETFArbitrageBot(TEST_URL, USERNAME, PASSWORD)

# Adjust parameters for testing
bot.MIN_SPREAD = 3  # Lower threshold for more opportunities
bot.ORDER_SIZE = 3  # Smaller size for testing
bot.MAX_POSITION = 50  # Conservative position limits

print("\nBot configuration:")
print(f"  Min spread: {bot.MIN_SPREAD}")
print(f"  Order size: {bot.ORDER_SIZE}")
print(f"  Max position: {bot.MAX_POSITION}")
print(f"\nMonitoring markets: {bot.MARKET_1}, {bot.MARKET_3}, {bot.MARKET_5}, {bot.MARKET_7}")

# Start the bot
bot.start()
print("\n✓ Bot started. Listening for arbitrage opportunities...")
print("Press Ctrl+C to stop.\n")

try:
    # Monitor positions and PnL periodically
    while True:
        time.sleep(10)

        # Show current positions
        positions = bot.get_positions()
        if positions:
            print("\nCurrent positions:")
            for product, pos in positions.items():
                print(f"  {product}: {pos:+d}")

        # Show PnL
        pnl = bot.get_pnl()
        if pnl:
            print(f"\nP&L: {pnl}")

except KeyboardInterrupt:
    print("\n\nStopping bot...")
    bot.cancel_all_orders()
    bot.stop()

    # Final summary
    print("\n" + "="*50)
    print("FINAL SUMMARY")
    print("="*50)

    positions = bot.get_positions()
    if positions:
        print("\nFinal positions:")
        for product, pos in sorted(positions.items()):
            print(f"  {product}: {pos:+d}")

    pnl = bot.get_pnl()
    if pnl:
        print(f"\nFinal P&L: {pnl}")

    print("\nBot stopped successfully.")
