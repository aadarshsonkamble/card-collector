import os
import asyncio
import json
import signal
import sys
import io
import subprocess
from datetime import datetime
from playwright.async_api import async_playwright
import discord
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path


# Load environment variables
load_dotenv()


# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
UPLOAD_CHANNEL_ID = int(os.getenv('UPLOAD_CHANNEL_ID'))


# Scraping settings - OPTIMIZED FOR MOBILE DATA
PARALLEL_BROWSERS = 2
RANKS = [0, 1, 2, 3, 4, 5]
BATCH_SIZE = 100
BREAK_DURATION = 120
PROGRESS_FILE = 'progress.json'


# Global state
stop_flag = False
supabase: Client = None
discord_client = None
upload_channel = None
stats = {
    'total_players': 0,
    'processed_players': 0,
    'total_screenshots': 0,
    'completed_screenshots': 0,
    'failed_screenshots': 0,
    'data_used_gb': 0.0,
    'start_time': None,
    'failed_players': []
}


# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    global stop_flag
    print("\n\n🛑 Stop signal received! Finishing current players...")
    print("⏳ Please wait for graceful shutdown...")
    stop_flag = True


signal.signal(signal.SIGINT, signal_handler)


# Discord client setup
class DiscordUploader(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.ready = False
    
    async def on_ready(self):
        self.ready = True
        print(f'✅ Discord bot logged in as {self.user}')


async def init_discord():
    global discord_client, upload_channel
    discord_client = DiscordUploader()
    
    # Start bot in background
    asyncio.create_task(discord_client.start(DISCORD_TOKEN))
    
    # Wait for bot to be ready
    while not discord_client.ready:
        await asyncio.sleep(0.5)
    
    # Get upload channel
    upload_channel = discord_client.get_channel(UPLOAD_CHANNEL_ID)
    if not upload_channel:
        raise Exception(f"❌ Could not find channel {UPLOAD_CHANNEL_ID}")
    
    print(f'✅ Upload channel ready: #{upload_channel.name}')


def init_supabase():
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print('✅ Supabase connected')


def load_progress():
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return None


def save_progress():
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(stats, f, indent=2, default=str)
    print(f'💾 Progress saved to {PROGRESS_FILE}')


async def get_pending_players():
    """Get players that need screenshots - OPTIMIZED with BATCH SUPPORT"""
    try:
        print("   Fetching all player data...")
        # Get ALL data in one query
        response = supabase.table('player_refresh_data').select('player_id, card_rank_0_url, card_rank_1_url, card_rank_2_url, card_rank_3_url, card_rank_4_url, card_rank_5_url').execute()
        
        print(f"   Analyzing {len(response.data)} players...")
        
        pending = []
        for row in response.data:
            player_id = row['player_id']
            # Check if any rank URLs are missing
            needs_processing = False
            for rank in RANKS:
                col_name = f'card_rank_{rank}_url'
                if col_name not in row or not row[col_name]:
                    needs_processing = True
                    break
            
            if needs_processing:
                pending.append(player_id)
        
        # ===== BATCH SUPPORT ADDED =====
        # Check if batch processing is requested
        batch_number = os.getenv('BATCH_NUMBER')
        batch_size = os.getenv('BATCH_SIZE')
        
        if batch_number and batch_size:
            batch_num = int(batch_number)
            batch_sz = int(batch_size)
            
            # Calculate slice
            start_idx = (batch_num - 1) * batch_sz
            end_idx = batch_num * batch_sz
            
            print(f"   🔢 Batch mode: Processing batch {batch_num} (players {start_idx+1} to {min(end_idx, len(pending))})")
            pending = pending[start_idx:end_idx]
        # ===== END BATCH SUPPORT =====
        
        return pending
    except Exception as e:
        print(f"❌ Error fetching players: {e}")
        return []


async def screenshot_player_card(page, player_id, rank):
    """Screenshot a single player card at specific rank - FIXED"""
    try:
        url = f'https://renderz.app/24/player/{player_id}'
        if rank > 0:
            url += f'?rankUp={rank}'
        
        # Load page with longer timeout
        await page.goto(url, wait_until='domcontentloaded', timeout=90000)
        
        # Wait for the card element specifically
        await page.wait_for_selector('[data-player-card]', timeout=30000)
        
        # Extra wait for any animations/rendering
        await asyncio.sleep(8)
        
        # Screenshot ONLY the card element
        card_element = page.locator('[data-player-card]').first
        screenshot_bytes = await card_element.screenshot()
        
        return screenshot_bytes
    except Exception as e:
        print(f"  ❌ Rank {rank} failed: {str(e)[:80]}")
        return None


async def upload_to_discord(screenshot_bytes, player_id, rank):
    """Upload screenshot to Discord and get CDN URL"""
    try:
        filename = f"player_{player_id}_rank_{rank}.png"
        file = discord.File(fp=io.BytesIO(screenshot_bytes), filename=filename)
        
        message = await upload_channel.send(file=file)
        
        if message.attachments:
            cdn_url = message.attachments[0].url
            return cdn_url
        return None
    except Exception as e:
        print(f"  ❌ Upload failed: {str(e)[:50]}")
        return None


async def update_database(player_id, rank, cdn_url):
    """Update Supabase with card URL"""
    try:
        col_name = f'card_rank_{rank}_url'
        supabase.table('player_refresh_data').update({
            col_name: cdn_url
        }).eq('player_id', player_id).execute()
        return True
    except Exception as e:
        print(f"  ❌ DB update failed: {str(e)[:50]}")
        return False


async def process_player(browser, player_id, player_index, total_players):
    """Process all 6 ranks for a single player"""
    global stop_flag, stats
    
    if stop_flag:
        return False
    
    print(f"\n[{player_index}/{total_players}] Processing player {player_id}...")
    
    # Create new page
    page = await browser.new_page()
    
    player_success = True
    for rank in RANKS:
        if stop_flag:
            await page.close()
            return False
        
        # Check if this rank already exists
        try:
            col_name = f'card_rank_{rank}_url'
            existing = supabase.table('player_refresh_data').select(col_name).eq('player_id', player_id).execute()
            if existing.data and existing.data[0].get(col_name):
                print(f"  ✓ Rank {rank}: Already cached (skipping)")
                stats['completed_screenshots'] += 1
                continue
        except:
            pass
        
        # Screenshot
        screenshot = await screenshot_player_card(page, player_id, rank)
        if not screenshot:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        # Upload to Discord
        cdn_url = await upload_to_discord(screenshot, player_id, rank)
        if not cdn_url:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        # Update database
        success = await update_database(player_id, rank, cdn_url)
        if success:
            print(f"  ✓ Rank {rank}: Cached")
            stats['completed_screenshots'] += 1
        else:
            stats['failed_screenshots'] += 1
            player_success = False
        
        # Estimate data usage (rough: 4MB per rank)
        stats['data_used_gb'] += 0.004
    
    await page.close()
    
    if not player_success:
        stats['failed_players'].append(player_id)
    
    stats['processed_players'] += 1
    return True


async def worker(browser, player_queue, worker_id, total_players):
    """Worker that processes players from queue"""
    while True:
        if stop_flag:
            break
        
        try:
            player_index, player_id = await asyncio.wait_for(player_queue.get(), timeout=1.0)
            await process_player(browser, player_id, player_index, total_players)
            player_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
            continue


def install_chromium():
    """Install Chromium browser synchronously"""
    print("📥 Installing Chromium browser (this will take 2-3 minutes)...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Chromium installed successfully!")
            return True
        else:
            print(f"❌ Failed to install Chromium:")
            print(f"   {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Chromium installation timed out")
        return False
    except Exception as e:
        print(f"❌ Error installing Chromium: {e}")
        return False


async def main():
    global stop_flag, stats
    
    print("=" * 60)
    print("🎮 FC MOBILE PLAYER CARD SCRAPER")
    print("=" * 60)
    
    # Initialize connections
    print("\n📡 Initializing connections...")
    init_supabase()
    await init_discord()
    
    # Load previous progress
    prev_progress = load_progress()
    if prev_progress:
        print(f"\n📊 Previous session found:")
        print(f"   Last run: {prev_progress.get('start_time', 'Unknown')}")
        print(f"   Completed: {prev_progress.get('processed_players', 0)} players")
        print(f"   Screenshots: {prev_progress.get('completed_screenshots', 0)}")
    
    # Get pending players
    print("\n🔍 Checking for pending players...")
    pending_players = await get_pending_players()
    
    if not pending_players:
        print("\n✅ All players already cached! Nothing to do.")
        await discord_client.close()
        return
    
    stats['total_players'] = len(pending_players)
    stats['total_screenshots'] = len(pending_players) * 6
    stats['start_time'] = datetime.now().isoformat()
    
    print(f"\n📋 Found {len(pending_players)} players to process")
    print(f"📸 Total screenshots needed: {stats['total_screenshots']}")
    print(f"🌐 Parallel browsers: {PARALLEL_BROWSERS}")
    print(f"\n💡 Press Ctrl+C to stop gracefully\n")
    
    # Create player queue
    player_queue = asyncio.Queue()
    for idx, player_id in enumerate(pending_players, 1):
        await player_queue.put((idx, player_id))
    
    # Start Playwright and create browsers
    async with async_playwright() as p:
        print(f"🚀 Checking browser installation...")
        
        # Try to launch browser to check if installed
        browser_installed = False
        try:
            test_browser = await p.chromium.launch(headless=True)
            await test_browser.close()
            browser_installed = True
            print("✅ Chromium already installed")
        except Exception as e:
            if "Executable doesn't exist" in str(e):
                print("⚠️ Chromium not found")
                browser_installed = False
            else:
                print(f"❌ Unexpected error: {e}")
                raise
        
        # Install if needed
        if not browser_installed:
            success = install_chromium()
            if not success:
                print("❌ Cannot proceed without browser")
                await discord_client.close()
                return
        
        print(f"\n🚀 Launching {PARALLEL_BROWSERS} browsers...")
        
        browsers = []
        for i in range(PARALLEL_BROWSERS):
            browser = await p.chromium.launch(headless=True)
            browsers.append(browser)
        
        print(f"✅ Browsers ready!\n")
        
        # Create workers
        workers = []
        for i, browser in enumerate(browsers):
            worker_task = asyncio.create_task(
                worker(browser, player_queue, i+1, len(pending_players))
            )
            workers.append(worker_task)
        
        # Monitor progress
        while not player_queue.empty() and not stop_flag:
            await asyncio.sleep(10)
            
            # Show progress
            if stats['processed_players'] > 0:
                progress_pct = (stats['processed_players'] / stats['total_players']) * 100
                print(f"\n📊 Progress: {stats['processed_players']}/{stats['total_players']} players ({progress_pct:.1f}%)")
                print(f"   Screenshots: {stats['completed_screenshots']}/{stats['total_screenshots']}")
                print(f"   Failed: {stats['failed_screenshots']}")
                print(f"   Data used: ~{stats['data_used_gb']:.2f} GB")
            
            # Save progress periodically
            save_progress()
        
        # Wait for remaining tasks or stop
        if stop_flag:
            print("\n⏸️  Waiting for current players to finish...")
        
        await player_queue.join()
        
        # Cancel workers
        for w in workers:
            w.cancel()
        
        # Close browsers
        print("\n🔒 Closing browsers...")
        for browser in browsers:
            await browser.close()
    
    # Final stats
    print("\n" + "=" * 60)
    print("📊 FINAL STATISTICS")
    print("=" * 60)
    print(f"Players processed: {stats['processed_players']}/{stats['total_players']}")
    print(f"Screenshots completed: {stats['completed_screenshots']}/{stats['total_screenshots']}")
    print(f"Screenshots failed: {stats['failed_screenshots']}")
    print(f"Data used: ~{stats['data_used_gb']:.2f} GB")
    
    if stats['failed_players']:
        print(f"\n⚠️  Failed players ({len(stats['failed_players'])}): {stats['failed_players'][:10]}")
    
    # Save final progress
    save_progress()
    
    if stats['processed_players'] < stats['total_players']:
        print(f"\n⏸️  Stopped early. Run again to resume.")
    else:
        print("\n✅ All done! Your bot is ready to serve player cards instantly!")
    
    await discord_client.close()


if __name__ == "__main__":
    asyncio.run(main())
