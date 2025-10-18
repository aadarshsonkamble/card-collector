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

print("🔍 [CARD] Module loading...", flush=True)

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
UPLOAD_CHANNEL_ID = int(os.getenv('UPLOAD_CHANNEL_ID'))

# Scraping settings
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
    print("\n\n🛑 [CARD] Stop signal received! Finishing current players...", flush=True)
    print("⏳ [CARD] Please wait for graceful shutdown...", flush=True)
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
        print(f'✅ [CARD] Discord bot logged in as {self.user}', flush=True)

async def init_discord():
    global discord_client, upload_channel
    print("🔍 [CARD] Initializing Discord client...", flush=True)
    discord_client = DiscordUploader()
    
    # Start bot in background
    print("🔍 [CARD] Starting Discord bot in background...", flush=True)
    asyncio.create_task(discord_client.start(DISCORD_TOKEN))
    
    # Wait for bot to be ready
    print("🔍 [CARD] Waiting for Discord bot to be ready...", flush=True)
    timeout = 30
    elapsed = 0
    while not discord_client.ready and elapsed < timeout:
        await asyncio.sleep(0.5)
        elapsed += 0.5
    
    if not discord_client.ready:
        raise Exception(f"❌ [CARD] Discord bot failed to start within {timeout}s")
    
    # Get upload channel
    print(f"🔍 [CARD] Getting upload channel {UPLOAD_CHANNEL_ID}...", flush=True)
    upload_channel = discord_client.get_channel(UPLOAD_CHANNEL_ID)
    if not upload_channel:
        raise Exception(f"❌ [CARD] Could not find channel {UPLOAD_CHANNEL_ID}")
    
    print(f'✅ [CARD] Upload channel ready: #{upload_channel.name}', flush=True)

def init_supabase():
    global supabase
    print("🔍 [CARD] Creating Supabase client...", flush=True)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print('✅ [CARD] Supabase connected', flush=True)

def load_progress():
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return None

def save_progress():
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(stats, f, indent=2, default=str)
    print(f'💾 [CARD] Progress saved to {PROGRESS_FILE}', flush=True)

async def get_pending_players():
    """Get players that need screenshots - OPTIMIZED with BATCH SUPPORT"""
    try:
        print("🔍 [CARD] Fetching player data from Supabase...", flush=True)
        response = supabase.table('player_refresh_data').select('player_id, card_rank_0_url, card_rank_1_url, card_rank_2_url, card_rank_3_url, card_rank_4_url, card_rank_5_url').execute()
        
        print(f"✅ [CARD] Fetched {len(response.data)} players", flush=True)
        print(f"🔍 [CARD] Analyzing which players need processing...", flush=True)
        
        pending = []
        for row in response.data:
            player_id = row['player_id']
            needs_processing = False
            for rank in RANKS:
                col_name = f'card_rank_{rank}_url'
                if col_name not in row or not row[col_name]:
                    needs_processing = True
                    break
            
            if needs_processing:
                pending.append(player_id)
        
        print(f"✅ [CARD] Found {len(pending)} players needing processing", flush=True)
        
        # Batch support
        batch_number = os.getenv('BATCH_NUMBER')
        batch_size = os.getenv('BATCH_SIZE')
        
        if batch_number and batch_size:
            batch_num = int(batch_number)
            batch_sz = int(batch_size)
            
            start_idx = (batch_num - 1) * batch_sz
            end_idx = batch_num * batch_sz
            
            print(f"🔍 [CARD] Batch mode enabled: Batch {batch_num}, size {batch_sz}", flush=True)
            print(f"🔍 [CARD] Slicing players {start_idx+1} to {min(end_idx, len(pending))}", flush=True)
            pending = pending[start_idx:end_idx]
            print(f"✅ [CARD] Batch contains {len(pending)} players", flush=True)
        
        return pending
    except Exception as e:
        print(f"❌ [CARD] Error fetching players: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []

async def screenshot_player_card(page, player_id, rank):
    """Screenshot a single player card at specific rank"""
    try:
        url = f'https://renderz.app/24/player/{player_id}'
        if rank > 0:
            url += f'?rankUp={rank}'
        
        await page.goto(url, wait_until='domcontentloaded', timeout=90000)
        await page.wait_for_selector('[data-player-card]', timeout=30000)
        await asyncio.sleep(8)
        
        card_element = page.locator('[data-player-card]').first
        screenshot_bytes = await card_element.screenshot()
        
        return screenshot_bytes
    except Exception as e:
        print(f"  ❌ [CARD] Rank {rank} failed: {str(e)[:80]}", flush=True)
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
        print(f"  ❌ [CARD] Upload failed: {str(e)[:50]}", flush=True)
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
        print(f"  ❌ [CARD] DB update failed: {str(e)[:50]}", flush=True)
        return False

async def process_player(browser, player_id, player_index, total_players):
    """Process all 6 ranks for a single player"""
    global stop_flag, stats
    
    if stop_flag:
        return False
    
    print(f"\n[{player_index}/{total_players}] [CARD] Processing player {player_id}...", flush=True)
    
    page = await browser.new_page()
    
    player_success = True
    for rank in RANKS:
        if stop_flag:
            await page.close()
            return False
        
        # Check if already cached
        try:
            col_name = f'card_rank_{rank}_url'
            existing = supabase.table('player_refresh_data').select(col_name).eq('player_id', player_id).execute()
            if existing.data and existing.data[0].get(col_name):
                print(f"  ✓ [CARD] Rank {rank}: Already cached", flush=True)
                stats['completed_screenshots'] += 1
                continue
        except:
            pass
        
        screenshot = await screenshot_player_card(page, player_id, rank)
        if not screenshot:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        cdn_url = await upload_to_discord(screenshot, player_id, rank)
        if not cdn_url:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        success = await update_database(player_id, rank, cdn_url)
        if success:
            print(f"  ✓ [CARD] Rank {rank}: Cached", flush=True)
            stats['completed_screenshots'] += 1
        else:
            stats['failed_screenshots'] += 1
            player_success = False
        
        stats['data_used_gb'] += 0.004
    
    await page.close()
    
    if not player_success:
        stats['failed_players'].append(player_id)
    
    stats['processed_players'] += 1
    return True

async def worker(browser, player_queue, worker_id, total_players):
    """Worker that processes players from queue"""
    print(f"🔍 [CARD] Worker {worker_id} started", flush=True)
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
            print(f"❌ [CARD] Worker {worker_id} error: {e}", flush=True)
            continue
    print(f"🔍 [CARD] Worker {worker_id} stopped", flush=True)

def install_chromium():
    """Install Chromium browser synchronously"""
    print("📥 [CARD] Installing Chromium (2-3 minutes)...", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            print("✅ [CARD] Chromium installed successfully!", flush=True)
            return True
        else:
            print(f"❌ [CARD] Failed to install Chromium:", flush=True)
            print(f"   {result.stderr}", flush=True)
            return False
    except subprocess.TimeoutExpired:
        print("❌ [CARD] Chromium installation timed out", flush=True)
        return False
    except Exception as e:
        print(f"❌ [CARD] Error installing Chromium: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return False

async def main():
    global stop_flag, stats
    
    print("=" * 60, flush=True)
    print("🎮 [CARD] FC MOBILE PLAYER CARD SCRAPER", flush=True)
    print("=" * 60, flush=True)
    
    try:
        # Initialize connections
        print("\n📡 [CARD] Initializing connections...", flush=True)
        init_supabase()
        await init_discord()
        
        # Get pending players
        print("\n🔍 [CARD] Checking for pending players...", flush=True)
        pending_players = await get_pending_players()
        
        if not pending_players:
            print("\n✅ [CARD] All players cached! Nothing to do.", flush=True)
            await discord_client.close()
            return
        
        stats['total_players'] = len(pending_players)
        stats['total_screenshots'] = len(pending_players) * 6
        stats['start_time'] = datetime.now().isoformat()
        
        print(f"\n📋 [CARD] Players to process: {len(pending_players)}", flush=True)
        print(f"📸 [CARD] Screenshots needed: {stats['total_screenshots']}", flush=True)
        print(f"🌐 [CARD] Parallel browsers: {PARALLEL_BROWSERS}\n", flush=True)
        
        # Create player queue
        print("🔍 [CARD] Creating player queue...", flush=True)
        player_queue = asyncio.Queue()
        for idx, player_id in enumerate(pending_players, 1):
            await player_queue.put((idx, player_id))
        print(f"✅ [CARD] Queue created with {len(pending_players)} players", flush=True)
        
        # Start Playwright
        print("\n🔍 [CARD] Starting Playwright...", flush=True)
        async with async_playwright() as p:
            print("✅ [CARD] Playwright started", flush=True)
            print("🔍 [CARD] Checking browser installation...", flush=True)
            
            # Check if browser is installed
            browser_installed = False
            try:
                print("🔍 [CARD] Attempting test browser launch...", flush=True)
                test_browser = await p.chromium.launch(headless=True)
                await test_browser.close()
                browser_installed = True
                print("✅ [CARD] Chromium already installed", flush=True)
            except Exception as e:
                error_str = str(e)
                print(f"⚠️  [CARD] Browser launch failed: {error_str[:100]}", flush=True)
                if "Executable doesn't exist" in error_str:
                    print("🔍 [CARD] Chromium not found, will install", flush=True)
                    browser_installed = False
                else:
                    print(f"❌ [CARD] Unexpected error during browser check", flush=True)
                    raise
            
            # Install if needed
            if not browser_installed:
                success = install_chromium()
                if not success:
                    print("❌ [CARD] Cannot proceed without browser", flush=True)
                    await discord_client.close()
                    return
            
            # Launch browsers
            print(f"\n🔍 [CARD] Launching {PARALLEL_BROWSERS} browsers...", flush=True)
            browsers = []
            for i in range(PARALLEL_BROWSERS):
                print(f"🔍 [CARD] Launching browser {i+1}...", flush=True)
                browser = await p.chromium.launch(headless=True)
                browsers.append(browser)
                print(f"✅ [CARD] Browser {i+1} ready", flush=True)
            
            print(f"✅ [CARD] All {PARALLEL_BROWSERS} browsers ready!\n", flush=True)
            
            # Create workers
            print(f"🔍 [CARD] Creating {len(browsers)} workers...", flush=True)
            workers = []
            for i, browser in enumerate(browsers):
                worker_task = asyncio.create_task(
                    worker(browser, player_queue, i+1, len(pending_players))
                )
                workers.append(worker_task)
            print(f"✅ [CARD] All workers created\n", flush=True)
            
            # Monitor progress
            print("🔍 [CARD] Starting progress monitor...", flush=True)
            while not player_queue.empty() and not stop_flag:
                await asyncio.sleep(10)
                
                if stats['processed_players'] > 0:
                    progress_pct = (stats['processed_players'] / stats['total_players']) * 100
                    print(f"\n📊 [CARD] Progress: {stats['processed_players']}/{stats['total_players']} ({progress_pct:.1f}%)", flush=True)
                    print(f"   Screenshots: {stats['completed_screenshots']}/{stats['total_screenshots']}", flush=True)
                    print(f"   Failed: {stats['failed_screenshots']}", flush=True)
            
            # Cleanup
            print("\n🔍 [CARD] Waiting for queue to finish...", flush=True)
            await player_queue.join()
            
            print("🔍 [CARD] Cancelling workers...", flush=True)
            for w in workers:
                w.cancel()
            
            print("🔍 [CARD] Closing browsers...", flush=True)
            for browser in browsers:
                await browser.close()
            print("✅ [CARD] Browsers closed", flush=True)
        
        # Final stats
        print("\n" + "=" * 60, flush=True)
        print("📊 [CARD] FINAL STATISTICS", flush=True)
        print("=" * 60, flush=True)
        print(f"Players: {stats['processed_players']}/{stats['total_players']}", flush=True)
        print(f"Screenshots: {stats['completed_screenshots']}/{stats['total_screenshots']}", flush=True)
        print(f"Failed: {stats['failed_screenshots']}", flush=True)
        
        await discord_client.close()
        print("✅ [CARD] main() completed successfully", flush=True)
        
    except Exception as e:
        print(f"\n❌ [CARD] CRITICAL ERROR in main():", flush=True)
        print(f"   Error type: {type(e).__name__}", flush=True)
        print(f"   Error message: {str(e)}", flush=True)
        import traceback
        print("   Full traceback:", flush=True)
        traceback.print_exc()
        raise

if __name__ == "__main__":
    asyncio.run(main())
