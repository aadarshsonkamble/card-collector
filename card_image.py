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

# Scraping settings - OPTIMIZED
PARALLEL_BROWSERS = 2
RANKS = [0, 1, 2, 3, 4, 5]
PAGE_TIMEOUT = 120000  # 120 seconds (reduced from 90)
SELECTOR_TIMEOUT = 120000  # 120 seconds
SCREENSHOT_TIMEOUT = 120000  # 30 seconds
WAIT_AFTER_LOAD = 30  # 30 seconds (reduced from 8)

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
    'failed_players': [],
    'timeout_errors': 0,
    'network_errors': 0
}

def signal_handler(sig, frame):
    global stop_flag
    print("\n\n🛑 [CARD] Stop signal received!", flush=True)
    stop_flag = True

signal.signal(signal.SIGINT, signal_handler)

class DiscordUploader(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.ready = False
    
    async def on_ready(self):
        self.ready = True
        print(f'✅ [CARD] Discord: {self.user}', flush=True)

async def init_discord():
    global discord_client, upload_channel
    print("🔍 [CARD] Init Discord...", flush=True)
    discord_client = DiscordUploader()
    asyncio.create_task(discord_client.start(DISCORD_TOKEN))
    
    timeout = 30
    elapsed = 0
    while not discord_client.ready and elapsed < timeout:
        await asyncio.sleep(0.5)
        elapsed += 0.5
    
    if not discord_client.ready:
        raise Exception("Discord timeout")
    
    upload_channel = discord_client.get_channel(UPLOAD_CHANNEL_ID)
    if not upload_channel:
        raise Exception(f"Channel {UPLOAD_CHANNEL_ID} not found")
    
    print(f'✅ [CARD] Channel: #{upload_channel.name}', flush=True)

def init_supabase():
    global supabase
    print("🔍 [CARD] Init Supabase...", flush=True)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print('✅ [CARD] Supabase OK', flush=True)

async def get_pending_players():
    """Get players needing screenshots with batch support"""
    try:
        print("🔍 [CARD] Fetching players...", flush=True)
        response = supabase.table('player_refresh_data').select(
            'player_id, card_rank_0_url, card_rank_1_url, card_rank_2_url, '
            'card_rank_3_url, card_rank_4_url, card_rank_5_url'
        ).execute()
        
        print(f"✅ [CARD] Got {len(response.data)} players", flush=True)
        
        pending = []
        for row in response.data:
            player_id = row['player_id']
            for rank in RANKS:
                col_name = f'card_rank_{rank}_url'
                if col_name not in row or not row[col_name]:
                    pending.append(player_id)
                    break
        
        print(f"✅ [CARD] {len(pending)} need processing", flush=True)
        
        # Batch support
        batch_number = os.getenv('BATCH_NUMBER')
        batch_size = os.getenv('BATCH_SIZE')
        
        if batch_number and batch_size:
            batch_num = int(batch_number)
            batch_sz = int(batch_size)
            start_idx = (batch_num - 1) * batch_sz
            end_idx = batch_num * batch_sz
            
            print(f"🔍 [CARD] Batch {batch_num}: [{start_idx+1}..{min(end_idx, len(pending))}]", flush=True)
            pending = pending[start_idx:end_idx]
            print(f"✅ [CARD] Batch size: {len(pending)}", flush=True)
        
        return pending
    except Exception as e:
        print(f"❌ [CARD] Fetch failed: {e}", flush=True)
        return []

async def test_website_access(browser):
    """Test if renderz.app is accessible"""
    print("🔍 [CARD] Testing renderz.app...", flush=True)
    try:
        page = await browser.new_page()
        test_url = "https://renderz.app/24/player/19072866"
        
        print(f"🔍 [CARD] Loading test page...", flush=True)
        await page.goto(test_url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
        print(f"✅ [CARD] Page loaded", flush=True)
        
        # Check for element
        await page.wait_for_selector('[data-player-card]', timeout=SELECTOR_TIMEOUT)
        print(f"✅ [CARD] Element found", flush=True)
        
        # Try screenshot
        card = page.locator('[data-player-card]').first
        screenshot = await card.screenshot(timeout=SCREENSHOT_TIMEOUT)
        print(f"✅ [CARD] Screenshot OK ({len(screenshot)} bytes)", flush=True)
        
        await page.close()
        return True
        
    except Exception as e:
        print(f"❌ [CARD] Website test failed: {str(e)[:150]}", flush=True)
        print(f"⚠️  [CARD] Renderz.app may be blocked/slow from this server", flush=True)
        return False

async def screenshot_player_card(page, player_id, rank):
    """Screenshot with aggressive timeout handling"""
    try:
        url = f'https://renderz.app/24/player/{player_id}'
        if rank > 0:
            url += f'?rankUp={rank}'
        
        # Load page
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
        except Exception as e:
            if "Timeout" in str(e):
                stats['timeout_errors'] += 1
                print(f"  ⏱️ [CARD] R{rank}: Page timeout", flush=True)
            else:
                stats['network_errors'] += 1
                print(f"  🌐 [CARD] R{rank}: Network error", flush=True)
            return None
        
        # Wait for element
        try:
            await page.wait_for_selector('[data-player-card]', timeout=SELECTOR_TIMEOUT)
        except Exception as e:
            print(f"  ❌ [CARD] R{rank}: Element not found", flush=True)
            return None
        
        # Small wait for rendering
        await asyncio.sleep(WAIT_AFTER_LOAD)
        
        # Screenshot
        try:
            card_element = page.locator('[data-player-card]').first
            screenshot_bytes = await card_element.screenshot(timeout=SCREENSHOT_TIMEOUT)
            return screenshot_bytes
        except Exception as e:
            print(f"  📸 [CARD] R{rank}: Screenshot failed", flush=True)
            return None
        
    except Exception as e:
        print(f"  ❌ [CARD] R{rank}: {str(e)[:60]}", flush=True)
        return None

async def upload_to_discord(screenshot_bytes, player_id, rank):
    """Upload with retry logic"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            filename = f"player_{player_id}_rank_{rank}.png"
            file = discord.File(fp=io.BytesIO(screenshot_bytes), filename=filename)
            message = await upload_channel.send(file=file)
            
            if message.attachments:
                return message.attachments[0].url
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️ [CARD] Upload retry {attempt+1}", flush=True)
                await asyncio.sleep(2)
            else:
                print(f"  ❌ [CARD] Upload failed: {str(e)[:40]}", flush=True)
    
    return None

async def update_database(player_id, rank, cdn_url):
    """Update Supabase"""
    try:
        col_name = f'card_rank_{rank}_url'
        supabase.table('player_refresh_data').update({
            col_name: cdn_url
        }).eq('player_id', player_id).execute()
        return True
    except Exception as e:
        print(f"  ❌ [CARD] DB update: {str(e)[:40]}", flush=True)
        return False

async def process_player(browser, player_id, player_index, total_players):
    """Process one player with all ranks"""
    global stop_flag, stats
    
    if stop_flag:
        return False
    
    print(f"\n[{player_index}/{total_players}] Player {player_id}", flush=True)
    
    page = await browser.new_page()
    player_success = True
    ranks_completed = 0
    
    for rank in RANKS:
        if stop_flag:
            await page.close()
            return False
        
        # Check if already exists
        try:
            col_name = f'card_rank_{rank}_url'
            existing = supabase.table('player_refresh_data').select(col_name).eq('player_id', player_id).execute()
            if existing.data and existing.data[0].get(col_name):
                print(f"  ✓ R{rank}: Cached", flush=True)
                stats['completed_screenshots'] += 1
                ranks_completed += 1
                continue
        except:
            pass
        
        # Screenshot
        screenshot = await screenshot_player_card(page, player_id, rank)
        if not screenshot:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        # Upload
        cdn_url = await upload_to_discord(screenshot, player_id, rank)
        if not cdn_url:
            stats['failed_screenshots'] += 1
            player_success = False
            continue
        
        # Save
        success = await update_database(player_id, rank, cdn_url)
        if success:
            print(f"  ✅ R{rank}: Done", flush=True)
            stats['completed_screenshots'] += 1
            ranks_completed += 1
        else:
            stats['failed_screenshots'] += 1
            player_success = False
        
        stats['data_used_gb'] += 0.004
    
    await page.close()
    
    if not player_success:
        stats['failed_players'].append(player_id)
        print(f"  ⚠️  Player incomplete ({ranks_completed}/6 ranks)", flush=True)
    else:
        print(f"  ✅ Player complete (6/6 ranks)", flush=True)
    
    stats['processed_players'] += 1
    return True

async def worker(browser, player_queue, worker_id, total_players):
    """Worker thread"""
    print(f"🔍 [CARD] Worker {worker_id} ready", flush=True)
    
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
            print(f"❌ [CARD] Worker {worker_id}: {e}", flush=True)
            continue
    
    print(f"✅ [CARD] Worker {worker_id} stopped", flush=True)

def install_chromium():
    """Install browser"""
    print("📥 [CARD] Installing Chromium...", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            print("✅ [CARD] Chromium installed", flush=True)
            return True
        else:
            print(f"❌ [CARD] Install failed", flush=True)
            return False
    except Exception as e:
        print(f"❌ [CARD] Install error: {e}", flush=True)
        return False

async def main():
    global stop_flag, stats
    
    print("=" * 60, flush=True)
    print("🎮 [CARD] FC MOBILE CARD SCRAPER v2.0", flush=True)
    print("=" * 60, flush=True)
    
    try:
        # Initialize
        print("\n📡 [CARD] Initializing...", flush=True)
        init_supabase()
        await init_discord()
        
        # Get players
        print("\n🔍 [CARD] Loading player list...", flush=True)
        pending_players = await get_pending_players()
        
        if not pending_players:
            print("\n✅ [CARD] Nothing to do!", flush=True)
            await discord_client.close()
            return
        
        stats['total_players'] = len(pending_players)
        stats['total_screenshots'] = len(pending_players) * 6
        stats['start_time'] = datetime.now().isoformat()
        
        print(f"\n📋 [CARD] Queue: {len(pending_players)} players", flush=True)
        print(f"📸 [CARD] Target: {stats['total_screenshots']} screenshots", flush=True)
        print(f"🌐 [CARD] Workers: {PARALLEL_BROWSERS}\n", flush=True)
        
        # Create queue
        player_queue = asyncio.Queue()
        for idx, player_id in enumerate(pending_players, 1):
            await player_queue.put((idx, player_id))
        
        # Playwright
        async with async_playwright() as p:
            print("🔍 [CARD] Starting Playwright...", flush=True)
            
            # Check/install browser
            browser_installed = False
            try:
                test_browser = await p.chromium.launch(headless=True)
                await test_browser.close()
                browser_installed = True
                print("✅ [CARD] Browser ready", flush=True)
            except Exception as e:
                if "Executable doesn't exist" in str(e):
                    print("🔍 [CARD] Browser not found", flush=True)
                    success = install_chromium()
                    if not success:
                        await discord_client.close()
                        return
                else:
                    raise
            
            # Launch browsers
            print(f"🔍 [CARD] Launching {PARALLEL_BROWSERS} browsers...", flush=True)
            browsers = []
            for i in range(PARALLEL_BROWSERS):
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu'
                    ]
                )
                browsers.append(browser)
            print(f"✅ [CARD] {PARALLEL_BROWSERS} browsers ready", flush=True)
            
            # Test website access
            print("\n🧪 [CARD] Testing target website...", flush=True)
            accessible = await test_website_access(browsers[0])
            if not accessible:
                print("❌ [CARD] Cannot access renderz.app!", flush=True)
                print("⚠️  [CARD] Website may be geo-blocked or too slow", flush=True)
                print("💡 [CARD] Consider using a proxy or VPN", flush=True)
                for browser in browsers:
                    await browser.close()
                await discord_client.close()
                return
            
            print("✅ [CARD] Website accessible!\n", flush=True)
            
            # Create workers
            print(f"🔍 [CARD] Starting workers...", flush=True)
            workers = []
            for i, browser in enumerate(browsers):
                worker_task = asyncio.create_task(
                    worker(browser, player_queue, i+1, len(pending_players))
                )
                workers.append(worker_task)
            
            # Monitor progress
            print("🔍 [CARD] Processing started...\n", flush=True)
            last_update = 0
            
            while not player_queue.empty() and not stop_flag:
                await asyncio.sleep(10)
                
                if stats['processed_players'] > last_update:
                    last_update = stats['processed_players']
                    progress_pct = (stats['processed_players'] / stats['total_players']) * 100
                    
                    print(f"\n📊 [CARD] Progress Report:", flush=True)
                    print(f"   Players: {stats['processed_players']}/{stats['total_players']} ({progress_pct:.1f}%)", flush=True)
                    print(f"   Screenshots: {stats['completed_screenshots']}/{stats['total_screenshots']}", flush=True)
                    print(f"   Failed: {stats['failed_screenshots']}", flush=True)
                    print(f"   Timeouts: {stats['timeout_errors']}", flush=True)
                    print(f"   Network: {stats['network_errors']}\n", flush=True)
            
            # Cleanup
            print("🔍 [CARD] Finishing up...", flush=True)
            await player_queue.join()
            
            for w in workers:
                w.cancel()
            
            for browser in browsers:
                await browser.close()
        
        # Final report
        print("\n" + "=" * 60, flush=True)
        print("📊 [CARD] FINAL REPORT", flush=True)
        print("=" * 60, flush=True)
        print(f"Players processed: {stats['processed_players']}/{stats['total_players']}", flush=True)
        print(f"Screenshots completed: {stats['completed_screenshots']}/{stats['total_screenshots']}", flush=True)
        print(f"Screenshots failed: {stats['failed_screenshots']}", flush=True)
        print(f"Timeout errors: {stats['timeout_errors']}", flush=True)
        print(f"Network errors: {stats['network_errors']}", flush=True)
        print(f"Data used: ~{stats['data_used_gb']:.2f} GB", flush=True)
        
        if stats['failed_players']:
            print(f"\n⚠️  Failed players ({len(stats['failed_players'])}): {stats['failed_players'][:20]}", flush=True)
        
        success_rate = (stats['completed_screenshots'] / stats['total_screenshots'] * 100) if stats['total_screenshots'] > 0 else 0
        print(f"\n✅ Success rate: {success_rate:.1f}%", flush=True)
        
        await discord_client.close()
        print("\n✅ [CARD] Scraper finished!", flush=True)
        
    except Exception as e:
        print(f"\n❌ [CARD] FATAL ERROR:", flush=True)
        print(f"   {type(e).__name__}: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    asyncio.run(main())


