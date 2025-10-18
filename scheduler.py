import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
from aiohttp import web
import sys

print("🔍 [INIT] Scheduler starting...", flush=True)

# Load environment variables
load_dotenv()
print("🔍 [INIT] Environment variables loaded", flush=True)

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))

print(f"🔍 [CONFIG] SUPABASE_URL: {SUPABASE_URL[:30]}..." if SUPABASE_URL else "❌ [ERROR] SUPABASE_URL missing", flush=True)
print(f"🔍 [CONFIG] SUPABASE_KEY: {SUPABASE_KEY[:20]}..." if SUPABASE_KEY else "❌ [ERROR] SUPABASE_KEY missing", flush=True)
print(f"🔍 [CONFIG] BATCH_SIZE: {BATCH_SIZE}", flush=True)

# Initialize Supabase
print("🔍 [INIT] Creating Supabase client...", flush=True)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("✅ [INIT] Supabase client created", flush=True)

# Progress tracking
PROGRESS_TABLE = 'card_scraper_progress'

# Track if scraper is currently running
scraper_running = False

def init_progress_table():
    """Create progress tracking table if it doesn't exist"""
    try:
        print(f"🔍 [DB] Checking if table '{PROGRESS_TABLE}' exists...", flush=True)
        result = supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        print(f"✅ [DB] Table '{PROGRESS_TABLE}' exists with {len(result.data)} rows", flush=True)
        return True
    except Exception as e:
        print(f"❌ [DB] Table check failed: {e}", flush=True)
        print(f"⚠️  [DB] Please create table manually in Supabase:", flush=True)
        print(f"""
        CREATE TABLE {PROGRESS_TABLE} (
            id SERIAL PRIMARY KEY,
            current_batch INTEGER DEFAULT 1,
            total_batches INTEGER DEFAULT 10,
            initial_mode BOOLEAN DEFAULT true,
            last_run TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        INSERT INTO {PROGRESS_TABLE} (current_batch, total_batches, initial_mode)
        VALUES (1, 10, true);
        """, flush=True)
        return False

def get_current_batch():
    """Get current batch number from database"""
    try:
        print(f"🔍 [DB] Fetching current batch from '{PROGRESS_TABLE}'...", flush=True)
        result = supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        if result.data:
            batch_info = result.data[0]
            print(f"✅ [DB] Current batch: {batch_info['current_batch']}/{batch_info['total_batches']}, Mode: {'Initial' if batch_info['initial_mode'] else 'Maintenance'}", flush=True)
            return batch_info
        else:
            print(f"⚠️  [DB] No data in table, creating initial record...", flush=True)
            supabase.table(PROGRESS_TABLE).insert({
                'current_batch': 1,
                'total_batches': 10,
                'initial_mode': True
            }).execute()
            return {'current_batch': 1, 'total_batches': 10, 'initial_mode': True}
    except Exception as e:
        print(f"❌ [DB] Error getting batch: {e}", flush=True)
        return {'current_batch': 1, 'total_batches': 10, 'initial_mode': True}

def update_batch(batch_num, initial_mode=True):
    """Update current batch number"""
    try:
        print(f"🔍 [DB] Updating batch to {batch_num}, mode: {'Initial' if initial_mode else 'Maintenance'}...", flush=True)
        supabase.table(PROGRESS_TABLE).update({
            'current_batch': batch_num,
            'initial_mode': initial_mode,
            'last_run': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }).eq('id', 1).execute()
        print(f"✅ [DB] Batch updated to {batch_num}", flush=True)
    except Exception as e:
        print(f"❌ [DB] Error updating batch: {e}", flush=True)

async def run_scraper():
    """Run the card scraper"""
    global scraper_running
    
    if scraper_running:
        print("⚠️  [SCRAPER] Already running, skipping...", flush=True)
        return {"status": "already_running"}
    
    scraper_running = True
    print("🔍 [SCRAPER] Scraper task started", flush=True)
    
    try:
        print("=" * 70, flush=True)
        print(f"🎴 [SCRAPER] Starting Card Scraper", flush=True)
        print(f"⏰ [SCRAPER] Time: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p IST')}", flush=True)
        print("=" * 70, flush=True)
        
        # Get current progress
        print("🔍 [SCRAPER] Getting current batch info...", flush=True)
        progress = get_current_batch()
        current_batch = progress['current_batch']
        total_batches = progress['total_batches']
        initial_mode = progress['initial_mode']
        
        # Import here to avoid circular imports
        print("🔍 [SCRAPER] Importing card_image module...", flush=True)
        try:
            import card_image
            print("✅ [SCRAPER] card_image module imported successfully", flush=True)
        except Exception as e:
            print(f"❌ [SCRAPER] Failed to import card_image: {e}", flush=True)
            raise
        
        if initial_mode:
            print(f"📦 [SCRAPER] Initial Mode - Processing Batch {current_batch}/{total_batches}", flush=True)
            print(f"📊 [SCRAPER] Players: {(current_batch-1)*BATCH_SIZE + 1} to {current_batch*BATCH_SIZE}", flush=True)
            
            # Set environment variable for batch processing
            os.environ['BATCH_NUMBER'] = str(current_batch)
            os.environ['BATCH_SIZE'] = str(BATCH_SIZE)
            print(f"🔍 [SCRAPER] Environment variables set: BATCH_NUMBER={current_batch}, BATCH_SIZE={BATCH_SIZE}", flush=True)
            
            # Run scraper
            print("🚀 [SCRAPER] Calling card_image.main()...", flush=True)
            await card_image.main()
            print("✅ [SCRAPER] card_image.main() completed successfully", flush=True)
            
            # Move to next batch
            next_batch = current_batch + 1
            if next_batch > total_batches:
                print("🎉 [SCRAPER] Initial scraping complete! Switching to maintenance mode.", flush=True)
                update_batch(1, initial_mode=False)
            else:
                update_batch(next_batch, initial_mode=True)
                print(f"✅ [SCRAPER] Batch {current_batch} complete! Next: Batch {next_batch}", flush=True)
            
            return {"status": "success", "batch": current_batch, "mode": "initial"}
        else:
            print("🔧 [SCRAPER] Maintenance Mode - Checking for new/missing cards", flush=True)
            
            # In maintenance mode, don't use batches
            os.environ.pop('BATCH_NUMBER', None)
            os.environ.pop('BATCH_SIZE', None)
            print("🔍 [SCRAPER] Batch environment variables cleared", flush=True)
            
            print("🚀 [SCRAPER] Calling card_image.main()...", flush=True)
            await card_image.main()
            print("✅ [SCRAPER] Maintenance check complete!", flush=True)
            
            return {"status": "success", "mode": "maintenance"}
        
    except Exception as e:
        print(f"❌ [SCRAPER] Critical error occurred:", flush=True)
        print(f"   Error type: {type(e).__name__}", flush=True)
        print(f"   Error message: {str(e)}", flush=True)
        print(f"   Full traceback:", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
    finally:
        scraper_running = False
        print("🔍 [SCRAPER] Scraper task completed, flag reset", flush=True)

# HTTP endpoints
async def health_check(request):
    """Health check endpoint"""
    print(f"🔍 [HTTP] Health check requested from {request.remote}", flush=True)
    progress = get_current_batch()
    status = {
        'status': 'running',
        'scraper_active': scraper_running,
        'mode': 'initial' if progress['initial_mode'] else 'maintenance',
        'current_batch': progress['current_batch'],
        'total_batches': progress['total_batches'],
        'last_run': progress.get('last_run', 'Never')
    }
    print(f"✅ [HTTP] Health check response: {status['mode']} mode, batch {status['current_batch']}", flush=True)
    return web.json_response(status)

async def trigger_scrape(request):
    """Endpoint to trigger scraping"""
    print(f"🔔 [HTTP] Scrape triggered from {request.remote} at {datetime.now().strftime('%I:%M %p IST')}", flush=True)
    
    # Run scraper in background
    print("🔍 [HTTP] Creating background task for scraper...", flush=True)
    asyncio.create_task(run_scraper())
    
    response = {
        "status": "triggered",
        "message": "Scraper started in background",
        "timestamp": datetime.now().isoformat()
    }
    print(f"✅ [HTTP] Scrape trigger response sent: {response}", flush=True)
    return web.json_response(response)

async def start_server():
    """Start HTTP server"""
    print("=" * 70, flush=True)
    print("🚀 FC MOBILE CARD SCRAPER - ON-DEMAND", flush=True)
    print("=" * 70, flush=True)
    
    # Initialize progress tracking
    print("🔍 [INIT] Initializing progress table...", flush=True)
    table_exists = init_progress_table()
    
    if not table_exists:
        print("❌ [INIT] Progress table missing! Scheduler will work but scraper may fail.", flush=True)
        print("⚠️  [INIT] Create the table in Supabase and restart service.", flush=True)
    
    # Show current status
    progress = get_current_batch()
    if progress['initial_mode']:
        print(f"\n📊 [STATUS] Mode: Initial", flush=True)
        print(f"📦 [STATUS] Current Batch: {progress['current_batch']}/{progress['total_batches']}", flush=True)
        print(f"📅 [STATUS] Estimated completion: {10 - progress['current_batch'] + 1} runs remaining", flush=True)
    else:
        print(f"\n📊 [STATUS] Mode: Maintenance", flush=True)
        print(f"🔄 [STATUS] Ready to process new/missing cards", flush=True)
    
    print(f"\n💡 [STATUS] Endpoints available:", flush=True)
    print(f"   GET /health - Check status", flush=True)
    print(f"   GET /scrape - Trigger scraper manually", flush=True)
    
    # Create web app
    print("\n🔍 [HTTP] Creating web application...", flush=True)
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/scrape', trigger_scrape)
    print("✅ [HTTP] Routes configured", flush=True)
    
    # Start server
    port = int(os.environ.get('PORT', 10000))
    print(f"🔍 [HTTP] Starting server on port {port}...", flush=True)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print(f"🌐 [HTTP] Server running on port {port}", flush=True)
    print(f"✅ [INIT] Scheduler ready to receive triggers!\n", flush=True)
    sys.stdout.flush()
    
    # Keep running forever
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        print("🔍 [MAIN] Starting asyncio event loop...", flush=True)
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("\n\n🛑 [MAIN] Scheduler stopped by user", flush=True)
    except Exception as e:
        print(f"\n❌ [MAIN] Fatal error: {e}", flush=True)
        import traceback
        traceback.print_exc()
