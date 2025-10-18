print("🔍 DEBUG: Scheduler starting...", flush=True)
import os
print("🔍 DEBUG: Imports starting...", flush=True)
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
from aiohttp import web

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Progress tracking
PROGRESS_TABLE = 'card_scraper_progress'

# Track if scraper is currently running
scraper_running = False

def init_progress_table():
    """Create progress tracking table if it doesn't exist"""
    try:
        supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        print("✅ Progress table exists")
    except Exception as e:
        print(f"⚠️ Progress table may not exist. Create it in Supabase")

def get_current_batch():
    """Get current batch number from database"""
    try:
        result = supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        if result.data:
            return result.data[0]
        else:
            supabase.table(PROGRESS_TABLE).insert({
                'current_batch': 1,
                'total_batches': 10,
                'initial_mode': True
            }).execute()
            return {'current_batch': 1, 'total_batches': 10, 'initial_mode': True}
    except Exception as e:
        print(f"❌ Error getting batch: {e}")
        return {'current_batch': 1, 'total_batches': 10, 'initial_mode': True}

def update_batch(batch_num, initial_mode=True):
    """Update current batch number"""
    try:
        supabase.table(PROGRESS_TABLE).update({
            'current_batch': batch_num,
            'initial_mode': initial_mode,
            'last_run': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }).eq('id', 1).execute()
        print(f"✅ Updated to batch {batch_num}")
    except Exception as e:
        print(f"❌ Error updating batch: {e}")

async def run_scraper():
    """Run the card scraper"""
    global scraper_running
    
    if scraper_running:
        print("⚠️ Scraper already running, skipping...")
        return {"status": "already_running"}
    
    scraper_running = True
    
    try:
        print("=" * 70)
        print(f"🎴 Starting Card Scraper")
        print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p IST')}")
        print("=" * 70)
        
        # Get current progress
        progress = get_current_batch()
        current_batch = progress['current_batch']
        total_batches = progress['total_batches']
        initial_mode = progress['initial_mode']
        
        # Import here to avoid circular imports
        import card_image
        
        if initial_mode:
            print(f"📦 Initial Mode - Processing Batch {current_batch}/{total_batches}")
            print(f"📊 Players: {(current_batch-1)*BATCH_SIZE + 1} to {current_batch*BATCH_SIZE}")
            
            # Set environment variable for batch processing
            os.environ['BATCH_NUMBER'] = str(current_batch)
            os.environ['BATCH_SIZE'] = str(BATCH_SIZE)
            
            # Run scraper
            await card_image.main()
            
            # Move to next batch
            next_batch = current_batch + 1
            if next_batch > total_batches:
                print("✅ Initial scraping complete! Switching to maintenance mode.")
                update_batch(1, initial_mode=False)
            else:
                update_batch(next_batch, initial_mode=True)
                print(f"✅ Batch {current_batch} complete! Next: Batch {next_batch}")
            
            return {"status": "success", "batch": current_batch, "mode": "initial"}
        else:
            print("🔧 Maintenance Mode - Checking for new/missing cards")
            
            # In maintenance mode, don't use batches
            os.environ.pop('BATCH_NUMBER', None)
            os.environ.pop('BATCH_SIZE', None)
            
            await card_image.main()
            print("✅ Maintenance check complete!")
            
            return {"status": "success", "mode": "maintenance"}
        
    except Exception as e:
        print(f"❌ Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
    finally:
        scraper_running = False

# HTTP endpoints
async def health_check(request):
    """Health check endpoint"""
    progress = get_current_batch()
    status = {
        'status': 'running',
        'scraper_active': scraper_running,
        'mode': 'initial' if progress['initial_mode'] else 'maintenance',
        'current_batch': progress['current_batch'],
        'total_batches': progress['total_batches'],
        'last_run': progress.get('last_run', 'Never')
    }
    return web.json_response(status)

async def trigger_scrape(request):
    """Endpoint to trigger scraping"""
    print(f"\n🔔 Scrape triggered at {datetime.now().strftime('%I:%M %p IST')}")
    
    # Run scraper in background
    asyncio.create_task(run_scraper())
    
    return web.json_response({
        "status": "triggered",
        "message": "Scraper started in background",
        "timestamp": datetime.now().isoformat()
    })

async def start_server():
    """Start HTTP server"""
    print("=" * 70)
    print("🚀 FC MOBILE CARD SCRAPER - ON-DEMAND")
    print("=" * 70)
    
    # Initialize progress tracking
    init_progress_table()
    
    # Show current status
    progress = get_current_batch()
    if progress['initial_mode']:
        print(f"\n📊 Status: Initial Mode")
        print(f"📦 Current Batch: {progress['current_batch']}/{progress['total_batches']}")
        print(f"📅 Will complete in {10 - progress['current_batch'] + 1} runs")
    else:
        print(f"\n📊 Status: Maintenance Mode")
        print(f"🔄 Ready to process new/missing cards")
    
    print(f"\n💡 Trigger scraping:")
    print(f"   GET /scrape - Start scraper manually")
    print(f"   GET /health - Check status")
    
    # Create web app
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/scrape', trigger_scrape)
    
    # Start server
    port = int(os.environ.get('PORT', 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print(f"\n🌐 HTTP server running on port {port}")
    print(f"✅ Ready to receive scrape triggers!\n")
    
    # Keep running forever
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        import traceback
        traceback.print_exc()

