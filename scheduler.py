import os
import asyncio
import schedule
import time as time_module
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

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

def init_progress_table():
    """Create progress tracking table if it doesn't exist"""
    try:
        # Check if table exists by trying to query it
        supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        print("✅ Progress table exists")
    except Exception as e:
        print(f"⚠️ Progress table may not exist. Please create it manually:")
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
        """)

def get_current_batch():
    """Get current batch number from database"""
    try:
        result = supabase.table(PROGRESS_TABLE).select('*').limit(1).execute()
        if result.data:
            return result.data[0]
        else:
            # Initialize if empty
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

async def run_scraper(batch_num=None):
    """Run the card scraper"""
    print("=" * 70)
    print(f"🎴 Starting Card Scraper")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p IST')}")
    print("=" * 70)
    
    # Get current progress
    progress = get_current_batch()
    current_batch = progress['current_batch']
    total_batches = progress['total_batches']
    initial_mode = progress['initial_mode']
    
    if batch_num:
        current_batch = batch_num
    
    # Import here to avoid circular imports
    import card_image
    
    try:
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
        else:
            print("🔧 Maintenance Mode - Checking for new/missing cards")
            
            # In maintenance mode, don't use batches - check all
            os.environ.pop('BATCH_NUMBER', None)
            os.environ.pop('BATCH_SIZE', None)
            
            await card_image.main()
            print("✅ Maintenance check complete!")
        
    except Exception as e:
        print(f"❌ Scraper failed: {e}")
        import traceback
        traceback.print_exc()

def scheduled_job():
    """Wrapper for scheduled runs"""
    print(f"\n🔔 Scheduled run triggered at {datetime.now().strftime('%I:%M %p IST')}")
    asyncio.run(run_scraper())

def keep_alive():
    """Periodic ping to prevent spin-down"""
    print(f"💓 Keep-alive ping at {datetime.now().strftime('%I:%M:%S %p')}")

def main():
    print("=" * 70)
    print("🚀 FC MOBILE CARD SCRAPER - SCHEDULER")
    print("=" * 70)
    
    # Initialize progress tracking
    init_progress_table()
    
    # Show current status
    progress = get_current_batch()
    if progress['initial_mode']:
        print(f"\n📊 Status: Initial Mode")
        print(f"📦 Current Batch: {progress['current_batch']}/{progress['total_batches']}")
        print(f"📅 Estimated Completion: 5 days from start")
    else:
        print(f"\n📊 Status: Maintenance Mode")
        print(f"🔄 Automatically processing new/missing cards")
    
    print(f"\n⏰ Schedule:")
    print(f"   Session 1: 10:00 AM IST (4:30 AM UTC)")
    print(f"   Session 2: 10:00 PM IST (4:30 PM UTC)")
    print(f"\n💡 Press Ctrl+C to stop\n")
    
    # Schedule jobs - 10 AM IST = 4:30 AM UTC, 10 PM IST = 4:30 PM UTC
    schedule.every().day.at("04:30").do(scheduled_job)  # 10 AM IST
    schedule.every().day.at("16:30").do(scheduled_job)  # 10 PM IST
    
    # Keep-alive ping every 10 minutes to prevent Render spin-down
    schedule.every(10).minutes.do(keep_alive)
    
    print("✅ Scheduler initialized! Waiting for scheduled times...\n")
    
    # Keep running
    while True:
        schedule.run_pending()
        time_module.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Scheduler stopped by user")
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}")
        import traceback
        traceback.print_exc()
