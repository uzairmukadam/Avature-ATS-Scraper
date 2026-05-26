import asyncio
import json
import logging
import sqlite3
import csv
import os
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

from datetime import datetime

# Import core pipelines
from discovery_engine import main_discovery_pipeline
from extractor import main as run_extractor

# Configure orchestration logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AvatureOrchestrator")

def export_data(db_path="avature_jobs.db", json_out="scraped_jobs.json", csv_out="scraped_jobs.csv", domains_out="discovered_domains.txt"):
    """
    Exports scraped job data from the SQLite database to clean, pristine formats
    (JSON, CSV) and writes the validated list of discovered domains.
    """
    logger.info("Exporting scraped database records to local files...")
    
    if not os.path.exists(db_path):
        logger.error(f"Database {db_path} not found. Cannot export.")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Export active domains
    try:
        with open("validated_targets.json", "r") as f:
            targets = json.load(f)
        active_urls = sorted(list(set([t["base_url"] for t in targets if t.get("is_active")])))
        with open(domains_out, "w", encoding="utf-8") as f:
            for url in active_urls:
                f.write(f"{url}\n")
        logger.info(f"Saved {len(active_urls)} active portal URLs to '{domains_out}'")
    except Exception as e:
        logger.error(f"Failed to export active domains: {e}")
        
    # 2. Fetch all jobs
    try:
        cursor.execute('''
            SELECT url, raw_html FROM jobs
        ''')
        rows = cursor.fetchall()
        
        job_list = []
        for row in rows:
            job_list.append({
                "url": row[0],
                "raw_html": row[1]
            })
            
        # Write to JSON
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(job_list, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully exported {len(job_list)} jobs to clean JSON: '{json_out}'")
        
        # Write to CSV
        try:
            with open(csv_out, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Application URL", "Raw HTML"])
                for job in job_list:
                    writer.writerow([
                        job["url"],
                        job["raw_html"]
                    ])
            logger.info(f"Successfully exported {len(job_list)} jobs to clean CSV: '{csv_out}'")
        except PermissionError:
            fallback_csv = csv_out.replace(".csv", "_fallback.csv")
            logger.warning(f"⚠️ Permission Denied: Could not write to '{csv_out}' because the file is locked (likely open in Excel or another spreadsheet viewer).")
            logger.warning(f"⚠️ Writing export data to fallback file instead: '{fallback_csv}'")
            with open(fallback_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Application URL", "Raw HTML"])
                for job in job_list:
                    writer.writerow([
                        job["url"],
                        job["raw_html"]
                    ])
            logger.info(f"Successfully exported {len(job_list)} jobs to fallback CSV: '{fallback_csv}'")
        
    except Exception as e:
        logger.error(f"Failed to export scraped jobs: {e}")
    finally:
        conn.close()

def display_dashboard(db_path="avature_jobs.db"):
    """Displays a beautiful, high-fidelity ASCII report summary of the scraping metrics."""
    if not os.path.exists(db_path):
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Total Jobs
        cursor.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cursor.fetchone()[0]
        
        # Total Portals
        cursor.execute("SELECT COUNT(DISTINCT domain) FROM jobs")
        total_portals = cursor.fetchone()[0]
        
        # Jobs per portal
        cursor.execute("SELECT domain, COUNT(*) as c FROM jobs GROUP BY domain ORDER BY c DESC LIMIT 10")
        top_portals = cursor.fetchall()
        
        # Unique Locations
        cursor.execute("SELECT COUNT(DISTINCT location) FROM jobs WHERE location != 'Unknown'")
        total_locations = cursor.fetchone()[0]
        
        print("\n" + "="*60)
        print("          AVATURE ATS SCRAPER - PIPELINE REPORT          ")
        print("="*60)
        print(f"  📅 Execution Time:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  🏢 Active Portals:      {total_portals}")
        print(f"  💼 Total Jobs Scraped:  {total_jobs}")
        print(f"  📍 Unique Locations:    {total_locations}")
        print("-"*60)
        print("  📊 Top 10 Portals by Job Count:")
        for idx, (dom, count) in enumerate(top_portals):
            print(f"     {idx+1:2d}. {dom:<30} {count:5d} jobs")
        print("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Error compiling dashboard metrics: {e}")
    finally:
        conn.close()

async def main():
    print("\n" + "#"*70)
    print("      LAUNCHING END-TO-END AVATURE ATS SCRAPER PIPELINE      ")
    print("#"*70 + "\n")
    
    start_time = datetime.now()
    
    # 1. Run Target Discovery & Capability Probing
    import sys
    force_discovery = "--force" in sys.argv or not os.path.exists("validated_targets.json")
    if force_discovery:
        logger.info("PHASE 1: RUNNING SITE DISCOVERY ENGINE...")
        await main_discovery_pipeline()
    else:
        logger.info("PHASE 1: Found existing 'validated_targets.json'. Reusing target list. (Pass '--force' to re-run discovery)")
    
    # 2. Run High-Performance Concurrent Job Extraction
    logger.info("PHASE 2: RUNNING CONCURRENT EXTRACTION ENGINE...")
    await run_extractor()
    
    # 3. Export Data to Clean JSON & CSV Formats
    logger.info("PHASE 3: EXPORTING SCRAPED RECORDS...")
    export_data()
    
    duration = datetime.now() - start_time
    logger.info(f"Pipeline executed in {duration.total_seconds():.2f} seconds.")
    
    # 4. Display Premium Metrics Dashboard
    display_dashboard()

if __name__ == "__main__":
    asyncio.run(main())