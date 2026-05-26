import asyncio
import json
import logging
import sqlite3
import re
import urllib.parse
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AvatureExtractor")

import random

def get_browser_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

# Setup robust database connection with full schema
def setup_database(db_path="avature_jobs.db"):
    conn = sqlite3.connect(db_path)
    # Enable WAL (Write-Ahead Logging) mode for concurrent high-speed writing
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            url TEXT PRIMARY KEY,
            domain TEXT,
            title TEXT,
            description TEXT,
            location TEXT,
            posted_date TEXT,
            department TEXT,
            ref_id TEXT,
            raw_metadata TEXT,
            raw_html TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Gracefully upgrade existing table if it doesn't have the new columns
    try:
        cursor.execute("ALTER TABLE jobs ADD COLUMN raw_metadata TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE jobs ADD COLUMN raw_html TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.commit()
    return conn

def clean_job_title(title, domain=None, ref_id=None, department=None):
    if not title:
        return "Unknown Title"
        
    import html
    title = html.unescape(title)
    
    # 1. If it contains ' - - ', split by it and take the first part
    if " - - " in title:
        title = title.split(" - - ")[0].strip()
        
    # 2. Strip standard suffixes like | Avature, - Careers, etc.
    title = re.split(r"\s*\|\s*Avature", title, flags=re.IGNORECASE)[0]
    title = re.split(r"\s*-\s*Careers", title, flags=re.IGNORECASE)[0]
    
    # 3. Smart trailing parts popping (iteratively strip company, ID, and division from the right)
    parts = [p.strip() for p in re.split(r'\s+-\s+', title)]
    company_name = domain.split(".")[0].lower() if domain else None
    
    changed = True
    while len(parts) > 1 and changed:
        changed = False
        last_lower = parts[-1].lower()
        
        # A. Strip company name
        if company_name and (last_lower == company_name or last_lower == "company" or last_lower == "careers"):
            parts.pop()
            changed = True
            continue
            
        # B. Strip numeric ID
        if re.match(r"^\d+$", parts[-1]):
            parts.pop()
            changed = True
            continue
            
        # C. Strip division/department if it overlaps with parsed department (sharing words > 3 chars)
        if department and department != "Unknown":
            dept_words = set(re.findall(r"\w+", department.lower()))
            part_words = set(re.findall(r"\w+", last_lower))
            overlap = {w for w in (dept_words & part_words) if len(w) > 3}
            if overlap:
                parts.pop()
                changed = True
                continue
                
        # D. Strip common division keywords
        division_keywords = ["solutions", "administration", "cto", "department", "division", "corporate"]
        if any(k in last_lower for k in division_keywords):
            parts.pop()
            changed = True
            continue
            
    title = " - ".join(parts).strip()
        
    # 4. Strip trailing domain name or company name if present
    if domain:
        company_name = domain.split(".")[0]  # e.g., "bloomberg"
        title = re.sub(rf"\s*-\s*{company_name}\b", "", title, flags=re.IGNORECASE)
        title = re.sub(r"\s*-\s*Company\b", "", title, flags=re.IGNORECASE)
        
    # 5. Strip trailing Ref ID if it matches a number
    title = re.sub(r"\s*-\s*\d+\b", "", title)
    
    # 6. If it's Mettler Toledo style and starts with "RefID-", strip it
    if ref_id and ref_id != "Unknown":
        ref_esc = re.escape(ref_id)
        title = re.sub(rf"^{ref_esc}\s*-\s*", "", title)
        
    # Clean double spaces and hyphens at ends
    title = re.sub(r"\s+", " ", title).strip()
    title = title.strip("-").strip()
    
    return title or "Unknown Title"

def parse_job_details(html: str, job_url: str, domain: str):
    """
    Parses Avature JobDetail pages to extract Title, clean Description, 
    and structured Metadata using Avature's standard white-label class patterns.
    Supports Bloomberg/Standard, NVA/Standard, and Mettler Toledo/Standard templates.
    """
    import copy
    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Extract Job Title
    title = "Unknown Title"
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.text.strip()
        
    h1_tag = soup.find("h1") or soup.find(class_=lambda x: x and "job-title" in x.lower())
    if h1_tag and (title == "Unknown Title" or not title):
        title = h1_tag.text.strip()
        
    # 2. Extract Metadata Key-Value pairs dynamically (covering multiple layouts)
    metadata = {}
    
    # --- Layout 1: article__content__view__field (Bloomberg / Standard) ---
    field_elements = soup.find_all(class_=lambda x: x and "article__content__view__field" in x)
    for field in field_elements:
        label_elem = field.find(class_=lambda x: x and "label" in x.lower())
        value_elem = field.find(class_=lambda x: x and "value" in x.lower())
        if label_elem and value_elem:
            lbl = label_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
            val = value_elem.text.strip()
            if lbl and val:
                metadata[lbl] = val
                
    # --- Layout 2: data-map="item-title" and data-map="item-value" (NVA / Standard) ---
    title_spans = soup.find_all(attrs={"data-map": "item-title"})
    for ts in title_spans:
        parent = ts.parent
        if parent:
            val_elem = parent.find(attrs={"data-map": "item-value"})
            if val_elem:
                lbl = ts.text.strip().lower().replace(":", "").replace("#", "").strip()
                val = val_elem.text.strip()
                if lbl and val:
                    metadata[lbl] = val
                    
    # --- Layout 3: fieldSetLabel and fieldSetValue (Mettler Toledo / Standard) ---
    fieldset_elements = soup.find_all(class_=lambda x: x and "fieldset" in x.lower())
    for fs in fieldset_elements:
        lbl_elem = fs.find(class_=lambda x: x and "label" in x.lower()) or fs.find(class_=lambda x: x and "fieldsetlabel" in x.lower())
        val_elem = fs.find(class_=lambda x: x and "value" in x.lower()) or fs.find(class_=lambda x: x and "fieldsetvalue" in x.lower())
        if lbl_elem and val_elem:
            lbl = lbl_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
            val = val_elem.text.strip()
            if lbl and val:
                metadata[lbl] = val
                
    # Alternate Layout 3 Check
    for label_elem in soup.find_all(class_=lambda x: x and "fieldsetlabel" in x.lower()):
        parent = label_elem.parent
        if parent:
            value_elem = parent.find(class_=lambda x: x and "fieldsetvalue" in x.lower())
            if value_elem:
                lbl = label_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
                val = value_elem.text.strip()
                if lbl and val:
                    metadata[lbl] = val
                    
    # Normalize extracted metadata fields into standard schema columns
    location = "Unknown"
    posted_date = "Unknown"
    department = "Unknown"
    ref_id = "Unknown"
    
    location_parts = []
    for lbl, val in metadata.items():
        lbl_clean = lbl.replace("_", " ").replace("-", " ").strip()
        # Location mapping: combine 'city', 'state', 'country', 'site' into unified string
        if lbl_clean in ["city", "state", "country", "location", "preferred location", "site", "place"]:
            if val and val.lower() != "unknown" and val not in location_parts:
                location_parts.append(val)
        # Date mapping
        elif any(k in lbl_clean for k in ["posted", "date", "created", "published"]):
            posted_date = val
        # Department mapping
        elif any(k in lbl_clean for k in ["department", "business area", "division", "team", "function"]):
            department = val
        # Ref ID mapping
        elif any(k in lbl_clean for k in ["ref", "req", "id", "reference", "code"]):
            ref_id = val

    if location_parts:
        location = ", ".join(location_parts)

    # Apply title cleaning helper to get pristine job title
    title = clean_job_title(title, domain, ref_id, department)

    # 3. Extract Clean Description
    description_blocks = []
    
    # A. Search for standard rich-text divs (Bloomberg / Standard)
    rich_text_divs = soup.find_all("div", class_=lambda x: x and "field--rich-text" in x)
    for rtd in rich_text_divs:
        txt = rtd.get_text(separator="\n", strip=True)
        if len(txt) > 50:
            description_blocks.append(txt)
            
    # B. If no rich text, search for article__content divs (NVA / Standard)
    # Uses bs4 decomposition on a cloned element to dynamically strip metadata fields
    if not description_blocks:
        article_divs = soup.find_all("div", class_="article__content")
        for ad in article_divs:
            ad_copy = copy.copy(ad)
            
            # Decompose metadata fields within the description container
            for meta_elem in ad_copy.find_all(class_=lambda x: x and any(k in x.lower() for k in ["label", "value", "view__field"])):
                meta_elem.decompose()
            for meta_elem in ad_copy.find_all(attrs={"data-map": ["item-title", "item-value"]}):
                parent = meta_elem.parent
                if parent and parent != ad_copy:
                    parent.decompose()
                else:
                    meta_elem.decompose()
                
            txt = ad_copy.get_text(separator="\n", strip=True)
            # Filter standard action buttons, headers, or social shares
            if len(txt) > 100 and not any(k in txt.lower() for k in ["apply now", "save this job", "share this job"]):
                description_blocks.append(txt)
                
    # C. Search for Mettler Toledo style descriptions (fieldset where label matches description/requirements)
    if not description_blocks:
        fieldset_elements = soup.find_all(class_=lambda x: x and "fieldset" in x.lower())
        for fs in fieldset_elements:
            lbl_elem = fs.find(class_=lambda x: x and "label" in x.lower()) or fs.find(class_=lambda x: x and "fieldsetlabel" in x.lower())
            val_elem = fs.find(class_=lambda x: x and "value" in x.lower()) or fs.find(class_=lambda x: x and "fieldsetvalue" in x.lower())
            if lbl_elem and val_elem:
                lbl = lbl_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
                if any(k in lbl for k in ["description", "responsibilities", "requirements", "profile", "what you will do", "what you need"]):
                    txt = val_elem.get_text(separator="\n", strip=True)
                    if len(txt) > 80 and txt not in description_blocks:
                        description_blocks.append(txt)
                        
    # D. Final general fallback
    if not description_blocks:
        fallback_div = soup.find("div", class_="job-description") or soup.find("div", class_="job-body")
        if fallback_div:
            description_blocks.append(fallback_div.get_text(separator="\n", strip=True))
            
    description = "\n\n".join(description_blocks).strip()
    if not description:
        description = "No description found"

    import json
    return {
        "url": job_url,
        "domain": domain,
        "title": title,
        "description": description,
        "location": location,
        "posted_date": posted_date,
        "department": department,
        "ref_id": ref_id,
        "raw_metadata": json.dumps(metadata, ensure_ascii=False),
        "raw_html": html
    }

async def fetch_sitemap_urls(session: aiohttp.ClientSession, sitemap_url: str):
    """Downloads the XML sitemap and extracts direct job links."""
    headers = get_browser_headers()
    try:
        async with session.get(sitemap_url, headers=headers, timeout=15) as response:
            if response.status == 200:
                xml_data = await response.text()
                # Parse XML safely, stripping namespaces
                root = ET.fromstring(xml_data.encode("utf-8", errors="ignore"))
                urls = [elem.text for elem in root.iter() if "loc" in elem.tag]
                # Filter for job posting urls
                job_urls = [u for u in urls if u and ("/JobDetail" in u or "/FolderDetail" in u)]
                return job_urls
    except Exception as e:
        logger.error(f"Failed to parse sitemap at {sitemap_url}: {e}")
    return []

async def scrape_job_html(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, job_url: str, domain: str, db_conn):
    """Scrapes individual job page and performs upsert into SQLite."""
    headers = get_browser_headers()
    
    async with semaphore:
        for attempt in range(3):  # Simple retry loop
            # Jittered sleep to act like a real browser and prevent concurrent WAF triggers
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                async with session.get(job_url, headers=headers, timeout=12) as response:
                    if response.status == 200:
                        html = await response.text()
                        job_data = parse_job_details(html, job_url, domain)
                        
                        # SQLite Upsert
                        cursor = db_conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO jobs (url, domain, title, description, location, posted_date, department, ref_id, raw_metadata, raw_html)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            job_data["url"],
                            job_data["domain"],
                            job_data["title"],
                            job_data["description"],
                            job_data["location"],
                            job_data["posted_date"],
                            job_data["department"],
                            job_data["ref_id"],
                            job_data["raw_metadata"],
                            job_data["raw_html"]
                        ))
                        db_conn.commit()
                        logger.info(f"✅ [{domain}] Saved Job: {job_data['title'][:40]}... ({job_data['location']})")
                        return
                    elif response.status in [403, 406, 429, 500]:
                        # Backoff and retry
                        backoff = 3 * (attempt + 1)
                        logger.warning(f"⚠️ [{domain}] Hit rate limit/WAF status {response.status} for job: {job_url}. Retrying in {backoff}s...")
                        await asyncio.sleep(backoff)
                    else:
                        logger.warning(f"Failed to fetch job: {job_url} - Status: {response.status}")
            except Exception as e:
                if attempt == 2:
                    logger.debug(f"Failed to scrape {job_url} after 3 attempts: {e}")
                await asyncio.sleep(1)

async def crawl_search_page(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, search_url: str, domain: str, db_conn):
    """
    Track B: Crawls SearchJobs endpoint using GET-pagination parameters 
    and concurrently launches HTML scrapers for all found job links.
    """
    logger.info(f"[{domain}] Initiating Track B Search Page Crawler...")
    
    headers = get_browser_headers()
    
    page_size = 12
    offset = 0
    scraped_urls = set()
    consecutive_empty_pages = 0
    max_pages = 50  # Cap at 50 pages (~600 jobs max per portal to prevent endless loops)
    
    # We maintain a list of futures to wait on at the end of the crawl
    extraction_tasks = []
    
    for page in range(max_pages):
        # Build standard pagination URL
        parsed_url = urllib.parse.urlparse(search_url)
        query = urllib.parse.parse_qs(parsed_url.query)
        query["jobOffset"] = [str(offset)]
        query["jobRecordsPerPage"] = [str(page_size)]
        
        # Keep original query fields but overwrite offset/records
        new_query = urllib.parse.urlencode(query, doseq=True)
        paginated_url = urllib.parse.urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query,
            parsed_url.fragment
        ))
        
        logger.info(f"[{domain}] Crawling page {page+1} - Offset: {offset}...")
        
        try:
            async with session.get(paginated_url, headers=headers, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"[{domain}] Crawl failed on page {page+1} with status: {response.status}")
                    break
                    
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                
                # Extract all JobDetail/FolderDetail links
                links = [a.get("href") for a in soup.find_all("a", href=True)]
                page_job_urls = set()
                
                for link in links:
                    if "/JobDetail" in link or "/FolderDetail" in link:
                        # Construct absolute URL if relative
                        absolute_url = urllib.parse.urljoin(paginated_url, link)
                        # Normalize out URL query parameters for clean unique tracking
                        normalized_url = absolute_url.split("?")[0]
                        page_job_urls.add(normalized_url)
                
                # Check for empty page or loop
                new_urls = page_job_urls - scraped_urls
                if not new_urls:
                    consecutive_empty_pages += 1
                    logger.info(f"[{domain}] No new jobs found on page {page+1}.")
                    if consecutive_empty_pages >= 2:
                        logger.info(f"[{domain}] Crawl complete. No new listings found after 2 consecutive pages.")
                        break
                else:
                    consecutive_empty_pages = 0
                    scraped_urls.update(new_urls)
                    
                    # Concurrently launch scraping task for each newly found URL
                    for job_url in new_urls:
                        task = asyncio.create_task(scrape_job_html(session, semaphore, job_url, domain, db_conn))
                        extraction_tasks.append(task)
                        
                # Increment offset
                offset += page_size
                # Throttle slightly between page crawls
                await asyncio.sleep(0.5)
                
        except Exception as e:
            logger.error(f"[{domain}] Error crawling search page at offset {offset}: {e}")
            break
            
    # Wait for all individual job scraping tasks to finish for this domain
    if extraction_tasks:
        logger.info(f"[{domain}] Waiting for {len(extraction_tasks)} scraping tasks to complete...")
        await asyncio.gather(*extraction_tasks)
    logger.info(f"🎉 [{domain}] Track B extraction finished. Processed {len(scraped_urls)} jobs.")

async def process_domain(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, target: dict, db_conn):
    domain = target["domain"]
    # Polite domain-level concurrency to evade corporate edge WAFs/rate limits
    domain_semaphore = asyncio.Semaphore(5)
    
    if target.get("sitemap_url"):
        logger.info(f"[{domain}] Initiating Track A: Sitemap Extraction...")
        job_urls = await fetch_sitemap_urls(session, target["sitemap_url"])
        
        if job_urls:
            # Deduplicate URLs
            unique_urls = list(set([u.split("?")[0] for u in job_urls]))
            logger.info(f"[{domain}] Found {len(unique_urls)} jobs in sitemap. Queueing HTML scraping...")
            
            # Spin up concurrent workers using the polite domain-specific semaphore
            tasks = [scrape_job_html(session, domain_semaphore, url, domain, db_conn) for url in unique_urls]
            await asyncio.gather(*tasks)
        else:
            logger.warning(f"[{domain}] Sitemap extraction yielded 0 URLs. Falling back to Track B...")
            # If sitemap was empty, fallback to search page crawl if search_url exists
            if target.get("search_url"):
                await crawl_search_page(session, domain_semaphore, target["search_url"], domain, db_conn)
        
    elif target.get("search_url"):
        await crawl_search_page(session, domain_semaphore, target["search_url"], domain, db_conn)
        
    else:
        logger.warning(f"[{domain}] Target does not support sitemaps or SearchJobs crawls. Skipping.")

async def main():
    db_conn = setup_database()
    
    try:
        with open("validated_targets.json", "r") as f:
            targets = json.load(f)
    except FileNotFoundError:
        logger.error("validated_targets.json not found. Run discovery_engine.py first.")
        return

    # Total concurrency control: 30 parallel requests to not trigger corporate firewalls/WAFs
    semaphore = asyncio.Semaphore(30)
    
    timeout = aiohttp.ClientTimeout(total=45)
    connector = aiohttp.TCPConnector(ssl=False)
    
    logger.info(f"Starting Avature Job Extraction Pipeline on {len(targets)} active domains...")
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Run extraction domain-by-domain to prevent concurrent WAF detection on a single IP 
        # but let the jobs within each domain crawl concurrently.
        for target in targets:
            await process_domain(session, semaphore, target, db_conn)
            
    # Final Metric Reporting
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT domain) FROM jobs")
    total_jobs, total_domains = cursor.fetchone()
    logger.info(f"\n🎉 EXTRACTION COMPLETE: Successfully stored {total_jobs} unique jobs across {total_domains} corporate portals in local database.")

if __name__ == "__main__":
    asyncio.run(main())