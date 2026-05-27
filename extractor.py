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
    try:
        cursor.execute("ALTER TABLE jobs ADD COLUMN salary TEXT")
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

def extract_salary_from_text(text: str) -> str:
    if not text:
        return "Unknown"
        
    # Pattern 1: $15 - $25 / hour or $50,000 - $80,000 a year or £13.28 an hour or €3.000 / month
    pattern = r'(?:[\$£€]\s*\d+(?:\.\d+)?(?:,\d{3})*(?:\.\d{2})?\s*(?:-|to)\s*)?[\$£€]\s*\d+(?:\.\d+)?(?:,\d{3})*(?:\.\d{2})?\s*(?:per\s+|/|\ban?\s+)?(?:hour|hr|hourly|yr|year|annually|annum|month|mo|weekly|wk)\b'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
        
    # Pattern 2: $30.56 - 60.82 Hourly
    pattern_hourly2 = r'[\$£€]\s*\d+(?:\.\d+)?\s*(?:-|to)\s*\d+(?:\.\d+)?\s*(?:hour|hr|hourly)\b'
    match_hourly2 = re.search(pattern_hourly2, text, re.IGNORECASE)
    if match_hourly2:
        return match_hourly2.group(0).strip()
    
    # Pattern 3: simple currency ranges like "$100,000 - $120,000" or "£35,000 - £45,000"
    pattern_range = r'[\$£€]\s*\d{2,}(?:,\d{3})*(?:\.\d{2})?\s*(?:-|to)\s*[\$£€]\s*\d{2,}(?:,\d{3})*(?:\.\d{2})?'
    match_range = re.search(pattern_range, text)
    if match_range:
        return match_range.group(0).strip()
        
    return "Unknown"

def parse_job_details(html: str, job_url: str, domain: str):
    """
    Parses Avature JobDetail pages to extract Title, clean Description, 
    and structured Metadata using Avature's standard white-label class patterns.
    Supports Bloomberg/Standard, NVA/Standard, and Mettler Toledo/Standard templates.
    """
    import copy
    import json
    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Collect all key-value pairs (metadata) and large text fields dynamically
    metadata = {}
    description_blocks = []
    seen_desc_texts = set()
    
    # --- Pattern 1: Class-Suffix Prefix Scanner (e.g. jobDetailTableLocation or jobDetailDescription) ---
    for item in soup.find_all(class_=lambda x: x and any(k in x.lower() for k in ["jobdetailtable", "jobdetail_"])):
        cls_list = item.get("class")
        for c in cls_list:
            c_lower = c.lower()
            if "jobdetaildescription" in c_lower or "jobdetailtabledescription" in c_lower:
                txt = item.get_text(separator="\n", strip=True)
                if txt and txt not in seen_desc_texts:
                    seen_desc_texts.add(txt)
                    description_blocks.append(txt)
            elif "jobdetailtable" in c_lower or "jobdetail_" in c_lower:
                lbl = c_lower.replace("jobdetailtable", "").replace("jobdetail_", "").strip()
                # Split camelCase if present
                lbl = re.sub(r'(?<!^)(?=[A-Z])', ' ', lbl).lower()
                
                txt = item.get_text(separator="\n", strip=True)
                if "\n" in txt:
                    parts = txt.split("\n", 1)
                    val = parts[1].strip()
                elif ":" in txt:
                    parts = txt.split(":", 1)
                    val = parts[1].strip()
                else:
                    val = txt
                    
                if lbl and val:
                    metadata[lbl] = val

    # --- Process Layout: elements with classes containing list-item- or list_item_ ---
    for item in soup.find_all(class_=lambda x: x and any(k in x.lower() for k in ["list-item-", "list_item_"])):
        cls_list = item.get("class")
        for c in cls_list:
            if "list-item-" in c or "list_item_" in c:
                lbl = c.replace("list-item-", "").replace("list_item_", "").strip()
                # Split camelCase
                lbl = re.sub(r'(?<!^)(?=[A-Z])', ' ', lbl).lower()
                val = item.text.strip()
                if lbl and val:
                    metadata[lbl] = val

    # --- Process Layout: Sibling label/value pairs (e.g. class ends with label/value or contains it) ---
    for lbl_elem in soup.find_all(class_=lambda x: x and any(k in x.lower() for k in ["label", "fieldlabel", "fieldsetlabel"])):
        lbl_text = lbl_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
        if lbl_text and len(lbl_text) < 50:
            val_elem = lbl_elem.find_next(class_=lambda x: x and any(k in x.lower() for k in ["value", "fieldvalue", "fieldsetvalue"]))
            if val_elem:
                val_text = val_elem.text.strip()
                if val_text and lbl_text not in metadata:
                    metadata[lbl_text] = val_text
                    
    # --- General Layout: view__field or fieldset ---
    field_elements = soup.find_all(class_=lambda x: x and any(k in x.lower() for k in ["article__content__view__field", "fieldset"]))
    for field in field_elements:
        lbl_elem = field.find(class_=lambda x: x and any(k in x.lower() for k in ["label", "fieldsetlabel"]))
        val_elem = field.find(class_=lambda x: x and any(k in x.lower() for k in ["value", "fieldsetvalue"]))
        
        if lbl_elem and val_elem:
            lbl = lbl_elem.text.strip().lower().replace(":", "").replace("#", "").strip()
            val = val_elem.text.strip()
            if lbl and val:
                metadata[lbl] = val
        elif val_elem:
            val_text = val_elem.text.strip()
            if len(val_text) > 80 and val_text not in seen_desc_texts:
                if not any(k in val_text.lower() for k in ["apply now", "sign in", "skip to content"]):
                    seen_desc_texts.add(val_text)
                    description_blocks.append(val_text)
                    
    # --- Process Layout: NVA standard spans with data-map ---
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

    # --- Process Layout: Inline colons "Label: Value" in small text elements ---
    for tag in soup.find_all(["p", "div", "span", "li"]):
        txt = tag.text.strip()
        if 0 < len(txt) < 200 and ":" in txt:
            parts = txt.split(":", 1)
            lbl = parts[0].strip().lower().strip()
            val = parts[1].strip()
            if lbl and val and len(lbl) < 40:
                metadata_words = ["company", "position", "territory", "therapy", "vacancy", "location", "salary", "ref", "job id", "req", "department", "date", "contract", "hours", "shift"]
                if any(w in lbl for w in metadata_words) or lbl in ["ref", "req", "id"]:
                    if lbl not in metadata:
                        metadata[lbl] = val

    # --- Fallback: scan for any strong/b tags inside paragraphs that look like labels ---
    if not metadata:
        for p in soup.find_all("p"):
            strong = p.find(["strong", "b"])
            if strong:
                lbl_text = strong.text.strip().lower().replace(":", "").strip()
                full_text = p.text.strip()
                strong_text = strong.text.strip()
                if full_text.startswith(strong_text) and len(full_text) > len(strong_text):
                    val_text = full_text[len(strong_text):].strip().strip("-").strip()
                    if lbl_text and val_text and len(lbl_text) < 30 and len(val_text) < 150:
                        metadata[lbl_text] = val_text

    # 2. Extract Standard Schema Columns from Metadata
    location = "Unknown"
    posted_date = "Unknown"
    department = "Unknown"
    ref_id = "Unknown"
    salary = "Unknown"
    
    location_parts = []
    
    for lbl, val in metadata.items():
        lbl_clean = lbl.replace("_", " ").replace("-", " ").strip()
        lbl_words = set(re.findall(r"\w+", lbl_clean))
        
        # Location mapping: city, state, country, location, site, place, workplace
        if any(k in lbl_clean for k in ["city", "state", "province", "region", "country", "location", "preferred location", "site", "place", "facility", "hospital", "clinic", "office", "workplace", "advertising location"]):
            if not any(k in lbl_clean for k in ["required", "qualification", "experience", "closing"]):
                if val and val.lower() != "unknown" and val not in location_parts and len(val) < 100:
                    location_parts.append(val)
                    
        # Date mapping
        if any(k in lbl_words for k in ["posted", "date", "created", "published"]) or "date" in lbl_clean or lbl_clean == "apply by":
            if not any(k in lbl_clean for k in ["closing", "close", "end"]):
                if val and val.lower() != "unknown" and posted_date == "Unknown":
                    clean_val = re.sub(r'(?i)apply\s+by\s+', '', val).strip()
                    posted_date = clean_val
            
        # Department mapping
        if any(k in lbl_clean for k in ["department", "business area", "business unit", "division", "team", "function", "category", "specialty", "discipline", "profession", "practice area", "career field", "job field", "career area", "subcategory", "domain", "therapy area"]):
            if lbl_clean == "domain" and "." in val:
                continue
            if val and val.lower() != "unknown" and department == "Unknown":
                department = val
            
        # Ref ID mapping
        if any(w in lbl_words for w in ["ref", "req", "id", "code", "number"]) or any(k in lbl_clean for k in ["reference", "requisition", "job number"]):
            if not any(k in lbl_clean for k in ["required", "requirement", "qualification", "experience"]):
                if val and val.lower() != "unknown" and ref_id == "Unknown":
                    ref_id = val
                    
        # Exact Ref ID matching
        if lbl_clean in ["job", "job #", "job no", "req", "req #", "req no", "ref", "ref #", "ref no"]:
            if val and val.lower() != "unknown" and ref_id == "Unknown":
                ref_id = val

        # Salary/Compensation mapping
        if any(k in lbl_clean for k in ["salary", "compensation", "pay rate", "pay range", "remuneration", "hourly rate", "annual base salary", "starting pay", "pay rate"]):
            if val and val.lower() != "unknown" and salary == "Unknown":
                salary = val

    if location_parts:
        location = ", ".join(location_parts)

    # Fallback Ref ID from URL
    if ref_id == "Unknown" or not ref_id:
        parsed_url = urllib.parse.urlparse(job_url)
        path_parts = [p for p in parsed_url.path.split("/") if p]
        if path_parts:
            last_part = path_parts[-1]
            if re.match(r"^\d+$", last_part):
                ref_id = last_part

    # 3. Pull out the Job Title
    title = "Unknown Title"
    for k in ["job title", "title", "position", "role", "position title", "posting title - english", "posting job title"]:
        if metadata.get(k):
            title = metadata[k]
            break
            
    if title == "Unknown Title" or not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.text.strip()
            
    if title == "Unknown Title" or not title:
        h1_tag = soup.find("h1") or soup.find(class_=lambda x: x and "job-title" in x.lower())
        if h1_tag:
            title = h1_tag.text.strip()
            
    title = clean_job_title(title, domain, ref_id, department)

    # 4. Construct Job Description
    # Append values from metadata fields that represent descriptions/requirements/benefits
    description_keywords = [
        "description", "job description", "summary", "responsibilities", "requirements", 
        "qualifications", "benefits", "highlights", "about the role", "about us", "our story", 
        "what you'll do", "key responsibilities", "essential duties", "training and/or experience required",
        "introduction", "intro", "tasks", "task", "profil", "profile", "aufgaben", "wir suchen", 
        "what is in it for you", "you will need", "you will be responsible for", "about the team", 
        "candidate profile", "experience required", "what we offer", "who you are", "ideal candidate",
        "skills", "what we're looking for", "key duties", "essential skills"
    ]
    
    for lbl, val in metadata.items():
        lbl_lower = lbl.lower()
        if any(k in lbl_lower for k in description_keywords):
            if len(val) > 5 and val not in seen_desc_texts:
                seen_desc_texts.add(val)
                description_blocks.append(val)
                
    rich_text_divs = soup.find_all(class_=lambda x: x and any(k in x.lower() for k in ["rich-text", "field--rich-text", "job-description", "job-body", "description-content", "jobdescription"]))
    for rtd in rich_text_divs:
        txt = rtd.get_text(separator="\n", strip=True)
        if len(txt) > 80 and txt not in seen_desc_texts:
            seen_desc_texts.add(txt)
            description_blocks.append(txt)

    # General fallback: check if we can get text from a central container
    if not description_blocks:
        body_content = soup.find("div", class_=lambda x: x and any(k in x.lower() for k in ["body__content", "main__content", "article__content", "jobdetailboxcontainter", "jobdetail"]))
        if body_content:
            body_copy = copy.copy(body_content)
            # Decompose only labels
            for small_elem in body_copy.find_all(class_=lambda x: x and any(k in x.lower() for k in ["label", "view__field__label", "fieldsetlabel"])):
                small_elem.decompose()
            txt = body_copy.get_text(separator="\n", strip=True)
            # Clean up boilerplate navigation or footer terms
            lines = [line.strip() for line in txt.split("\n")]
            cleaned_lines = []
            for line in lines:
                if not line:
                    continue
                # Skip typical navigation lines
                if any(nav in line.lower() for nav in ["skip to content", "go back", "apply now", "share this job", "cookie policy", "privacy policy"]):
                    continue
                cleaned_lines.append(line)
            txt = "\n".join(cleaned_lines).strip()
            if len(txt) > 100:
                description_blocks.append(txt)
                
    description = "\n\n".join(description_blocks).strip()
    if not description:
        description = "No description found"

    # Fallback Salary extraction from Description/HTML
    if salary == "Unknown" or not salary:
        salary = extract_salary_from_text(description)
    if salary == "Unknown" or not salary:
        salary = extract_salary_from_text(soup.text)

    return {
        "url": job_url,
        "domain": domain,
        "title": title,
        "description": description,
        "location": location,
        "posted_date": posted_date,
        "department": department,
        "ref_id": ref_id,
        "salary": salary,
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
                            INSERT OR REPLACE INTO jobs (url, domain, title, description, location, posted_date, department, ref_id, salary, raw_metadata, raw_html)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            job_data["url"],
                            job_data["domain"],
                            job_data["title"],
                            job_data["description"],
                            job_data["location"],
                            job_data["posted_date"],
                            job_data["department"],
                            job_data["ref_id"],
                            job_data["salary"],
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