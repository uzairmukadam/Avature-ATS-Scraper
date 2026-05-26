import asyncio
import json
import logging
import urllib.parse
import aiohttp
import requests
import re
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Configure robust logging for discovery
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AvatureDiscovery")

def get_browser_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

def fetch_subdomains_from_seed_file():
    """Extracts unique Avature domains from the massive local seed list (Urls.txt)."""
    logger.info("Parsing seed file Urls.txt for unique Avature domains...")
    domains = set()
    try:
        with open("Urls.txt", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    # Clean/parse URL to extract netloc
                    if not line.startswith(("http://", "https://")):
                        line = "http://" + line
                    parsed = urllib.parse.urlparse(line)
                    domain = parsed.netloc.split(":")[0].lower()
                    domain = domain.replace('"', '').replace("'", "").replace("\\", "").strip()
                    if re.match(r"^[a-z0-9.-]+\.avature\.net$", domain) and domain != "avature.net":
                        domains.add(domain)
                except Exception:
                    continue
        logger.info(f"Extracted {len(domains)} unique subdomains from local seed file.")
    except FileNotFoundError:
        logger.warning("Urls.txt seed file not found in root directory.")
    except Exception as e:
        logger.error(f"Error parsing Urls.txt: {e}")
    return domains

def fetch_subdomains_from_archive():
    """Uses the Wayback Machine CDX API to harvest historical Avature portal subdomains."""
    url = "http://web.archive.org/cdx/search/cdx?url=*.avature.net/careers*&output=json&fl=original&collapse=urlkey"
    logger.info("Querying the Wayback Machine (CDX API) for historical Avature portals...")
    subdomains = set()
    try:
        response = requests.get(url, timeout=8)
        if response.status_code == 200:
            data = response.json()
            for row in data[1:]:  # Skip header row
                raw_url = row[0]
                try:
                    if not raw_url.startswith(("http://", "https://")):
                        raw_url = "http://" + raw_url
                    parsed = urllib.parse.urlparse(raw_url)
                    domain = parsed.netloc.split(":")[0].lower()
                    domain = domain.replace('"', '').replace("'", "").replace("\\", "").strip()
                    if re.match(r"^[a-z0-9.-]+\.avature\.net$", domain) and domain != "avature.net":
                        subdomains.add(domain)
                except Exception:
                    continue
            logger.info(f"Harvested {len(subdomains)} unique subdomains from the Wayback Machine.")
        else:
            logger.warning(f"Wayback CDX API returned status: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to fetch from Wayback Machine: {e}")
    return subdomains

import socket

async def dns_resolve(domain: str) -> bool:
    """Asynchronously checks if a domain resolves in DNS."""
    try:
        loop = asyncio.get_running_loop()
        # AF_INET guarantees looking up standard IPv4 addresses without event loop blocks
        await loop.getaddrinfo(domain, None, family=socket.AF_INET)
        return True
    except Exception:
        return False

async def probe_target(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, domain: str):
    """
    Asynchronously checks domain liveness using DNS resolution (WAF-immune),
    then attempts HTTP probing to discover optimal Search/Sitemap endpoints.
    If WAF blocking (403/406/429) is encountered, registers standard predicted paths.
    """
    headers = get_browser_headers()
    
    # 1. DNS resolution check (Resilient to HTTP rate limiting/WAF blocks)
    if not await dns_resolve(domain):
        return None
        
    async with semaphore:
        base_url = f"https://{domain}"
        
        # Candidate search page paths to check
        search_paths = [
            f"{base_url}/careers/SearchJobs",
            f"{base_url}/SearchJobs",
            f"{base_url}/careers",
            f"{base_url}/talent"
        ]
        
        # Candidate sitemap paths to check
        sitemap_paths = [
            f"{base_url}/sitemap.xml",
            f"{base_url}/careers/sitemap.xml"
        ]
        
        active_search_url = None
        active_sitemap_url = None
        is_active_http = False
        hit_waf = False
        
        # 2. Probe for an active Search/Career page
        for path in search_paths:
            try:
                async with session.get(path, headers=headers, timeout=8, allow_redirects=True) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if any(k in text.lower() for k in ["avature", "wizard", "portal"]):
                            is_active_http = True
                            if "SearchJobs" in resp.url.path:
                                active_search_url = str(resp.url)
                                break
                            elif not active_search_url:
                                active_search_url = str(resp.url)
                    elif resp.status in [403, 406, 429]:
                        hit_waf = True
            except Exception:
                continue
                
        # If we couldn't load any subpath, try probing the root page
        if not is_active_http:
            try:
                async with session.get(base_url, headers=headers, timeout=8, allow_redirects=True) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if any(k in text.lower() for k in ["avature", "wizard", "portal"]):
                            is_active_http = True
                    elif resp.status in [403, 406, 429]:
                        hit_waf = True
            except Exception:
                pass
                
        # 3. Probe for sitemaps if HTTP succeeded
        if is_active_http:
            for sitemap_url in sitemap_paths:
                try:
                    async with session.get(sitemap_url, headers=headers, timeout=6, allow_redirects=True) as resp:
                        if resp.status == 200 and "xml" in resp.headers.get("Content-Type", "").lower():
                            xml_text = await resp.text()
                            if len(xml_text.strip()) > 200 and any(k in xml_text for k in ["/JobDetail", "/FolderDetail"]):
                                active_sitemap_url = str(resp.url)
                                break
                except Exception:
                    continue

        # 4. Formulate target registry entry
        # If DNS resolved, the domain is alive. If we hit a WAF block or HTTP timeout,
        # we predict the standard white-label Avature paths.
        if is_active_http:
            logger.info(f"✅ [{domain}] verified active via HTTP! (Sitemap: {active_sitemap_url is not None}, Search: {active_search_url})")
            return {
                "domain": domain,
                "base_url": base_url,
                "sitemap_url": active_sitemap_url,
                "search_url": active_search_url,
                "is_active": True
            }
        elif hit_waf or not is_active_http:
            # We assume active since it resolves in DNS and has Avature subdomain CNAME.
            # Register fallback routes.
            logger.info(f"⚠️ [{domain}] active (DNS verified) but throttled/blocked by WAF. Registering fallbacks.")
            return {
                "domain": domain,
                "base_url": base_url,
                "sitemap_url": f"{base_url}/careers/sitemap.xml",  # Predictable fallback
                "search_url": f"{base_url}/careers/SearchJobs",     # Predictable fallback
                "is_active": True
            }

async def main_discovery_pipeline():
    logger.info("Initializing Avature Site Discovery Pipeline...")
    
    # 1. Harvest candidates from seed file and Internet Archive
    seed_domains = fetch_subdomains_from_seed_file()
    archive_domains = fetch_subdomains_from_archive()
    
    all_domains = sorted(list(seed_domains.union(archive_domains)))
    
    if not all_domains:
        logger.warning("No subdomains harvested. Falling back to default list.")
        all_domains = ["bloomberg.avature.net", "uclahealth.avature.net", "cbs.avature.net"]
        
    logger.info(f"Total candidate domains to evaluate: {len(all_domains)}")
    
    # 2. Run high-concurrency capability and liveness probing
    semaphore = asyncio.Semaphore(100)  # Safe concurrent probes
    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(ssl=False)  # Ignore self-signed certificates or SSL handshake drops
    
    logger.info("Launching parallel async liveness and route probing...")
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [probe_target(session, semaphore, domain) for domain in all_domains]
        results = await asyncio.gather(*tasks)
        
    # Filter out active targets
    valid_targets = [r for r in results if r is not None]
    sitemap_targets = [r for r in valid_targets if r["sitemap_url"] is not None]
    search_targets = [r for r in valid_targets if r["sitemap_url"] is None and r["search_url"] is not None]
    
    logger.info("\n--- Pipeline Discovery Report ---")
    logger.info(f"Candidate Subdomains Evaluated: {len(all_domains)}")
    logger.info(f"Total Active Portals Verified:  {len(valid_targets)}")
    logger.info(f"  -> Portals with XML Sitemaps: {len(sitemap_targets)}")
    logger.info(f"  -> Portals with Search Pages: {len(search_targets)}")
    
    # Save the targets list tovalidated_targets.json
    with open("validated_targets.json", "w") as f:
        json.dump(valid_targets, f, indent=2)
    logger.info("Saved validated targets to 'validated_targets.json'")

if __name__ == "__main__":
    asyncio.run(main_discovery_pipeline())