import requests
from bs4 import BeautifulSoup
import time
import random
import concurrent.futures
import logging
from typing import Optional, Tuple, List
import json
from urllib.parse import urlparse
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IndianKanoonScraper:
    def __init__(self, max_workers: int = 2, delay_range: Tuple[float, float] = (3.0, 7.0)):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        })
        self.max_workers = max_workers
        self.delay_range = delay_range
        self.processed_ids = set()
        self.failed_ids = set()
        self.proxies = self.load_proxies()
        self.proxy_index = 0
        
        # Load already processed IDs to resume from where we left off
        try:
            with open("processed_ids.txt", "r") as f:
                self.processed_ids = set(int(line.strip()) for line in f if line.strip())
        except FileNotFoundError:
            pass
            
        # Load failed IDs
        try:
            with open("failed_ids.txt", "r") as f:
                self.failed_ids = set(int(line.strip()) for line in f if line.strip())
        except FileNotFoundError:
            pass

    def load_proxies(self) -> List[str]:
        """Load proxies from file if available"""
        proxies = []
        try:
            with open("proxies.txt", "r") as f:
                proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(proxies)} proxies")
        except FileNotFoundError:
            logger.info("No proxies file found, using direct connection")
        return proxies

    def get_next_proxy(self) -> Optional[dict]:
        """Get next proxy from the list in round-robin fashion"""
        if not self.proxies:
            return None
            
        proxy = self.proxies[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        
        # Parse proxy URL to determine scheme
        parsed = urlparse(proxy)
        if parsed.scheme:
            return {
                "http": proxy,
                "https": proxy
            }
        else:
            # Assume HTTP if no scheme provided
            return {
                "http": f"http://{proxy}",
                "https": f"https://{proxy}"
            }

    def _random_delay(self):
        """Add random delay between requests to avoid being blocked"""
        time.sleep(random.uniform(*self.delay_range))

    def fetch_case(self, doc_id: int) -> Optional[Tuple[int, str, str]]:
        # Skip already processed IDs
        if doc_id in self.processed_ids:
            logger.info(f"⏭️  Skipping already processed Case {doc_id}")
            return None
            
        # Skip IDs that failed multiple times
        if doc_id in self.failed_ids:
            logger.info(f"⏭️  Skipping previously failed Case {doc_id}")
            return None
            
        url = f"https://indiankanoon.org/doc/{doc_id}/"
        
        # Use proxy if available
        proxies = self.get_next_proxy()
        
        try:
            response = self.session.get(url, timeout=15, proxies=proxies)
            
            # Check for rate limiting
            if response.status_code == 429:
                logger.warning(f"⏸️  Rate limited on Case {doc_id}, adding delay")
                time.sleep(random.uniform(10, 20))  # Longer delay for rate limiting
                # Try one more time with longer delay
                response = self.session.get(url, timeout=15, proxies=proxies)
                
            response.raise_for_status()
            
        except requests.RequestException as e:
            logger.warning(f"⚠️ Failed to fetch {url}: {e}")
            # Add to failed IDs
            self.failed_ids.add(doc_id)
            with open("failed_ids.txt", "a") as f:
                f.write(f"{doc_id}\n")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract only the judgments section
        judgment_div = soup.find("div", class_="judgments")
        if not judgment_div:
            logger.warning(f"⚠️ No judgment text found for Case {doc_id}")
            # Mark as processed to avoid retrying
            self.processed_ids.add(doc_id)
            with open("processed_ids.txt", "a") as f:
                f.write(f"{doc_id}\n")
            return None

        # Extract text more efficiently
        judgment_text = judgment_div.get_text(separator="\n", strip=True)
        
        # Extract case title
        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else f"Case {doc_id}"

        # Add to processed IDs
        self.processed_ids.add(doc_id)
        with open("processed_ids.txt", "a") as f:
            f.write(f"{doc_id}\n")

        return doc_id, title, judgment_text

    def process_single_case(self, doc_id: int, filename: str):
        """Process and save a single case"""
        result = self.fetch_case(doc_id)
        if result:
            doc_id, title, content = result
            with open(filename, "a", encoding="utf-8") as f:
                f.write(f"\n\n{'='*100}\n")
                f.write(f"Case ID: {doc_id}\nTitle: {title}\n")
                f.write(f"{'='*100}\n\n")
                f.write(content)
                f.write("\n\n")
            logger.info(f"✅ Added Case {doc_id}")
        else:
            logger.info(f"⚠️ Skipped Case {doc_id}")
        
        # Add delay between requests
        self._random_delay()

    def save_cases(self, start: int, end: int, filename: str):
        """Process cases with controlled parallelism"""
        # Create file if it doesn't exist
        with open(filename, "a", encoding="utf-8") as f:
            pass
            
        # Filter out already processed IDs
        ids_to_process = [doc_id for doc_id in range(start, end + 1) 
                         if doc_id not in self.processed_ids and doc_id not in self.failed_ids]
        
        logger.info(f"📊 Processing {len(ids_to_process)} new cases out of {end - start + 1} total")
        
        # Use slower sequential processing to avoid rate limiting
        for doc_id in ids_to_process:
            self.process_single_case(doc_id, filename)
            
            # Occasionally take a longer break
            if random.random() < 0.1:  # 10% chance after each request
                nap_time = random.uniform(5, 8)
                logger.info(f"😴 Taking a longer nap for {nap_time:.1f} seconds...")
                time.sleep(nap_time)

    def retry_failed_cases(self, filename: str, max_retries: int = 3):
        """Retry cases that previously failed"""
        if not self.failed_ids:
            logger.info("No failed cases to retry")
            return
            
        logger.info(f"🔄 Retrying {len(self.failed_ids)} failed cases")
        
        # Make a copy as the set will change during iteration
        failed_ids = list(self.failed_ids)
        
        for retry in range(max_retries):
            logger.info(f"Retry attempt {retry + 1}/{max_retries}")
            success_count = 0
            
            for doc_id in failed_ids[:]:  # Iterate over copy
                result = self.fetch_case(doc_id)
                if result:
                    doc_id, title, content = result
                    with open(filename, "a", encoding="utf-8") as f:
                        f.write(f"\n\n{'='*100}\n")
                        f.write(f"Case ID: {doc_id}\nTitle: {title}\n")
                        f.write(f"{'='*100}\n\n")
                        f.write(content)
                        f.write("\n\n")
                    logger.info(f"✅ Added previously failed Case {doc_id}")
                    success_count += 1
                    # Remove from failed list
                    failed_ids.remove(doc_id)
                
                # Add delay between requests
                self._random_delay()
                
                # Occasionally take a longer break
                if random.random() < 0.1:
                    nap_time = random.uniform(10, 30)
                    logger.info(f"😴 Taking a longer nap for {nap_time:.1f} seconds...")
                    time.sleep(nap_time)
            
            logger.info(f"Retry {retry + 1}: Successfully processed {success_count} cases")
            if not failed_ids:
                break
                
        # Update the failed IDs set
        self.failed_ids = set(failed_ids)
        with open("failed_ids.txt", "w") as f:
            for doc_id in self.failed_ids:
                f.write(f"{doc_id}\n")

if __name__ == "__main__":
    # Initialize scraper with conservative settings
    scraper = IndianKanoonScraper(
        max_workers=1,  # Single worker to avoid rate limiting
        delay_range=(0.0, 3.0)  # Longer delays between requests
    )
    
    # For large-scale scraping, run in small batches
    batch_size = 50  # Smaller batches to avoid detection
    total_cases = 10000  # Adjust based on how many cases you want
    
    # Process cases in batches with breaks between batches
    for batch_start in range(1, total_cases + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, total_cases)
        logger.info(f"🚀 Processing batch {batch_start} to {batch_end}")
        
        try:
            scraper.save_cases(batch_start, batch_end, "all_cases.txt")
            
            # Take a break between batches
            break_time = random.uniform(10, 20)
            logger.info(f"☕ Taking a break for {break_time:.1f} seconds between batches...")
            time.sleep(break_time)
            
        except Exception as e:
            logger.error(f"❌ Error processing batch {batch_start}-{batch_end}: {e}")
            # Longer wait if we hit an error
            time.sleep(120)
    
    # Retry failed cases at the end
    logger.info("🔄 Starting retry of failed cases")
    scraper.retry_failed_cases("all_cases.txt")