from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
		consecutive_failures = 0
		max_consecutive_failures = 10  # If frontier returns None 10 times in a row, it's likely empty
        while True:
            tbd_url = self.frontier.get_tbd_url()
                   if not tbd_url:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                # Brief sleep to avoid busy-waiting when waiting for politeness
                time.sleep(self.config.time_delay / 2)
                continue
            
            consecutive_failures = 0
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
			
            # Note: per-domain politeness is now handled in Frontier.get_tbd_url()
      		# time.sleep(self.config.time_delay)
     
    
   
  
 

           
          
         
            
