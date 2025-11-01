import os
import shelve
import time
from collections import defaultdict

from threading import RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.lock = RLock()
        self.domain_last_request = defaultdict(lambda: 0)
    
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def _get_domain(self, url):
        """Extract domain from URL for politeness tracking."""
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def get_tbd_url(self):
        with self.lock:
            if not self.to_be_downloaded:
                return None
            
            current_time = time.time()
            politeness_delay = self.config.time_delay
            
            # Try to find a URL from a domain that's ready for a new request
            for i in range(len(self.to_be_downloaded)):
                url = self.to_be_downloaded[i]
                domain = self._get_domain(url)
                last_request_time = self.domain_last_request[domain]
                
                time_since_last = current_time - last_request_time
                
                if time_since_last >= politeness_delay:
                    # Found a URL from a domain that's ready
                    self.to_be_downloaded.pop(i)
                    return url
            
            # No URL is ready yet due to politeness
            return None        
       
      
     

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
			if urlhash not in self.save:
            	self.save[urlhash] = (url, False)
            	self.save.sync()
            	self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
		with self.lock:

        	if urlhash not in self.save:
            	# This should not happen.
            	self.logger.error(
                	f"Completed url {url}, but have not seen it before.")

        	self.save[urlhash] = (url, True)
        	self.save.sync()
