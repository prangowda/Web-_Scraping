# Install required packages
!pip install requests beautifulsoup4 pandas

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import re
import random
import os

class SimpleKudlaScraper:
    def __init__(self):
        self.base_url = "https://www.timesofkudla.com/ARDC.in/category/%E0%B2%9F%E0%B3%88%E0%B2%AE%E0%B3%8D%E0%B2%B8%E0%B3%8D-%E0%B2%86%E0%B2%AB%E0%B3%8D-%E0%B2%95%E0%B3%81%E0%B2%A1%E0%B3%8D%E0%B2%B2-%E0%B2%A8%E0%B3%8D%E0%B2%AF%E0%B3%82%E0%B2%B8%E0%B3%8D-times-of-kudla-n/"
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime.now()
        self.data = []
        self.output_file = "times_of_kudla_data.csv"
        self.debug_file = "debug_html.html"
        
        # Create a session with multiple user agents to rotate
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        ]
        self.session = requests.Session()
        
    def get_random_user_agent(self):
        """Return a random user agent from the list"""
        return random.choice(self.user_agents)
    
    def fetch_page(self, url, max_retries=3):
        """Fetch and parse a webpage with retry logic"""
        for attempt in range(max_retries):
            try:
                # Use a different user agent for each attempt
                headers = {"User-Agent": self.get_random_user_agent()}
                
                # Add a small random delay to mimic human behavior
                time.sleep(random.uniform(1, 3))
                
                print(f"Fetching URL (attempt {attempt+1}): {url}")
                response = self.session.get(url, headers=headers, timeout=30)
                
                # Log status code
                print(f"Status code: {response.status_code}")
                
                # Handle common HTTP errors
                if response.status_code == 200:
                    # Save the HTML for debugging if this is the first page
                    if url == self.base_url:
                        with open(self.debug_file, "w", encoding="utf-8") as f:
                            f.write(response.text)
                        print(f"Saved debug HTML to {self.debug_file}")
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return soup
                elif response.status_code == 403 or response.status_code == 409:
                    print("Access forbidden. Website may have anti-scraping measures.")
                elif response.status_code == 404:
                    print("Page not found.")
                    return None
                elif response.status_code == 500:
                    print("Server error.")
                else:
                    print(f"Unexpected status code: {response.status_code}")
                
                # Wait longer before retrying
                wait_time = (attempt + 1) * 5
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                time.sleep((attempt + 1) * 5)
        
        print(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def extract_date(self, date_text):
        """Extract and parse date from article"""
        if not date_text:
            return None
            
        try:
            # Clean up the date text
            date_text = date_text.strip()
            
            # Print the date text for debugging
            print(f"Attempting to parse date: '{date_text}'")
            
            # Try different date formats
            # Format: DD/MM/YYYY or DD-MM-YYYY
            date_match = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', date_text)
            if date_match:
                day, month, year = map(int, date_match.groups())
                return datetime(year, month, day)
                
            # Format: Month DD, YYYY (e.g., January 15, 2024)
            months = {
                'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
                'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
            }
            
            date_match = re.search(r'(\w+)\s+(\d{1,2}),\s+(\d{4})', date_text, re.IGNORECASE)
            if date_match:
                month_name, day, year = date_match.groups()
                month = months.get(month_name.lower(), 1)
                return datetime(int(year), month, int(day))
                
            return None
        except Exception as e:
            print(f"Error parsing date '{date_text}': {e}")
            return None
    
    def is_within_date_range(self, article_date):
        """Check if article date is within our target range"""
        if article_date:
            return self.start_date <= article_date <= self.end_date
        return False
    
    def print_page_structure(self, soup):
        """Print the basic structure of the page for debugging"""
        print("\n==== Page Structure ====")
        
        # Print title
        print(f"Page title: {soup.title.string if soup.title else 'No title'}")
        
        # Print all heading elements
        headings = soup.find_all(['h1', 'h2', 'h3'])
        print(f"Found {len(headings)} headings:")
        for i, heading in enumerate(headings[:5]):  # Show first 5 only
            print(f"  {i+1}. {heading.get_text().strip()}")
        
        # Print potential article containers
        for container_class in ['post', 'article', 'entry', 'news-item']:
            containers = soup.select(f'.{container_class}')
            if containers:
                print(f"Found {len(containers)} elements with class '{container_class}'")
        
        # Print potential pagination elements
        pagination = soup.select('.pagination, .nav-links, .page-numbers')
        if pagination:
            print(f"Found pagination elements: {len(pagination)}")
        
        print("========================\n")
    
    def extract_articles(self, soup):
        """Extract articles from the page soup"""
        articles_found = []
        
        # Print page structure for debugging
        self.print_page_structure(soup)
        
        # Try different selectors for article containers
        container_selectors = [
            '.post', 'article', '.entry', '.news-item', '.card',
            'div.content > div', 'div.main > div', '.main-content > div',
            '.post-box', '.article-box'
        ]
        
        for selector in container_selectors:
            containers = soup.select(selector)
            print(f"Found {len(containers)} potential article containers with selector '{selector}'")
            
            if containers:
                for container in containers:
                    article_data = self.extract_article_data(container)
                    if article_data:
                        articles_found.append(article_data)
        
        # If no articles found with specific selectors, try a more general approach
        if not articles_found:
            print("No articles found with specific selectors, trying general approach...")
            
            # Find all divs that might contain articles
            for div in soup.find_all('div', class_=True):
                # If the div has a title and content, it might be an article
                title_element = div.find(['h1', 'h2', 'h3', 'h4'])
                
                if title_element and len(div.get_text().strip()) > 100:  # Must have some substantial content
                    article_data = self.extract_article_data(div)
                    if article_data:
                        articles_found.append(article_data)
        
        # Remove duplicates (by URL if available, otherwise by title)
        unique_articles = []
        seen_urls = set()
        seen_titles = set()
        
        for article in articles_found:
            if article['url'] and article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
            elif article['title'] not in seen_titles:
                seen_titles.add(article['title'])
                unique_articles.append(article)
        
        print(f"Found {len(unique_articles)} unique articles")
        return unique_articles
    
    def extract_article_data(self, article_element):
        """Extract data from an article element"""
        try:
            # Try different selectors for title
            title = None
            for selector in ['h2', '.entry-title', '.post-title', 'h3 a', 'h2 a', 'h3', 'h4 a', 'h4', 'a.title']:
                title_element = article_element.select_one(selector)
                if title_element:
                    title = title_element.get_text().strip()
                    break
            
            # If no title found, try to find any heading
            if not title:
                title_elements = article_element.select('h1, h2, h3, h4, h5')
                if title_elements:
                    title = title_elements[0].get_text().strip()
            
            # If still no title, this might not be an article
            if not title or len(title) < 5:  # Title should be at least 5 chars
                return None
            
            # Try different selectors for URL
            url = None
            for selector in ['a', 'h2 a', '.entry-title a', '.post-title a', 'h3 a', 'h4 a']:
                url_element = article_element.select_one(selector)
                if url_element and url_element.has_attr('href'):
                    url = url_element['href']
                    # Make sure URL is absolute
                    if url.startswith('/'):
                        # Get domain from base URL
                        domain_match = re.match(r'^(https?://[^/]+)', self.base_url)
                        if domain_match:
                            domain = domain_match.group(1)
                            url = domain + url
                    break
            
            # Try different selectors for date
            date_text = None
            article_date = None
            for selector in ['.date', '.entry-date', '.post-date', '.time', 'time', '.meta', '.posted-on', '.post-meta']:
                date_element = article_element.select_one(selector)
                if date_element:
                    date_text = date_element.get_text().strip()
                    article_date = self.extract_date(date_text)
                    if article_date:
                        break
            
            # If no date found with specific selectors, try to extract from any text
            if not article_date:
                text = article_element.get_text()
                # Look for date patterns in the text
                date_patterns = [
                    r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD/MM/YYYY or DD-MM-YYYY
                    r'(\w+)\s+(\d{1,2}),\s+(\d{4})'  # Month DD, YYYY
                ]
                
                for pattern in date_patterns:
                    matches = re.search(pattern, text)
                    if matches:
                        date_text = matches.group(0)
                        article_date = self.extract_date(date_text)
                        if article_date:
                            break
            
            # Try different selectors for excerpt/content
            excerpt = None
            for selector in ['.excerpt', '.entry-summary', '.post-excerpt', '.summary', 'p']:
                excerpt_elements = article_element.select(selector)
                if excerpt_elements:
                    excerpt = ' '.join([elem.get_text().strip() for elem in excerpt_elements[:2]])
                    break
            
            # If still no excerpt, just use first paragraph or div text
            if not excerpt:
                first_p = article_element.find('p')
                if first_p:
                    excerpt = first_p.get_text().strip()
                else:
                    # Get all text in article element
                    excerpt = article_element.get_text().strip()
                    # Truncate if too long
                    if len(excerpt) > 200:
                        excerpt = excerpt[:197] + '...'
            
            # Skip if date is outside our range (if we have a date)
            if article_date and not self.is_within_date_range(article_date):
                print(f"Skipping article outside date range: {title} - {article_date}")
                return None
            
            article_data = {
                'title': title,
                'url': url,
                'date': article_date,
                'date_text': date_text,
                'excerpt': excerpt
            }
            
            print(f"Extracted article: {title}")
            return article_data
            
        except Exception as e:
            print(f"Error extracting article data: {e}")
            return None
    
    def find_next_page_url(self, soup):
        """Find the URL for the next page"""
        try:
            # Print potential next page links for debugging
            print("\nLooking for next page link...")
            
            # Try different selectors for next page
            next_selectors = [
                '.next', '.pagination a', 'a.next', 'a[rel="next"]', 
                '.nav-previous a', '.nav-links a', '.page-numbers.next',
                'a:contains("Next")', 'a:contains("Next Page")', 'a:contains("»")'
            ]
            
            for selector in next_selectors:
                next_elements = soup.select(selector)
                print(f"Found {len(next_elements)} elements with selector '{selector}'")
                
                for element in next_elements:
                    text = element.get_text().strip().lower()
                    href = element.get('href')
                    
                    print(f"  - Text: '{text}', Link: '{href}'")
                    
                    if href and (
                        'next' in text.lower() or 
                        '»' in text or 
                        '>' in text or 
                        'page' in href
                    ):
                        # Make sure URL is absolute
                        if href.startswith('/'):
                            # Get domain from base URL
                            domain_match = re.match(r'^(https?://[^/]+)', self.base_url)
                            if domain_match:
                                domain = domain_match.group(1)
                                href = domain + href
                                
                        print(f"Found next page URL: {href}")
                        return href
            
            # If no next page link found with clear indicators, look for pagination numbers
            page_numbers = soup.select('.page-numbers, .pagination a')
            current_page_found = False
            highest_page = 0
            
            for page_num in page_numbers:
                if 'current' in page_num.get('class', []):
                    current_page_found = True
                    continue
                    
                # If we found the current page already, this could be the next page
                if current_page_found and page_num.has_attr('href'):
                    print(f"Found next page by pagination: {page_num['href']}")
                    return page_num['href']
                
                # Try to extract page number
                try:
                    num = int(page_num.get_text().strip())
                    highest_page = max(highest_page, num)
                except:
                    pass
            
            # Last resort, look for any link with "page/2" or similar pattern
            if highest_page == 0:
                page_links = soup.select('a[href*="page/"]')
                for link in page_links:
                    href = link.get('href', '')
                    match = re.search(r'page/(\d+)', href)
                    if match:
                        print(f"Found page link: {href}")
                        return href
                        
            print("No next page URL found")
            return None
            
        except Exception as e:
            print(f"Error finding next page: {e}")
            return None
    
    def scrape_all_pages(self):
        """Scrape all pages with articles in the date range"""
        current_url = self.base_url
        page_num = 1
        max_pages = 50  # Safety limit
        
        while current_url and page_num <= max_pages:
            print(f"\n==== Scraping page {page_num}: {current_url} ====")
            
            # Fetch the page
            soup = self.fetch_page(current_url)
            if not soup:
                print(f"Failed to fetch page {page_num}")
                break
            
            # Extract articles
            articles = self.extract_articles(soup)
            
            # Add valid articles to the dataset
            new_articles = 0
            for article in articles:
                if article:
                    self.data.append(article)
                    new_articles += 1
            
            print(f"Added {new_articles} new articles from page {page_num}")
            
            # Get next page URL
            next_url = self.find_next_page_url(soup)
            
            # If no new URL or it's the same as current, stop
            if not next_url or next_url == current_url:
                print("No more pages to scrape")
                break
                
            current_url = next_url
            page_num += 1
            
            # Be nice to the server
            delay = random.uniform(3, 7)
            print(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    def save_results(self):
        """Save the scraped data to files"""
        if not self.data:
            print("No data to save")
            return
            
        # Save to CSV
        df = pd.DataFrame(self.data)
        
        # Filter out None values and convert date objects to string format
        df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else '')
        
        # Save to CSV
        df.to_csv(self.output_file, index=False, encoding='utf-8')
        print(f"Data saved to {self.output_file}")
        
        # Save raw data to JSON as backup
        try:
            import json
            json_file = self.output_file.replace('.csv', '.json')
            
            # Convert dates to strings for JSON serialization
            json_data = []
            for item in self.data:
                json_item = item.copy()
                if json_item['date']:
                    json_item['date'] = json_item['date'].strftime('%Y-%m-%d')
                json_data.append(json_item)
                
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            print(f"Data also saved to {json_file}")
        except Exception as e:
            print(f"Error saving JSON backup: {e}")
    
    def run(self):
        """Run the full scraping process"""
        try:
            print(f"Starting scrape from {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
            self.scrape_all_pages()
            print(f"Scraping complete. Found {len(self.data)} articles.")
            self.save_results()
        except Exception as e:
            print(f"Error during scraping: {e}")
            
            # Try to save whatever data we collected
            if self.data:
                print("Attempting to save partial data...")
                self.save_results()

# Run the scraper
print("Starting Times of Kudla scraper...")
scraper = SimpleKudlaScraper()
scraper.run()

# Display the results
if os.path.exists('times_of_kudla_data.csv'):
    df = pd.read_csv('times_of_kudla_data.csv')
    print(f"\nSuccessfully scraped {len(df)} articles")
    print("\nFirst 5 articles:")
    display(df.head())
else:
    print("\nNo data was saved to CSV")

# Display saved HTML for debugging
if os.path.exists('debug_html.html'):
    with open('debug_html.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    print(f"\nSaved HTML file size: {len(html_content)} bytes")
    print(f"First 500 characters of HTML:\n{html_content[:500]}...")
