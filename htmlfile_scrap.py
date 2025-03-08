import requests
from bs4 import BeautifulSoup
import csv
import datetime
import os
import re
from urllib.parse import urlparse

def scrape_website(url):
    """
    Scrapes content from a website and returns relevant data
    """
    try:
        # Send request to the URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get current date and time
        now = datetime.datetime.now()
        date = now.strftime("%Y-%m-%d")
        time = now.strftime("%H:%M:%S")
        
        # Extract page title
        title = soup.title.text.strip() if soup.title else "No title found"
        
        # Extract meta description
        meta_description = ""
        meta_tag = soup.find("meta", attrs={"name": "description"})
        if meta_tag and meta_tag.get("content"):
            meta_description = meta_tag.get("content").strip()
        
        # Extract main content (this is a simple approach, might need customization)
        # First try to find main content tags
        main_content = ""
        content_tags = soup.find_all(['article', 'main', 'div', 'section'], class_=re.compile('(content|article|main|post)'))
        
        if content_tags:
            # Use the largest content block
            main_content = max(content_tags, key=lambda x: len(x.get_text())).get_text(separator=" ", strip=True)
        else:
            # Fallback: get body text
            body = soup.find('body')
            if body:
                main_content = body.get_text(separator=" ", strip=True)
        
        # Clean content (remove extra whitespaces)
        main_content = re.sub(r'\s+', ' ', main_content).strip()
        
        # Extract domain
        domain = urlparse(url).netloc
        
        # Count words in content
        word_count = len(main_content.split())
        
        # Find all links on the page
        links = [a.get('href') for a in soup.find_all('a', href=True)]
        internal_links = [link for link in links if link.startswith('/') or domain in link]
        external_links = [link for link in links if not link.startswith('/') and domain not in link]
        
        return {
            'date': date,
            'time': time,
            'url': url,
            'domain': domain,
            'title': title,
            'meta_description': meta_description,
            'content': main_content,
            'word_count': word_count,
            'internal_links_count': len(internal_links),
            'external_links_count': len(external_links)
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {str(e)}")
        return {
            'date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'time': datetime.datetime.now().strftime("%H:%M:%S"),
            'url': url,
            'domain': urlparse(url).netloc if url else "",
            'title': "Error",
            'meta_description': "",
            'content': f"Failed to scrape: {str(e)}",
            'word_count': 0,
            'internal_links_count': 0,
            'external_links_count': 0
        }

def save_to_csv(data_list, filename="scraped_data.csv"):
    """
    Saves scraped data to a CSV file
    """
    # Define field names for CSV
    fieldnames = [
        'date', 'time', 'url', 'domain', 'title', 
        'meta_description', 'content', 'word_count',
        'internal_links_count', 'external_links_count'
    ]
    
    # Determine if file exists to handle headers
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header only if file doesn't exist
        if not file_exists:
            writer.writeheader()
        
        # Write data rows
        for data in data_list:
            writer.writerow(data)
    
    print(f"Data saved to {filename}")

def main():
    # List of URLs to scrape
    urls = [
        # Add URLs here
        "https://example.com",
        "https://en.wikipedia.org/wiki/Web_scraping"
    ]
    
    # Create a list to store all scraped data
    all_data = []
    
    for url in urls:
        print(f"Scraping {url}...")
        data = scrape_website(url)
        all_data.append(data)
    
    # Save all data to CSV
    save_to_csv(all_data)

if __name__ == "__main__":
    main()
