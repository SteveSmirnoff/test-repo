import scrapy
import csv
from urllib.parse import urlparse
import re
from urllib.parse import urlparse


def extract_domain(url):
    domain = (urlparse(url if url.startswith('http') else f'https://{url}').netloc or url.split('/')[0]).removeprefix('www.')
    return f"https://www.{domain}/"


class InternationalRealEstateSpider(scrapy.Spider):
    name = 'international_real_estate'

    # Common country names and codes to search for
    COUNTRIES = {
        'spain', 'portugal', 'france', 'italy', 'greece', 'turkey', 'croatia',
        'usa', 'united states', 'canada', 'mexico', 'brazil', 'argentina',
        'uk', 'united kingdom', 'ireland', 'germany', 'netherlands', 'belgium',
        'australia', 'new zealand', 'thailand', 'philippines', 'dubai', 'uae',
        'switzerland', 'austria', 'poland', 'czech republic', 'hungary',
        'morocco', 'egypt', 'south africa', 'cyprus', 'malta', 'montenegro',
        'bulgaria', 'romania', 'estonia', 'latvia', 'lithuania'
    }

    # International indicators
    INTERNATIONAL_KEYWORDS = {
        'international', 'worldwide', 'global', 'overseas', 'abroad',
        'multiple countries', 'country selector', 'select country',
        'choose location', 'browse by country', 'properties worldwide'
    }

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_DELAY': 0.5,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'DOWNLOAD_TIMEOUT': 30,
        'RETRY_TIMES': 0,
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
        },
        'FEEDS': {
            'international_results.csv': {
                'format': 'csv',
                'overwrite': True,
                'fields': ['url', 'is_international', 'countries_found', 'international_indicators', 'status']
            }
        }
    }

    def __init__(self, text_file=r'C:\Users\steph\Downloads\websites.txt', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text_file = text_file
        self.start_urls = self.load_urls()

    def load_urls(self):
        """Load URLs from CSV file"""
        urls = []
        try:
            with open(self.text_file, 'r', encoding='utf-8') as f:
                rows = f.read().split("\n")
                for row in rows:
                    urls.append(extract_domain(row))
        except Exception as e:
            self.logger.error(f"Error loading CSV: {e}")

        self.logger.info(f"Loaded {len(urls)} URLs from {self.text_file}")
        return urls

    def parse(self, response):
        """Parse the landing page and detect international indicators"""
        url = response.url

        # Get all text content (lowercased for matching)
        text_content = ' '.join(response.xpath('//body//text()').getall()).lower()
        text_content = re.sub(r'\s+', ' ', text_content)

        # Get all links and their text
        links = response.xpath('//a/@href | //a/text()').getall()
        links_text = ' '.join(links).lower()

        # Combine for comprehensive search
        full_content = text_content + ' ' + links_text

        # Find countries mentioned
        countries_found = set()
        for country in self.COUNTRIES:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(country) + r'\b'
            if re.search(pattern, full_content):
                countries_found.add(country)

        # Find international indicators
        indicators_found = set()
        for indicator in self.INTERNATIONAL_KEYWORDS:
            pattern = r'\b' + re.escape(indicator) + r'\b'
            if re.search(pattern, full_content):
                indicators_found.add(indicator)

        # Check for language/country selectors in common HTML patterns
        has_country_selector = bool(
            response.xpath('//select[contains(@class, "country") or contains(@id, "country")]') or
            response.xpath('//div[contains(@class, "country-selector") or contains(@class, "language-selector")]') or
            response.xpath('//a[contains(@href, "/country/") or contains(@href, "/location/")]')
        )

        if has_country_selector:
            indicators_found.add('country_selector_element')

        # Determine if international
        is_international = (
                len(countries_found) >= 3 or  # Mentions 3+ countries
                len(indicators_found) >= 1 or  # Has international keywords
                has_country_selector
        )

        yield {
            'url': url,
            'is_international': is_international,
            'countries_found': ', '.join(sorted(countries_found)) if countries_found else '',
            'international_indicators': ', '.join(sorted(indicators_found)) if indicators_found else '',
            'status': 'success'
        }

    def errback(self, failure):
        """Handle failed requests"""
        request = failure.request
        yield {
            'url': request.url,
            'is_international': False,
            'countries_found': '',
            'international_indicators': '',
            'status': f'error: {failure.type.__name__}'
        }