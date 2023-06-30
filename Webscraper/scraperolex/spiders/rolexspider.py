import scrapy


class RolexspiderSpider(scrapy.Spider):
    name = "rolexspider"
    allowed_domains = ["www.rolex.com"]
    start_urls = ["https://www.rolex.com/en-us/watches"]

    def parse(self, response):
        watch_types = response.css('div.dark-theme.css-1bss45e.e1y25pk71') + response.css('div.light-theme.css-1bss45e.e1y25pk71')
        
        for watch in watch_types:
            specs = yield {
                'name' : watch.css('div div h2::text').get(),
                'phrase': watch.css('div div div::text').get(),
                'url' :   watch.css('div div a').attrib['href']
            }

        #links = watch_types.css('div div a').attrib['href']
        #1. follow the link to each watch_type
        #for link in links:
        #    yield response.follow('www.rolex.com'+link)
        #2. Find the all models link
        #3. Loop through all models for price, description

        