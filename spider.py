import json
import copy
import csv
import requests
import asyncio
from aiohttp import ClientSession
from lxml import html

def normalize_to_json(string):
    return '[' + string.replace('}{','},{') + ']'

class Spider:
    unscraped = []
    save_handler = open('products_result.json', 'a')
    save_inter_handler = open('products_inter.json', 'a')
    counter = 0

    def __init__(self):
        self.sections = open('sections.txt', 'r').read().split('\n')

    def navigate(self, url):
        response = requests.get(url)
        if response:
            return html.fromstring(response.text)
        return None

    def xpath(self, element, xpath, i=0):
        items = element.xpath(xpath)
        if items:
            return items[i]
        return ''

    def get_link_on_pages(self):
        for url in self.sections:
            print('Scraping section ' + url + ' ' + str(self.counter))
            self.counter += 1
            document = self.navigate(url + '?limit=25')
            pages_num = 1
            if document:
                total_xpath = "//p[@class='amount amount--has-pages']//text()"
                total = document.xpath(total_xpath)
                if not total:
                    yield [url]
                    continue
                total = total[-1].split('of')[-1].strip()
                balance = int(total) % 25
                pages_num = int(int(total) / 25)
                if balance:
                    pages_num += 1
                    yield list(url + '?p=%s&limit=%s' % (i, 25) for i in range(1, pages_num + 1))
        self.counter = 0

    async def fetch_product(self, product_raw, session):
        product = copy.deepcopy(product_raw)
        async with session.get(product['webpage']) as response:
            text = await response.text()
            if text:
                document = html.fromstring(text)
                idcas = document.xpath(".//div[@class='delivery-info']//span[@class='value']/text()")
                ID, CAS = idcas[0], idcas[1]
                product['id'], product['cas'] = ID, CAS
                product['name'] = self.xpath(document, ".//span[@class='h1']/text()")
                product['availability'] = self.xpath(document, ".//div[@class='stock-info']//span[@class='value']/text()")
                product['delivery'] = self.xpath(document, ".//div[@class='stock-info']//p[position()=2]/text()")
                product['packs'] = {}
                packs = document.xpath(".//div[@class='product-shop']/table//tr")
                for tr in packs:
                    try:
                        volume, price = tr.xpath("./td/span[@class='value']/text()")
                        product['packs'][volume] = price
                    except ValueError:
                        break
                table = self.xpath(document, ".//div[@class='product-collateral toggle-content tabs']/table")
                def fromt(xp):
                    item = table.xpath(xp)
                    if item: return item[-1] if len(item) == 2 else ''
                    else: ''
                product['formula'] = fromt(".//tr/td[contains(.,'Formula')]//text()")
                product['purity'] = fromt(".//tr/td[contains(.,'Purity')]//text()")
                product['supplier'] = fromt(".//tr/td[contains(.,'Supplier Name')]//text()")
                product['supplierid'] = fromt(".//tr/td[contains(.,'Supplier ID')]//text()")
                product['acd no'] = fromt(".//tr/td[contains(.,'ACD no')]//text()")
                product['mw'] = fromt(".//tr/td[contains(.,'MW')]//text()")
                product['synonym'] = fromt(".//tr/td[contains(.,'Synonym')]//text()")
                product['img'] = self.xpath(document, ".//img[@id='image-main']/@src")
                related_titles = document.xpath(".//ul[@id='upsell-product-table']/li/a/@title")
                related_ids = document.xpath(".//ul[@id='upsell-product-table']//span[position()=2]/text()")
                if not related_ids:
                    related_ids = list('' for s in range(len(related_titles)))
                product['related'] = {k:v for k,v in zip(related_titles, related_ids)}

            s = json.dumps(product)
            self.save_handler.write(s)
            print('Scraped product ' + product['webpage'] + '. Counter ' + str(self.counter))
            self.counter += 1
            return 1

    async def bound_fetch_products(self, sem, product, session):
        try:
            async with sem:
                await self.fetch_product(product, session)
        except:
            print('Exception')

    async def run_products(self, products):
        tasks = []
        sem = asyncio.Semaphore(10)

        async with ClientSession() as session:
            for product in products:
                task = asyncio.ensure_future(self.bound_fetch_products(sem, product, session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            await responses

    def fill_products(self, products):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run_products(products))
        loop.run_until_complete(future)

    async def fetch_search_pages(self, page, session):
        async with session.get(page) as response:
            text = await response.text()
            document = html.fromstring(text)
            categories = [section.replace('.html', '').split('?')[0] for section in page.split('/') if section and
                          'www.keyorganics.net' not in section and
                          'https:' not in section]
            products = [{'root':page,'webpage': href} for href in document.xpath(".//h2[@class='product-name']/a/@href")]
            for product in products:
                for i, category in enumerate(categories):
                    product['category' + str(i + 1)] = category
            print('Fetched search page: ' + page)
            s = json.dumps(products)[1:-1]
            self.save_inter_handler.write(s)
            return

    async def bound_fetch_search_pages(self, sem, page, session):
        try:
            async with sem:
                await self.fetch_search_pages(page, session)
        except:
            print('Exception')
            return page

    async def get_products_from_search_pages(self, urls):
        tasks = []
        sem = asyncio.Semaphore(10)

        async with ClientSession() as session:
            for url in urls:
                task = asyncio.ensure_future(self.bound_fetch_search_pages(sem, url, session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            return await responses

    def get_products_template(self, urls):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.get_products_from_search_pages(urls))
        return loop.run_until_complete(future)


if __name__ == '__main__':
    spider = Spider()
    #urls = []
    # Get pages from sections
    #for pack in spider.get_link_on_pages():
    #    urls += pack
    #print(' - - - Total search pages: ' + str(len(urls)))
    #unscraped = spider.get_products_template(urls)
    #spider.get_products_template(unscraped)
    string = ''
    # Normalize and get products templates from file
    with open('products_inter.json', 'r') as file:
        string = normalize_to_json(file.read())
    products = json.loads(string)
    del string
    spider.fill_products(products)






