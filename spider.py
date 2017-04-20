import requests
import asyncio
from aiohttp import ClientSession
from lxml import html

class Spider:
    bionet_shop = 'https://www.keyorganics.net/bionet-shop'

    def __init__(self):
        document = self.navigate(self.bionet_shop)
        if document:
            self.sections = [h for h in document.xpath(".//div[@class='col-left sidebar']//a/@href")
                             if h != '#']

    def navigate(self, url):
        response = requests.get(url)
        if response:
            return html.fromstring(response.text)
        return None

    def products_from_page(self, document):
        return [{'webpage': href} for href in document.xpath(".//h2[@class='product-name']/a/@href")]

    def get_products_urls(self):
        urls = []

        for section in self.sections:
            document = self.navigate(section + '?limit=25')
            urls += self.products_from_page(document)
            pages_num = 1
            if document:
                total_xpath = "//p[@class='amount amount--has-pages']//text()"
                total = document.xpath(total_xpath)
                if not total:
                    continue
                total = total[-1].split('of')[-1].strip()
                balance = int(total) % 25
                pages_num = int(int(total) / 25)
                if balance:
                    pages_num += 1
            for i in range(2, pages_num + 1):
                document = self.navigate(section + '?limit=25&p=' + str(i))
                urls += self.products_from_page(document)

        return urls

    async def fetch(self, product, session):
        async with session.get(product) as response:
            text = await response.read()
            if text:
                document = html.fromstring(text)
                ID, CAS, _ = document.xpath(".//div[@class='info']/span[@class='value']/text()")
                product['id'], product['cas'] = ID, CAS
                product['name'] = document.xpath(".//span[@class='h1']/text()")



            return product

    async def bound_fetch(self, sem, product, session):
        async with sem:
            await self.fetch(product, session)

    async def run(self, products):
        tasks = []
        sem = asyncio.Semaphore(1000)

        async with ClientSession() as session:
            for url in urls:
                task = asyncio.ensure_future(self.bound_fetch(sem, product, session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            await responses


if __name__ == '__main__':
    spider = Spider()
    urls = spider.get_products_urls()
    pass

