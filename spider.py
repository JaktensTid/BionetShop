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

    # NOT SCRAPED - category 3, 4
    async def fetch(self, product, session):
        async with session.get(product['webpage']) as response:
            text = await response.text()

            if text:
                document = html.fromstring(text)
                ID, CAS, _ = document.xpath(".//div[@class='delivery-info']//span[@class='value']/text()")
                product['id'], product['cas'] = ID, CAS
                product['name'] = document.xpath(".//span[@class='h1']/text()")
                product['category1'] = document.xpath(".//li[@class='category9']/a/text()")
                product['category2'] = document.xpath(".//li[@class='category11']/a/text()")
                product['availability'] = document.xpath(".//div[@class='stock-info']//span[@class='value']/text()")
                product['delivery'] = document.xpath(".//div[@class='stock-info']//p[position()=2]/text()")
                prices = document.xpath(".//div[@class='product-shop']/table//tr/td[position()=3]//text()")
                packs = document.xpath(".//div[@class='product-shop']/table//tr/td[position()=1]/text()")[0:prices+1]
                product['packs'] = {k:v for k,v in zip(prices, packs)}
                table = document.xpath(".//div[@class='product-collateral toggle-content tabs']/table")
                formula = table.xpath(".//tr/td[contains(.,'Formula')]")
                product['formula'] = formula.replace('Formula', '')
                purity = table.xpath(".//tr/td[contains(.,'Purity')]")
                product['purity'] = purity.replace('Purity', '')
                supplier = table.xpath(".//tr/td[contains(.,'Supplier Name')]").replace('Supplier Name', '')
                product['supplier'] = supplier
                supplierid = table.xpath(".//tr/td[contains(.,'Supplier ID')]").replace('Supplier ID', '')
                product['supplierid'] = supplierid
                acdno = table.xpath(".//tr/td[contains(.,'ACD no')]").replace('ACD no', '')
                product['acd no'] = acdno
                mw = table.xpath(".//tr/td[contains(.,'MW')]").replace('MW', '')
                product['mw'] = mw
                synonym = table.xpath(".//tr/td[contains(.,'Synonym')]").replace('Synonym', '')
                product['synonym'] = synonym
                product['img'] = document.xpath(".//img[@id='image-main']/@src")
                product['related'] = document.xpath(".//ul[@id='upsell-product-table']//span[position()=2]/text()")


            return product

    async def bound_fetch(self, sem, product, session):
        async with sem:
            await self.fetch(product, session)

    async def run(self, products):
        tasks = []
        sem = asyncio.Semaphore(20)

        async with ClientSession() as session:
            for product in products:
                task = asyncio.ensure_future(self.bound_fetch(sem, product, session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            await responses


if __name__ == '__main__':
    spider = Spider()
    products = [{'webpage' : 'https://www.keyorganics.net/biochemicals/research-area/oncology/-34.html'},
                {'webpage' : 'https://www.keyorganics.net/fragments-screening/screening/-15848.html'}]
    loop = asyncio.get_event_loop()

    future = asyncio.ensure_future(spider.run(products))
    loop.run_until_complete(future)

