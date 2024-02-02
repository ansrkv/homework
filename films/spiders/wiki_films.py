import scrapy
import pandas as pd
import numpy as np


class WikiFilmsSpider(scrapy.Spider):
    name = "wiki_films"
    result = pd.DataFrame()
    allowed_domains = ["ru.wikipedia.org"]
    start_url = "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"
    custom_settings = {'FEED_URI': "films.csv",
                       'FEED_FORMAT': 'csv'}
    def start_requests(self):
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response):
        print("procesing:" + response.url)
        film_link = response.css('#mw-pages > div > div > div > ul > li>a::attr(href)').extract()

        for url in film_link:
            new_url = 'https://ru.wikipedia.org'+url
            yield scrapy.Request(url=new_url, callback=self.parse_detail)

        next_page = response.xpath('//*[@id="mw-pages"]/a[2]/@href').get()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page),  callback=self.parse)

    def parse_detail(self,response):
            row_data = {}
            table = response.xpath('//*[@id="mw-content-text"]/div[1]/table')
            table_new = table.get()
            df = pd.read_html(table_new)[0]

            df = df.transpose()
            header = np.array(df.iloc[0])
            df.columns = header
            new_df = df.drop(index=df.index[0])
            print(new_df.columns)
            row_data['title'] = df.index[0]
            if 'Жанр' in header.tolist():
                row_data['genre'] = new_df['Жанр'][0]
            elif 'Жанры' in header.tolist():
                row_data['genre'] = new_df['Жанры'][0]
            if 'Режиссёр' in header.tolist():
                row_data['director'] = new_df['Режиссёр'][0]
            elif 'Режиссёры' in header.tolist():
                row_data['director'] = new_df['Режиссёры'][0]
            row_data['year'] = new_df['Год'][0]

            yield row_data









