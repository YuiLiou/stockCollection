# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pandas as pd
import scrapy
from htmltable_df.extractor import Extractor

class StockDaySpider(scrapy.Spider):
    def __init__(self, beginDate=None, endDate=None, *args, **kwargs):
        super(StockDaySpider, self).__init__(beginDate=beginDate, endDate=endDate, *args, **kwargs)

    def start_requests(self):
        if not self.beginDate and not self.endDate:
            date = (datetime.today() - timedelta(days=2)).strftime("%Y/%m/%d")
            self.beginDate = date
            self.endDate = date

        url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'

        for date in pd.date_range(self.beginDate, self.endDate, freq='W-FRI')[::-1]:
            scaDate = '{}{:02d}{:02d}'.format(date.year, date.month, date.day)
            date = '{}/{:02d}/{:02d}'.format(date.year, date.month, date.day)
            for code in ['2330']:
                payload = {
                    'scaDates': scaDate,
                    'scaDate': scaDate,
                    'SqlMethod': 'StockNo',
                    'StockNo': code,
                    'radioStockNo': code,
                    'StockName': '',
                    'REQ_OPR': 'SELECT',
                    'clkStockNo': code,
                    'clkStockName': ''
                }
                yield scrapy.FormRequest(url, formdata=payload, meta={'code': code, 'date': date},
                                         dont_filter=True)

    def parse(self, response):
        m = response.meta
        data = Extractor(response.dom, 'table.mt:eq(1)').df(1)
        del data['持股/單位數分級']
        data.loc[15, '序'] = 17
        data.columns = ['持股分級', '人數', '股數', '佔集保庫存數比例%']

        data.insert(0, 'code', m['code'])
        data.insert(0, 'date', m['date'])

        for item in data.to_dict('row'):
            yield item

    


