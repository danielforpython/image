# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item,Field#可简写


class Images360Item(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    collection=table='image'#mongodb,mysql存储名称
    id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    thumb = scrapy.Field()
    
