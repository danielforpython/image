# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline

class MysqlPipeline(object):
    def process_item(self, host,user,database,password,port,item, spider):
		self.host=host
		self.pory=port
		self.password=password
		self.database=database
		self.user=user
		
	@Classmethod
	def from_crawler(cls,crawler):
		return cls(host=crawler.settings.get('MYSQL_HOST'),
					port=crawler.settings.get('MYSQL_PORT'),
					password=crawler.settings.get('MYSQL_PASSWORD'),
					database=crawler.settings.get('MYSQL_DATABASE'),
					user=crawler.settings.get('MYSQL_USER'))
					
					
	def openspider(self,spider):
		self.db=pymysql.connect(self.host,self.user,self.password,self.database,charset='utf8',port=self.port)
		self.cursor=self.db.cursor()
		
	def closespider(self,spider):
		self.db.close()
		
		
	def process_item(self,item,spider):
		data=dict(item)
		keys=','.join(data.keys())
		values=','.join(['%s']*len(data))
		#动态构造sql语句
		sql='insert into %s(%s) values(%s)'%(item.table,keys,values)
		try:
			self.cursor.execute(sql,tuple(data.values)))
			self.db.commit()
			
		except:
			self.db.rollback()
			print('save failed')
			
		return item

#继承内置的ImagesPipeline，重写内部的方法
class ImagePipeline(ImagesPipeline):
	def file_path(self,request,response=None,info=None):
		url=request.url
		file_name=url.split('/')[-1]
		return file_name

	def item_completed(self,results,item,info):
		image_paths=[x['path']for ok,x in results if ok]
		if not image_paths:
			raise DropItem('Image Download Failed')
		return item


	def get_media_requests(self,item,info):
		yield Request(item['url'])

			
