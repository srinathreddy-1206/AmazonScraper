from amazonproduct import API
import bottlenose
from bs4 import BeautifulSoup
from collections import defaultdict
import os

AWS_ASSOCIATE_TAG = ""
AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""


class AmazonCrawler(object):
    AMAZON_MAX_PAGES = 10
    def __init__(self, ASSOCIATE_TAG = None, ACCESS_KEY = None, SECRET_KEY = None):
        self.ASSOCIATE_TAG = ASSOCIATE_TAG
        self.ACCESS_KEY = ACCESS_KEY
        self.SECRET_KEY = SECRET_KEY
        self.amazon = None
        self._prepare()
        self.current_page = 1
        self.objects = []

    def _prepare(self):
         self.amazon = bottlenose.Amazon(self.ACCESS_KEY, self.SECRET_KEY, self.ASSOCIATE_TAG, MaxQPS = 0.9)


    def item_search(self, **kwargs):
        response = self.amazon.ItemSearch(ResponseGroup="ItemAttributes,SalesRank",**kwargs)
        pages = self._no_of_pages(response)
        max_pages = pages if pages < self.AMAZON_MAX_PAGES else self.AMAZON_MAX_PAGES
        for i in range(max_pages):
            kwargs['ItemPage']=i+1
            yield  self.amazon.ItemSearch(ResponseGroup="ItemAttributes,SalesRank,Images",**kwargs)

    def _no_of_pages(self, response):
        soup = BeautifulSoup(response, 'xml')
        pages = soup.find('TotalPages')
        return int(pages.text)

    def _no_of_results(self, response):
        soup=BeautifulSoup(response, 'xml')
        results = soup.find('TotalResults')
        return int(results.text)

    def get_primary_large_image(self, item):
        try:
            return item.find('LargeImage').find('URL').text
        except KeyError:
            return None
    def get_items(self, response, attrs = ['ASIN',"Author"]):
        soup = BeautifulSoup(response, 'xml')
        for item in soup.findAll('Item'):
            result = defaultdict(list)
            for attr in attrs:
                if attr=="LargeURL":
                    result[attr]=self.get_primary_large_image(item)
                elems = item.findAll(attr)
                for elem in elems:
                    try:
                        result[attr].append(elem.text)
                    except Exception as e:
                        print e
                        result[attr].append(None)
            yield result

    def pretty_print(self, xml_string):
        if not isinstance(xml_string, str):
            xml_string=str(xml_string)
        import xml.dom.minidom
        xml = xml.dom.minidom.parseString(xml_string)
        print xml.toprettyxml()


def amazon_author_books_crawler(*authors):
    c=AmazonCrawler(AWS_ASSOCIATE_TAG, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    objects = []
    for author in authors:
        for page in c.item_search(Author=author, SearchIndex="Books", IncludeReviewsSummary="True"):
            for item in c.get_items(page, attrs = ["ASIN", "Author", "NumberOfPages", "Publisher", "FormattedPrice", "Title", "PublicationDate", "SalesRank", "DetailPageURL" ]):
                objects.append(item)
    return objects
import time, csv
def csv_write_record(output_file="output.csv", row=[]):
    row = [elem.strip().encode('utf-8') for elem in row]
    with open(output_file, 'a') as f:
        writer=csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(row)
        
import sys
if __name__ == "__main__":
    authors = sys.argv[1:]
    records=amazon_author_books_crawler(*authors)
    keys=["ASIN", "Author", "NumberOfPages", "Publisher", "FormattedPrice", "Title", "PublicationDate", "SalesRank", "DetailPageURL" ]
    headers =["Crawled Time"]+keys
    csv_write_record(output_file="output.csv", row=headers)
    print len(records)
    for record in records:
        row = [time.strftime("%m/%d/%Y - %H:%M:%S"),]
        for key in keys:
            row.append(', '.join(record[key]))
        csv_write_record(output_file="output.csv", row=row)
        
    
    
