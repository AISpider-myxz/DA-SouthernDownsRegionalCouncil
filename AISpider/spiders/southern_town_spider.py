
import scrapy
from scrapy import Request
from bs4 import BeautifulSoup
from AISpider.items.southern_town_item import SouthernTownItem
from DrissionPage import ChromiumOptions, ChromiumPage
import time
from datetime import date, datetime, timedelta
from common._date import get_all_month_
from common.set_date import get_this_month
DATE_FORMATE = "%d/%m/%Y"


class SouthernTownSpider(scrapy.Spider):
    name = "southern_town"
    allowed_domains = ["sdrc-web.t1cloud.com"]
    start_urls = [
        "https://sdrc-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/eTrackApplicationSearchResults.aspx"]
    USER_TYPE = 'P1.WEBGUEST'
    QUERY_TYPE = {'view': '$P1.ETR.RESULTS.VIW', 'query': '$P1.ETR.SEARCH.ENQ', 'appdet': '$P1.ETR.APPDET.VIW'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Origin': 'https://sdrc-web.t1cloud.com'
    }
    #start_date = '29/02/2024'  # 网站最早的记录为 04/03/1991

    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'LOG_STDOUT': True,
        #'LOG_FILE': 'scrapy_lockyer_valley.log',
        'DOWNLOAD_TIMEOUT': 1200
    }

    def __init__(self, run_type='all', days=30, *args, **kwargs):
        """
        run_type: 表示爬虫运行的类型， `all`代表爬取到当前日期的所有记录， `part`代表爬取部分
        days: 结合`run_type`一起使用， 当`run_type`不等于`all`,只爬取指定天数的数据默认为30天
        """
        super(SouthernTownSpider, self).__init__(*args, **kwargs)
        self.run_type = run_type
        if days == None:
            # 如果没有传days默认为这个月的数据
            self.days = get_this_month()
        else:
            now = datetime.now()
            days = int(days)
            date_from = (now - timedelta(days)).date().strftime('%d/%m/%Y')
            # 这里计算出开始时间 设置到self.days
            self.days = date_from
        self.cookie = None
        self.search_option = None

    def parse(self,response):
        all_month = get_all_month_(self.days, datetime.now().date().strftime('%d/%m/%Y'))
        for index, y_date in enumerate(all_month):
            if y_date == all_month[-1]:
                break
            start_time = y_date
            end_time = all_month[index + 1]
            print(start_time + "-----" + end_time)
            for item in self.get_data(start_date=start_time,end_date=end_time):
                yield item
    
    def get_data(self, start_date=None, end_date=None):
        co = ChromiumOptions().auto_port()
        co.incognito()  # 匿名模式
        co.headless()  # 无头模式
        co.set_browser_path(path='/opt/google/chrome/google-chrome')# 设置路径
        co.set_argument('--no-sandbox')  # 无沙盒模式
        co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
        self.browser = ChromiumPage(co)
        self.browser.set.user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
        self.browser.get('https://sdrc-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/eTrackApplicationSearch.aspx?r=P1.WEBGUEST&f=$P1.ETR.SEARCH.ENQ')

        # 输入开始时间
        self.browser.ele('x://*[@id="ctl00_Content_txtDateFrom_txtText"]').input(start_date)
        # 输入结束时间
        self.browser.ele('x://*[@id="ctl00_Content_txtDateTo_txtText"]').input(end_date)
        self.browser.ele('x://*[@id="ctl00_Content_btnSearch"]').click()
        try:
            self.browser.wait.doc_loaded()
            for item in self.click_page_detail():
                yield item
        except:
            pass
        try:
            judge_next_page = self.browser.eles('c:.pagerRow a')
            for page_number in range(len(judge_next_page)):
                self.browser.ele(f'x://*[@id="ctl00_Content_cusResultsGrid_repWebGrid_ctl00_grdWebGridTabularView"]/tbody/tr[17]/td/table/tbody/tr/td[{page_number+2}]/a').click()
                for item in self.click_page_detail():
                    yield item
        except:
            pass
        self.browser.quit()
    def click_page_detail(self):
        normalRow = self.browser.eles('c:.normalRow a')
        for url in normalRow:
            temp_str = url.text
            tab = self.browser.new_tab()
            #tab.get("https://sdrc-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/eTrackApplicationDetails.aspx?r=P1.WEBGUEST&f=$P1.ETR.APPDET.VIW&ApplicationId="+'MCU\\02563')
            tab.get("https://sdrc-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/eTrackApplicationDetails.aspx?r=P1.WEBGUEST&f=$P1.ETR.APPDET.VIW&ApplicationId="+temp_str)
            tab.wait.doc_loaded()
            for item in self.parse_detail(resp=tab.html):
                tab.close()
                yield item
            
            
        alternateRow = self.browser.eles('c:.alternateRow a')
        for url in alternateRow:
            temp_str = url.text
            tab = self.browser.new_tab()
            tab.get("https://sdrc-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/eTrackApplicationDetails.aspx?r=P1.WEBGUEST&f=$P1.ETR.APPDET.VIW&ApplicationId="+temp_str)
            tab.wait.doc_loaded()
            for item in self.parse_detail(resp=tab.html):
                tab.close()
                yield item

    def parse_detail(self,resp):
        soup = BeautifulSoup(resp,'html.parser')
        item = SouthernTownItem()
        app_data = soup.select('#ctl00_Content_cusPageComponents_repPageComponents_ctl00_cusPageComponentGrid_pnlCustomisationGrid tr td')
        temp_dict = {}
        temp_str = ''
        num = 0
        for x in app_data:
            if num %2 == 0:
                temp_str = x.get_text()
            else:
                temp_dict[temp_str] = x.get_text()
            num += 1
        a = temp_dict['Application ID'].replace("\\",'-')
        item['application_id'] = a
        item['description'] = temp_dict['Description']
        item['application_group'] = temp_dict['Group']
        item['category'] = temp_dict['Category']
        item['sub_category'] = temp_dict['Sub Category']
        item['stage'] = temp_dict['Stage/Decision']
        # item['lodgement_date'] = temp_dict['Lodgement Date']
        try: 
            lodged_date = temp_dict['Lodgement Date']
            time_array = time.strptime(lodged_date, '%d/%m/%Y')
            temp_data = int(time.mktime(time_array))
            item['lodgement_date'] = temp_data if lodged_date else 0  
        except:
            item['lodgement_date'] = 0
        #item['certifier_approval_date'] = temp_dict['Council Decision Date']
        try: 
            lodged_date = temp_dict['Council Decision Date']
            time_array = time.strptime(lodged_date, '%d/%m/%Y')
            temp_data = int(time.mktime(time_array))
            item['certifier_approval_date'] = temp_data if lodged_date else 0  
        except:
            item['certifier_approval_date'] = 0

        try:
            item['names'] = soup.select('#ctl00_Content_cusPageComponents_repPageComponents_ctl02_cusPageComponentGrid_repWebGrid_ctl00_dtvWebGridListView .normalRow td')[1].get_text()
        except:
            pass
        try:
            item['address'] = soup.select('#ctl00_Content_cusPageComponents_repPageComponents_ctl03_cusPageComponentGrid_repWebGrid_ctl00_dtvWebGridListView .normalRow td')[1].get_text()
        except:
            pass
        try:
            temp_list = soup.select('#ctl00_Content_cusPageComponents_repPageComponents_ctl04_cusPageComponentGrid_pnlCustomisationGrid .normalRow td')
            temp_str = ''
            i = 0
            for data in temp_list:
                if i % 3 == 0:
                    data = 'https://sdrc-web.t1cloud.com' + data.select_one('a').get('href')+';'
                    temp_str += data
                i+=1
            item['documents'] = temp_str
        except:
            pass
        del item['metadata']
        yield item