import csv
from lxml import etree
import re
import requests
import simplejson as json

BASE_URL = 'http://lgdirectory.gov.in/globalviewvillageforcitizen.do'
STATES_XPATH = '//select[@name="stateNameEnglish"]/option[not(@value="")]'
DISTRICT_LIST_URL = 'http://lgdirectory.gov.in/dwr/call/plaincall/lgdDwrDistrictService.getDistrictList.dwr'
SUB_DISTRICT_LIST_URL = 'http://lgdirectory.gov.in/dwr/call/plaincall/lgdDwrSubDistrictService.getSubDistListbyDistCodeShift.dwr'
VILLAGE_LIST_URL = 'http://lgdirectory.gov.in/dwr/call/plaincall/lgdDwrVillageService.getVillageList.dwr'
OUT_FILE = 'village_list.csv'
OUT_CSV_HEADER = ['village_census_code_2011', 'village_name', 'village_is_pesa', 'block_census_code_2011', 'block_name', 'block_is_pesa', 'district_census_code_2011', 'district_name', 'district_is_pesa',  'state_census_code_2011', 'state_name']

class VillageListGenerator(object):
    def __init__(self):
        self.session_id = ""

    def get_state_list(self): 
        state_list = []
        response = requests.get(BASE_URL)
        if response.text:
            try:
                dom_tree = etree.HTML(response.text)
                state_raw_list = dom_tree.xpath(STATES_XPATH)
                for state_element in state_raw_list:
                    state_dict = {}
                    state_dict["census_code"] = state_element.xpath("@value")[0]
                    state_dict["name"] = state_element.text.strip()
                    state_list.append(state_dict)
            except Exception, e:
                print("%s\t%s\t%s" % ("Unable to process response text for URL:", BASE_URL, format(e))) 
        return state_list

    def get_district_list(self, state_dict):
        district_list = []
        payload = "callCount=1&windowName=&c0-scriptName=lgdDwrDistrictService&c0-methodName=getDistrictList&c0-id=0&c0-param0=string:%s&batchId=3&page=Fglobalviewvillageforcitizen.do&httpSessionId=&scriptSessionId=%s" % (state_dict["census_code"], self.session_id)
        response = requests.post(DISTRICT_LIST_URL, payload)
        if response.text:
            json_raw_text = response.text
            try:
                self.set_session_id(response.text)
                json_raw_text = "[{" + response.text.split(",[{")[1].split(");")[0]
                json_raw_text = self.clean_raw_text(json_raw_text, ['districtCode', 'districtNameEnglish'])
                district_list = json.loads(json_raw_text)
            except Exception, e:
                print("%s\t%s\t%s\t%s" % ("Unable to parse response text for:", format(state_dict), format(e), json_raw_text)) 
        return district_list

    def get_sub_district_list(self, district_dict):
        sub_district_list = []
        payload = "callCount=1&windowName=&c0-scriptName=lgdDwrSubDistrictService&c0-methodName=getSubDistListbyDistCodeShift&c0-id=0&c0-param0=string:%s&batchId=3&page=Fglobalviewvillageforcitizen.do&httpSessionId=&scriptSessionId=%s" % (district_dict["districtCode"], self.session_id)
        response = requests.post(SUB_DISTRICT_LIST_URL, payload)
        if response.text:
            json_raw_text = response.text
            try:
                self.set_session_id(response.text)
                json_raw_text = "[{" + response.text.split(",[{")[1].split(");")[0].replace('subdistrictNameEnglish', 'subdistrict_Name_English')
                json_raw_text = self.clean_raw_text(json_raw_text, ['subdistrictCode', 'subdistrict_Name_English', 'tlc', 'districtNameEnglish'])
                sub_district_list = json.loads(json_raw_text)
            except Exception, e:
                print("%s\t%s\t%s\t%s" % ("Unable to parse response text for:", format(district_dict), format(e), json_raw_text)) 
        return sub_district_list

    def get_village_list(self, sub_district_dict):
        village_list = []
        payload = "callCount=1&windowName=&c0-scriptName=lgdDwrVillageService&c0-methodName=getVillListbySubDistCodeShift&c0-id=0&c0-param0=string:%s&batchId=3&page=Fglobalviewvillageforcitizen.do&httpSessionId=&scriptSessionId=%s" % (sub_district_dict["subdistrictCode"], self.session_id)
        response = requests.post(VILLAGE_LIST_URL, payload)
        if response.text:
            json_raw_text = response.text
            try:
                self.set_session_id(response.text)
                json_raw_text = "[{" + response.text.split(",[{")[1].split(");")[0].replace('subdistrict:', 'sub_district:')
                json_raw_text = self.clean_raw_text(json_raw_text, ['sub_district', 'subdistrictCode', 'subdistrictNameEnglish', 'vlc', 'partFullFlag', 'villageNameEnglish', 'villageCode', 'renameNameVillageList'])
                village_list = json.loads(json_raw_text)
            except Exception, e:
                print("%s\t%s\t%s\t%s" % ("Unable to parse response text for:", format(sub_district_dict), format(e), json_raw_text)) 
        return village_list

    def clean_raw_text(self, raw_text, keys_to_escape):
        for key in keys_to_escape:
            raw_text = raw_text.replace(key, '"%s"' % key) 
        raw_text = re.sub(r'"([^,:]*)"([^,:]*)"([^,:]*)"', r"\"\1'\2'\3\"", raw_text)
        raw_text = raw_text.replace("\\", '').encode('utf-8')
        return raw_text

    def set_session_id(self, response_text):
        if "handleNewScriptSession(" in response_text:
            self.session_id = response_text.split('handleNewScriptSession("')[1].split('")')[0]

    def generate_village_list(self):
        with open(OUT_FILE, "wb") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',') 
            csv_writer.writerow(OUT_CSV_HEADER)
            state_list = self.get_state_list()
            for state_dict in state_list:
                district_list = self.get_district_list(state_dict)
                for district_dict in district_list:
                    sub_district_list = self.get_sub_district_list(district_dict)
                    for sub_district_dict in sub_district_list:
                        village_list = self.get_village_list(sub_district_dict)
                        for village_dict in village_list:
                            temp_row = ([village_dict["villageCode"], village_dict["villageNameEnglish"], village_dict["is_pesa"], sub_district_dict["subdistrictCode"], sub_district_dict["subdistrict_Name_English"], sub_district_dict["is_pesa"], district_dict["districtCode"], district_dict["districtNameEnglish"], district_dict["is_pesa"], state_dict["census_code"], state_dict["name"]])
                            village_row = []
                            for element in temp_row:
                                village_row.append(str(element).strip())
                            csv_writer.writerow(village_row)

if __name__ == '__main__':
    obj = VillageListGenerator()
    obj.generate_village_list()                         
