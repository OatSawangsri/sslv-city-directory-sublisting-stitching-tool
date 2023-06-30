import pandas as pd
import os

from utils.database.DatabaseFactory import DatabaseFactory
from doug_parser import parse_sublisting as d_parser
from doug_parser import extract_sublistings_xml as d_xml_extract
import xml.etree.ElementTree as ET


LISTING_TABLE = "CityDir.dbo.CdListings"
class Segment:
    def __init__(self, seg_id, df = None,  parent = False):
        
        self.parent = parent
        if df is not None:
            self.all_listing = df.query("CdStreetSegmentsID == " + str(seg_id)).sort_values(by="ID")
            self.seg_len = len(self.all_listing)
        else:
            self.all_listing = None
            self.seg_len = 0
        self.segment_id = seg_id
        self.sublisting_dict = {}
        self.process(seg_id)

    def is_parent(self):
        return self.parent
    
    def connect(self):
        server = 'labdatadev-db.dev.corp.lightboxre.com' 
        database = 'CityDir' 
        write_db = 'CityDirDev'
        user = os.environ.get("CD_DB_USER")
        pswrd = os.environ.get("CD_DB_PASS")

        self.db_factory = DatabaseFactory(default_type='mssql', default_host=server, default_database=database, default_username=user, default_password=pswrd)

        # return an acutal connection not the factory 
        return self.db_factory.create_connection().connection

    def process(self, seg_id):
        
        if self.all_listing is None:
            conn = self.connect()
            query = "select * from " + LISTING_TABLE +" where CdStreetSegmentsID = " + str(seg_id) + " order by ID  " 
            self.all_listing = pd.read_sql(query, conn) 
            self.seg_len = len(self.all_listing)

    def get_listing_on(self, index, key = None):
        if not self.check_index(index):
            return None

        if key is None:
            return self.all_listing.iloc[index]
        elif key in self.all_listing.columns:
            return self.all_listing.iloc[index][str(key)]
        else:
            return None
        
    def get_sublisting_on(self, index):
        if not self.check_index(index):
            return None
        
        # if index in self.sublisting_dict:
        #     return self.sublisting_dict[index] if len(self.sublisting_dict[index]) == 0 else None

        working_list = self.all_listing.iloc[index]
        if working_list["SubListingsXML"] is None:
            return None
        else:
            xml_root = ET.fromstring(str(working_list['SubListingsXML']))
            self.sublisting_dict[index] = self.parse_sublisting_xml(xml_root)
            


            return self.sublisting_dict[index] if len(self.sublisting_dict[index]) > 0 else None

    def get_house_number_on(self, index):
        if not self.check_index(index):
            return None

        working_list = self.all_listing.iloc[index]
        if working_list["HOUSE_NUMBER"] is None:
            return None
        else:
            return working_list["HOUSE_NUMBER"]

    # other utility
    def check_index(self, index):
        if self.seg_len < index:
            return False
        return True
    
    def is_empty(self):
        return self.all_listing.empty

    def get_segment_range():
        # not include sublisitng
        # not possible yet until house number is clearn up
        pass
       
    def get_all_occupants(self):
        l_1 = self.get_all_listing_occupants()
        l_2 = self.get_all_sublisting_occupants()

        return l_1 + l_2
    
    def get_all_phonenumbers(self):
        l_1 = self.get_all_listing_phonenumbers_on()
        l_2 = self.get_all_sublisting_phonenumbers_on()

        return l_1 + l_2
    


    def get_all_listing_phonenumbers_on(self):
        return self.get_all_listing_column("PhoneText")
    
    def get_all_sublisting_phonenumbers_on(self):
        return self.get_all_sublisting_column("PhoneText")
    
    def get_all_listing_occupants(self):
        return self.get_all_listing_column("OCCUPANT")
    
    def get_all_sublisting_occupants(self):
        return self.get_all_sublisting_column("OCCUPANT")
    
    # generic 

    def get_all_listing_column(self, column):
        output =[]
        for index,data in self.all_listing.iterrows():
            output.append(data[column])
        return output

    def get_all_sublisting_column(self, column):
        output =[]

        for index in range(0, len(self.all_listing)):
             # build all sublisting          
            sublists = self.get_sublisting_on(index)
            # get all sublisting occupant
            if sublists is not None:
              for val in sublists:
                output.append(val[column])
        return output
        
    # internal use

    def parse_sublisting_xml(self,xml_root):
      
        sublisting_output_list = []
        #xml_string = lead_last_listing.iloc[0]['SUBLISTINGS']
        #xml_root = ET.fromstring(xml_string)
        #print(xml_root)
        for sub_listing in xml_root.iter('subListing'):
            extractedText = sub_listing.find('textHCL').text
            houseNumber, occupant, phoneNum  = d_parser(extractedText)
            sublisting_output_list.append({"HouseText": houseNumber, "OCCUPANT": occupant, "PhoneText": phoneNum})

        #print(sublisting_output_list)          
        return sublisting_output_list
    