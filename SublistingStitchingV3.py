import pandas as pd
import numpy as np
from scipy import stats
import os
import csv

from utils.database.DatabaseFactory import DatabaseFactory
from doug_parser import parse_sublisting as d_parser
from doug_parser import extract_sublistings_xml as d_xml_extract
from SegmentInfoV3 import Segment as s_segment
import xml.etree.ElementTree as ET
import re


SEGMENT_PARENT_CHILD_TABLE = "CityDirDev.dbo.CdSTD_street_segement_parent_lookup_v2"
LISTING_TABLE = "CityDir.dbo.CdListings"
SUBLISTING_TABLE = "CityDir.dbo.CdSubListings "
SELECT_LIST = "cl.ID,  csl.SubListingsID , csl.ID as sublist_id, cl.CdStreetSegmentsID , cl.BookKey , cl.HouseText , cl.ListingExtent ,  csl.SubListingsExtent , csl.SubListingsXML "

INDENTATION_BUFFER = 5

class SublistingStitching:
    
    def __init__(self, show = False):
        #self.book = book
        self.conn = self.connect()
        self.chain_list =[]
        self.summary = show



    def connect(self):
        server = 'labdatadev-db.dev.corp.lightboxre.com' 
        database = 'CityDir' 
        write_db = 'CityDirDev'
        user = os.environ.get("CD_DB_USER")
        pswrd = os.environ.get("CD_DB_PASS")

        self.db_factory = DatabaseFactory(default_type='mssql', default_host=server, default_database=database, default_username=user, default_password=pswrd)

        # return an acutal connection not the factory 
        return self.db_factory.create_connection().connection

    def run_query(self, query):
        print(query)
        return pd.read_sql(query, self.conn) 
    
    def get_all_listing_from_book(self, book, min_l=None, max_l=None):
        
        # !!TEST 
        if min_l is not None and max_l is not None:
            #self.all_listing = self.run_query("select * from " + LISTING_TABLE +" where BookKey = '" + book + "' and CdStreetSegmentsID BETWEEN " + str(min_l) + " and " + str(max_l))    
            self.all_listing = self.run_query("select " + SELECT_LIST + " from " + LISTING_TABLE + " cl left outer join " +  SUBLISTING_TABLE + " csl on csl.SubListingsID = cl.ID and csl.BookKey = '" + book + "' where cl.BookKey = '" + book + "' and cl.CdStreetSegmentsID BETWEEN " + str(min_l) + " and " + str(max_l)) 

        else: 
            #self.all_listing = self.run_query("select * from " + LISTING_TABLE +" where BookKey = '" + book + "'")  
            self.all_listing = self.run_query("select " + SELECT_LIST + " from " + LISTING_TABLE + " cl left outer join " +  SUBLISTING_TABLE + " csl on csl.SubListingsID = cl.ID  and csl.BookKey = '" + book + "' where cl.BookKey = '" + book + "'") ;

        return self.all_listing  

    def get_parent_child_relation(self, df, test = False):
        if test[0] is not None and test[1] is not None:
            temp_relation = self.run_query("select * from "+ SEGMENT_PARENT_CHILD_TABLE +" where ParentID != ChildID and ParentID BETWEEN "+ str(test[0]) + " and " + str(test[1]) )
            self.parent_child_source = temp_relation.groupby('ParentID')['ChildID'].apply(list)
            return self.parent_child_source

        min_val = df['CdStreetSegmentsID'].min() 
        max_val = df['CdStreetSegmentsID'].max()

        temp_relation = self.run_query("select * from "+ SEGMENT_PARENT_CHILD_TABLE +" where ParentID != ChildID and ParentID BETWEEN "+ str(min_val) + " and " + str(max_val) )

        self.parent_child_source = temp_relation.groupby('ParentID')['ChildID'].apply(list)
        return self.parent_child_source

    def extent_list(self, a):
        extent_list = []
        for key,value in a.items():
            if len(value) > 1:
                value.insert(0, key)
                #print(value)
                extent_list.append(value)
        return extent_list

    def process(self,book, min_list=None, max_list = None):
        self.book = book
        self.all_list_l = self.get_all_listing_from_book(book, min_list, max_list)
        parent_child_source_l = self.get_parent_child_relation(self.all_list_l, [min_list, max_list])

        extent_lists = self.extent_list(parent_child_source_l)
        #print(extent_lists)
        self.process_all_lists(extent_lists)

    def process_all_lists(self, process_lists):
        

        for data in process_lists:

            if(self.all_listing.query("CdStreetSegmentsID == " + str(data[0])).empty):
                print(data[0])
                continue

            out = self.process_list(data)
            self.write_result(out)
            
    def write_result(self, data):
        #print("writeoutto" +  f'./summary/{self.book}_result.csv')
        self.write_csv(data, f'./summary/{self.book}_result.csv')

    def write_csv(self, data, filename = "test1.csv"):
        file_exists = os.path.isfile(filename)

        #print(data)
        if(len(data) >= 1):
            #print("Not data to write")
            fields = list(data[0].keys())

            with open(filename, 'a+', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fields)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(data)
            
            print(f'{filename} created successfully')

    def process_list(self, in_list):

        parent = True
        parent_seg_id = None
        prior_segment = None
        last_house_num_prior = None

        summary = []
        output_dict = {
            "parent_seg_id": None,
            "major_seg_id" : None,
            "minor_seg_id" : None,
            "major_listing_id" : None,
            "minor_lead_sublisting_listing_id": None,
            "minor_sublisting_lists_listing_id": []
        }
        #print(in_list)

        for index, listing in enumerate(in_list):

            # basice setup
            current_segment = s_segment(listing, self.all_listing, parent)
            if current_segment.is_empty() and parent:
                break
            elif current_segment.is_empty():
                continue

            # run for the first item should be parent
            if parent:
                # setup the first item
                parent_seg_id = current_segment.segment_id
                prior_segment = current_segment
                parent = False
                continue

            print("p_segment")
            print(prior_segment.segment_id);
            # should start at the second item in list
            current_house_first = current_segment.get_listing_on(0, "HouseText")
            prior_house_last = prior_segment.get_listing_on(-1, "HouseText")

            if prior_house_last is None:
                self.segment_error(prior_segment.segment_id, "last house is None");
                prior_segment = current_segment
                continue

            
            p_sublisting = prior_segment.get_sublisting_on(-1)
            c_sublisting = current_segment.get_sublisting_on(0)
            to_add = None

            # assumption to at this point
            # - prior_house - always have house number (EX, 101, 101A, B101)
            if prior_house_last is not None and current_house_first is None:
                # current is a sublisting of prior 
                to_add = True
                pass
            else:
                # compare with house number format

                # if prior have sublisting with use it as a base
                if p_sublisting is not None:
                    # print("p")
                    # print(p_sublisting)
                    # print(current_house_first)
                    # print(prior_house_last)
                    test_p_sub = self.get_closet_address_string_format(current_house_first, prior_house_last, p_sublisting[0]["HouseText"])
                    if test_p_sub == p_sublisting[0]["HouseText"]:
                        ## this is sub
                        to_add = True
                    elif test_p_sub == prior_house_last:
                        to_add = False
                # if prior do not have sublisting then we use current sub as base
                elif c_sublisting is not None:
                    # print("c")
                    # print(c_sublisting)
                    test_c_sub = self.get_closet_address_string_format(current_house_first, prior_house_last, c_sublisting[0]["HouseText"])
                    if test_c_sub == c_sublisting[0]["HouseText"]:
                        ## this is sub
                        to_add =True
                    elif test_c_sub == prior_house_last:
                        to_add = False
                # if both do not have sublisting then we need to relie on pixel
                else:
                    # if both do nto have sublisting then we cannot determine at all ------
                    # self.segment_error(current_segment.segment_id, " Segment does not have enought data(format)")
                    pass

                # compare with hosue number range
                if to_add is None:
                    # if prior have sublisting with use it as a base
                    if p_sublisting is not None:
                        if last_house_num_prior is not None:
                            p_house_last_temp = last_house_num_prior
                        else:
                            p_house_last_temp = p_sublisting[-1]["HouseText"]
                        #print(str(current_house_first) + " :: " + str(prior_house_last) + " :: " + str(p_house_last_temp))
                        test_p_sub = self.get_closest_address_range(current_house_first, prior_house_last, p_house_last_temp)
                        #print(test_p_sub)
                        if test_p_sub == p_house_last_temp:
                            ## this is sub
                            to_add = True
                        elif test_p_sub == prior_house_last:
                            to_add = False
                    # if prior do not have sublisting then we use current sub as base
                    elif c_sublisting is not None:
                        test_c_sub = self.get_closest_address_range(current_house_first, prior_house_last, c_sublisting[0]["HouseText"])

                        
                        if test_c_sub == c_sublisting[0]["HouseText"]:
                            ## this is sub
                            to_add =True
                        elif test_c_sub == prior_house_last:
                            to_add = False
                    # if both do not have sublisting then we need to relie on pixel
                    else:
                        if(self.check_same_format(prior_house_last, current_house_first)):
                            #to_add = True
                            if(not self.check_continuation_number(prior_house_last, current_house_first)): 
                                to_add = True
                                


            # check what to add
            if to_add:
                last_house_num_prior = None
                # print("current")
                # print(current_segment.segment_id)
                # print(current_segment.get_sublisting_on(0))
                write_out = {
                            "parent_seg_id": parent_seg_id,
                            "major_seg_id" : prior_segment.segment_id,
                            "minor_seg_id" : current_segment.segment_id,
                            "major_listing_id" : prior_segment.get_listing_on(-1, "ID"),
                            "minor_lead_sublisting_listing_id": current_segment.get_listing_on(0, "ID"),
                            "minor_lead_sublisting_contain_sublisting": current_segment.get_sublisting_on(0) is not None,
                            "minor_house_first_based" : current_house_first,
                            "major_house_last_based" : prior_house_last,
                            "minor_sublisting_lists_listing_id": None,
                        }
                # check subsecquent listing if sub or not sub
                if c_sublisting is None:
                    #print("221 --- current do not have sublisting")
                    # this mean we need to look at every single sublisting
                    sub_list = self.check_subsequence_listing(current_segment, prior_house_last, current_house_first)
                    if(sub_list is not None):
                        indent_list = self.check_sublisting_extent(current_segment, current_house_first)
                        

                    #print(current_segment.get_listing_on(-1, "HouseText"))
                    if len(sub_list) == 0:
                        #only one items
                        write_out["minor_sublisting_lists_listing_id"] = None 
                    elif (current_segment.get_listing_on(-1, "HouseText") is not None) and (len(indent_list) + 1 == len(current_segment.get_all_listing_column("HouseText"))):
                        # the whole segment is sublisting
                        # prior segment need to be adjust - manually

                        # prior segemnt do not move but the last  p_sublisting[-1]["HouseText"] need to be modify
                        last_house_num_prior = current_segment.get_listing_on(-1, "HouseText")
                        write_out["minor_sublisting_lists_listing_id"] = sub_list
                        summary.append(write_out)
                        print("whole segment is sublisting")
                        print(prior_segment.segment_id)

                        continue
                    else:
                        # that is where the list end
                        write_out["minor_sublisting_lists_listing_id"] = sub_list


                summary.append(write_out)

            prior_segment = current_segment
        # end for loop
        return summary
    
    def check_sublisting_extent(self, current_seg, c_house_first):
        output_list = []
        extent_set = current_seg.get_all_listing_column("ListingExtent")
        active_extent = extent_set[0];
        for index, extent in enumerate(extent_set):
            if index == 0:
                continue
            same_indent = self.check_similar_indentation(active_extent, extent)
            if not same_indent:
                break;

            output_list.append(current_seg.get_listing_on(index, "ID"))
            active_extent = extent
        # end for
        return output_list        
    
    def check_similar_indentation(self, base, value):
        base_list = eval(base)
        value_list = eval(value)

        if value_list[0] < base_list[0] + INDENTATION_BUFFER and value_list[0] > base_list[0] - INDENTATION_BUFFER:
            return True
        return False

    def check_subsequence_listing(self, current_seg, p_house_last, c_house_first):
        output_list = []
        hn_set = current_seg.get_all_listing_column("HouseText")
        active_house = c_house_first
        for index, h_num in enumerate(hn_set):
            # skip the first item
            if index == 0:
                continue
            format_t = self.get_closet_address_string_format(h_num, p_house_last, active_house)
            if format_t == p_house_last :
                break
            else: 
                format_r = self.get_closest_address_range(h_num, p_house_last, active_house)
                if format_r == p_house_last :
                    break
                # elif format_r == 0 and format_t == 0:
                #     break
            
            output_list.append(current_seg.get_listing_on(index, "ID"))
            active_house = h_num

        return output_list

    def segment_error(self, segment_id, reason):
        print("Issue with: " + str(segment_id) + " ::: " + reason   )
            
    def get_format_similarity(self, soruce, target_a, target_b):
        pass
            
    def check_same_format(self, in_a, in_b):
        if (in_a[0].isalpha() and in_b[0].isalpha()
            or (in_a[-1].isalpha() and in_b[-1].isalpha())):
            return True
        elif(not in_a[0].isalpha() and not in_b[0].isalpha()
            or (not in_a[-1].isalpha() and  not in_b[-1].isalpha())):
            return True
        return False

    def get_closet_address_string_format(self, source, target_a, target_b):
        # the idea is that source should be different from target iff source and targets is different format
        # return the closer target in its original format
        # -- return None when the number is likely to be all number
        
        if target_a == target_b:
            return 0
        elif source is None:
            return 0
        
        # detact None
        if target_a is None and  target_b is not None:
            return target_b
        elif target_b is None and target_a is not None:
            return target_a
        elif target_a is None and target_b is None :
            return 0

        # check char structure
        if source and source[0].isalpha():
            # leading char
            if target_a[0].isalpha() and not target_b[0].isalpha():
                return target_a
            elif not target_a[0].isalpha() and target_b[0].isalpha():
                return target_b
            else:
                # if both a and b not start with alpha
                return 0

        elif source and source[-1].isalpha():
            # ending char
            if target_a[-1].isalpha() and not target_b[-1].isalpha():
                return target_a
            elif not target_a[-1].isalpha() and target_b[-1].isalpha():
                return target_b
            else:
                # if both a and b not end with alpha
                return 0

        elif source and not source[-1].isalpha() and not source[0].isalpha():
            # source have not lead or end in alpha
            t_a = False
            t_b = False
            if(target_a[-1].isalpha() or target_a[0].isalpha()):
                t_a = True
            if(target_b[-1].isalpha() or target_b[0].isalpha()):
                t_b = True

            if not t_a and t_b: # if b contain any char
                return target_a
            elif t_a and not t_b: # if a contain any char
                return target_b
        else:
            return 0

    def check_continuation_number(self,target, value, buffer = 2):
        c_set = 'abcdefghijklmnopqrstuvwxyz'

        i_target = int(target.lower().lstrip(c_set).rstrip(c_set))
        i_value = int(value.lower().lstrip(c_set).rstrip(c_set))
        if i_target > i_value:
            return False

        if len(target) == len(value)  or  len(target) == len(value) - 1:
            #cal buffer
            buffer_zero = len(target) - 2 if len(target) > 2 else 0
            zeros = "0" * buffer_zero
            buffer_size = eval(str(buffer) + zeros)
            if i_value <= i_target + buffer_size:
                return True

        return False  

    def get_closest_address_range(self, source, target_a, target_b):
        # assumption (to this point) - all house number did not start or end with char - those should get pick upi by  get_closet_address_string_format

        #clean up
        c_set = 'abcdefghijklmnopqrstuvwxyz'
        
        #check number
        # occur when all data do not have char lead/tail
        try:

            i_source = int(source.lower().lstrip(c_set).rstrip(c_set))
            i_target_a = int(target_a.lower().lstrip(c_set).rstrip(c_set))
            i_target_b = int(target_b.lower().lstrip(c_set).rstrip(c_set))

            diff_s_a = abs(i_source - i_target_a)
            diff_s_b = abs(i_source - i_target_b)

            if(diff_s_a < diff_s_b):
                return target_a
            elif diff_s_a == diff_s_b:
                return min(i_target_a, i_target_b)
            else:
                return target_b
        except:
            # if anu number contain number in the middle then ignore comparison
            return 0

