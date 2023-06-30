#Utilities to manage address clean up tasks
import re
import math
import numpy as np
from lid import LBX_LID

ADDR_LID_TYPE = "06"
LID_NAMESPACE = ""

class AddressUtils:
    
    def get_auzkey(address,unit=None,zipcode=None):
        """
        Returns a auzkey for the address
        Params:
            address(str): address of the property
            unit(str): unit of the property
            zipcode(str): zipcode of the property
        Returns:
            str: auzkey of the address
        """
        auzkey = ""
        if address is None:
            auzkey = f"|"
        else:
            auzkey = f"{address.upper()}|"
        if unit is not None:
            auzkey += f"{unit.upper()}|"
        else:
            auzkey += "|"

        if zipcode is not None:
            auzkey += f"{zipcode.upper()}"        
        return auzkey
    def get_address_lid(auzkey):
        """
        Returns a lid for the address
        Params:
            auzkey(str): auzkey of the address
        Returns:
            str: lid of the address
        """
        lids = LBX_LID()
        return lids.lid(type=ADDR_LID_TYPE, name=auzkey, namespace="")

    def find_house_numbers_outliers(house_number_list):
        """
        Returns a list of house number outliers that don't fit on the range pattern on a street segment
        Params:
            house_number_list([ints]): a range of integers representing house numbers in a street segment
        Returns:
            [ints]: outlier house text, numbers that don't seem to fit the range
        Example:
            house_number_list = [10,11,12,103]
            outliers = [103] # doesn't seem to fit on this segment. Perhaps bad OCR
        """
        outliers = []
        # Calculate the quartiles and IQR
        quartiles = np.percentile(house_number_list, [15, 85])
        q1, q3 = quartiles[0], quartiles[1]
        iqr = q3 - q1

        # Calculate the lower and upper bounds for outliers
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)

        # Identify the outliers
        outliers = [x for x in house_number_list if x < lower_bound or x > upper_bound]
        return outliers
    def get_house_number_from_text(house_text):
        house_num = house_text
        if house_num.endswith("1/2"):#
            house_num = house_num.split("1/2")[0].rstrip()
            
        if house_num[-1].isalpha():# how about bad ocrs such as 184% street_name. ie listing 1793695835 hcl is orcring some "1/2" into "%" flag thiss
            # Remove letters from string
            while house_num[-1].isalpha():
                house_num = house_num[:-1]
        return int(house_num)
    def split_address_ranges(house_text):
        """
        Splits a string containing a range of house numbers into a list of house numbers
        Params:
            house_text: string containing a range of house numbers. i.e 10-14, 10a-16b, 10-16b, 10a-14, 10-14b
        Returns:
            [str]: list of house numbers i.e. [10a, 11, 12, 13, 14]
        """
        ranges_units = []
        range_split = house_text.replace(" ", "").split("-")
        if len(range_split) > 2:#i.e. 10-14-16 just split, remove empty elements
            #remove elements with empty space
            range_split =  [x for x in range_split if x]
            #need to bring up to magnitude any elements less than the start range
            for i in range(len(range_split)):
                element = range_split[i]
                if element.isdigit():
                    start_range = range_split[0]
                    if len(element) < len(start_range):
                        element = start_range[:-len(element)] + element
                        range_split[i] = element
            return range_split

        start_range = range_split[0].upper()
        end_range = range_split[1].upper()
        #does end_range ends with '1/2'?
        ends_with_half = end_range.endswith('1/2')
        starts_with_half = start_range.endswith('1/2')
        if ends_with_half:
            end_range = end_range[:-3]
        if starts_with_half:
            start_range = start_range[:-3]
        
        if start_range.isdigit() and end_range.isdigit(): #i.e. 10-14
            start_range = int(start_range)
            end_range = int(end_range)
            if start_range > end_range:# i.e. 201-02, 20-15 
                #are the numbers in the same order of magnitude?
                start_order = math.floor(math.log10(start_range)) + 1
                end_order = math.floor(math.log10(end_range)) + 1
                if start_order == end_order:#reverse range????
                    return range_split
                else:
                    end_range = int(str(start_range)[:-end_order] + str(end_range))
            
            if starts_with_half:
                ranges_units.append(f'{start_range}1/2')
                start_range += 1
            ranges_units = ranges_units + (list(range(start_range, end_range + 1)))
            if ends_with_half:
                ranges_units.append(f'{end_range}1/2')
        else: #possible letters on the range, i.e. 10a-16b or 10-16b
            if not start_range.isdigit():
                try:
                    start_number, start_letter = re.match(r"(\d+)([a-z]+)", start_range, re.I).groups()
                    start_number = int(start_number)
                except AttributeError:
                    raise ValueError('Invalid range: start range has invalid format')
                if not end_range.isdigit():
                    try:
                        end_number, end_letter = re.match(r"(\d+)([a-z]+)", end_range, re.I).groups()
                        end_number = int(end_number)
                    except AttributeError:
                        raise ValueError('Invalid range: end range has invalid format')
                    if start_number == end_number:#i.e. 10a-10c
                        ranges_units = [f'{start_number}{chr(i)}' for i in range(ord(start_letter), ord(end_letter) + 1)]
                    elif start_number > end_number:#i.e. 20a-15
                        start_order = math.floor(math.log10(start_range)) + 1
                        end_order = math.floor(math.log10(end_range)) + 1
                        if start_order == end_order:#reverse range????
                            return range_split
                    else:#i.e. 10a-11b, no letters inbetween range
                        ranges_units = [f'{i}{start_letter}' if i == start_number else
                                        f'{i}{end_letter}' if i == end_number and end_letter else
                                        str(i) for i in range(start_number, end_number + 1)]
                        ranges_units.append(end_number)
                else:#i.e. 10a-14
                    if len(end_range) == len(str(start_number)):
                        if start_number > int(end_range):
                            return range_split
                    elif len(end_range) < len(str(start_number)):
                        end_range = str(start_number)[:-len(end_range)] + end_range
                    end_number = int(end_range)
                    ranges_units = [f'{i}{start_letter}' if i == start_number else str(i)
                                    for i in range(start_number, end_number + 1)]
                    if ends_with_half:
                        ranges_units.append(f'{end_range}1/2')
            else:#i.e. 10-14b
                try:
                    end_number, end_letter = re.match(r"(\d+)([a-z]+)", end_range, re.I).groups()
                    if len(end_number) < len(start_range):
                        end_number = start_range[:-len(end_number)] + end_number
                    end_number = int(end_number)
                except AttributeError:
                    raise ValueError('Invalid range: end range has invalid format')
                ranges_units = [f'{i}{end_letter}' if i == end_number and end_letter else str(i)
                                for i in range(int(start_range), end_number + 1)]
                ranges_units.append(end_number)
        return ranges_units