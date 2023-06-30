import pyodbc 
import pandas as pd
import re
import numpy as np
import xml.etree.ElementTree as ET
import warnings
import re
import os

# Text only contains phone number (e.g., "123-4567" or "123 4567")
def handle_phone_only(text):
    match = re.match(r'^[\d\s-]+$', text.strip())
    if match is not None:
        phone_number = match.group()
        # Remove any leading dashes from the phone number
        phone_number = phone_number.lstrip('-')
        return None, None, phone_number
    return None, None, None


# The word "Building" followed by a number (e.g., "Building 3")
def handle_building_number(text):
    # Lowercase text for matching, but we'll use the original text when returning the match
    lower_text = text.lower()
    match = re.match(r'^(building \d+)', lower_text.strip())
    if match is not None:
        # Get the start and end of the match in the original string
        start, end = match.span(1)
        return text[start:end], None, None
    return None, None, None

# The word "Rear" followed by the occupant's name and possibly phone number (e.g., "Rear David Renshaw 123-4567")
def handle_rear_occupant(text):
    # Lowercase text for matching, but we'll use the original text when returning the match
    lower_text = text.lower()
    match = re.match(r'^(rear [\w\s]+)( [\d-]+)?$', lower_text.strip())
    if match is not None:
        # Get the start and end of the match in the original string
        rear_and_name_start, rear_and_name_end = match.span(1)
        rear_and_name = text[rear_and_name_start:rear_and_name_end]
        phoneNum = None
        if match.group(2) is not None:
            phone_start, phone_end = match.span(2)
            phoneNum = text[phone_start:phone_end].strip()

        # Separate 'Rear' from the occupant's name.
        split_result = rear_and_name.split(maxsplit=1)
        houseNumber = split_result[0] if len(split_result) > 0 else None
        occupant = split_result[1] if len(split_result) > 1 else None

        return houseNumber, occupant, phoneNum
    return None, None, None

# The word "Apt" or "Apartment" followed by a space and a number with optional letter (e.g., "Apt 10B")
def handle_apt(text):
    # Lowercase text for matching, but we'll use the original text when returning the match
    lower_text = text.lower()
    match = re.match(r'^((apt|apartment)\s\d+[a-z]?)([\w\s]*)( [\d-]+)?$', lower_text.strip())
    if match is not None:
        # Get the start and end of the match in the original string for the apartment number
        start, end = match.span(1)
        houseNumber = text[start:end]
        
        # Get the start and end of the match in the original string for the occupant
        if match.group(3):
            start, end = match.span(3)
            occupant = text[start:end].strip()
        else:
            occupant = None

        # Extract phone number if it exists
        phoneNum = match.group(4).strip() if match.group(4) is not None else None
        
        return houseNumber, occupant, phoneNum
    return None, None, None

def handle_standard_case(text):
    houseNumber, occupant, phoneNum = None, None, None

    # Match house number at the beginning of the string if it is followed by a space.
        # a letter followed by a number, with optional dashes and additional numbers (e.g., "A12", "A12-14"), 
        # a number (which may contain dashes), or a sequence of digits followed by a lowercase letter and digits (e.g., "11b3").
    houseNumber_match = re.match(r'^([A-Za-z]?\d+(-[A-Za-z]?\d+)?|\d+[A-Za-z\d]*(-\d+[A-Za-z\d]*)?)(?=\s)', text)
    if houseNumber_match is not None:
        # if a match is found in the standard regex, remove matched part and strip spaces
        houseNumber = houseNumber_match.group()
        text = text[len(houseNumber):].strip()
    
    # Match phone number at the end of the string (may contain spaces and dashes)
    phoneNum_match = re.search(r'([\d-]+(?:\s[\d-]+)*$)', text)
    if phoneNum_match is not None:
        # if a match is found in the standard regex, remove matched part and strip spaces
        phoneNum = phoneNum_match.group()
        # Remove any leading dashes from the phone number
        phoneNum = phoneNum.lstrip('-')
        text = text[:len(text)-len(phoneNum)].strip()

    # The remaining part is the occupant
    occupant = text if text else None

    return houseNumber, occupant, phoneNum

def parse_sublisting(text):
    # check if the input is not a string
    if not isinstance(text, str):
        return None, None, None
    # check if the input is only spaces
    if text.isspace():
        return None, None, None
    
    # a list of special case handlers in order of priority
    handlers = [
        handle_phone_only,
        handle_building_number,
        handle_rear_occupant,
        handle_apt
        #... add more handlers here
    ]

    # loop through special case handlers
    for handler in handlers:
        houseNumber, occupant, phoneNum = handler(text)
        if houseNumber is not None or occupant is not None or phoneNum is not None:
            return houseNumber, occupant, phoneNum
    
    # handle the standard case
    houseNumber, occupant, phoneNum = handle_standard_case(text) 

    return houseNumber, occupant, phoneNum


def extract_sublistings_xml(df, textField = "textHCL"):
    if 'SUBLISTINGS' not in df.columns:
        return df

    # Initialize an empty list to store the extracted data
    data = []

    for index, row in df.iterrows():
        if ('SUBLISTINGS' in row and row['SUBLISTINGS'] is not None and isinstance(row['SUBLISTINGS'], str)
        and 'LISTING_ID' in row and row['LISTING_ID'] is not None
        and 'STREET_SEGMENT_ID' in row and row['STREET_SEGMENT_ID'] is not None
        and 'BookKey' in row and row['BookKey'] is not None):
            sub_xml = row['SUBLISTINGS']
            root = ET.fromstring(sub_xml)

            # Iterate over each 'subListing' in the XML tree
            for sub_listing in root.iter('subListing'):
                extractedText = sub_listing.find(textField).text
                data.append({
                    f"{textField}": extractedText,
                    'LISTING_ID': row['LISTING_ID'],
                    'STREET_SEGMENT_ID': row['STREET_SEGMENT_ID'],
                    'BookKey': row['BookKey']
                })

    # Convert the list of data into a DataFrame
    temp_df = pd.DataFrame(data)
    return temp_df