from SublistingStitchingV3 import SublistingStitching


t_book_list_1 = [
  "gaatlantasub1975hain",
  "gaatlantasub1981hain",
  "gaatlanta1966haines",
  "gaatlantasub1986hain",
  "gaatlantasub1991hain",
  "gaatlantacity1970haines",
  "gaatlantacity1991haines",
  "gaatlantacityan1967haines",
  "gaatlantasuburba1970haine",
]

t_book_list_2 = [
  "gaatlantasuburban1984polk",
  "gaatlantasuburban1988polk",
  "gaatlantasuburban1965atla",
  "gaatlantasuburban1969atla",
  "gaatlantasuburban1974atla",
  "gaatlantasuburban1979atla",
  "gaatlanta1940atlantacityd",
  "gaatlanta1986polkdirector",
  "gaatlanta1991polkdirectoryco",
]

t_book_list_3 = [
  "gaatlantacounty1953atlantacitydire",
  "gaatlantacounty1956atlantacitydire",
  "gaatlantacounty1960atlantacitydire",
  "gaatlantasub1960atlantacitydire",
  "gaatlantasub1966atlantacitydire",
  "gaatlantasub1970atlantacitydire",
  "gaatlantasub1982atlantacitydire",
  "gaatlantasub1975atlantacitydire"
]

t_book_list_4 = [
  "gaatlantacounty1953atlantacitydire",
]

# first 3 are sublisting without housenumber
test_set = {
    "1975" : {
        "book": "gaatlantasub1975hain",
        "t_set": "217156143,217156144, 217156145, 217156146, 217156163, 217156164",
        "note" : "145 have sublisting promote to listing and contain all other sublisting, do not have address",
        "tested": True,
        "min": 217156143,
        "max": 217156164
    },
    "1986" : {
        "book" : "gaatlantasub1986hain",
        "t_set" : "217156158, 217156159, 217156160",
        "note" : "no sublisting connection -- just basic stitching",
        "tested": True,
        "min": 217156158,
        "max": 217156160
    },
    "1986_2" : {
        "book" : "gaatlantasub1986hain",
        "t_set" : "217156269, 217156270,217156271, 217156277",
        "note" : "similar to 1975 but with bad house number ocr -> duplicate recordhouse number",
        "tested": True,
        "min": 217156269,
        "max": 217156277
    },
    "1986_v3": {
        "book": "gaatlanta1986polkdirector",
        "page" : 919,
        "t_set" : "192552642,192552643,192552644,192552645,192552646",
        "note": "!! shit ocr in every singel cell - sublisting have address - the whole column is promoted to listing - with bad house number - select * from CityDirDev.dbo.CD_STD_LISTINGS_V2 where BookKey = 'gaatlanta1986polkdirector' and STREET_SEGMENT_ID  in (192552642, 192552643, 192552644)",
        "tested": False,
        "min": 192552642,
        "max": 192552646
    },
    "1986_v4": {
        "book": "gaatlanta1986polkdirector",
        "page" : 921,
        "note": "!! shit ocr in every singel cell - sublisting have address - the whole column is promoted to listing - with bad house number - select * from CityDirDev.dbo.CD_STD_LISTINGS_V2 where BookKey = 'gaatlanta1986polkdirector' and STREET_SEGMENT_ID  in (192552642, 192552643, 192552644)",
        "tested": False,
        "min": 192552669,
        "max": 192552671
    },

    "1986_v5": {
        "book": "gaatlanta1986polkdirector",
        "page" : 921,
        "note": "!! shit ocr in every singel cell - sublisting have address - the whole column is promoted to listing - with bad house number - select * from CityDirDev.dbo.CD_STD_LISTINGS_V2 where BookKey = 'gaatlanta1986polkdirector' and STREET_SEGMENT_ID  in (192552642, 192552643, 192552644)",
        "tested": False,
        "min": 192559960,
        "max": 192559968
    },

    "1986_v6": {
        "book": "gaatlanta1986polkdirector",
        "page" : 921,
        "note": "!! shit ocr in every singel cell - sublisting have address - the whole column is promoted to listing - with bad house number - select * from CityDirDev.dbo.CD_STD_LISTINGS_V2 where BookKey = 'gaatlanta1986polkdirector' and STREET_SEGMENT_ID  in (192552642, 192552643, 192552644)",
        "tested": False,
        "min": 192564999,
        "max": 192565002
    }
}

ss_a = SublistingStitching(show="aa_summary.csv")

test = test_set["1986_v6"]


#ss_a.process(test["book"])
ss_a.process(test["book"], min_list=test["min"], max_list=test["max"])


# currently code cannot handle one case where - segment end but the last address have sublisting that begin on the next column



