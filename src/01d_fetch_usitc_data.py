#
# src/01d_fetch_usitc_data.py
# FINAL version with corrected argparse and a clean JSON payload.
#
import os
import pandas as pd
import argparse
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURATION ---
load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = 'https://datawebws.usitc.gov/dataweb'
requests.packages.urllib3.disable_warnings() 
##############################################
basicQuery = {
    "savedQueryName":"",
    "savedQueryDesc":"",
    "isOwner":True,
    "runMonthly":False,
    "reportOptions":{
        "tradeType":"Import",
        "classificationSystem":"HTS"
    },
    "searchOptions":{
        "MiscGroup":{
            "districts":{
                "aggregation":"Aggregate District",
                "districtGroups":{
                    "userGroups":[]
                },
                "districts":[],
                "districtsExpanded":
                    [
                        {
                            "name":"All Districts",
                            "value":"all"
                        }
                    ],
                "districtsSelectType":"all"
            },
            "importPrograms":{
                "aggregation":None,
                "importPrograms":[],
                "programsSelectType":"all"
            },
            "extImportPrograms":{
                "aggregation":"Aggregate CSC",
                "extImportPrograms":[],
                "extImportProgramsExpanded":[],
                "programsSelectType":"all"
            },
            "provisionCodes":{
                "aggregation":"Aggregate RPCODE",
                "provisionCodesSelectType":"all",
                "rateProvisionCodes":[],
                "rateProvisionCodesExpanded":[]
            }
        },
        "commodities":{
            "aggregation":"Aggregate Commodities",
            "codeDisplayFormat":"YES",
            "commodities":[],
            "commoditiesExpanded":[],
            "commoditiesManual":"",
            "commodityGroups":{
                "systemGroups":[],
                "userGroups":[]
            },
            "commoditySelectType":"all",
            "granularity":"2",
            "groupGranularity":None,
            "searchGranularity":None
        },
        "componentSettings":{
            "dataToReport":
                [
                    "CONS_FIR_UNIT_QUANT"
                ],
            "scale":"1",
            "timeframeSelectType":"fullYears",
            "years":
                [
                    "2022","2023"
                ],
            "startDate":None,
            "endDate":None,
            "startMonth":None,
            "endMonth":None,
            "yearsTimeline":"Annual"
        },
        "countries":{
            "aggregation":"Aggregate Countries",
            "countries":[],
            "countriesExpanded":
                [
                    {
                        "name":"All Countries",
                        "value":"all"
                    }
                ],
            "countriesSelectType":"all",
            "countryGroups":{
                "systemGroups":[],
                "userGroups":[]
            }
        }
    },
    "sortingAndDataFormat":{
        "DataSort":{
            "columnOrder":[],
            "fullColumnOrder":[],
            "sortOrder":[]
        },
        "reportCustomizations":{
            "exportCombineTables":False,
            "showAllSubtotal":True,
            "subtotalRecords":"",
            "totalRecords":"20000",
            "exportRawData":False
        }
    }
}
token = os.getenv("USITC_API_KEY")
baseUrl = 'https://datawebws.usitc.gov/dataweb'
headers = {
    "Content-Type": "application/json; charset=utf-8", 
    "Authorization": "Bearer " + token
}
requests.packages.urllib3.disable_warnings() 

requestData = basicQuery
response = requests.get(baseUrl+"/api/v2/savedQuery/getAllSavedQueries", 
                        headers=headers, verify=False)
response