from datetime import datetime
import os
from dotenv import load_dotenv
from airflow.sdk import dag, task
import clickhouse_connect
import psycopg2

load_dotenv()

def pg():
    return psycopg2.connect(
        dbname=os.getenv("ADVENTUREWORKS_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        secure=True
    )

def get_target_table_name(schema: str, source_table: str) -> str:
    config = TABLE_CONFIG.get(schema, {}).get(source_table, {})
    return config.get("target_name", source_table)

def make_pg_safe(cols: list[str]) -> list[str]:
    return [f'"{c}"' for c in cols]

TABLE_CONFIG = {
    ## Schemas
    "hr": {
        "d": {
            "target_name": "department",
            "columns": ["departmentid", "name", "groupname", "modifieddate"],
            "pk_columns": ["departmentid"],
            "columns_with_types": [
                "departmentid UInt64",
                "name String",
                "groupname String",
                "modifieddate DateTime"
            ]
        },
        "e": {
            "target_name": "employee",
            "columns": ["businessentityid", "nationalidnumber", "loginid", "organizationnode", "jobtitle", "birthdate", "maritalstatus", "gender", "hiredate", "salariedflag", "vacationhours", "sickleavehours", "currentflag", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "nationalidnumber String",
                "loginid String",
                "organizationnode String",
                "jobtitle String",
                "birthdate Date",
                "maritalstatus String",
                "gender String",
                "hiredate Date",
                "salariedflag Bool",
                "vacationhours UInt16",
                "sickleavehours UInt16",
                "currentflag Bool",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "edh": {
            "target_name": "employee_department_history",
            "columns": ["businessentityid", "departmentid", "shiftid", "startdate", "enddate", "modifieddate"],
            "pk_columns": ["businessentityid", "startdate", "departmentid", "shiftid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "departmentid UInt64",
                "shiftid UInt8",
                "startdate Date",
                "enddate Date",
                "modifieddate DateTime"
            ]
        },
        "jc": {
            "target_name": "job_candidate",
            "columns": ["jobcandidateid", "businessentityid", "resume", "modifieddate"],
            "pk_columns": ["jobcandidateid"],
            "columns_with_types": [
                "jobcandidateid UInt64",
                "businessentityid UInt64",
                "resume String",
                "modifieddate DateTime"
            ]
        },
        "s": {
            "target_name": "shift",
            "columns": ["shiftid", "name", "starttime", "endtime", "modifieddate"],
            "pk_columns": ["shiftid"],
            "columns_with_types": [
                "shiftid UInt64",
                "name String",
                "starttime DateTime",
                "endtime DateTime",
                "modifieddate DateTime"
            ]
        }
    },
    "pe": {
        "a": {
            "target_name": "address",
            "columns": ["addressid", "addressline1", "addressline2", "city", "stateprovinceid", "postalcode", "spatiallocation", "rowguid", "modifieddate"],
            "pk_columns": ["addressid"],
            "columns_with_types": [
                "addressid UInt64",
                "addressline1 String",
                "addressline2 String",
                "city String",
                "stateprovinceid UInt64",
                "postalcode String",
                "spatiallocation String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "at": {
            "target_name": "address_type",
            "columns": ["addresstypeid", "name", "rowguid", "modifieddate"],
            "pk_columns": ["addresstypeid"],
            "columns_with_types": [
                "addresstypeid UInt64",
                "name String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "be": {
            "target_name": "business_entity",
            "columns": ["businessentityid", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "bea": {
            "target_name": "business_entity_address",
            "columns": ["businessentityid", "addressid", "addresstypeid", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid", "addressid", "addresstypeid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "addressid UInt64",
                "addresstypeid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "bec": {
            "target_name": "business_entity_contact",
            "columns": ["businessentityid", "personid", "contacttypeid", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid", "personid", "contacttypeid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "personid UInt64",
                "contacttypeid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "ct": {
            "target_name": "contact_type",
            "columns": ["contacttypeid", "name", "modifieddate"],
            "pk_columns": ["contacttypeid"],
            "columns_with_types": [
                "contacttypeid UInt64",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "cr": {
            "target_name": "country_region",
            "columns": ["countryregioncode", "name", "modifieddate"],
            "pk_columns": ["countryregioncode"],
            "columns_with_types": [
                "countryregioncode String",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "e": {
            "target_name": "email_address",
            "columns": ["businessentityid", "emailaddressid", "emailaddress", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid", "emailaddressid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "emailaddressid UInt64",
                "emailaddress String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "p": {
            "target_name": "person",
            "columns": ["businessentityid", "persontype", "namestyle", "title", "firstname", "middlename", "lastname", "suffix", "emailpromotion", "additionalcontactinfo", "demographics", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "persontype String",
                "namestyle Bool",
                "title String",
                "firstname String",
                "middlename String",
                "lastname String",
                "suffix String",
                "emailpromotion UInt32",
                "additionalcontactinfo String",
                "demographics String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pa": {
            "target_name": "password",
            "columns": ["businessentityid", "passwordhash", "passwordsalt", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "passwordhash String",
                "passwordsalt String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pp": {
            "target_name": "person_phone",
            "columns": ["businessentityid", "phonenumber", "phonenumbertypeid", "modifieddate"],
            "pk_columns": ["businessentityid", "phonenumber", "phonenumbertypeid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "phonenumber String",
                "phonenumbertypeid UInt64",
                "modifieddate DateTime"
            ]
        },
        "pnt": {
            "target_name": "phone_number_type",
            "columns": ["phonenumbertypeid", "name", "modifieddate"],
            "pk_columns": ["phonenumbertypeid"],
            "columns_with_types": [
                "phonenumbertypeid UInt64",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "sp": {
            "target_name": "state_province",
            "columns": ["stateprovinceid", "stateprovincecode", "countryregioncode", "isonlystateprovinceflag", "name", "territoryid", "rowguid", "modifieddate"],
            "pk_columns": ["stateprovinceid"],
            "columns_with_types": [
                "stateprovinceid UInt64",
                "stateprovincecode String",
                "countryregioncode String",
                "isonlystateprovinceflag Bool",
                "name String",
                "territoryid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        }
    },
    "pr": {
        "bom": {
            "target_name": "bill_of_materials",
            "columns": ["billofmaterialsid", "productassemblyid", "componentid", "startdate", "enddate", "unitmeasurecode", "bomlevel", "perassemblyqty", "modifieddate"],
            "pk_columns": ["billofmaterialsid"],
            "columns_with_types": [
                "billofmaterialsid UInt64",
                "productassemblyid UInt64",
                "componentid UInt64",
                "startdate Date",
                "enddate Date",
                "unitmeasurecode String",
                "bomlevel UInt16",
                "perassemblyqty Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "c": {
            "target_name": "culture",
            "columns": ["cultureid", "name", "modifieddate"],
            "pk_columns": ["cultureid"],
            "columns_with_types": [
                "cultureid String",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "d": {
            "target_name": "document",
            "columns": ["documentnode", "title", "owner", "folderflag", "filename", "fileextension", "revision", "changenumber", "status", "documentsummary", "document", "rowguid", "modifieddate"],
            "pk_columns": ["documentnode"],
            "columns_with_types": [
                "documentnode String",
                "title String",
                "owner UInt64",
                "folderflag Bool",
                "filename String",
                "fileextension String",
                "revision String",
                "changenumber UInt64",
                "status UInt16",
                "documentsummary String",
                "document String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "i": {
            "target_name": "illustration",
            "columns": ["illustrationid", "diagram", "modifieddate"],
            "pk_columns": ["illustrationid"],
            "columns_with_types": [
                "illustrationid UInt64",
                "diagram String",
                "modifieddate DateTime"
            ]
        },
        "l": {
            "target_name": "location",
            "columns": ["locationid", "name", "costrate", "availability", "modifieddate"],
            "pk_columns": ["locationid"],
            "columns_with_types": [
                "locationid UInt64",
                "name String",
                "costrate Decimal(19,4)",
                "availability Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "p": {
            "target_name": "product",
            "columns": ["productid", "name", "productnumber", "makeflag", "finishedgoodsflag", "color", "safetystocklevel", "reorderpoint", "standardcost", "listprice", "size", "sizeunitmeasurecode", "weightunitmeasurecode", "weight", "daystomanufacture", "productline", "class", "style", "productsubcategoryid", "productmodelid", "sellstartdate", "sellenddate", "discontinueddate", "rowguid", "modifieddate"],
            "pk_columns": ["productid"],
            "columns_with_types": [
                "productid UInt64",
                "name String",
                "productnumber String",
                "makeflag Bool",
                "finishedgoodsflag Bool",
                "color String",
                "safetystocklevel UInt16",
                "reorderpoint UInt16",
                "standardcost Decimal(19,4)",
                "listprice Decimal(19,4)",
                "size String",
                "sizeunitmeasurecode String",
                "weightunitmeasurecode String",
                "weight Decimal(19,4)",
                "daystomanufacture UInt16",
                "productline String",
                "class String",
                "style String",
                "productsubcategoryid UInt64",
                "productmodelid UInt64",
                "sellstartdate Date",
                "sellenddate Date",
                "discontinueddate Date",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pc": {
            "target_name": "product_category",
            "columns": ["productcategoryid", "name", "rowguid", "modifieddate"],
            "pk_columns": ["productcategoryid"],
            "columns_with_types": [
                "productcategoryid UInt64",
                "name String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pch": {
            "target_name": "product_cost_history",
            "columns": ["productid", "startdate", "enddate", "standardcost", "modifieddate"],
            "pk_columns": ["productid", "startdate"],
            "columns_with_types": [
                "productid UInt64",
                "startdate Date",
                "enddate Date",
                "standardcost Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "pd": {
            "target_name": "product_description",
            "columns": ["productdescriptionid", "description", "rowguid", "modifieddate"],
            "pk_columns": ["productdescriptionid"],
            "columns_with_types": [
                "productdescriptionid UInt64",
                "description String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pdoc": {
            "target_name": "product_document",
            "columns": ["productid", "documentnode", "modifieddate"],
            "pk_columns": ["productid", "documentnode"],
            "columns_with_types": [
                "productid UInt64",
                "documentnode String",
                "modifieddate DateTime"
            ]
        },
        "pi": {
            "target_name": "product_inventory",
            "columns": ["productid", "locationid", "shelf", "bin", "quantity", "rowguid", "modifieddate"],
            "pk_columns": ["productid", "locationid"],
            "columns_with_types": [
                "productid UInt64",
                "locationid UInt64",
                "shelf String",
                "bin String",
                "quantity UInt16",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "plph": {
            "target_name": "product_list_price_history",
            "columns": ["productid", "startdate", "enddate", "listprice", "modifieddate"],
            "pk_columns": ["productid", "startdate"],
            "columns_with_types": [
                "productid UInt64",
                "startdate Date",
                "enddate Date",
                "listprice Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "pm": {
            "target_name": "product_model",
            "columns": ["productmodelid", "name", "catalogdescription", "instructions", "rowguid", "modifieddate"],
            "pk_columns": ["productmodelid"],
            "columns_with_types": [
                "productmodelid UInt64",
                "name String",
                "catalogdescription String",
                "instructions String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "pmi": {
            "target_name": "product_model_illustration",
            "columns": ["productmodelid", "illustrationid", "modifieddate"],
            "pk_columns": ["productmodelid", "illustrationid"],
            "columns_with_types": [
                "productmodelid UInt64",
                "illustrationid UInt64",
                "modifieddate DateTime"
            ]
        },
        "pmpdc": {
            "target_name": "product_model_product_description_culture",
            "columns": ["productmodelid", "productdescriptionid", "cultureid", "modifieddate"],
            "pk_columns": ["productmodelid", "productdescriptionid", "cultureid"],
            "columns_with_types": [
                "productmodelid UInt64",
                "productdescriptionid UInt64",
                "cultureid String",
                "modifieddate DateTime"
            ]
        },
        "pp": {
            "target_name": "product_photo",
            "columns": ["productphotoid", "thumbnailphoto", "thumbnailphotofilename", "largephoto", "largephotofilename", "modifieddate"],
            "pk_columns": ["productphotoid"],
            "columns_with_types": [
                "productphotoid UInt64",
                "thumbnailphoto String",
                "thumbnailphotofilename String",
                "largephoto String",
                "largephotofilename String",
                "modifieddate DateTime"
            ]
        },
        "ppp": {
            "target_name": "product_product_photo",
            "columns": ["productid", "productphotoid", "primary", "modifieddate"],
            "pk_columns": ["productid", "productphotoid"],
            "columns_with_types": [
                "productid UInt64",
                "productphotoid UInt64",
                "`primary` Bool",
                "modifieddate DateTime"
            ]
        },
        "pr": {
            "target_name": "product_review",
            "columns": ["productreviewid", "productid", "reviewername", "reviewdate", "emailaddress", "rating", "comments", "modifieddate"],
            "pk_columns": ["productreviewid"],
            "columns_with_types": [
                "productreviewid UInt64",
                "productid UInt64",
                "reviewername String",
                "reviewdate Date",
                "emailaddress String",
                "rating UInt16",
                "comments String",
                "modifieddate DateTime"
            ]
        },
        "psc": {
            "target_name": "product_subcategory",
            "columns": ["productsubcategoryid", "productcategoryid", "name", "rowguid", "modifieddate"],
            "pk_columns": ["productsubcategoryid"],
            "columns_with_types": [
                "productsubcategoryid UInt64",
                "productcategoryid UInt64",
                "name String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sr": {
            "target_name": "scrap_reason",
            "columns": ["scrapreasonid", "name", "modifieddate"],
            "pk_columns": ["scrapreasonid"],
            "columns_with_types": [
                "scrapreasonid UInt64",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "th": {
            "target_name": "transaction_history",
            "columns": ["transactionid", "productid", "referenceorderid", "referenceorderlineid", "transactiondate", "transactiontype", "quantity", "actualcost", "modifieddate"],
            "pk_columns": ["transactionid"],
            "columns_with_types": [
                "transactionid UInt64",
                "productid UInt64",
                "referenceorderid UInt64",
                "referenceorderlineid UInt64",
                "transactiondate DateTime",
                "transactiontype String",
                "quantity UInt16",
                "actualcost Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "tha": {
            "target_name": "transaction_history_archive",
            "columns": ["transactionid", "productid", "referenceorderid", "referenceorderlineid", "transactiondate", "transactiontype", "quantity", "actualcost", "modifieddate"],
            "pk_columns": ["transactionid"],
            "columns_with_types": [
                "transactionid UInt64",
                "productid UInt64",
                "referenceorderid UInt64",
                "referenceorderlineid UInt64",
                "transactiondate DateTime",
                "transactiontype String",
                "quantity UInt16",
                "actualcost Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "um": {
            "target_name": "unit_measure",
            "columns": ["unitmeasurecode", "name", "modifieddate"],
            "pk_columns": ["unitmeasurecode"],
            "columns_with_types": [
                "unitmeasurecode String",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "w": {
            "target_name": "work_order",
            "columns": ["workorderid", "productid", "orderqty", "scrappedqty", "startdate", "enddate", "duedate", "scrapreasonid", "modifieddate"],
            "pk_columns": ["workorderid"],
            "columns_with_types": [
                "workorderid UInt64",
                "productid UInt64",
                "orderqty UInt16",
                "scrappedqty UInt16",
                "startdate Date",
                "enddate Date",
                "duedate Date",
                "scrapreasonid UInt64",
                "modifieddate DateTime"
            ]
        },
        "wr": {
            "target_name": "work_order_routing",
            "columns": ["workorderid", "productid", "operationsequence", "locationid", "scheduledstartdate", "scheduledenddate", "actualstartdate", "actualenddate", "actualresourcehrs", "plannedcost", "actualcost", "modifieddate"],
            "pk_columns": ["workorderid", "productid", "operationsequence"],
            "columns_with_types": [
                "workorderid UInt64",
                "productid UInt64",
                "operationsequence UInt64",
                "locationid UInt64",
                "scheduledstartdate DateTime",
                "scheduledenddate DateTime",
                "actualstartdate DateTime",
                "actualenddate DateTime",
                "actualresourcehrs Decimal(19,4)",
                "plannedcost Decimal(19,4)",
                "actualcost Decimal(19,4)",
                "modifieddate DateTime"
            ]
        }
    },
    "pu": {
        "pod": {
            "target_name": "purchase_order_detail",
            "columns": ["purchaseorderid", "purchaseorderdetailid", "duedate", "orderqty", "productid", "unitprice", "receivedqty", "rejectedqty", "modifieddate"],
            "pk_columns": ["purchaseorderid", "purchaseorderdetailid"],
            "columns_with_types": [
                "purchaseorderid UInt64",
                "purchaseorderdetailid UInt64",
                "duedate Date",
                "orderqty UInt16",
                "productid UInt64",
                "unitprice Decimal(19,4)",
                "receivedqty UInt16",
                "rejectedqty UInt16",
                "modifieddate DateTime"
            ]
        },
        "poh": {
            "target_name": "purchase_order_header",
            "columns": ["purchaseorderid", "revisionnumber", "status", "employeeid", "vendorid", "shipmethodid", "orderdate", "shipdate", "subtotal", "taxamt", "freight", "modifieddate"],
            "pk_columns": ["purchaseorderid"],
            "columns_with_types": [
                "purchaseorderid UInt64",
                "revisionnumber UInt16",
                "status UInt16",
                "employeeid UInt64",
                "vendorid UInt64",
                "shipmethodid UInt64",
                "orderdate Date",
                "shipdate Date",
                "subtotal Decimal(19,4)",
                "taxamt Decimal(19,4)",
                "freight Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "pv": {
            "target_name": "product_vendor",
            "columns": ["productid", "businessentityid", "averageleadtime", "standardprice", "lastreceiptcost", "lastreceiptdate", "minorderqty", "maxorderqty", "onorderqty", "unitmeasurecode", "modifieddate"],
            "pk_columns": ["productid", "businessentityid"],
            "columns_with_types": [
                "productid UInt64",
                "businessentityid UInt64",
                "averageleadtime UInt16",
                "standardprice Decimal(19,4)",
                "lastreceiptcost Decimal(19,4)",
                "lastreceiptdate Date",
                "minorderqty UInt16",
                "maxorderqty UInt16",
                "onorderqty UInt16",
                "unitmeasurecode String",
                "modifieddate DateTime"
            ]
        },
        "sm": {
            "target_name": "ship_method",
            "columns": ["shipmethodid", "name", "shipbase", "shiprate", "rowguid", "modifieddate"],
            "pk_columns": ["shipmethodid"],
            "columns_with_types": [
                "shipmethodid UInt64",
                "name String",
                "shipbase Decimal(19,4)",
                "shiprate Decimal(19,4)",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "v": {
            "target_name": "vendor",
            "columns": ["businessentityid", "accountnumber", "name", "creditrating", "preferredvendorstatus", "activeflag", "purchasingwebserviceurl", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "accountnumber String",
                "name String",
                "creditrating UInt16",
                "preferredvendorstatus Bool",
                "activeflag Bool",
                "purchasingwebserviceurl String",
                "modifieddate DateTime"
            ]
        }
    },
    "sa": {
        "c": {
            "target_name": "customer",
            "columns": ["customerid", "personid", "storeid", "territoryid", "rowguid", "modifieddate"],
            "pk_columns": ["customerid"],
            "columns_with_types": [
                "customerid UInt64",
                "personid UInt64",
                "storeid UInt64",
                "territoryid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "cc": {
            "target_name": "credit_card",
            "columns": ["creditcardid", "cardtype", "cardnumber", "expmonth", "expyear", "modifieddate"],
            "pk_columns": ["creditcardid"],
            "columns_with_types": [
                "creditcardid UInt64",
                "cardtype String",
                "cardnumber String",
                "expmonth UInt16",
                "expyear UInt16",
                "modifieddate DateTime"
            ]
        },
        "cr": {
            "target_name": "currency_rate",
            "columns": ["currencyrateid", "currencyratedate", "fromcurrencycode", "tocurrencycode", "averagerate", "endofdayrate", "modifieddate"],
            "pk_columns": ["currencyrateid"],
            "columns_with_types": [
                "currencyrateid UInt64",
                "currencyratedate Date",
                "fromcurrencycode String",
                "tocurrencycode String",
                "averagerate Decimal(19,4)",
                "endofdayrate Decimal(19,4)",
                "modifieddate DateTime"
            ]
        },
        "crc": {
            "target_name": "country_region_currency",
            "columns": ["countryregioncode", "currencycode", "modifieddate"],
            "pk_columns": ["countryregioncode", "currencycode"],
            "columns_with_types": [
                "countryregioncode String",
                "currencycode String",
                "modifieddate DateTime"
            ]
        },
        "cu": {
            "target_name": "currency",
            "columns": ["currencycode", "name", "modifieddate"],
            "pk_columns": ["currencycode"],
            "columns_with_types": [
                "currencycode String",
                "name String",
                "modifieddate DateTime"
            ]
        },
        "pcc": {
            "target_name": "person_credit_card",
            "columns": ["businessentityid", "creditcardid", "modifieddate"],
            "pk_columns": ["businessentityid", "creditcardid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "creditcardid UInt64",
                "modifieddate DateTime"
            ]
        },
        "s": {
            "target_name": "store",
            "columns": ["businessentityid", "name", "salespersonid", "demographics", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "name String",
                "salespersonid UInt64",
                "demographics String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sci": {
            "target_name": "shopping_cart_item",
            "columns": ["shoppingcartitemid", "shoppingcartid", "quantity", "productid", "datecreated", "modifieddate"],
            "pk_columns": ["shoppingcartitemid"],
            "columns_with_types": [
                "shoppingcartitemid UInt64",
                "shoppingcartid UInt64",
                "quantity UInt16",
                "productid UInt64",
                "datecreated DateTime",
                "modifieddate DateTime"
            ]
        },
        "so": {
            "target_name": "special_offer",
            "columns": ["specialofferid", "description", "discountpct", "type", "category", "startdate", "enddate", "minqty", "maxqty", "rowguid", "modifieddate"],
            "pk_columns": ["specialofferid"],
            "columns_with_types": [
                "specialofferid UInt64",
                "description String",
                "discountpct Decimal(19,4)",
                "type String",
                "category String",
                "startdate Date",
                "enddate Date",
                "minqty UInt16",
                "maxqty UInt16",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sod": {
            "target_name": "sales_order_detail",
            "columns": ["salesorderid", "salesorderdetailid", "carriertrackingnumber", "orderqty", "productid", "specialofferid", "unitprice", "unitpricediscount", "rowguid", "modifieddate"],
            "pk_columns": ["salesorderid", "salesorderdetailid"],
            "columns_with_types": [
                "salesorderid UInt64",
                "salesorderdetailid UInt64",
                "carriertrackingnumber String",
                "orderqty UInt16",
                "productid UInt64",
                "specialofferid UInt64",
                "unitprice Decimal(19,4)",
                "unitpricediscount Decimal(19,4)",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "soh": {
            "target_name": "sales_order_header",
            "columns": ["salesorderid", "revisionnumber", "orderdate", "duedate", "shipdate", "status", "onlineorderflag", "purchaseordernumber", "accountnumber", "customerid", "salespersonid", "territoryid", "billtoaddressid", "shiptoaddressid", "shipmethodid", "creditcardid", "creditcardapprovalcode", "currencyrateid", "subtotal", "taxamt", "freight", "totaldue", "comment", "rowguid", "modifieddate"],
            "pk_columns": ["salesorderid"],
            "columns_with_types": [
                "salesorderid UInt64",
                "revisionnumber UInt16",
                "orderdate Date",
                "duedate Date",
                "shipdate Date",
                "status UInt16",
                "onlineorderflag Bool",
                "purchaseordernumber String",
                "accountnumber String",
                "customerid UInt64",
                "salespersonid UInt64",
                "territoryid UInt64",
                "billtoaddressid UInt64",
                "shiptoaddressid UInt64",
                "shipmethodid UInt64",
                "creditcardid UInt64",
                "creditcardapprovalcode String",
                "currencyrateid UInt64",
                "subtotal Decimal(19,4)",
                "taxamt Decimal(19,4)",
                "freight Decimal(19,4)",
                "totaldue Decimal(19,4)",
                "comment String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sohsr": {
            "target_name": "sales_order_header_sales_reason",
            "columns": ["salesorderid", "salesreasonid", "modifieddate"],
            "pk_columns": ["salesorderid", "salesreasonid"],
            "columns_with_types": [
                "salesorderid UInt64",
                "salesreasonid UInt64",
                "modifieddate DateTime"
            ]
        },
        "sop": {
            "target_name": "special_offer_product",
            "columns": ["specialofferid", "productid", "rowguid", "modifieddate"],
            "pk_columns": ["specialofferid", "productid"],
            "columns_with_types": [
                "specialofferid UInt64",
                "productid UInt64",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sp": {
            "target_name": "sales_person",
            "columns": ["businessentityid", "territoryid", "salesquota", "bonus", "commissionpct", "salesytd", "saleslastyear", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "territoryid UInt64",
                "salesquota Decimal(19,4)",
                "bonus Decimal(19,4)",
                "commissionpct Decimal(19,4)",
                "salesytd Decimal(19,4)",
                "saleslastyear Decimal(19,4)",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "spqh": {
            "target_name": "sales_person_quota_history",
            "columns": ["businessentityid", "quotadate", "salesquota", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid", "quotadate"],
            "columns_with_types": [
                "businessentityid UInt64",
                "quotadate Date",
                "salesquota Decimal(19,4)",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sr": {
            "target_name": "sales_reason",
            "columns": ["salesreasonid", "name", "reasontype", "modifieddate"],
            "pk_columns": ["salesreasonid"],
            "columns_with_types": [
                "salesreasonid UInt64",
                "name String",
                "reasontype String",
                "modifieddate DateTime"
            ]
        },
        "st": {
            "target_name": "sales_territory",
            "columns": ["territoryid", "name", "countryregioncode", "group", "salesytd", "saleslastyear", "costytd", "costlastyear", "rowguid", "modifieddate"],
            "pk_columns": ["territoryid"],
            "columns_with_types": [
                "territoryid UInt64",
                "name String",
                "countryregioncode String",
                "group String",
                "salesytd Decimal(19,4)",
                "saleslastyear Decimal(19,4)",
                "costytd Decimal(19,4)",
                "costlastyear Decimal(19,4)",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "sth": {
            "target_name": "sales_territory_history",
            "columns": ["businessentityid", "territoryid", "startdate", "enddate", "rowguid", "modifieddate"],
            "pk_columns": ["businessentityid", "startdate", "territoryid"],
            "columns_with_types": [
                "businessentityid UInt64",
                "territoryid UInt64",
                "startdate Date",
                "enddate Date",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        },
        "tr": {
            "target_name": "sales_tax_rate",
            "columns": ["salestaxrateid", "stateprovinceid", "taxtype", "taxrate", "name", "rowguid", "modifieddate"],
            "pk_columns": ["salestaxrateid"],
            "columns_with_types": [
                "salestaxrateid UInt64",
                "stateprovinceid UInt64",
                "taxtype UInt16",
                "taxrate Decimal(19,4)",
                "name String",
                "rowguid UUID",
                "modifieddate DateTime"
            ]
        }
    }
}

@dag(
    dag_id="adventureworks_etl",
    schedule="@hourly",
    dag_display_name="ETL for AdventureWorks",
    description="AdventureWorks",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "adventureworks", "postgres", "clickhouse"],
)
def sync():
    @task
    def create_tables(target, columns, pk):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DL.{target}
        (
            {",".join(columns)}
        )
        ENGINE = ReplacingMergeTree(modifieddate)
        ORDER BY ({",".join(pk)});
        """

        ddr = f"""
        CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DL.{target}_staging
        AS ADVENTUREWORKS_DL.{target}
        ENGINE = Memory;
        """

        ch().query(ddl)
        ch().query(ddr)

    @task
    def extract_delta(schema, table, columns):
        conn = pg()
        cur = conn.cursor()
        safe_cols = make_pg_safe(columns)
        qry = f"""
        SELECT {", ".join(safe_cols)}
        FROM {schema}.{table}
        WHERE modifieddate > (
            SELECT COALESCE(max(modifieddate), '1900-01-01')
            FROM {schema}.{table}
        );
        """
        cur.execute(qry)
        data = cur.fetchall()
        cur.close()
        conn.close()
        return data

    @task
    def load_staging(target, columns, rows):
        if rows:
            ch().insert(f"{target}_staging", rows, column_names=columns)

    @task
    def merge_upsert(target):
        ch().query(f"""
        INSERT INTO ADVENTUREWORKS_DL.{target}
        SELECT * FROM ADVENTUREWORKS_DL.{target}_staging;
        """)

        ch().query(f"""TRUNCATE TABLE ADVENTUREWORKS_DL.{target}_staging;""")

    @task
    def delete_obsolete(target, pk):
        pk_join = " AND ".join([f"t.{c}=p.{c}" for c in pk])
        ch().query(f"""
            ALTER TABLE ADVENTUREWORKS_DL.{target}
            DELETE WHERE {pk} NOT IN (SELECT {pk} FROM ADVENTUREWORKS_DL.{target}_staging);
        """)

    for schema, tables in TABLE_CONFIG.items():
        for tbl, cfg in tables.items():
            target = cfg["target_name"]
            columns = cfg["columns_with_types"]
            pure_cols = cfg["columns"]
            pk = cfg["pk_columns"]

            create = create_tables.override(task_id=f"create_{target}")(target, columns, pk)
            task_delta = extract_delta.override(task_id=f"delta_{target}")(schema, tbl, pure_cols)
            load = load_staging.override(task_id=f"stage_{target}")(target, pure_cols, task_delta)
            merge = merge_upsert.override(task_id=f"upsert_{target}")(target)
            delete = delete_obsolete.override(task_id=f"delete_obsolete_{target}")(target, pk)

            create >> task_delta >> load >> merge >> delete

sync()