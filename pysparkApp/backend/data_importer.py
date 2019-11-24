import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, unix_timestamp, udf
from pyspark.sql.types import IntegerType

DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"
NEIGHBORHOODS = ["Seacliff", "Lake Street", "Presidio National Park", "Presidio Terrace", "Inner Richmond",
                 "Sutro Heights", "Lincoln Park / Ft. Miley", "Outer Richmond", "Golden Gate Park", "Presidio Heights",
                 "Laurel Heights / Jordan Par", "Lone Mountain", "Anza Vista", "Cow Hollow", "Union Street", "Nob Hill",
                 "Marina", "Telegraph Hill", "Downtown / Union Square", "Tenderloin", "Civic Center", "Hayes Valley",
                 "Alamo Square", "Panhandle", "Haight Ashbury", "Lower Haight", "Mint Hill", "Duboce Triangle",
                 "Cole Valley", "Rincon Hill", "South Beach", "South of Market", "Showplace Square", "Mission Bay",
                 "Yerba Buena Island", "Treasure Island", "Mission Dolores", "Castro", "Outer Sunset", "Parkside",
                 "Stonestown", "Parkmerced", "Lakeshore", "Golden Gate Heights", "Forest Hill", "West Portal",
                 "Clarendon Heights", "Midtown Terrace", "Laguna Honda", "Lower Nob Hill", "Upper Market",
                 "Dolores Heights", "Mission", "Potrero Hill", "Dogpatch", "Central Waterfront", "Diamond Heights",
                 "Crocker Amazon", "Fairmount", "Peralta Heights", "Holly Park", "Merced Manor", "Balboa Terrace",
                 "Ingleside", "Merced Heights", "Outer Mission", "Ingleside Terraces", "Mt. Davidson Manor",
                 "Monterey Heights", "Westwood Highlands", "Westwood Park", "Miraloma Park", "McLaren Park",
                 "Sunnydale", "Visitacion Valley", "India Basin", "Northern Waterfront", "Hunters Point",
                 "Candlestick Point SRA", "Cayuga", "Oceanview", "Apparel City", "Bernal Heights", "Noe Valley",
                 "Produce Market", "Bayview", "Silver Terrace", "Bret Harte", "Little Hollywood", "Excelsior",
                 "Portola", "University Mound", "St. Marys Park", "Mission Terrace", "Sunnyside", "Glen Park",
                 "Western Addition", "Aquatic Park / Ft. Mason", "Fishermans Wharf", "Cathedral Hill", "Japantown",
                 "Pacific Heights", "Lower Pacific Heights", "Chinatown", "Polk Gulch", "North Beach", "Russian Hill",
                 "Financial District", "Inner Sunset", "Parnassus Heights", "Forest Knolls", "Buena Vista",
                 "Corona Heights", "Ashbury Heights", "Eureka Valley", "St. Francis Wood", "Sherwood Forest"]
CATEGORIES = ["Street and Sidewalk Cleaning", "Graffiti", "Abandoned Vehicle", "Encampments", "MUNI Feedback",
              "Parking Enforcement", "Damaged Property", "Sewer Issues", "General Request - PUBLIC WORKS",
              "Tree Maintenance", "General Request - MTA", "Streetlights", "SFHA Requests", "Street Defects",
              "Litter Receptacles", "Illegal Postings", "Rec and Park Requests", "Sign Repair", "Sidewalk or Curb",
              "Blocked Street or SideWalk", "Homeless Concerns", "General Request - PUC", "Temporary Sign Request",
              "General Request - DPH", "Noise Report", "311 External Request", "General Request - COUNTY CLERK",
              "Color Curb", "General Request - 311CUSTOMERSERVICECENTER", "Residential Building Request",
              "Construction Zone Permits", "General Request - ASSESSOR RECORDER",
              "General Request - BUILDING INSPECTION", "Catch Basin Maintenance",
              "General Request - ANIMAL CARE CONTROLKB Events", "General Request - CITY ATTORNEY",
              "General Request - RPD", "General Request - FIRE DEPARTMENT",
              "General Request - MONSDPW Volunteer Programs", "General Request - DTIS", "General Request - SFPD",
              "General Request - DISTRICT ATTORNEY", "General Request - PLANNING", "General Request - PORT AUTHORITY",
              "General Request - TT COLLECTOR", "General Request - HUMAN SERVICES AGENCY",
              "General Request - BOARD OF SUPERVISORS", "General Request - ENTERTAINMENT COMMISSION",
              "General Request - DPT", "General Request - HUMAN RESOURCES", "General Request - ART COMMISSION",
              "General Request - TAXI COMMISSION", "General Request - ENVIRONMENT", "General Request - RENT BOARD",
              "General Request -", "General Request - CITYADMINISTRATOR GSA", "General Request - MOD",
              "General Request - CITY HALL", "General Request - MOCD", "General Request - OCC",
              "General Request - ELECTIONS", "General Request - HSH", "General Request - LABOR STANDARDS ENFORCEMENT",
              "General Request - SMALL BUSINESS", "General Request - LIBRARY PUBLIC", "General Request - SHERIFF",
              "General Request - MOH", "General Request - CONTROLLER", "General Request - CENTRAL SHOPS",
              "General Request - REAL ESTATE DEPARTMENT", "General Request - AGING ADULT SERVICES",
              "General Request - CONTRACT ADMINISTRATION", "General Request - HUMAN RIGHTS COMMISSION",
              "General Request - FILM COMMISSION", "General Request - ECONOMICS AND WORKFORCE DEVELOPMENT",
              "General Request - CHILDSUPPORTSERVICES", "General Request - MOCJ", "General Request - TIDA",
              "General Request - AIRPORT SFO", "General Request - MEDICAL EXAMINER",
              "General Request - SHORT TERM RENTALS", "General Request - CHILDREN YOUTH FAMILIES",
              "General Request - PUBLIC DEFENDER", "General Request - ADULT PROBATION", "General Request - DEM",
              "General Request - ETHICS COMMISSION", "General Request - AGINGADULTSERVICES",
              "General Request - STATUS OF WOMEN", "General Request - WAR MEMORIAL", "General Request - CITY COLLEGE",
              "General Request - MUNI", "General Request - CONVENTION FACILITIES",
              "General Request - LANGUAGE SERVICES", "General Request - RISK MANAGEMENT",
              "General Request - GRANTS FOR THE ARTS", "General Request - JUVENILE PROBATION",
              "General Request - BOARD OF APPEALS", "General Request - MUSEUMS",
              "General Request - REDEVELOPMENT AGENCY"]

if __name__ == "__main__":
    # SparkContext is old tech! Therefore we use the modern SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .config('spark.driver.host', '127.0.0.1') \
        .config("spark.jars", "/backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "60s") \
        .appName("SfDataImporter") \
        .getOrCreate()

    # Read csv file
    # The multiline config is necessary to support strings with line breaks in the csv file
    file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/311_Cases.csv"
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", "true") \
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
        .load(file) \
        # .limit(10)  # For testing purposes

    # Remove rows missing category or location information
    df = df.where(df["Category"].isNotNull() & df["Neighborhood"].isNotNull())

    # User defined function for calculating category and neighborhood numbers
    category_id = udf(
        lambda category_str: CATEGORIES.index(category_str),  # Conversion function
        IntegerType()  # Return type
    )
    neighborhood_id = udf(
        lambda neighborhood_str: NEIGHBORHOODS.index(neighborhood_str),
        IntegerType()
    )

    # Select useful columns and rename them
    # We also convert timestamp strings into seconds since epoch and prepare some columns for our row keys
    df = df.select(unix_timestamp(to_timestamp("Opened", "MM/dd/yyyy hh:mm:ss a")).alias("opened"),
                   df["Status Notes"].alias("status_notes"),
                   df["Category"].alias("category"),
                   category_id("Category").alias("category_id"),
                   df["Request Type"].alias("request_type"),
                   df["Request Details"].alias("request_details"),
                   df["Neighborhood"].alias("neighborhood"),
                   neighborhood_id("Neighborhood").alias("neighborhood_id"),
                   df["Address"].alias("address"),
                   df["Street"].alias("street"),
                   df["Latitude"].alias("latitude"),
                   df["Longitude"].alias("longitude"),
                   df["Responsible Agency"].alias("responsible_agency"),
                   df["Supervisor District"].alias("supervisor_district"),
                   df["Police District"].alias("police_district"),
                   df["Source"].alias("source"),
                   unix_timestamp(to_timestamp("Updated", "MM/dd/yyyy hh:mm:ss a")).alias("updated"),
                   df["Status"].alias("status"),
                   df["CaseID"].alias("case_id"))

    # Describes how to convert between HBase tables and DataFrames
    # Types: string, int, float, double, boolean, tinyint (byte), smallint (short), bigint (long)
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"service_cases"},
        "rowkey":"key_neighborhood:key_category:key_case_id",
        "columns":{
            "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
            "category_id":{"cf":"rowkey", "col":"key_category", "type":"int"},
            "case_id":{"cf":"rowkey", "col":"key_case_id", "type":"bigint"},
            
            "opened":{"cf":"a", "col":"opened", "type":"bigint"},
            "status_notes":{"cf":"a", "col":"status_notes", "type":"string"},
            "category":{"cf":"a", "col":"category", "type":"string"},
            "request_type":{"cf":"a", "col":"request_type", "type":"string"},
            "request_details":{"cf":"a", "col":"request_details", "type":"string"},
            "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},
            
            "address":{"cf":"l", "col":"address", "type":"string"},            
            "street":{"cf":"l", "col":"street", "type":"string"},            
            "latitude":{"cf":"l", "col":"latitude", "type":"double"},            
            "longitude":{"cf":"l", "col":"longitude", "type":"double"},
            
            "updated":{"cf":"m", "col":"updated", "type":"bigint"},
            "status":{"cf":"m", "col":"status", "type":"string"},
            "responsible_agency":{"cf":"m", "col":"responsible_agency", "type":"string"},
            "supervisor_district":{"cf":"m", "col":"supervisor_district", "type":"int"},
            "police_district":{"cf":"m", "col":"police_district", "type":"string"},
            "source":{"cf":"m", "col":"source", "type":"string"}
        }
    }""".split())
    # catalog = ''.join("""{
    #     "table":{"namespace":"default", "name":"service_cases"},
    #     "rowkey":"key",
    #     "columns":{
    #         "case_id":{"cf":"rowkey", "col":"key", "type":"string"},
    #         "opened":{"cf":"a", "col":"opened", "type":"bigint"},
    #         "status_notes":{"cf":"a", "col":"status_notes", "type":"string"},
    #         "category":{"cf":"a", "col":"category", "type":"string"},
    #         "request_type":{"cf":"a", "col":"request_type", "type":"string"},
    #         "request_details":{"cf":"a", "col":"request_details", "type":"string"},
    #         "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},
    #         "address":{"cf":"l", "col":"address", "type":"string"},            
    #         "street":{"cf":"l", "col":"street", "type":"string"},            
    #         "latitude":{"cf":"l", "col":"latitude", "type":"string"},            
    #         "longitude":{"cf":"l", "col":"longitude", "type":"string"},
    #         "updated":{"cf":"m", "col":"updated", "type":"bigint"},
    #         "status":{"cf":"m", "col":"status", "type":"string"},
    #         "responsible_agency":{"cf":"m", "col":"responsible_agency", "type":"string"},
    #         "supervisor_district":{"cf":"m", "col":"supervisor_district", "type":"string"},
    #         "police_district":{"cf":"m", "col":"police_district", "type":"string"},
    #         "source":{"cf":"m", "col":"source", "type":"string"}
    #     }
    # }""".split())

    # simple_catalog = ''.join("""{
    #         "table":{"namespace":"default", "name":"service_cases"},
    #         "rowkey":"key",
    #         "columns":{
    #             "id":{"cf":"rowkey", "col":"key", "type":"string"},
    #             "name":{"cf":"a", "col":"category", "type":"string"}
    #         }
    #     }""".split())
    #
    # simple_df = spark.sparkContext.parallelize({Person("4", "Kasper"),
    #                                             Person("5", "John"),
    #                                             Person("6", "Catherine")}).toDF()

    # Store data in HBase table
    df.write.options(catalog=catalog, newtable="5").format(DATA_SOURCE_FORMAT).save()
    # simple_df.write.options(catalog=simple_catalog, newtable="5").format(DATA_SOURCE_FORMAT).save()

    # Read the table contents
    # df = sql.read.options(catalog=catalog).format(DATA_SOURCE_FORMAT).load()
    # df.show()
