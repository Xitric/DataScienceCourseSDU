from argparse import ArgumentParser
from pyspark.sql import SparkSession


categories_service = ["Abandoned Vehicle", "Blocked Street or SideWalk",
    "Catch Basin Maintenance", "Color Curb"]

categories_incident = ["Arson", "Assault", "Burglary", "Case Closure"]

if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument("catServ", type=str, default="")
    parser.add_argument("catInci", type=str, default="")
    args = parser.parse_args()
    

    if args.catServ != "":
        categories_service = args.catServ.replace("+", " ").split(';')

    if args.catInci != "":
        categories_incident = args.catInci.replace("+", " ").split(';')
    
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Get the rates from the different categories in services
    df_service = spark.read.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').load().na.fill(0)
        
    # Get the rates from the different categories in incident incidents
    df_incident = spark.read.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='incident_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').load().na.fill(0)
    
    df_serv_corr = df_service.select(categories_service + ["neighborhood"])
    df_inci_corr = df_incident.select(categories_incident + ["neighborhood"])
    df = df_serv_corr.join(df_inci_corr, "neighborhood").drop("neighborhood")

    values = []
    for serv in categories_service:
        tup = [serv] + [df.corr(serv, inci) for inci in categories_incident]
        values += [tuple(tup)]

    newrow = spark.createDataFrame(values, tuple(["counter"] + categories_incident))
    
    # newrow.show()

    # Save to MySQL
    newrow.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='correlation',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()
