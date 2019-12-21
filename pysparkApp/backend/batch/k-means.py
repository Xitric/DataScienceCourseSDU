from argparse import ArgumentParser

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

from util.spark_session_utils import get_spark_session_instance

categories_service = ["Abandoned Vehicle", "Blocked Street or SideWalk",
                      "Catch Basin Maintenance", "Color Curb", "DPW Volunteer Programs",
                      "Damaged Property", "Encampments", "Graffiti", "Homeless Concerns",
                      "Illegal Postings", "Litter Receptacles", "MUNI Feedback",
                      "Noise Report", "Parking Enforcement", "Rec and Park Requests",
                      "Residential Building Request", "SFHA Requests", "Sewer Issues",
                      "Sidewalk or Curb", "Sign Repair", "Street Defects",
                      "Street and Sidewalk Cleaning", "Streetlights",
                      "Temporary Sign Request", "Tree Maintenance"]

categories_incident = ["Arson", "Assault", "Burglary", "Case Closure",
                       "Civil Sidewalks", "Courtesy Report", "Disorderly Conduct",
                       "Drug Offense", "Drug Violation", "Embezzlement", "Family Offense",
                       "Fire Report", "Forgery And Counterfeiting", "Fraud", "Gambling",
                       "Homicide", "Human Trafficking (A), Commercial Sex Acts",
                       "Human Trafficking, Commercial Sex Acts", "Larceny Theft",
                       "Liquor Laws", "Lost Property", "Malicious Mischief",
                       "Miscellaneous Investigation", "Missing Person", "Motor Vehicle Theft",
                       "Motor Vehicle Theft?", "Non-Criminal",
                       "Offences Against The Family And Children", "Other",
                       "Other Miscellaneous", "Other Offenses", "Prostitution", "Rape",
                       "Recovered Vehicle", "Robbery", "Sex Offense", "Stolen Property",
                       "Suicide", "Suspicious", "Suspicious Occ", "Traffic Collision",
                       "Traffic Violation Arrest", "Vandalism", "Vehicle Impounded",
                       "Vehicle Misplaced", "Warrant", "Weapons Carrying Etc",
                       "Weapons Offence", "Weapons Offense"]

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("catServ", type=str, default="")
    parser.add_argument("catInci", type=str, default="")
    args = parser.parse_args()

    # If both are empty, just do the commands with all the categories
    if args.catServ == "" and args.catInci == "":
        combined_category = categories_service + categories_incident
    else:
        if args.catServ != "":
            catString = args.catServ.replace("+", " ")

            if args.catInci != "":
                catString += ";" + args.catInci.replace("+", " ")
        else:
            catString = args.catInci.replace("+", " ")

        combined_category = catString.split(';')

    spark = get_spark_session_instance()

    # Get the rates from the different categories in services
    df_service = spark.read.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').load().na.fill(0)

    # Get the rates from the different categories in incident incidents
    df_inci = spark.read.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='incident_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').load().na.fill(0)

    # Join the two DataFrames on the neighborhood column
    df_comb = df_service.join(df_inci, "neighborhood")

    # Generate a vector with all features to do k-mean analysis on
    vecAssembler = VectorAssembler(inputCols=combined_category, outputCol="features")
    new_df = vecAssembler.transform(df_comb)

    # Make the k-means setup and use the fitting on the DataFrame
    kmeans = KMeans(k=6, seed=1, maxIter=50)
    model = kmeans.fit(new_df.select('features'))

    # Put the k-means model together with the DataFrame
    df_trans = model.transform(new_df)

    # Select only what we need from the DataFrame
    df_cluster = df_trans.select("neighborhood",
                                 df_trans["prediction"].alias("cluster"))

    # Save to MySQL
    df_cluster.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='kmeans',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()
