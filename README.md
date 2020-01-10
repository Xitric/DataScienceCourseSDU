# Files

All the file necessary to use this project are available on [OneDrive](https://syddanskuni-my.sharepoint.com/:f:/g/personal/kdavi16_student_sdu_dk/Ev372veNCAhNiWASCXJZ8BUBAb4xyHgWPrM-2ROMWIif3Q?e=enLgO9)

# How to get started

In order to set up this repository on your local computer, you must do the following:
1. Clone the repository to your local computer
2. Download the prepared volumes from [OneDrive](https://syddanskuni-my.sharepoint.com/:f:/g/personal/kdavi16_student_sdu_dk/Ev372veNCAhNiWASCXJZ8BUBAb4xyHgWPrM-2ROMWIif3Q?e=enLgO9), and extract the zip file
3. From the root of the project, run _import/import.cmd_
    * Use the location of the extracted volumes as input, such as _C:Users/Name/Desktop/Volumes_
4. Start the cluster using _start.cmd_
5. Wait for [HDFS](http://localhost:9870/) to exit safemode and [HBase](http://localhost:16010/master-status) to initialize. This might take a few minutes
6. You can now view different visualizations on [localhost](http://localhost:3000/admin)

# Executing jobs in the cluster

1. Go to the [admin panel](http://localhost:3000/admin)
2. Ensure that all files necessary for the job are uploaded under "Upload" with the type "Spark application"
    * If using the volumes provided on [OneDrive](https://syddanskuni-my.sharepoint.com/:f:/g/personal/kdavi16_student_sdu_dk/Ev372veNCAhNiWASCXJZ8BUBAb4xyHgWPrM-2ROMWIif3Q?e=enLgO9), this has already been done
3. Under "Submit Spark application", write the name of the job to execute, such as _incident_aggregator_ and press "Submit"
4. The status of the job is most easily tracked on [Livy](http://localhost:8998/ui) or by using the "Spark job status" on the admin page

# Visualizations

Various visualizations are available from the [index page](http://localhost:3000/)
