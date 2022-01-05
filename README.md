# Prodigal-Technologies---Assignment 
This code was written to complete the task for the take home assesment for the Data Engineer position, and involves Designing an ingestion pipeline to fetch Mutual Fund data from the AMFI website, model, and store that data into a database for further processing.


# Deliverables
- [x] The Pipeline can do a full fetch and load and then also fetch incremental data every day
- [x] Deals with failures using try catch blocks and allows rerunning the script on failure
- [x] Initial Fetch and DB Insertion in under 22 seconds on the best run on average takes < 30 seconds
- [x] Queries for MongoDB can be written so that read queries are efficient over a ceratin time period
- [x] Dockerized the solution for anyone to run

## How To Run My Code
My Code is now dockerized and can be run using the dockerfile in my codebase. Please Note there is no need for you to setup any database since the MongoDB atlas is linked directly to my code as a link. Thus all the results are being added to my database table. To make this work on your MongoDB atlas table change the link near the import section, and put your link.

## My Tables
I have used a NoSQL database because integration of MongoDB Atlas was really simple, and there is no need to do it locally as all of it is done online,
this makes it much easier for me to deploy this using docker.

There is only a single Collection in my Cluster called MUTUAL-FUND-DATA
### Schema
![Image of my table row](https://github.com/higgsboson1209/Prodigal-Technologies---Assignment-/blob/main/image.png)

### Final Output
![Screen shot of final output](https://github.com/higgsboson1209/Prodigal-Technologies---Assignment-/blob/main/CODE-EXECUTION-TIME.png)

## SCOPE OF IMPROVEMENT

- Split data into multiple table and make them SQL based to ensure easy querying of data
- Come with better error handling, and automate the script so it runs everyday 
