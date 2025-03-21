# PySpark Data Analysis Project

This project leverages PySpark to perform data analysis and transformation on datasets such as category, country, proceeds, and project information. It provides answers to various business-related queries, including project success, prevalent categories, and country-specific insights.

---

## Features
1. **Task 1**: Identify successful projects with zero backers.
2. **Task 2**: Find the country with the least number of projects.
3. **Task 3**: List the top 10 projects with the highest goals and those that have reached their goals.
4. **Task 4**: Identify projects that have met their goals.
5. **Task 5**: Determine the category with the most number of projects.
6. **Task 6**: Identify the most successful category.
7. **Task 7**: Analyze the type of projects prevalent in each country.
8. **Task 8**: Determine the year with the most failed projects.
9. **Task 9**: Create a denormalized table with joined data.

---

## Prerequisites

Ensure you have the following installed:
- **Python 3.x**
- **PySpark**
- **Java Development Kit (JDK)**
- **Hadoop (for HDFS)** (if required)

### Installation Steps
1. Install Python libraries:
   
       pip install pyspark

2. Verify your JAVA_HOME environment variable is set correctly.

3. Ensure the CSV files (category.csv, country.csv, proceeds.csv, project.csv) are stored at their respective paths:

        eg: /home/deepika/my_project_dir/my_project_env/
---

### Usage
***Run the Python File:*** 

       python your_script_name.py

***Available Tasks:***
 - Each task is implemented as a function. Uncomment the required task at the bottom of the script to execute it.


---
### Task Explanations

***Task 1: Successful Projects with Zero Backers***

Identifies projects marked as "successful" with no backers.

***Task 2: Country with Least Projects***

Finds the country with the least number of projects based on the dataset.

***Task 3: Top 10 Projects and Reached Goals***

Lists the top 10 projects with the highest goals and identifies projects that have met their pledged goals.

***Task 4: Projects Reaching Goals***

Filters projects where the pledged amount meets or exceeds the goal.

***Task 5: Category with Most Projects***

Determines which category has the highest number of projects.

***Task 6: Most Successful Category***

Identifies the category with the most successful projects.

***Task 7: Prevalent Projects in Each Country***

Displays the most prevalent project type for each country.

***Task 8: Year with Most Failed Projects***

Analyzes the data to find the year with the highest number of failed projects.

***Task 9: Denormalized Table***

Joins all datasets to create a denormalized table for unified analysis.

---


### Sample Output
Expected outputs for each task are printed using .show() and can be further stored or visualized.



### Additional Notes
Dependencies: Ensure Spark and Hadoop configurations are properly set up for file paths and HDFS operations.

***Data: Replace the dataset paths with correct ones if not using the default file structure.*** 

---


### Steps to Set Up Airflow with Postgres

***1. Install Airflow and Its Dependencies***

Follow the official installation guide to set up Airflow along with its required dependencies.

***2. Set the Airflow Home Directory***

Define the location of your Airflow home directory:

        export AIRFLOW_HOME=<file_path>

***3. Install Postgres and Its Driver***

Install PostgreSQL on your machine and download the appropriate JDBC driver. Set the driver's classpath:

        export CLASSPATH=postgresql-<VERSION>.jdbc4.jar
        
***4. Create a User for Airflow***
Use the airflow create_user command to create an admin user for the Airflow system:

           airflow create_user \
               --email <EMAIL> \
               --firstname <FirstName> \
               --lastname <LastName> \
               --password <Password> \
               --role Admin \
               --username <Username>
    
***5. Run the Airflow Scheduler***

Start the Airflow scheduler to manage your workflows:

     airflow scheduler

***6. Run the Airflow Webserver***

Launch the Airflow webserver to access the Airflow UI:

     airflow webserver







