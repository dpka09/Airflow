from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
category = spark.read.csv("/home/deepika/my_project_dir/my_project_env/category.csv", header=True)
country = spark.read.csv("/home/deepika/my_project_dir/my_project_env/country.csv", header=True)
proceeds = spark.read.csv("/home/deepika/my_project_dir/my_project_env/category.csv", header=True)
project = spark.read.csv("/home/deepika/my_project_dir/my_project_env/project.csv", header=True)


# How many successful projects with zero backers?

def sucessful_zero_backers():
    self_join= project.join(proceeds, project.project_id==proceeds.project_id, "inner").select(project.state, proceeds.backers)
    successful_zero_backer= self_join.filter((self_join.state=="successful") & (self_join.backers=="0"))
    return sucessful_zero_backers


# Which country has least number of projects?

def least_projects():
    data = project.join(country, project.country_id==country.country_id, "inner").select(project.proj_name, country.country_code)
    least_projects=data.groupBy("country_code").count().sort(col("count").asc())
    least_projects.first()
    return least_projects


# List the top 10 projects that have the highest goal and list the ones that have reached their goals.

def projects_highest_goal():
    df= project.join(proceeds, project.project_id==proceeds.project_id).select(project.proj_name, proceeds.goal, proceeds.pledged)                                                                                       proceeds.pledged)
    projects_highest_goal=df.orderBy(col("goal").desc())   


    return projects_highest_goal
    

def reached_goals():
    reached_goals=df.select("proj_name","goal","pledged").filter(df.pledged>=df.goal)
    reached_goals = reached_goals.count()
    return reached_goals

#  Which category has the most number of projects?

def most_projects_category():
    cat=project.join(category,project.category_id == category.category_id,"inner").select("proj_name","category")
    most_projects_category=cat.groupby("category").count().sort(col("count").desc())
    most_projects_category.first()    
    return most_projects_category


# Which category is the most successful?

def sucessful_category():

    success=project.join(category,project.category_id == category.category_id,"inner").select("state","category").filter(project.state=="successful")
    sucessful_category=success.groupBy("category").count().sort(col("count").desc())
    sucessful_category.first()    
    
    return sucessful_category


# What type of projects are prevalent in each country?

def prevalent_projects():
    dataa = project.join(country, project.country_id==country.country_id, "inner").join(category,project.category_id == category.category_id,"inner").select(project.proj_name, country.country_code, category.category, category.main_category)                                                                                                       project.category_id == category.category_id).select(
    prevalent_projects = dataa.groupby('country_code','category').count().sort(col("count").desc())
    prevalent_projects=prevalent_projects.drop_duplicates(["country_code"]).show(22)
   
    return prevalent_projects


# Which year has the most number of failed projects?
def year_with_max_fail():
    year=project.select((year('deadline').alias('year')),"state").filter(project.state=="failed")
    year_with_max_fail=year.groupBy("year").count().sort(col("count").desc())
    year_with_max_fail=year_with_max_fail.first()
    return year_with_max_fail


def denormalize(category, country, proceeds, project):
    joined = project.join(country, 'country_id', 'inner') \
        .join(proceeds, 'project_id', 'inner') \
        .join(category, 'category_id', 'inner')
    denormalized = joined.drop('category_id', 'country_id', 'proceeds_id').withColumnRenamed('project_id', 'ID')
    return denormalize
