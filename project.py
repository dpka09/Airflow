from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import year

spark = SparkSession.builder.getOrCreate()
category = spark.read.csv("/home/deepika/my_project_dir/my_project_env/category.csv", header=True)
country = spark.read.csv("/home/deepika/my_project_dir/my_project_env/country.csv", header=True)
proceeds = spark.read.csv("/home/deepika/my_project_dir/my_project_env/proceeds.csv", header=True)
project = spark.read.csv("/home/deepika/my_project_dir/my_project_env/project.csv", header=True)

#print(proceeds.show())
# How many successful projects with zero backers?

def task1():
    self_join= project.join(proceeds, project.project_id==proceeds.project_id, "inner").select(project.state, proceeds.backers)
    successful_zero_backers= self_join.filter((self_join.state=="successful") & (self_join.backers=="0"))
    return successful_zero_backers
    print(successful_zero_backers.show())




# Which country has least number of projects?

def task2():
    data = project.join(country, project.country_id==country.country_id, "inner").select(project.proj_name, country.country_code)
    least_projects=data.groupBy("country_code").count().sort(col("count").asc())
    least_projects.first()
    return least_projects
    print(least_projects.show())



# List the top 10 projects that have the highest goal and list the ones that have reached their goals.

def task3():
    df= project.join(proceeds, project.project_id==proceeds.project_id).select(project.proj_name, proceeds.goal, proceeds.pledged)    

    projects_highest_goal=df.orderBy(col("goal").desc())   


    return projects_highest_goal
    print(projects_highest_goal.show())

    

def task4():
    df= project.join(proceeds, project.project_id==proceeds.project_id).select(project.proj_name, proceeds.goal, proceeds.pledged)
    reached_goals=df.select("proj_name","goal","pledged").filter(df.pledged>=df.goal)
    
    return reached_goals

    print(reached_goals.show())

#  Which category has the most number of projects?

def task5():
    cat=project.join(category,project.category_id == category.category_id,"inner").select("proj_name","category")
    most_projects_category=cat.groupby("category").count().sort(col("count").desc())
    most_projects_category.first()    
    return most_projects_category

    print(most_projects_category.show())

# Which category is the most successful?

def task6():

    success=project.join(category,project.category_id == category.category_id,"inner").select("state","category").filter(project.state=="successful")
    sucessful_category=success.groupBy("category").count().sort(col("count").desc())
    sucessful_category.first()    
    
    return sucessful_category
    print(sucessful_category.show())

# What type of projects are prevalent in each country?

def task7():
    dataa = project.join(country, project.country_id==country.country_id, "inner").join(category,project.category_id == category.category_id,"inner").select(project.proj_name, country.country_code, category.category, category.main_category)                                                                                                     
    prevalent_projects = dataa.groupby('country_code','category').count().sort(col("count").desc())
    prevalent_projects=prevalent_projects.drop_duplicates(["country_code"])
   
    return prevalent_projects
    print(prevalent_projects.show())

# Which year has the most number of failed projects?
def task8():
    from pyspark.sql.functions import year

    year=project.select((year('deadline').alias('year')),"state").filter(project.state=="failed")
    year_with_max_fail=year.groupBy("year").count().sort(col("count").desc())
    year_with_max_fail=year_with_max_fail.limit(1)
    return year_with_max_fail
    print(year_with_max_fail.show())

def task9(category, country, proceeds, project):
    joined = project.join(country, 'country_id', 'inner') \
        .join(proceeds, 'project_id', 'inner') \
        .join(category, 'category_id', 'inner')
    denormalized = joined.drop('category_id', 'country_id', 'proceeds_id').withColumnRenamed('project_id', 'ID')
    return denormalized

    print(denormalized.show())


#task2()
#task3()
#task4()
#task5()
#task6()
#task7()
#task8()
    