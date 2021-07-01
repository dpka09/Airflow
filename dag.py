import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from main import *
from project import *


default_args ={

    'owner':'postgres',
    'start_date':airflow.utils.dates.days_ago(1),
    'depends_on_past':True,
    'email':['sthadpka93@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=30)
    }


def sucessful_zero_backers():
    load_to_db(sucessful_zero_backers())


def least_projects():
    load_to_db(least_projects())


def projects_highest_goal():
    load_to_db(projects_highest_goal())


def reached_goals():
    load_to_db(reached_goals())


def most_projects_category():
    load_to_db(most_projects_category())


def sucessful_category():
    load_to_db(sucessful_category())



def prevalent_projects():
    load_to_db(prevalent_projects())


def year_with_max_fail():
    load_to_db(prevalent_projects())


def denormalize():
    load_to_db(denormalize(category, country, proceeds, project))


#creating Dag
dag = DAG(dag_id="Fusemachines_Airflow_project",
          default_args=default_args,
          schedule_interval="* * * * *")




sucessful_zero_backers = PythonOperator(task_id="sucessful_zero_backers",
                           python_callable=sucessful_zero_backers,
                           dag=dag)

least_projects = PythonOperator(task_id="least_projects",
                           python_callable=least_projects,
                           dag=dag)

projects_highest_goal = PythonOperator(task_id="projects_highest_goal",
                           python_callable=projects_highest_goal,
                           dag=dag)

reached_goals = PythonOperator(task_id="reached_goals",
                           python_callable=reached_goals,
                           dag=dag)

most_projects_category = PythonOperator(task_id="most_projects_category",
                           python_callable=most_projects_category,
                           dag=dag)

sucessful_category = PythonOperator(task_id="sucessful_category",
                           python_callable=sucessful_category,
                           dag=dag)

prevalent_projects = PythonOperator(task_id="prevalent_projects",
                           python_callable=prevalent_projects,
                           dag=dag)
year_with_max_fail = PythonOperator(task_id="year_with_max_fail",
                           python_callable=year_with_max_fail,
                           dag=dag)

denormalize = PythonOperator(task_id="denormalize",
                           python_callable=denormalize,
                           dag=dag)



sucessful_zero_backers >> least_projects >> projects_highest_goal >> reached_goals >> most_projects_category >> sucessful_category >> prevalent_projects >> year_with_max_fail >> denormalize
