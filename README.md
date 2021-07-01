 # deepika_airflow_ks


Install airflow and its dependencies

export airflow home:

export AIRFLOW_HOME= filepath


Install postgres and its driver, export driver classpath:

export CLASSPATH=postgresql-VERSION.jdbc4.jar ()


Create a user for airflow:

airflow create_user \
    --email EMAIL --firstname firstname \
    --lastname lastname --password password \
    --role Admin --username username


Run Scheduler:

airflow Scheduler


Run Webserver:

airflow Webserver






