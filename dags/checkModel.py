from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pickle

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

def createTable(ah=AIRFLOW_HOME):
    """

    :param ah:  the path where are the dags, logs, plugins and my data
    :return: a sql table with test data in airflow database
    """
    engine = create_engine('postgresql://' + "airflow" + ':' + "airflow" + '@' + "172.23.0.3" + ':' + "5432" + '/' + "airflow")
    df = pd.read_csv("{ah}/mydata/test.csv".format(ah=ah))
    df.to_sql('passengers', con=engine, if_exists='append', index=False)


def process_data(ah = AIRFLOW_HOME):
    """

    :param ah: the path where are the dags, logs, plugins and my data
    :return: a sql table with results data for the logistic regression in airflow database
    """

    # the logistic regression model
    model = pickle.load(open("{ah}/mydata/logistic_regression.pickle".format(ah=ah), "rb"))
    # engine for connection to postgres
    engine = create_engine('postgresql://' + "airflow" + ':' + "airflow" + '@' + "172.23.0.3" + ':' + "5432" + '/' + "airflow")
    # read data
    df = pd.read_sql_query('select * from passengers', con=engine)
    # transform age column with an ageband
    df.loc[df['Age'] <= 16, 'Age'] = 0
    df.loc[(df['Age'] > 16) & (df['Age'] <= 32), 'Age'] = 1
    df.loc[(df['Age'] > 32) & (df['Age'] <= 48), 'Age'] = 2
    df.loc[(df['Age'] > 48) & (df['Age'] <= 64), 'Age'] = 3
    df.loc[df['Age'] > 64, 'Age'] = 4
    # dummy sex variable
    df["Sex"] = np.where(df.Sex == "female", 1, 0)
    # transform fare column with an fare band
    df['Fare'].fillna(df['Fare'].dropna().median(), inplace=True)
    df.loc[df['Fare'] <= 7.91, 'Fare'] = 0
    df.loc[(df['Fare'] > 7.91) & (df['Fare'] <= 14.454), 'Fare'] = 1
    df.loc[(df['Fare'] > 14.454) & (df['Fare'] <= 31), 'Fare'] = 2
    df.loc[df['Fare'] > 31, 'Fare'] = 3
    df['Fare'] = df['Fare'].astype(int)
    # numerical variable for embarked
    df['Embarked'] = df['Embarked'].fillna("S")
    df['Embarked'] = df['Embarked'].map({'S': 0, 'C': 1, 'Q': 2}).astype(int)
    # numerical variable for title in the name of the passengers
    df['Title'] = df.Name.str.extract(' ([A-Za-z]+)\.', expand=False)
    df['Title'] = df['Title'].replace(['Lady', 'Countess', 'Capt', 'Col',
                                       'Don', 'Dr', 'Major', 'Rev', 'Sir', 'Jonkheer', 'Dona'], 'Rare')
    df['Title'] = df['Title'].replace('Mlle', 'Miss')
    df['Title'] = df['Title'].replace('Ms', 'Miss')
    df['Title'] = df['Title'].replace('Mme', 'Mrs')
    title_mapping = {"Mr": 1, "Miss": 2, "Mrs": 3, "Master": 4, "Rare": 5}
    df['Title'] = df['Title'].map(title_mapping)
    df['Title'] = df['Title'].fillna(0)
    # family size
    df['FamilySize'] = df['SibSp'] + df['Parch'] + 1
    # dummy variable if the passenger travel alone
    df['IsAlone'] = 0
    df.loc[df['FamilySize'] == 1, 'IsAlone'] = 1
    # column for age by class
    df['Age*Class'] = df.Age * df.Pclass
    # the variables of the model
    X = df[["Pclass", "Sex", "Age", "Fare", "Embarked", "Title", "IsAlone", "Age*Class"]]
    X = X.fillna(0)
    # the predictions
    y = model.predict(X)
    # just keep passenger id and the result of the prediction
    df = df[["PassengerId"]]
    df["Survive"] = y
    # append the data to the table in sql
    df.to_sql('results', con=engine, if_exists='append', index=False)

def results():
    """

    :return: show the results
    """
    engine = create_engine('postgresql://' + "airflow" + ':' + "airflow" + '@' + "172.23.0.3" + ':' + "5432" + '/' + "airflow")
    df = pd.read_sql_query('select * from results', con=engine)
    print(df.head())

with DAG(
        "my_dag",
        start_date=datetime(2021, 1 ,1),
        schedule_interval='@once',
) as dag:
    drop_table_passengers = PostgresOperator(
        task_id="drop_table_passengers",
        postgres_conn_id="postgres_default",
        sql="drop table if exists passengers;",
    )
    create_table_passengers = PythonOperator(
        task_id="createTable",
        python_callable=createTable,
    )

    drop_table_results = PostgresOperator(
        task_id="drop_table_results",
        postgres_conn_id="postgres_default",
        sql="drop table if exists results;",
    )

    read_process_table =  PythonOperator(
        task_id="processdata",
        python_callable=process_data,
    )

    results = PythonOperator(
        task_id="results",
        python_callable=results,
    )

    drop_table_passengers >> create_table_passengers >> drop_table_results >> read_process_table >> results