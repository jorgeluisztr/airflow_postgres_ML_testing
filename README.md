## airflow_postgres_ML_testing


**The logistic regression model used in this app (as a pickle) was obtained from** [here](https://www.kaggle.com/startupsci/titanic-data-science-solutions).

#### Step 1
run ```curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.1/docker-compose.yaml'```

#### Step 2
Replace the docker-compose.yaml and the dags folder with these ones 


#### Step 3
run ```docker-compose up --build```


#### Step 3
create a postgres connection with the right ip (to check your ip run ```docker inspect your-container-id | grep IPAdress```) for your app

