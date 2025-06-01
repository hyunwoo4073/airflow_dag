from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# DAG 기본 설정
default_args = {
    'start_date': days_ago(1),
    'catchup': False,
}

# 데이터 로드 함수
def load_data():
    iris = load_iris()
    df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
    df['target'] = iris.target
    df.to_csv('/tmp/iris.csv', index=False)
    print("Data loaded and saved to /tmp/iris.csv")

# 모델 훈련 함수
def train_model():
    df = pd.read_csv('/tmp/iris.csv')
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression(max_iter=200)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    print(f"Model accuracy: {accuracy}")

# DAG 정의
with DAG(
    dag_id='ml_pipeline_celery_executor',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    load_data_task >> train_model_task
