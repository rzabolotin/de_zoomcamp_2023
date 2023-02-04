# Prefect orchestration

## 1.Task и Flow

Prefect это оркестратор, аналог Airflow и даже основной разработчик Prefect был очень активным разработчиком Airflow ранее.   
Чтобы сделать свой flow нужно написать скрипт python, и функции обернуть декораторами @flow, @task

Пример flow:
- Загружаем данные через url
- Преобразуем типы df
- Сохраняем в parquet

```python
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
import pandas as pd
from prefect_gcp import GcsBucket, GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash)
def ingest_data(url: Path) -> pd.DataFrame:
    """Ingest the data"""
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean_it(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data"""
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task
def save_data_local(df: pd.DataFrame, path: Path) -> str:
    """ Save the file locally"""
    local_file = f"{path}.parquet"
    df.to_parquet(local_file, compression="gzip")
    return local_file

@flow(name="GCP flow")
def gcp_flow(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    raw_data = ingest_data(dataset_url)
    clean_data = clean_it(raw_data)
    path = save_data_local(clean_data, "data/" + dataset_file)
    

if __name__ == "__main__":
    color = "yellow"
    year = 2021
    month = 1

    gcp_flow(color, year, month)
```

Что это дает?
- история запусков сохраняется в локальной базе prefect (sqlite, но можно поменять на postgres)
- также логи выполнения сохраняются, и отображаются в UI
- можно кешировать таски, и не выполнять их если ранее выполнялись
- можно делать повторный запуск задачи, если она упала с ошибкой
- ну и еще много всего

## 2. Deployments
Чтобы запускать таски не помощью запуска python, а из интерфейса или по расписанию, или по событию - в prefect есть сущность deployment  
Она создается из командной строки или с помощью скрипта python.
```shell
prefect deployment build ./local-path-to-flow-file/flow-file.py:flow-name \
    -n "Github Storage Flow" \
    -sb github/gh-dtc-storage/github-path-to-flow-file \ 
    -o ./local-path-to-deployment-file/file-deployment.yaml \
    --apply
```

```python
from prefect.deployments import Deployment
from gcs_to_bq_flow_hw3 import gather_flow


my_dep = Deployment.build_from_flow(
    flow=gather_flow,
    name="GCP-to-BQ-flow",
    parameters={"year":2020, "color":"yellow","months":[1,2]}
)


if __name__ == "__main__":
    my_dep.apply()

```

После того как Deployment применен, он добавляется в раздел deployments на сервере.    
По существу это объект, который говорит, что меня можно запустить и я выполню такую-то команду, с такими-то параметрами.
Но его можно использовать уже в разных сценариях:
- запускать по расписанию
- запускать из UI (передавать различные параметры)
- ему можно указать, где брать код (по умолчанию он должен быть в той же папке где запущен агент, но можно указать взять код на github, s3)
- можно указать инфраструктуру для запуска, доступно 3 вида (процесс, докер, кубернетеc), но воркер потом должен быть готов выполнить этот флов, иметь программу для запуска этой инфраструктуры


Запуск деплоймента через CLI
```shell
prefect deployment run "Parent GCP flow/Gather flow"
```

## 3. Agent
Если указать для деплоймента расписание, или вызывать запуск вручную, то задача не будет выполнена, но будет поставлена в очередь.  
Чтобы она была выполнена нужен агент. Агент - это процесс, который подключается к оркестратору, получает от него задачи из очереди и выполняет их.  

Запустить агента можно командой:
```shell
#default - название очереди
prefect agent start -q default
```

Но до этого нужно установить префект, и подключить его в серверу оркестратора.
Если это Prefect cloud, то подключиться можно командой 
```shell
prefect cloud login -k API_KEY
```
Если сервер где-то в сети то нужно установить настройки
```shell
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```
Также если нужны какие-то библиотеки, то они должны быть установлены у агента. Или должен быть установлен докер если flow требует запуска в докере. (для кубера не знаю, нет опыта, но тоже нужно чтобы агент умел с ним работать)  

## 4. UI
Чтобы запустить UI нужно запустить команду
```shell
prefect orion start
```
В UI можно смотреть историю запусков, запускать новые деплойменты, или менять расписание.  
Можно настраивать уведомления по состоянию flow.

![Как он выглядит](https://raw.githubusercontent.com/boisalai/de-zoomcamp-2023/main/dtc/w2s15.png)

# 5. Блоки
Блоки это такой удобный расширяемый механизм, который позволяет сохранять настройки какие-то в хранилище prefect.
К примеру ты создаешь настройки корзины в Google Storage, а потом в коде можешь ее исползовать так:

```python
    from prefect_gcp import GcsBucket
    gcp_cloud_storage_bucket_block = GcsBucket.load("my-gcs-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
```

Также эти блоки используются когда создаешь deployment, через них можешь указать путь к файлам с кодом, или инфраструктуре:
```python

my_dep = Deployment.build_from_flow(
    flow=gcp_flow,
    name="GitHub flow: hw4",
    parameters={"year":2020, "color":"green","month":11},
    storage=GitHub.load("de-zoom-repo"), # это блок, с именем de-zoom-repo
    entrypoint="week2_prefect/gcp_flow_hw4.py:gcp_flow"
)
```

Блоки можно также создавать не в UI, а в коде python
```python

from prefect_gcp import GcpCredentials 
from prefect_gcp.cloud_storage import GcsBucket    
                                                                      
gcp_credentials_block = GcpCredentials.load("de-zoomcamp-gcp-creds")  
                                                                          
GcsBucket(bucket="de-zoomcamp-gcs",                                   
          gcp_credentials=gcp_credentials_block                
          ).save("de-zoomcamp-bucket")  
```

## 6. Компоненты префект и Cloud

Если разделить на компоненты, то можно выделить 6 составляющих:
- flow,task - код с бизнес логикой
- deployment - инструкция, как и когда выполнить этот flow
- prefect api - сервер который управляет запуском деплойментов, и собирает информацию о запусках
- db - база данных, которая сохраняет состояния flow
- ui - пользовательский интерфейс + настройка сервера
- agent + queues - воркер, который выполняет задачи, полученные от api
- block - хранилище различных настроек, которые можно исползовать в коде, или в различных настройках

В Cloud версии тебе дают все, кроме агентов.   
Т.е. ты делалешь свой код, оборачиваешь в деплойменты - а потом можешь подключить свой воркер к серверу и он будет выполнять задачи, которые ты ему написал. Также может отправлять уведомления, настраивать расписание.



## 7. Пример flow

```python

from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
import pandas as pd
from prefect_gcp import GcsBucket, GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash)
def ingest_data(url: Path) -> pd.DataFrame:
    """Ingest the data"""
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean_it(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data"""
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task
def save_data_local(df: pd.DataFrame, path: Path) -> str:
    """ Save the file locally"""
    local_file = f"{path}.parquet"
    df.to_parquet(local_file, compression="gzip")
    return local_file

@task
def save_to_gcp(path: str) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("my-gcs-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@task(log_prints=True)
def save_to_bq(df: pd.DataFrame, table_name: str) -> None:
    creds = GcpCredentials.load("my-cred")
    print(f"Save to BQ table {table_name}")
    df.to_gbq(destination_table=table_name,
              project_id="summer-flux-373415",
              chunksize=500_000,
              credentials=creds.get_credentials_from_service_account(),
              if_exists="append"
              )


@flow(name="GCP flow")
def gcp_flow(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    raw_data = ingest_data(dataset_url)
    clean_data = clean_it(raw_data)
    path = save_data_local(clean_data, "data/" + dataset_file)
    save_to_gcp(path)

    save_to_bq(clean_data, "zoom.taxiData")


@flow(name="Parent GCP flow")
def gather_flow(color: str, year: int, months: list[int]) -> None:
    for month in months:
        gcp_flow(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1,2,3]

    gather_flow(color, year, months)

```

[Подробный конспект по видео](https://github.com/boisalai/de-zoomcamp-2023/blob/main/week2.md)
