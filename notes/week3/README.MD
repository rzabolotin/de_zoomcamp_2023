# Неделя 3
## Темы: Data Warehouse, BigQuery

## Виды баз данных OLTP и OLAP
Базы данных OLAP (базы для аналитики) имеют отличия от традиционных баз данных OLTP (базы для транзакций).  
- Данные преимущественно хранятся в колонках.
- Не предполагают частых и маленьких изменений данных.
- Данные периодически загружаются (через ETL).

![OLTP vs OLAP](img1.png)


## Data Warehouse
Data Warehouse - это OLAP решение, предназначенная для аналитики и создания отчетов.  
В отличии от Data Lake, Data Warehouse использует методологию ETL (Extract, Transform, Load) для загрузки данных. Обычно данные в Data Warehouse более структурированы, чем в Data Lake.   
Примеры Data Warehouse:
- Google BigQuery
- Amazon Redshift
- Snowflake
- Microsoft Azure Synapse Analytics

![Data Warehouse](img2.png)


## Google BigQuery
BigQuery - это Data Warehouse, предоставляемый Google.  

- serverless. Нет необходимости в управлении серверами, не нужно ничего устанавливать и поддерживать.
- scalable. Масштабируется автоматически, не нужно вручную увеличивать мощность.
- high availability. Предоставляет высокую доступность, не нужно беспокоиться о сбоях.
- built-in features. Предоставляет встроенные функции, такие как Machine Learning, Geospatial Analysis и Business Intelligence.
- cost-effective. Предоставляет высокую производительность по относительно низкой цене.






