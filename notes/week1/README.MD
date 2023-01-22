# Неделя 1
## Темы: Data ingestion, Docker, Postgres, terraform


1. Как запустить Postgres + pgadmin в Docker
```yaml
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"

```
2. Как подключиться к нему через pgcli, pgadmin, через python
```python
from sqlalchemy import create_engine

# sqlalhemy engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

# clean table if exists, if no create new
df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

# append data to table
df.to_sql(name=table_name, con=engine, if_exists='append')

```
3. Как скачать файлы csv через wget, и загрузить их в pandas
```python
os.system(f"wget {url} -O {csv_name}")
df = pd.read_csv(csv_name)
```
4. Как с помощью pandas загрузить данные в Postgres `df.to_sql`
4. Как запустить скрипт на python в Docker, чтобы он видел базу запущенную в docker-compose (использование ключа --network)
```bash
# run image data_ingestion, and pass arguments to it
docker run --rm --network week1dataingestion_default data_ingestion \
 --user root \
 --password root \
 --host pgdatabase \
 --port 5432 \
 --db ny_taxi \
 --table_name taxi_data \
 --convert_dates\
 --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

```
5. Немного освежили в памяти SQL
6. Настроили аккаунт в GCP (90 дней пробного периода)
7. Установили terraform и настроили credentials
8. Введение в синтаксис terraform (создание ресурсов, переменные, выводы)
9. Написали простой terraform скрипт, который создает storage bucket и big query dataset
