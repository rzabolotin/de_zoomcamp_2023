import faust
from taxi_rides import TaxiRide
import settings

app = faust.App('datatalksclub.counting.v1', broker='kafka://localhost:9092')
topic = app.topic(settings.TOPIC_NAME, value_type=TaxiRide)


@app.agent(topic)
async def start_reading(records):
    async for record in records:
        print(record)

if __name__ == '__main__':
    app.main()