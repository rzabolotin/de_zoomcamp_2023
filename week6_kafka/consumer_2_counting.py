import faust
from taxi_rides import TaxiRide
import settings


app = faust.App('datatalksclub.stream.v3', broker='kafka://localhost:9092')
topic = app.topic(settings.TOPIC_NAME, value_type=TaxiRide)

location_rides = app.Table('pu_location_count', default=int)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.PULocationId):
        location_rides[event.PULocationId] += 1
        print(f"{event.PULocationId} - {location_rides[event.PULocationId]}")

if __name__ == '__main__':
    app.main()