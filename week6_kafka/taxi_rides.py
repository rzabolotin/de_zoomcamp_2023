import faust


class TaxiRide(faust.Record, validation=True):
    type: str
    PULocationId: str
    DOLocationID: str