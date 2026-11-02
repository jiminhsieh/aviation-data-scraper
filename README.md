# Aviation Data Scrapper
This repository is used to fetch data from opensky-network.

## Tech Stack
Python + Docker

## Core Logic of Python
1. Get data from https://opensky-network.org/api/states/all?lamin=-11&lamax=21&lomin=92&lomax=141
2. Convert the JSON response to Avro then publish it to Kafka.

2. [Data structure of the response in JSON format](example_response.json)
```json
{
  "time": 1793013825,
  "states": [
    [
      "8a044f",
      "GIA190  ",
      "Indonesia",
      1793013823,
      1793013825,
      99.9767,
      1.7853,
      10927.08,
      false,
      227.31,
      322.17,
      -5.85,
      null,
      11673.84,
      null,
      false,
      0
    ]
  ]
}
```

## Design Decision
1. Use k8s CronJob to do the scheduling.
