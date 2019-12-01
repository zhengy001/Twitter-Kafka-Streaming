# Twitter-Kafka-Streaming

## Overview
Twitter Real Time Kafka Streaming

* Kafka Producer: Import real-time tweets to Kafka topic "twitter-tweets"
* Kafka Streaming: Filter important tweets whoes owner has > 1000 followers and output into topic "import-tweets"
* Kafka Consumer: Poll tweets from "import-tweets" and save into ElasticSearch Cluster



