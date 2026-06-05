# TODO

- [ ] **Replace `consumer.js` with the MongoDB Kafka Connector (sink connector)**
  Use the official [MongoDB Kafka Connector](https://www.mongodb.com/docs/kafka-connector/current/) instead of the custom Node.js `consumer.js` app to sink CDC events from Kafka directly into MongoDB. This removes bespoke consumer maintenance and adds production-grade features out of the box: dead letter queues, configurable write models, idempotent upserts, and Atlas-native support.
