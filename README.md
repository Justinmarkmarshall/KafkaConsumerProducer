Kafka Producer has 2 methods

For this to work the server has to be running in a container locally.
Can download the docker compose by googling kafka docker compose file, and arrive at this command, to be run in wsl
curl --silent --output docker-compose.yml   https://raw.githubusercontent.com/confluentinc/cp-all-in-one/7.2.1-post/cp-all-in-one/docker-compose.yml

then 
Docker-compose up -d

Run the compose and create all the necessary containers

Then docker-ps to check that all the images are downloaded and containers created
Enterprise control center : visual representation of the kafka cluster
Ksql-db: database for Kafka (with cli and server)
Kafka schema registry
Kafka server
Zookeeper - kvp server used by Kafka to manage brokers and electing primary (the one where every request will come in)

So now we can go to the control center on port 9021 and see the cluster UI

1. Reads from a Wikipedia Stream and publishes to an already created Topic

When creating the Topic, the replication factor is not important locally because we only have one broker, but in a production environment it becomes very important.

So now if I add this new Topic name to the code, and update the url to localhost kafka, then I can see that the messages are coming into the broker. Because my code is reading from the wikimedia event stream, there is a consistent flow of messages coming onto the topic.


2. Reads from a user input about the state of the weather

I can also publish to a producer, which will create a new topic, and so long as it runs for the first time before the consumer, then the consumer will be able to read from the topic, and I can log the message to the console. Again, this requires the image to be up and running, but then I can see the messages logged to the topic in the UI
