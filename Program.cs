// See https://aka.ms/new-console-template for more information
//https://www.youtube.com/watch?v=A4Y7z6wFRk0 youtube tutorial for setting up the weather producer
//https://docs.confluent.io/cloud/current/get-started/index.html#cloud-quickstart confluent docs for setting up a producer
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;



Console.WriteLine("Hello, World!");

await Kafka.Produce();

//await Kafka.ProduceFromWikipediaStream();

public static class Kafka
{
    private const string LiveBootstrapServer = "bootstrap.servers=pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092"; // this is the live address so will not work
    /// <summary>
    /// must run docker-compose up -d in Kafka folder in WSL to spin up
    /// </summary>
    private const string localContainerBootstrapServer = "localhost:9092";
    private const string topicCreatedInKafkaUi = "net6Topic30_07";

    //using (var producer = new ProducerBuilder<Null, string>(config).Build())
    //{
    //    await producer.ProduceAsync("users", new Message<Null, string> { Value = "alog message" });
    //}

    //    
    //async Task Produce(string topicName, Clinet)

    public static async Task Produce(string topicName="weather-topic", string topicServerUrl = localContainerBootstrapServer)
    {
        var config = new ProducerConfig { BootstrapServers = topicServerUrl };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        try
        {
            string? state;
            while ((state = Console.ReadLine()) != null)
            {
                var response = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather() { State = state, Temp = 22 }) });
                Console.WriteLine(response.Value);
            }            
        }
        catch (ProduceException<Null, string> ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    public class Weather
    {
        public string State { get; set; }
        public int Temp { get; set; }
    }

    public static async Task ProduceFromWikipediaStream(string topicName=topicCreatedInKafkaUi, string topicUrl=localContainerBootstrapServer)
    {
        var config = new ClientConfig()
        {
            BootstrapServers = topicUrl,
        };

        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = "localhost:9092", //local instance of kafka running in container
            Acks = Acks.Leader //number of acknowledgements must receive before the leader broker can respond 
        };


        Console.WriteLine($"{nameof(Produce)} starting");

        // The URL of the EventStreams service.
        string eventStreamsUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Declare the producer reference here to enable calling the Flush
        // method in the finally block, when the app shuts down.
        IProducer<string, string> producer = null;

        try
        {
            // Build a producer based on the provided configuration.
            // It will be disposed in the finally block.
            producer = new ProducerBuilder<string, string>(config).Build();

            // Create an HTTP client and request the event stream.
            using (var httpClient = new HttpClient())

            // Get the RC stream. 
            using (var stream = await httpClient.GetStreamAsync(eventStreamsUrl))

            // Open a reader to get the events from the service.
            using (var reader = new StreamReader(stream))
            {
                // Read continuously until interrupted by Ctrl+C.
                while (!reader.EndOfStream)
                {
                    // Get the next line from the service.
                    var line = reader.ReadLine();

                    // The Wikimedia service sends a few lines, but the lines
                    // of interest for this demo start with the "data:" prefix. 
                    if (!line.StartsWith("data:"))
                    {
                        continue;
                    }

                    // Extract and deserialize the JSON payload.
                    int openBraceIndex = line.IndexOf('{');
                    string jsonData = line.Substring(openBraceIndex);
                    Console.WriteLine($"Data string: {jsonData}");

                    // Parse the JSON to extract the URI of the edited page.
                    var jsonDoc = JsonDocument.Parse(jsonData);
                    var metaElement = jsonDoc.RootElement.GetProperty("meta");
                    var uriElement = metaElement.GetProperty("uri");
                    var key = uriElement.GetString(); // Use the URI as the message key.

                    // For higher throughput, use the non-blocking Produce call
                    // and handle delivery reports out-of-band, instead of awaiting
                    // the result of a ProduceAsync call.
                    producer.Produce(topicName, new Message<string, string> { Key = key, Value = jsonData },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                            }
                        });
                }
            }
        }
        finally
        {
            var queueSize = producer.Flush(TimeSpan.FromSeconds(5));
            if (queueSize > 0)
            {
                Console.WriteLine("WARNING: Producer event queue has " + queueSize + " pending events on exit.");
            }
            producer.Dispose();
        }
    }
}

