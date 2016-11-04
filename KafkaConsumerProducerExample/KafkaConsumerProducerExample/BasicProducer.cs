using System;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using RdKafka;
using System.Collections.Generic;

namespace SampleKafkaConsumer
{
    public static class BasicProducer
    {
        public static CancellationTokenSource ts = new CancellationTokenSource();
        public static void ProduceUntilKeypress(string kafkaUrl, string kafkaTopic, string kafkaPublishMessage)
        {
            try
            {
                RunUntilUserKeypress(kafkaUrl, kafkaTopic, kafkaPublishMessage).Wait(ts.Token);
            } catch (OperationCanceledException)
            {
                Console.WriteLine("BasicProducer has been cancelled.");
            }
            ts.Dispose();
        }

        static async Task RunUntilUserKeypress(string kafkaUrl, string kafkaTopic, string kafkaPublishMessage)
        {
            Config config = new Config() { GroupId = "example-csharp-consumer" };
            config["security.protocol"] = "ssl";
            config["ssl.ca.location"] = "C:\\Users\\JasonBilyeu\\Documents\\ca-cert";
            //config["security.protocol"] = "SASL_PLAINTEXT";
            //config["sasl.mechanisms"] = "PLAIN";
            //config["sasl.username"] = "kafka1";
            //config["sasl.password"] = "k@fk@123";
            using (Producer producer = new Producer(config, kafkaUrl))
            using (Topic topic = producer.Topic(kafkaTopic))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. Press any key to stop producing, which will also stop consuming.");

                var i = 0;
                while (!Console.KeyAvailable)
                {
                    byte[] data = Encoding.UTF8.GetBytes($"{kafkaPublishMessage} {i}");
                    DeliveryReport deliveryReport = await topic.Produce(data);
                    Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");

                    i++;
                }
                ts.Cancel();                
            }
        }
    }
}
