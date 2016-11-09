using System;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using RdKafka;
using System.Collections.Generic;
using System.Configuration;

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
            Config config = new Config();
            config["security.protocol"] = "ssl";
            config["ssl.ca.location"] = ConfigurationManager.AppSettings["ssl.ca.location"];
            config["ssl.certificate.location"] = ConfigurationManager.AppSettings["ssl.certificate.location"];
            config["ssl.key.location"] = ConfigurationManager.AppSettings["ssl.key.location"];
            config["ssl.key.password"] = ConfigurationManager.AppSettings["ssl.key.password"];
            using (Producer producer = new Producer(config, kafkaUrl))
            using (Topic topic = producer.Topic(kafkaTopic))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. Press any key to stop producing, which will also stop consuming.");

                var i = 0;
                while (!Console.KeyAvailable)
                //while (i < 1)
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
