using System;
using System.Collections.Generic;
using System.Text;
using RdKafka;

namespace SampleKafkaConsumer
{
    public class NonStaticBasicConsumer
    {
        public EventConsumer consumer;
        public void Start(string kafkaUrl, string kafkaTopic, string consumerName)
        {
            Config config = new Config() { GroupId = "example-csharp-consumer" };
            config["security.protocol"] = "ssl";
            config["ssl.ca.location"] = "C:\\Users\\JasonBilyeu\\Documents\\ca-cert";
            config["ssl.certificate.location"] = "C:\\Users\\JasonBilyeu\\Documents\\client_10.3.19.213_client.pem";
            config["ssl.key.location"] = "C:\\Users\\JasonBilyeu\\Documents\\client_10.3.19.213_client.key";
            config["ssl.key.password"] = "abcdefgh";
            consumer = new EventConsumer(config, kafkaUrl);
            consumer.OnMessage += (obj, msg) =>
            {
                string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                Console.WriteLine($"ConsumerName: {consumerName} Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
            };

            consumer.Subscribe(new List<string> { kafkaTopic });
            consumer.Start();
        }

        public void Stop()
        {
            consumer.Stop();
        }
    }
}
