﻿using System;
using System.Collections.Generic;
using System.Text;
using RdKafka;
using System.Configuration;

namespace SampleKafkaConsumer
{
    public class NonStaticBasicConsumer
    {
        public EventConsumer consumer;
        public void Start(string kafkaUrl, string kafkaTopic, string consumerName)
        {
            Config config = new Config() { GroupId = "example-csharp-consumer" };
            config["security.protocol"] = "ssl";
            config["ssl.ca.location"] = ConfigurationManager.AppSettings["ssl.ca.location"];
            config["ssl.certificate.location"] = ConfigurationManager.AppSettings["ssl.certificate.location"];
            config["ssl.key.location"] = ConfigurationManager.AppSettings["ssl.key.location"];
            config["ssl.key.password"] = ConfigurationManager.AppSettings["ssl.key.password"];
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
