using System.Collections.Generic;
using System.Configuration;

namespace SampleKafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string kafkaUrl = ConfigurationManager.AppSettings["kafkaUrl"];
            string kafkaTopic = ConfigurationManager.AppSettings["kafkaTopic"];
            string kafkaPublishMessage = ConfigurationManager.AppSettings["kafkaPublishMessage"];
            bool isMultipleConsumers = bool.Parse(ConfigurationManager.AppSettings["isMultipleConsumers"]);
            var consumers = new List<NonStaticBasicConsumer>();

            if (isMultipleConsumers)
            {
                for (var i = 0; i < 2; i++)
                {
                    var consumer = new NonStaticBasicConsumer();
                    consumer.Start(kafkaUrl, kafkaTopic, "Consumer" + i);
                    System.Threading.Thread.Sleep(1000);
                    consumers.Add(consumer);
                }

            }
            else
            {
                BasicConsumer.Start(kafkaUrl, kafkaTopic);
            }

            // This will run until you press a key on the console
            BasicProducer.ProduceUntilKeypress(kafkaUrl, kafkaTopic, kafkaPublishMessage);

            if (isMultipleConsumers)
            {
                consumers.ForEach(delegate (NonStaticBasicConsumer consumer)
                {
                    consumer.Stop();
                });
            }
            else
            {
                BasicConsumer.Stop();
            }
        }
    }
}
