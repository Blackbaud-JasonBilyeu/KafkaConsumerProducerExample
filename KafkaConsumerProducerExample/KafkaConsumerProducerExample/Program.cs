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

            BasicConsumer.Start(kafkaUrl, kafkaTopic);
            BasicProducer.ProduceUntilKeypress(kafkaUrl, kafkaTopic, kafkaPublishMessage);

            BasicConsumer.Stop();
        }
    }
}
