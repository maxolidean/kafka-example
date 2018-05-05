using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaTest.Consumer
{
  public class Program
  { 
    static bool exit;

    static void Main(string[] args)
    {
      if (args.Length < 2)
        return;

      var server = $"{args[0]:args[1]}";

      var config = new Dictionary<string, object>
      {
          { "group.id", "sample-consumer" },
          { "bootstrap.servers", server },
          { "enable.auto.commit", "false"}
      };

      using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
      {                
        consumer.Subscribe(new string[]{"my-topic"});

        consumer.OnMessage += (_, msg) => 
        {
          Console.WriteLine($"{System.DateTime.UtcNow.Ticks} --> Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
          consumer.CommitAsync(msg);

          if (msg.Value == "FIN")
            exit = true;
        };

        while (!exit)
        {
            consumer.Poll(100);
        }
      }
    }
  }
}