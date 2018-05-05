using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaTest.Producer                                                                                                                  
{
  public class Program
  {
    public static async Task Main(string[] args)
    {
      if (args.Length < 2)
        return;
      var server = $"{args[0]:args[1]}";
      var iterations = 1000;
      var sleep = 300;

      if (args.Length > 2)
        iterations = Int32.Parse(args[2]);

      if (args.Length > 3)
        sleep = Int32.Parse(args[3]);

      Console.WriteLine($"Inicializando producer...{server}");
      Console.WriteLine($"Se enviaran {iterations} mensajes al cluster Kafka.");
      
      var config = new Dictionary<string, object>
      {
        { "bootstrap.servers", server }
      };

      using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
      {

        producer.OnError += (_, error) => {
          Console.WriteLine($"Code: {error.Code}, Reason: {error.Reason}");          
        };

        string text;
        for(int i = 0; i < iterations; i++ )
        {
          text = $"Este es el mensaje #{i}...";  
          var msg = await producer.ProduceAsync("my-topic", null, text);
          producer.Flush(100);
          Console.WriteLine($"{System.DateTime.UtcNow.Ticks} --> Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
          System.Threading.Thread.Sleep(sleep);
        }

        var exitMsg = await producer.ProduceAsync("my-topic", null, "FIN");
        Console.WriteLine($"Topic: {exitMsg.Topic} Partition: {exitMsg.Partition} Offset: {exitMsg.Offset} {exitMsg.Value}");

      }
    }

  }
}