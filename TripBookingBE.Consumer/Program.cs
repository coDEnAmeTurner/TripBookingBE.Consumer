using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

internal class Program
{
    const string QUEUE_NAME = "logs";
    private static async Task Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost", Port = 6078 };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: QUEUE_NAME, durable: true, exclusive: false,
    autoDelete: false, arguments: null);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        Console.WriteLine(" [*] Waiting for messages.");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, ea) =>
        {
            AsyncEventingBasicConsumer cons = (AsyncEventingBasicConsumer)sender;
            IChannel ch = cons.Channel;

            var body = ea.Body.ToArray();

            try
            {
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Message received: {message}");
            }
            catch (Exception e)
            {
                Console.WriteLine($" [.] {e.Message};{e.InnerException?.Message}");
            }
            finally
            {
                await ch.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
        };

        await channel.BasicConsumeAsync(QUEUE_NAME, autoAck: false, consumer: consumer);

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    static int Fib(int n)
    {
        if (n is 0 or 1)
        {
            return n;
        }

        return Fib(n - 1) + Fib(n - 2);
    }
}