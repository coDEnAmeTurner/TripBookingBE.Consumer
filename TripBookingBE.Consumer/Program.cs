using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SendGrid;
using SendGrid.Helpers.Mail;
using System.Text;
using System.Text.Json;
using TripBookingBE.Consumer.DTO.EmailDTO;

internal class Program
{   
    private static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath($"{Directory.GetCurrentDirectory()}")
            .AddJsonFile("appsettings.json")
            .Build();

        var hostname = config.GetValue<string>("RabbitMqConfigs:HostName");
        var port = config.GetValue<int>("RabbitMqConfigs:Port");
        var emailqueue = config.GetValue<string>("RabbitMqConfigs:EmailQueueName");
        var fromemail = config.GetValue<string>("SendGridConfigs:FromEmail");
        var fromname = config.GetValue<string>("SendGridConfigs:FromName");
        var sendGridClient = new SendGridClient(apiKey: config.GetValue<string>("SendGridConfigs:ApiKey"));

        var factory = new ConnectionFactory { HostName = hostname, Port = port };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: emailqueue, durable: true, exclusive: false,
    autoDelete: false, arguments: null);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, ea) =>
        {
            AsyncEventingBasicConsumer cons = (AsyncEventingBasicConsumer)sender;
            IChannel ch = cons.Channel;

            var body = ea.Body.ToArray();

            var dto = new EmailSendDTO();

            IReadOnlyBasicProperties props = ea.BasicProperties;
            var replyProps = new BasicProperties
            {
                CorrelationId = props.CorrelationId
            };

            try
            {
                var message = Encoding.UTF8.GetString(body);
                var messobj = JsonSerializer.Deserialize<EmailSendBrokerDTO>(message);

                var msg = new SendGridMessage()
                {
                    From = new EmailAddress(fromemail, fromname),
                    Subject = messobj.Subject,
                    HtmlContent = messobj.Htmlbody,
                    PlainTextContent = messobj.PlainText
                };
                msg.AddTo(new EmailAddress(messobj.ToEmail));
                var response = await sendGridClient.SendEmailAsync(msg);
                if (!response.IsSuccessStatusCode)
                {
                    dto.RespCode = (int)response.StatusCode;
                    dto.Message = response.Body.ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($" [Exception] {e.Message};{e.InnerException?.Message}");
                dto.RespCode = 500;
                dto.Message = $"{e.Message}\t{e.InnerException?.Message}";
            }
            finally
            {
                string response = JsonSerializer.Serialize(dto);
                var responseBytes = Encoding.UTF8.GetBytes(response);
                await ch.BasicPublishAsync(exchange: string.Empty, routingKey: props.ReplyTo!,
                    mandatory: true, basicProperties: replyProps, body: responseBytes);
                await ch.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

            }
        };

        await channel.BasicConsumeAsync(emailqueue, autoAck: false, consumer: consumer);
    }
}