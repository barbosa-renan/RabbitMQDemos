using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using Demo2;
using System.Text;

ConnectionFactory connectionFactory = new ConnectionFactory()
{
    Uri = new Uri(@"amqp://username:password@127.0.0.1:5672/demo_virtual_host"),
    NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = true,
};

using var connection = connectionFactory.CreateConnection();

using var model = connection.CreateModel();

model.ExchangeDeclare(exchange: "hello_exchange", type: "fanout", durable: true, autoDelete: false, arguments: null);
model.QueueDeclare(queue: "hello_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
model.QueueBind(queue: "hello_queue", exchange: "hello_exchange", routingKey: string.Empty, arguments: null);

model.BasicQos(0, 15_000 * 4, false);

var consumer = new EventingBasicConsumer(model);

consumer.Received += (innerModel, ea) =>
{
    var body = ea.Body;
    var message = Encoding.UTF8.GetString(body.ToArray());

    MyMessage myMessage;

    try
    {
        myMessage = System.Text.Json.JsonSerializer.Deserialize<MyMessage>(message);
    }
    catch (Exception)
    {
        model.BasicReject(ea.DeliveryTag, false);
        throw;
    }

    try
    {
        new ServiceXPTO().DoAnything(myMessage);
        model.BasicAck(ea.DeliveryTag, false);
    }
    catch (Exception)
    {
        model.BasicNack(ea.DeliveryTag, false, true);
        throw;
    }

};

model.BasicConsume(queue: "hello_queue",
                     autoAck: false,
                     consumer: consumer);

Console.ReadLine();

Console.WriteLine("Hello World!");