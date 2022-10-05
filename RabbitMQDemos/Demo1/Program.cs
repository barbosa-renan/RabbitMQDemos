using Demo1;
using RabbitMQ.Client;
using System.Text;

ConnectionFactory connectionFactory = new ConnectionFactory()
{
    Uri = new Uri(@"amqp://username:password@127.0.0.1:5672/demo_virtual_host"),
    NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = true
};

using var connection = connectionFactory.CreateConnection();
using var model = connection.CreateModel();
model.ConfirmSelect();

model.ExchangeDeclare(exchange: "hello_exchange", type: "FANOUT", durable: true, autoDelete: false, arguments: null);
model.QueueDeclare(queue: "hello_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
model.QueueBind(queue: "hello_queue", exchange: "hello_exchange", routingKey: String.Empty, arguments: null);

var myMessage = new MyMessage
{
    Message = $"Hello World! {DateTime.Now}"
};

string message = System.Text.Json.JsonSerializer.Serialize(myMessage);
var body = Encoding.UTF8.GetBytes(message);

var props = model.CreateBasicProperties();
props.Headers = new Dictionary<string, Object>
{
    {"content-type", "application/json" }
};
props.DeliveryMode = 2;

for (int i = 1; i <= 100_000_000; i++)
    model.BasicPublish(exchange: "hello_exchange",
        routingKey: string.Empty,
        basicProperties: props,
        body: body);

Console.WriteLine("Hello World!");