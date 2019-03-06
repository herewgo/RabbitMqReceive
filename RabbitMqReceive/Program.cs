using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;

namespace RabbitMqReceive
{
    class Program
    {
        //receive
        static void Main(string[] args)
        {
            //DirectExchange();
            //DirectAcceptExchangeEvent();
            DirectAcceptExchangeEventGetUnProcessed(); 
            Console.ReadKey();

            // Go to http://aka.ms/dotnet-get-started-console to continue learning how to build a console app! 
        }

        ////<1>RabbitMQ的direct类型Exchange
        ////<2> RabbitMQ的Topic类型Exchange
        private static readonly ConnectionFactory rabbitFactory = new ConnectionFactory()
        {
            HostName = "localhost",//"10.10.40.132",
            Port = 5672,
            UserName = "luf",
            Password = "123456"
        };


        private static string exchangeName = "luf.exchange";
        private static string queueName = "luf.queue";
        //private static string routeKey = "luf.routeKey";
        public static void DirectExchange()
        {
            using (IConnection conn = rabbitFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(exchangeName, "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queueName, true, false, false, null);
                    channel.QueueBind(queueName, exchangeName, routingKey: queueName);


                    while (true)
                    {
                        BasicGetResult msgResponse = channel.BasicGet(queueName, autoAck: false);
                        if (msgResponse != null)
                            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}:{System.Text.Encoding.UTF8.GetString(msgResponse.Body)}");

                        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1));
                    }
                }
            }
        }

        public static void DirectAcceptExchangeEvent()
        {
            using (IConnection conn = rabbitFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.QueueDeclare(queueName, true, false, false, null);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, e) =>
                    {
                        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}:send-DirectExchange:{System.Text.Encoding.UTF8.GetString(e.Body)}");
                    };
                    channel.BasicConsume(queueName, autoAck: false, consumer: consumer);

                    //Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }

        public static bool ArgsInput(out string exchangeName, out string queueName, out string routeKey)
        {
            exchangeName = queueName = routeKey = null;
            bool flag = true;
            while (flag)
            {
                Console.Write("请输入(Exchange,Queue,RouteKey)(输入exit退出)：");
                string str = Console.ReadLine();
                if (str == "exit")
                {
                    flag = false;
                    return false;
                }
                if (!str.Contains(","))
                {
                    Console.WriteLine("格式不正确");
                    continue;
                }
                string[] arr = str.Split(',');
                if (arr.Length == 2)
                {
                    exchangeName = arr[0];
                    queueName = arr[1];
                    routeKey = queueName;
                    flag = false;
                    return true;
                }
                else if (arr.Length == 3)
                {
                    exchangeName = arr[0];
                    queueName = arr[1];
                    routeKey = arr[2];
                    flag = false;
                    return true;
                }
                Console.WriteLine("格式不正确");
            }

            return true;
        }
        public enum ExchangeType
        {
            //https://www.cnblogs.com/knowledgesea/p/5296008.html

            //1.direct类型下，只要queue相同则分发  
            //2.只有Exchange类型和RouteKey相同的都会发送
            direct = 1,

            //只要与Exchange类型有绑定的，不管RouteKey是什么，都会发送
            fanout = 2,

            //Exchange类型一致，send的routeKey like receive的routeKey即可接收
            topic = 3
        }
        public static void DirectAcceptExchangeEventGetUnProcessed()
        {
            Console.Write("请输入Exchange类型(direct = 1, fanout = 2, topic = 3)：");
            string exchangeTypeString = null;
            int exchangeType = Convert.ToInt32(Console.ReadLine());
            if (exchangeType < 1 || exchangeType > 3)
            {
                Console.WriteLine("格式不正确");
            }
            else
            {
                exchangeTypeString = ((ExchangeType)exchangeType).ToString();
                Console.WriteLine($"Exchange:{exchangeTypeString}类型");
                string exchangeName, queueName, routeKey = null;
                if (ArgsInput(out exchangeName, out queueName, out routeKey))
                {
                    using (IConnection conn = rabbitFactory.CreateConnection())
                    {
                        using (IModel channel = conn.CreateModel())
                        {
                            channel.ExchangeDeclare(exchangeName, exchangeTypeString, durable: true, autoDelete: false, arguments: null);

                            channel.QueueDeclare(queueName, true, false, false, null);
                            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                            channel.QueueBind(queueName, exchangeName, routingKey: routeKey);
                            var consumer = new EventingBasicConsumer(channel);
                            consumer.Received += (model, e) =>
                            {
                                Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}-{exchangeTypeString}-{exchangeName}-{queueName}-{routeKey}:{System.Text.Encoding.UTF8.GetString(e.Body)}");
                                channel.BasicAck(e.DeliveryTag, false);
                            };
                            channel.BasicConsume(queueName, autoAck: false, consumer: consumer);

                            //Console.WriteLine("按任意值，退出程序");
                            Console.ReadKey();
                        }
                    }
                }
            }

        }

    }
}
