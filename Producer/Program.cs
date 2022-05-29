using Confluent.Kafka;
using Serilog;
class Program
{
    static async Task Main(string[] args)
    {
        var logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
        logger.Information("Testando o envio de mensagens com kafka");
        if (args.Length < 3)
        {
            logger.Error(
                 "Informe ao menos 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem, " +
                    "já no terceito em diante as mensagens a serem " +
                    "enviadas a um Topic no Kafka..."
            );
            return;
        }
        string bootstrapServers = args[0];
        string topic = args[1];
        logger.Information($"BootStrapServer = {bootstrapServers}");
        logger.Information($"Topic = {topic}");
        try
        {
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = args[i] });
                    logger.Information($"Message: {args[i]} |"
                                        + $"Status: {result.Status.ToString()}");
                }
            }
            logger.Information("Concluido o envio de mensagem");
        }
        catch (Exception ex)
        {
            logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
        }
    }
}