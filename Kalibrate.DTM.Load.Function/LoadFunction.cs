using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Kalibrate.DTM.DataMapper.Shared;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Kalibrate.DTM.Load.Function
{
    public class LoadFunction
    {
        [FunctionName("LoadFunction")]
        public static async Task Run([ServiceBusTrigger("load-test", Connection = "LoadQueueConnectionString", IsSessionsEnabled = true)] Message message, IMessageSession messageSession, ILogger log)
        {
            log.LogInformation($"MessageReceived: {message.MessageId}");

            //Force assembly to be in memory for this example - can be fixed with DI I think
            var _ = typeof(IntermediateModelRepository);

            var messages = new List<Message> { message };
            var entityName = (string)message.UserProperties["EntityName"];
            var taskId = (Guid)message.UserProperties["TaskId"];
            var isLast = (bool)message.UserProperties["Last"];
            await messageSession.CompleteAsync(message.SystemProperties.LockToken);

            //Get the model type
            var model = AppDomain.CurrentDomain
                .GetAssemblies()
                .FirstOrDefault(assembly => assembly.GetName().Name == "Kalibrate.DTM.DataMapper.Shared")
                ?.GetTypes()
                .FirstOrDefault(type => type.Name == entityName);

            //Keep receiving new messages in the session until the last message is received.
            while (!isLast)
            {
                message = await messageSession.ReceiveAsync();
                messages.Add(message);
                log.LogInformation($"MessageReceived: {message.MessageId} ");
                isLast = (bool)message.UserProperties["Last"];
                await messageSession.CompleteAsync(message.SystemProperties.LockToken);
            }

            log.LogInformation($"Messages - Task:{taskId}, Messages:{messages.Count}");

            //All messages in the session have been received, now process them
            ProcessMessages(messages, model, taskId, messageSession, log);

            //All messages have been processed and closed, so we can now close the session
            await messageSession.CloseAsync();
        }

        private static void ProcessMessages(List<Message> messages, Type model, Guid taskId, IMessageSession messageSession, ILogger log)
        {

            messages.ForEach(async message =>
            {

                var record = JsonSerializer.Deserialize(Encoding.UTF8.GetString(message.Body), model);

                //At this point the runtime will now handle this object as the model type
                //(i.e Kalibrate.DTM.DataMapper.Shared.IntermediateModels.OwnSite)
                //visual studio will still show as an object as this is converted at runtime
                //Is this suitable for what we will need?

                //do what we need to with the record
                await Task.Delay(0);
                log.LogInformation($"Processed: {message.MessageId} - Type: {record}");

            });


        }
    }

}
