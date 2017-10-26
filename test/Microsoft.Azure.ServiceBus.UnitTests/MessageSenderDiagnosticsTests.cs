using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.ServiceBus.UnitTests
{
    public sealed class MessageSenderDiagnosticsTests
    {
        private async Task cleanupqueue(QueueClient queueClient)
        {
            Message m;
            while (null != (m = await queueClient.InnerReceiver.ReceiveAsync(TimeSpan.FromSeconds(1))))
            {
                Debug.WriteLine("123");
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task StartAndStopAreFiredForSend()
        {
            var queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);
            await cleanupqueue(queueClient);
            bool startSendCalled = false;
            bool stopSendCalled = false;
            bool startReceiveCalled = false;
            bool stopReceiveCalled = false;

            bool otherCalled = false;

            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");
            Activity sendActivity = null;
            Activity receiveActivity = null;

            var listener = new FakeDiagnosticListener(kvp =>
            {
                if (kvp.Key == "Microsoft.Azure.ServiceBus.Send.Start")
                {
                    startSendCalled = true;
                    AssertCommonPayloadProperties(kvp.Value);
                    GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(kvp.Value, "Messages");

                    sendActivity = Activity.Current;
                    Assert.Equal(parentActivity, sendActivity.Parent);
                }
                else if (kvp.Key == "Microsoft.Azure.ServiceBus.Send.Stop")
                {
                    stopSendCalled = true;
                    AssertCommonPayloadProperties(kvp.Value);

                    Assert.Equal(sendActivity, Activity.Current);

                    var messages = GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(kvp.Value, "Messages");
                    Assert.Equal(1, messages.Count);
                }
                else if (kvp.Key == "Microsoft.Azure.ServiceBus.Receive.Start")
                {
                    startReceiveCalled = true;
                    AssertCommonPayloadProperties(kvp.Value);

                    Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<int>(kvp.Value, "MessageCount"));
                    
                    receiveActivity = Activity.Current;
                    Assert.Null(receiveActivity.Parent);
                }
                else if (kvp.Key == "Microsoft.Azure.ServiceBus.Receive.Stop")
                {
                    stopReceiveCalled = true;
                    AssertCommonPayloadProperties(kvp.Value);

                    Assert.Equal(receiveActivity, Activity.Current);

                    var messages = GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(kvp.Value, "Messages");
                    Assert.Equal(1, messages.Count);

                    Assert.Equal(sendActivity.Id, receiveActivity.Tags.Single(t => t.Key == "RelatedTo").Value);
                }
                else
                {
                    otherCalled = true;
                }
            });

            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();
                try
                {
                    parentActivity.Start();
                    await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                    parentActivity.Stop();

                    await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);
                }
                finally
                {
                    await queueClient.CloseAsync();
                }
            }
            Assert.True(startSendCalled);
            Assert.True(stopSendCalled);
            Assert.True(startReceiveCalled);
            Assert.True(stopReceiveCalled);

            Assert.False(otherCalled);
        }

        private static void AssertCommonPayloadProperties(object eventPayload)
        {
            var entity = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "Entity");
            GetPropertyValueFromAnonymousTypeInstance<Uri>(eventPayload, "Endpoint");

            Assert.Equal(TestConstants.NonPartitionedQueueName, entity);
            Assert.NotNull(Activity.Current);
        }

        private static void AssertCommonStopPayloadProperties(object eventPayload)
        {
            var entity = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "Entity");
            GetPropertyValueFromAnonymousTypeInstance<Uri>(eventPayload, "Endpoint");

            var status = GetPropertyValueFromAnonymousTypeInstance<TaskStatus>(eventPayload, "Status");
            Assert.Equal(TaskStatus.RanToCompletion, status);

            Assert.Equal(TestConstants.NonPartitionedQueueName, entity);
            Assert.NotNull(Activity.Current);
        }

        private static T GetPropertyValueFromAnonymousTypeInstance<T>(object obj, string propertyName)
        {
            Type t = obj.GetType();

            PropertyInfo p = t.GetRuntimeProperty(propertyName);

            object propertyValue = p.GetValue(obj);
            Assert.NotNull(propertyValue);
            Assert.IsAssignableFrom<T>(propertyValue);

            return (T)propertyValue;
        }
    }
}