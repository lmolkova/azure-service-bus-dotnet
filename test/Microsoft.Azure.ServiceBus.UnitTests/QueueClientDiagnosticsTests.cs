using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.ServiceBus.UnitTests
{
    public sealed class QueueClientDiagnosticsTests  : IDisposable
    {
        private QueueClient queueClient;

        [Fact]
        [DisplayTestMethodName]
        async Task EventsAreNotFiredWhenDiagnosticsIsDisabled()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            Activity processActivity = null;

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Disable();

                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                queueClient.RegisterMessageHandler((msg, ct) =>
                    {
                        processActivity = Activity.Current;
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs => Task.CompletedTask);

                processingDone.WaitOne(TimeSpan.FromSeconds(10));
                Assert.True(events.IsEmpty);
                Assert.Null(processActivity);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task EventsAreNotFiredWhenDiagnosticsIsDisabledForQueue()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            Activity processActivity = null;

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable((name, queueName, arg) =>
                    queueName == null || queueName.ToString() != TestConstants.NonPartitionedQueueName);

                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                queueClient.RegisterMessageHandler((msg, ct) =>
                    {
                        processActivity = Activity.Current;
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs => Task.CompletedTask);

                processingDone.WaitOne(TimeSpan.FromSeconds(10));

                Assert.True(events.IsEmpty);
                Assert.Null(processActivity);
            }
        }


        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveWithHandlerFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            Activity parentActivity = new Activity("test").AddBaggage("k1", "v1").AddBaggage("k2", "v2");
            Activity processActivity = null;
            bool exceptionCalled = false;
            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();

                parentActivity.Start();
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                parentActivity.Stop();

                queueClient.RegisterMessageHandler((msg, ct) =>
                    {
                        processActivity = Activity.Current;
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs =>
                    {
                        exceptionCalled = true;
                        return Task.CompletedTask;
                    });
                processingDone.WaitOne(TimeSpan.FromSeconds(10));

                Assert.True(events.TryDequeue(out var sendStart));
                AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, parentActivity);

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity);

                Assert.True(events.TryDequeue(out var receiveStart));
                AssertReceiveStart(receiveStart.eventName, receiveStart.payload, receiveStart.activity);

                Assert.True(events.TryDequeue(out var receiveStop));
                AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity,
                    receiveStart.activity, sendStart.activity);

                Assert.True(events.TryDequeue(out var processStart));
                AssertProcessStart(processStart.eventName, processStart.payload, processStart.activity,
                    sendStart.activity);

                // message is processed, but complete happens after that
                // let's wat until Complete starts and ends and Process ends
                int maxWait = 10;
                int wait = 0;
                while (wait++ < maxWait && events.Count < 3)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }

                Assert.True(events.TryDequeue(out var completeStart));
                AssertCompleteStart(completeStart.eventName, completeStart.payload, completeStart.activity,
                    processStart.activity);

                Assert.True(events.TryDequeue(out var completeStop));
                AssertCompleteStop(completeStop.eventName, completeStop.payload, completeStop.activity, completeStart.activity, processStart.activity);

                Assert.True(events.TryDequeue(out var processStop));
                AssertProcessStop(processStop.eventName, processStop.payload, processStop.activity,
                    processStart.activity);

                listener.Disable();
                while (events.TryDequeue(out var evnt))
                {
                    Assert.StartsWith("Microsoft.Azure.ServiceBus.Receive.", evnt.eventName);
                }

                Assert.Equal(processStop.activity, processActivity);
                Assert.False(exceptionCalled);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveWithHandlerFireExceptionEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            bool exceptionCalled = false;
            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);

                listener.Enable((name, queueName, arg) => !name.EndsWith("Start"));

                queueClient.RegisterMessageHandler((msg, ct) => throw new Exception("123"),
                exArgs =>
                {
                    exceptionCalled = true;
                    processingDone.Set();
                    return Task.CompletedTask;
                });
                processingDone.WaitOne(TimeSpan.FromSeconds(10));
                Assert.True(exceptionCalled);

                Assert.True(events.TryDequeue(out var receiveStop));
                AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity, null, null);

                // message is processed, but complete happens after that
                // let's spin until Complete call starts and ends
                Assert.True(SpinWait.SpinUntil(() => !events.IsEmpty, TimeSpan.FromSeconds(2)));

                Assert.True(events.TryDequeue(out var abandonStop));
                AssertAbandonStop(abandonStop.eventName, abandonStop.payload, abandonStop.activity, null);

                Assert.True(SpinWait.SpinUntil(() => !events.IsEmpty, TimeSpan.FromSeconds(2)));

                Assert.True(events.TryDequeue(out var exception));
                AssertException(exception.eventName, exception.payload, exception.activity, null);

                Assert.True(events.TryDequeue(out var processStop));
                AssertProcessStop(processStop.eventName, processStop.payload, processStop.activity, null);
                Assert.Equal(processStop.activity, abandonStop.activity.Parent);
                Assert.Equal(processStop.activity, exception.activity);

                listener.Disable();
                while (events.TryDequeue(out var evnt))
                {
                    Assert.StartsWith("Microsoft.Azure.ServiceBus.Receive.", evnt.eventName);
                }
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task AbandonCompleteFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);

                listener.Enable();
                await TestUtility.AbandonMessagesAsync(queueClient.InnerReceiver, messages);

                listener.Disable();
                messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);
                listener.Enable();

                await TestUtility.CompleteMessagesAsync(queueClient.InnerReceiver, messages);
 
            Assert.True(events.TryDequeue(out var abandonStart));
            AssertAbandonStart(abandonStart.eventName, abandonStart.payload, abandonStart.activity, null);

            Assert.True(events.TryDequeue(out var abandonStop));
            AssertAbandonStop(abandonStop.eventName, abandonStop.payload, abandonStop.activity, abandonStart.activity);

            Assert.True(events.TryDequeue(out var completeStart));
            AssertCompleteStart(completeStart.eventName, completeStart.payload, completeStart.activity, null);

            Assert.True(events.TryDequeue(out var completeStop));
            AssertCompleteStop(completeStop.eventName, completeStop.payload, completeStop.activity, completeStart.activity, null);

            Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task PeekFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                var message = await TestUtility.PeekMessageAsync(queueClient.InnerReceiver);

                listener.Disable();
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);
                await TestUtility.CompleteMessagesAsync(queueClient.InnerReceiver, messages);

                Assert.True(events.TryDequeue(out var sendStart));
                Assert.True(events.TryDequeue(out var sendStop));
                Assert.True(events.TryDequeue(out var peekStart));
                AssertPeekStart(peekStart.eventName, peekStart.payload, peekStart.activity);

                Assert.True(events.TryDequeue(out var peekStop));
                AssertPeekStop(peekStop.eventName, peekStop.payload, peekStop.activity, peekStart.activity, sendStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task DeadLetterFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);

                listener.Enable();
                await TestUtility.DeadLetterMessagesAsync(queueClient.InnerReceiver, messages);
                listener.Disable();

                QueueClient deadLetterQueueClient = null;
                try
                {
                    deadLetterQueueClient = new QueueClient(TestUtility.NamespaceConnectionString,
                        EntityNameHelper.FormatDeadLetterPath(queueClient.QueueName));
                    await TestUtility.ReceiveMessagesAsync(deadLetterQueueClient.InnerReceiver, 1);
                }
                finally
                {
                    deadLetterQueueClient?.CloseAsync().Wait(TimeSpan.FromSeconds(10));
                }

                Assert.True(events.TryDequeue(out var deadLetterStart));
                AssertDeadLetterStart(deadLetterStart.eventName, deadLetterStart.payload, deadLetterStart.activity, null);

                Assert.True(events.TryDequeue(out var deadLetterStop));
                AssertDeadLetterStop(deadLetterStop.eventName, deadLetterStop.payload, deadLetterStop.activity, deadLetterStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task RenewLockFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);

                listener.Enable();
                await queueClient.InnerReceiver.RenewLockAsync(messages[0]);
                listener.Disable();
                await TestUtility.CompleteMessagesAsync(queueClient.InnerReceiver, messages);

                Assert.True(events.TryDequeue(out var renewStart));
                AssertRenewLockStart(renewStart.eventName, renewStart.payload, renewStart.activity, null);

                Assert.True(events.TryDequeue(out var renewStop));
                AssertRenewLockStop(renewStop.eventName, renewStop.payload, renewStop.activity, renewStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task DeferReceiveDefferedFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 1);
                await TestUtility.DeferMessagesAsync(queueClient.InnerReceiver, messages);
                var message = await queueClient.InnerReceiver.ReceiveDeferredMessageAsync(messages[0].SystemProperties
                        .SequenceNumber);

                listener.Disable();
                await TestUtility.CompleteMessagesAsync(queueClient.InnerReceiver, new []{message});

                Assert.True(events.TryDequeue(out var sendStart));
                Assert.True(events.TryDequeue(out var sendStop));
                Assert.True(events.TryDequeue(out var receiveStart));
                Assert.True(events.TryDequeue(out var receiveStop));

                Assert.True(events.TryDequeue(out var deferStart));
                AssertDeferStart(deferStart.eventName, deferStart.payload, deferStart.activity, null);

                Assert.True(events.TryDequeue(out var deferStop));
                AssertDeferStop(deferStop.eventName, deferStop.payload, deferStop.activity, deferStart.activity);

                Assert.True(events.TryDequeue(out var receiveDefferedStart));
                AssertReceiveDefferedStart(receiveDefferedStart.eventName, receiveDefferedStart.payload,
                    receiveDefferedStart.activity);

                Assert.True(events.TryDequeue(out var receiveDefferedStop));
                AssertReceiveDefferedStop(receiveDefferedStop.eventName, receiveDefferedStop.payload,
                    receiveDefferedStop.activity, receiveDefferedStart.activity, sendStart.activity);

                Assert.True(events.IsEmpty);
            }
        }


        [Fact]
        [DisplayTestMethodName]
        async Task SendAndReceiveWithHandlerFilterOutStartEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable((name, queueName, arg) => !name.EndsWith("Start"));

                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                queueClient.RegisterMessageHandler((msg, ct) =>
                    {
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs => Task.CompletedTask);
                processingDone.WaitOne(TimeSpan.FromSeconds(10));

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, null);

                Assert.True(events.TryDequeue(out var receiveStop));
                AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity, null,
                    sendStop.activity);

                Assert.True(events.TryDequeue(out var processStop));
                AssertProcessStop(processStop.eventName, processStop.payload, processStop.activity, null);

                listener.Disable();

                while (events.TryDequeue(out var evnt))
                {
                    Assert.StartsWith("Microsoft.Azure.ServiceBus.Receive.", evnt.eventName);
                }
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task ScheduleAndCancelFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            Activity parentActivity = new Activity("test");
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();

                parentActivity.Start();
                var sequenceNumber = await queueClient.InnerSender.ScheduleMessageAsync(new Message(),
                        DateTimeOffset.UtcNow.AddHours(1));
                await queueClient.InnerSender.CancelScheduledMessageAsync(sequenceNumber);
                parentActivity.Stop();

                Assert.True(events.TryDequeue(out var scheduleStart));
                AssertScheduleStart(scheduleStart.eventName, scheduleStart.payload, scheduleStart.activity,
                    parentActivity);

                Assert.True(events.TryDequeue(out var scheduleStop));
                AssertScheduleStop(scheduleStop.eventName, scheduleStop.payload, scheduleStop.activity,
                    scheduleStart.activity);

                Assert.True(events.TryDequeue(out var cancelStart));
                AssertCancelStart(cancelStart.eventName, cancelStart.payload, cancelStart.activity, parentActivity);

                Assert.True(events.TryDequeue(out var cancelStop));
                AssertCancelStop(cancelStop.eventName, cancelStop.payload, cancelStop.activity, cancelStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        #region Send

        private void AssertSendStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Send.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(payload, "Messages");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertSendStop(string name, object payload, Activity activity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Send.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (sendActivity != null)
            {
                Assert.Equal(sendActivity, activity);
            }

            var messages = GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(payload, "Messages");
            Assert.Equal(1, messages.Count);
        }

        #endregion

        #region Complete

        private void AssertCompleteStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Complete.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<IList<string>>(payload, "LockTokens");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertCompleteStop(string name, object payload, Activity activity, Activity completeActivity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Complete.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (completeActivity != null)
            {
                Assert.Equal(completeActivity, activity);
            }

            var tokens = GetPropertyValueFromAnonymousTypeInstance<IList<string>>(payload, "LockTokens");
            Assert.Equal(1, tokens.Count);
        }

        #endregion

        #region Abandon

        private void AssertAbandonStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Abandon.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertAbandonStop(string name, object payload, Activity activity, Activity abandonActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Abandon.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (abandonActivity != null)
            {
                Assert.Equal(abandonActivity, activity);
            }

            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");
        }

        #endregion

        #region Defer

        private void AssertDeferStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Defer.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertDeferStop(string name, object payload, Activity activity, Activity deferActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Defer.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (deferActivity != null)
            {
                Assert.Equal(deferActivity, activity);
            }

            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");
        }

        #endregion

        #region Defer

        private void AssertDeadLetterStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.DeadLetter.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertDeadLetterStop(string name, object payload, Activity activity, Activity deadLetterActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.DeadLetter.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (deadLetterActivity != null)
            {
                Assert.Equal(deadLetterActivity, activity);
            }

            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");
        }

        #endregion

        #region Schedule

        private void AssertScheduleStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Schedule.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<Message>(payload, "Message");
            GetPropertyValueFromAnonymousTypeInstance<DateTimeOffset>(payload, "ScheduleEnqueueTimeUtc");
            
            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertScheduleStop(string name, object payload, Activity activity, Activity scheduleActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Schedule.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            Assert.Equal(scheduleActivity, activity);
            var message = GetPropertyValueFromAnonymousTypeInstance<Message>(payload, "Message");
            GetPropertyValueFromAnonymousTypeInstance<DateTimeOffset>(payload, "ScheduleEnqueueTimeUtc");
            GetPropertyValueFromAnonymousTypeInstance<long>(payload, "SequenceNumber");
            Assert.NotNull(message);
            Assert.Contains("Request-Id", message.UserProperties.Keys);
            if (scheduleActivity.Baggage.Any())
            {
                Assert.Contains("Correlation-Context", message.UserProperties.Keys);
            }
        }

        #endregion

        #region Cancel

        private void AssertCancelStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Cancel.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<long>(payload, "SequenceNumber");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertCancelStop(string name, object payload, Activity activity, Activity scheduleActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Cancel.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            Assert.Equal(scheduleActivity, activity);
            GetPropertyValueFromAnonymousTypeInstance<long>(payload, "SequenceNumber");
        }

        #endregion

        #region Receive

        private void AssertReceiveStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Receive.Start", name);
            AssertCommonPayloadProperties(payload);

            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<int>(payload, "MessageCount"));

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        private void AssertReceiveStop(string name, object payload, Activity activity, Activity receiveActivity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Receive.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (receiveActivity != null)
            {
                Assert.Equal(receiveActivity, activity);
            }
            var messages = GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(payload, "Messages");
            Assert.Equal(1, messages.Count);

            if (sendActivity != null)
            {
                Assert.Equal(sendActivity.Id, activity.Tags.Single(t => t.Key == "RelatedTo").Value);
            }
        }

        #endregion

        #region ReceiveDeffered

        private void AssertReceiveDefferedStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.ReceiveDeffered.Start", name);
            AssertCommonPayloadProperties(payload);

            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<IEnumerable<long>>(payload, "SequenceNumbers").Count());

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        private void AssertReceiveDefferedStop(string name, object payload, Activity activity, Activity receiveActivity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.ReceiveDeffered.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (receiveActivity != null)
            {
                Assert.Equal(receiveActivity, activity);
            }

            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<IEnumerable<long>>(payload, "SequenceNumbers").Count());
            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<IList<Message>>(payload, "Messages").Count);

            Assert.Equal(sendActivity.Id, activity.Tags.Single(t => t.Key == "RelatedTo").Value);
        }

        #endregion

        #region Process

        private void AssertProcessStart(string name, object payload, Activity activity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Process.Start", name);
            AssertCommonPayloadProperties(payload);

            GetPropertyValueFromAnonymousTypeInstance<Message>(payload, "Message");

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sendActivity.Id, activity.ParentId);
            Assert.Equal(sendActivity.Baggage.OrderBy(p => p.Key), activity.Baggage.OrderBy(p => p.Key));
        }

        private void AssertProcessStop(string name, object payload, Activity activity, Activity processActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Process.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (processActivity != null)
            {
                Assert.Equal(processActivity, activity);
            }
        }

        #endregion

        #region Peek

        private void AssertPeekStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Peek.Start", name);
            AssertCommonPayloadProperties(payload);
            
            GetPropertyValueFromAnonymousTypeInstance<long>(payload, "FromSequenceNumber");
            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<int>(payload, "MessageCount"));

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        private void AssertPeekStop(string name, object payload, Activity activity, Activity peekActivity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Peek.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (peekActivity != null)
            {
                Assert.Equal(peekActivity, activity);
            }

            GetPropertyValueFromAnonymousTypeInstance<long>(payload, "FromSequenceNumber");
            Assert.Equal(1, GetPropertyValueFromAnonymousTypeInstance<int>(payload, "MessageCount"));

            Assert.Equal(sendActivity.Id, activity.Tags.Single(t => t.Key == "RelatedTo").Value);
        }

        #endregion


        #region RenewLock

        private void AssertRenewLockStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RenewLock.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");

            Assert.NotNull(activity);
            Assert.Equal(parentActivity, activity.Parent);
        }

        private void AssertRenewLockStop(string name, object payload, Activity activity, Activity renewLockActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RenewLock.Stop", name);
            AssertCommonStopPayloadProperties(payload);

            if (renewLockActivity != null)
            {
                Assert.Equal(renewLockActivity, activity);
            }

            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "LockToken");
            GetPropertyValueFromAnonymousTypeInstance<DateTime>(payload, "LockedUntilUtc");
        }

        #endregion
        private void AssertException(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.Exception", name);
            AssertCommonPayloadProperties(payload);

            GetPropertyValueFromAnonymousTypeInstance<Exception>(payload, "Exception");

            Assert.NotNull(activity);
            if (parentActivity != null)
            {
                Assert.Equal(parentActivity, activity.Parent);
            }
        }

        private static void AssertCommonPayloadProperties(object eventPayload)
        {
            var entity = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "Entity");
            GetPropertyValueFromAnonymousTypeInstance<Uri>(eventPayload, "Endpoint");

            Assert.Equal(TestConstants.NonPartitionedQueueName, entity);
        }

        private static void AssertCommonStopPayloadProperties(object eventPayload)
        {
            var entity = GetPropertyValueFromAnonymousTypeInstance<string>(eventPayload, "Entity");
            GetPropertyValueFromAnonymousTypeInstance<Uri>(eventPayload, "Endpoint");

            var status = GetPropertyValueFromAnonymousTypeInstance<TaskStatus>(eventPayload, "Status");
            Assert.Equal(TaskStatus.RanToCompletion, status);

            Assert.Equal(TestConstants.NonPartitionedQueueName, entity);
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

        public void Dispose()
        {

            queueClient?.CloseAsync().Wait(TimeSpan.FromSeconds(10));
        }
    }
}