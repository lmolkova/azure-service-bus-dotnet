// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.ServiceBus.UnitTests.Diagnostics
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    public sealed class QueueClientDiagnosticsTests : DiagnosticsTests, IDisposable
    {
        protected override string EntityName => TestConstants.NonPartitionedQueueName;
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

                processingDone.WaitOne(TimeSpan.FromSeconds(2));
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

                processingDone.WaitOne(TimeSpan.FromSeconds(2));

                Assert.True(events.IsEmpty);
                Assert.Null(processActivity);
            }
        }


        [Fact]
        [DisplayTestMethodName]
        async Task SendAndWithHandlerFireEvents()
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
                listener.Enable( (name, queueName, arg) => !name.Contains("Receive"));

                parentActivity.Start();

                var message = new Message() { MessageId = "messageId" };
                await queueClient.SendAsync(message);
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
                processingDone.WaitOne(TimeSpan.FromSeconds(2));

                Assert.True(events.TryDequeue(out var sendStart));
                AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, parentActivity);

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity);

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
                Assert.False(events.TryDequeue(out var evnt));

                Assert.Equal(processStop.activity, processActivity);
                Assert.False(exceptionCalled);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SendAndHandlerFireExceptionEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.PeekLock);

            bool exceptionCalled = false;
            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);

                listener.Enable((name, queueName, arg) => !name.EndsWith("Start") && !name.Contains("Receive"));

                queueClient.RegisterMessageHandler((msg, ct) => throw new Exception("123"),
                exArgs =>
                {
                    exceptionCalled = true;
                    processingDone.Set();
                    return Task.CompletedTask;
                });
                processingDone.WaitOne(TimeSpan.FromSeconds(10));
                Assert.True(exceptionCalled);

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
        async Task BatchSendReceiveFireEvent()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();
            var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current)));

            using (listener)
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 2);
                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 3);
                var messages = await TestUtility.ReceiveMessagesAsync(queueClient.InnerReceiver, 5);

                Assert.True(events.TryDequeue(out var sendStart1));
                AssertSendStart(sendStart1.eventName, sendStart1.payload, sendStart1.activity, null, 2);

                Assert.True(events.TryDequeue(out var sendStop1));
                AssertSendStop(sendStop1.eventName, sendStop1.payload, sendStop1.activity, sendStop1.activity, 2);

                Assert.True(events.TryDequeue(out var sendStart2));
                AssertSendStart(sendStart2.eventName, sendStart2.payload, sendStart2.activity, null, 3);

                Assert.True(events.TryDequeue(out var sendStop2));
                AssertSendStop(sendStop2.eventName, sendStop2.payload, sendStop2.activity, sendStop2.activity, 3);

                int receivedStopCount = 0;
                string relatedTo = "";
                while (events.TryDequeue(out var receiveStart))
                {
                    var startCount = AssertReceiveStart(receiveStart.eventName, receiveStart.payload, receiveStart.activity, -1);

                    Assert.True(events.TryDequeue(out var receiveStop));
                    receivedStopCount += AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity, receiveStart.activity, null, startCount, -1);
                    relatedTo += receiveStop.activity.Tags.Single(t => t.Key == "RelatedTo").Value;
                }

                Assert.Equal(5, receivedStopCount);
                Assert.Contains(sendStart1.activity.Id, relatedTo);
                Assert.Contains(sendStart2.activity.Id, relatedTo);

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
                        EntityNameHelper.FormatDeadLetterPath(queueClient.QueueName), ReceiveMode.ReceiveAndDelete);
                    await TestUtility.ReceiveMessagesAsync(deadLetterQueueClient.InnerReceiver, 1);
                }
                finally
                {
                    deadLetterQueueClient?.CloseAsync().Wait(TimeSpan.FromSeconds(2));
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
        async Task SendAndHandlerFilterOutStartEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.NonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable((name, queueName, arg) => !name.EndsWith("Start") && !name.Contains("Receive"));

                await TestUtility.SendMessagesAsync(queueClient.InnerSender, 1);
                queueClient.RegisterMessageHandler((msg, ct) =>
                    {
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs => Task.CompletedTask);
                processingDone.WaitOne(TimeSpan.FromSeconds(2));

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, null);

                Assert.True(events.TryDequeue(out var processStop));
                AssertProcessStop(processStop.eventName, processStop.payload, processStop.activity, null);

                listener.Disable();

                Assert.False(events.TryDequeue(out var evnt));
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

        public void Dispose()
        {
            queueClient?.CloseAsync().Wait(TimeSpan.FromSeconds(1));
        }
    }
}