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

    using Microsoft.Azure.ServiceBus.Core;
    using Xunit;

    public class SessionDiagnosticsTests : DiagnosticsTests, IDisposable
    {
        protected override string EntityName => TestConstants.SessionNonPartitionedQueueName;
        private IMessageSession messageSession;
        private SessionClient sessionClient;
        private MessageSender messageSender;
        private QueueClient queueClient;

        [Fact]
        [DisplayTestMethodName]
        async Task AcceptSetAndGetStateGetFireEvents()
        {
            messageSender = new MessageSender(TestUtility.NamespaceConnectionString, TestConstants.SessionNonPartitionedQueueName);
            sessionClient = new SessionClient(TestUtility.NamespaceConnectionString, TestConstants.SessionNonPartitionedQueueName);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();

                var sessionId = Guid.NewGuid().ToString();
                await messageSender.SendAsync(new Message { MessageId = "messageId", SessionId = sessionId });
                messageSession = await sessionClient.AcceptMessageSessionAsync(sessionId);

                await messageSession.SetStateAsync(new byte []{1});
                await messageSession.GetStateAsync();
                await messageSession.SetStateAsync(new byte[] {});

                await messageSession.ReceiveAsync();

                Assert.True(events.TryDequeue(out var sendStart));
                AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, null);

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity);

                Assert.True(events.TryDequeue(out var acceptStart));
                AssertAcceptMessageSessionStart(acceptStart.eventName, acceptStart.payload, acceptStart.activity);

                Assert.True(events.TryDequeue(out var acceptStop));
                AssertAcceptMessageSessionStop(acceptStop.eventName, acceptStop.payload, acceptStop.activity, acceptStart.activity);

                Assert.True(events.TryDequeue(out var setStateStart));
                AssertSetSessionStateStart(setStateStart.eventName, setStateStart.payload, setStateStart.activity);

                Assert.True(events.TryDequeue(out var setStateStop));
                AssertSetSessionStateStop(setStateStop.eventName, setStateStop.payload, setStateStop.activity, setStateStart.activity);

                Assert.True(events.TryDequeue(out var getStateStart));
                AssertGetSessionStateStart(getStateStart.eventName, getStateStart.payload, getStateStart.activity);

                Assert.True(events.TryDequeue(out var getStateStop));
                AssertGetSessionStateStop(getStateStop.eventName, getStateStop.payload, getStateStop.activity, getStateStop.activity);

                Assert.True(events.TryDequeue(out var setStateStart2));
                Assert.True(events.TryDequeue(out var setStateStop2));

                Assert.True(events.TryDequeue(out var receiveStart));
                AssertReceiveStart(receiveStart.eventName, receiveStart.payload, receiveStart.activity);

                Assert.True(events.TryDequeue(out var receiveStop));
                AssertReceiveStop(receiveStop.eventName, receiveStop.payload, receiveStop.activity, receiveStart.activity, sendStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task EventsAreNotFiredWhenDiagnosticsIsDisabled()
        {
            messageSender = new MessageSender(TestUtility.NamespaceConnectionString, TestConstants.SessionNonPartitionedQueueName);
            sessionClient = new SessionClient(TestUtility.NamespaceConnectionString, TestConstants.SessionNonPartitionedQueueName);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Disable();

                var sessionId = Guid.NewGuid().ToString();
                await messageSender.SendAsync(new Message { MessageId = "messageId", SessionId = sessionId });
                messageSession = await sessionClient.AcceptMessageSessionAsync(sessionId);

                await messageSession.SetStateAsync(new byte[] { 1 });
                await messageSession.GetStateAsync();
                await messageSession.SetStateAsync(new byte[] { });

                await messageSession.ReceiveAsync();

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task SessionHandlerFireEvents()
        {
            queueClient = new QueueClient(TestUtility.NamespaceConnectionString, TestConstants.SessionNonPartitionedQueueName, ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            ManualResetEvent processingDone = new ManualResetEvent(false);
            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable((name, queueName, arg) => !name.Contains("AcceptMessageSession") && !name.Contains("Receive"));

                var sessionId = Guid.NewGuid().ToString();
                var message = new Message() { MessageId = "messageId", SessionId = sessionId };
                await queueClient.SendAsync(message);

                queueClient.RegisterSessionHandler((session, msg, ct) =>
                    {
                        processingDone.Set();
                        return Task.CompletedTask;
                    },
                    exArgs => Task.CompletedTask);
                processingDone.WaitOne(TimeSpan.FromSeconds(2));

                Assert.True(events.TryDequeue(out var sendStart));
                AssertSendStart(sendStart.eventName, sendStart.payload, sendStart.activity, null);

                Assert.True(events.TryDequeue(out var sendStop));
                AssertSendStop(sendStop.eventName, sendStop.payload, sendStop.activity, sendStart.activity);

                listener.Enable((name, queueName, arg) => !name.Contains("Receive"));

                Assert.True(events.TryDequeue(out var processStart));
                AssertProcessSessionStart(processStart.eventName, processStart.payload, processStart.activity, sendStart.activity);

                Assert.True(events.TryDequeue(out var processStop));
                AssertProcessSessionStop(processStop.eventName, processStop.payload, processStop.activity, processStart.activity);

                listener.Disable();
                while (events.TryDequeue(out var evnt))
                {
                    Assert.StartsWith("Microsoft.Azure.ServiceBus.Receive.", evnt.eventName);
                }
            }
        }

        public void Dispose()
        {
            queueClient?.CloseAsync().Wait();
            messageSession?.CloseAsync().Wait(TimeSpan.FromSeconds(1));
            sessionClient?.CloseAsync().Wait(TimeSpan.FromSeconds(1));
            messageSender?.CloseAsync().Wait(TimeSpan.FromSeconds(1));
        }

        protected void AssertAcceptMessageSessionStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.AcceptMessageSession.Start", name);
            this.AssertCommonPayloadProperties(payload);

            var sessionId = this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            this.GetPropertyValueFromAnonymousTypeInstance<TimeSpan>(payload, "ServerWaitTime");
            
            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sessionId, activity.Tags.Single(t => t.Key == "SessionId").Value);
        }

        protected void AssertAcceptMessageSessionStop(string name, object payload, Activity activity, Activity acceptActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.AcceptMessageSession.Stop", name);
            this.AssertCommonStopPayloadProperties(payload);
            this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            this.GetPropertyValueFromAnonymousTypeInstance<TimeSpan>(payload, "ServerWaitTime");
            this.GetPropertyValueFromAnonymousTypeInstance<IMessageSession>(payload, "Session");

            if (acceptActivity != null)
            {
                Assert.Equal(acceptActivity, activity);
            }
        }

        protected void AssertGetSessionStateStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.GetSessionState.Start", name);
            this.AssertCommonPayloadProperties(payload);

            var sessionId = this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            
            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sessionId, activity.Tags.Single(t => t.Key == "SessionId").Value);
        }

        protected void AssertGetSessionStateStop(string name, object payload, Activity activity, Activity getStateActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.GetSessionState.Stop", name);
            this.AssertCommonStopPayloadProperties(payload);
            this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            this.GetPropertyValueFromAnonymousTypeInstance<byte[]>(payload, "State");

            if (getStateActivity != null)
            {
                Assert.Equal(getStateActivity, activity);
            }
        }

        protected void AssertSetSessionStateStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.SetSessionState.Start", name);
            this.AssertCommonPayloadProperties(payload);
            var sessionId = this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            this.GetPropertyValueFromAnonymousTypeInstance<byte[]>(payload, "State");

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sessionId, activity.Tags.Single(t => t.Key == "SessionId").Value);
        }

        protected void AssertSetSessionStateStop(string name, object payload, Activity activity, Activity setStateActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.SetSessionState.Stop", name);

            this.AssertCommonStopPayloadProperties(payload);
            this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");
            this.GetPropertyValueFromAnonymousTypeInstance<byte[]>(payload, "State");

            if (setStateActivity != null)
            {
                Assert.Equal(setStateActivity, activity);
            }
        }

        protected void AssertRenewSessionLockStart(string name, object payload, Activity activity, Activity parentActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RenewSessionLock.Start", name);
            this.AssertCommonPayloadProperties(payload);
            var sessionId= this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sessionId, activity.Tags.Single(t => t.Key == "SessionId").Value);
        }

        protected void AssertRenewSessionLockStop(string name, object payload, Activity activity, Activity renewActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RenewSessionLock.Stop", name);

            this.AssertCommonStopPayloadProperties(payload);
            this.GetPropertyValueFromAnonymousTypeInstance<string>(payload, "SessionId");

            if (renewActivity != null)
            {
                Assert.Equal(renewActivity, activity);
            }
        }

        protected void AssertProcessSessionStart(string name, object payload, Activity activity, Activity sendActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.ProcessSession.Start", name);
            AssertCommonPayloadProperties(payload);

            GetPropertyValueFromAnonymousTypeInstance<IMessageSession>(payload, "Session");
            var message = GetPropertyValueFromAnonymousTypeInstance<Message>(payload, "Message");

            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
            Assert.Equal(sendActivity.Id, activity.ParentId);
            Assert.Equal(sendActivity.Baggage.OrderBy(p => p.Key), activity.Baggage.OrderBy(p => p.Key));

            AssertTags(message, activity);
        }

        protected void AssertProcessSessionStop(string name, object payload, Activity activity, Activity processActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.ProcessSession.Stop", name);
            AssertCommonStopPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<IMessageSession>(payload, "Session");
            var message = GetPropertyValueFromAnonymousTypeInstance<Message>(payload, "Message");

            if (processActivity != null)
            {
                Assert.Equal(processActivity, activity);
            }
        }
    }
}
