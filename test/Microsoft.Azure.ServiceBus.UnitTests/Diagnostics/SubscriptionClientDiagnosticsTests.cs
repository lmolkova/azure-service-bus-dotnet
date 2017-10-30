// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.ServiceBus.UnitTests.Diagnostics
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Xunit;

    public class SubscriptionClientDiagnosticsTests : DiagnosticsTests, IDisposable
    {
        protected override string EntityName => $"{TestConstants.NonPartitionedTopicName}/{TestConstants.SubscriptionName}";
        private SubscriptionClient subscriptionClient;

        [Fact]
        [DisplayTestMethodName]
        async Task AddRemoveGetFireEvents()
        {
            subscriptionClient = new SubscriptionClient(
                TestUtility.NamespaceConnectionString,
                TestConstants.NonPartitionedTopicName,
                TestConstants.SubscriptionName,
                ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Enable();

                var ruleName = Guid.NewGuid().ToString();
                await subscriptionClient.AddRuleAsync(ruleName, new TrueFilter());
                await subscriptionClient.GetRulesAsync();
                await subscriptionClient.RemoveRuleAsync(ruleName);

                Assert.True(events.TryDequeue(out var addRuleStart));
                AssertAddRuleStart(addRuleStart.eventName, addRuleStart.payload, addRuleStart.activity);

                Assert.True(events.TryDequeue(out var addRuleStop));
                AssertAddRuleStop(addRuleStop.eventName, addRuleStop.payload, addRuleStop.activity, addRuleStart.activity);

                Assert.True(events.TryDequeue(out var getRulesStart));
                AssertGetRulesStart(getRulesStart.eventName, getRulesStart.payload, getRulesStart.activity);

                Assert.True(events.TryDequeue(out var getRulesStop));
                AssertGetRulesStop(getRulesStop.eventName, getRulesStop.payload, getRulesStop.activity, getRulesStart.activity);

                Assert.True(events.TryDequeue(out var removeRuleStart));
                AssertRemoveRuleStart(removeRuleStart.eventName, removeRuleStart.payload, removeRuleStart.activity);

                Assert.True(events.TryDequeue(out var removeRuleStop));
                AssertRemoveRuleStop(removeRuleStop.eventName, removeRuleStop.payload, removeRuleStop.activity, removeRuleStart.activity);

                Assert.True(events.IsEmpty);
            }
        }

        [Fact]
        [DisplayTestMethodName]
        async Task EventsAreNotFiredWhenDiagnosticsIsDisabled()
        {
            subscriptionClient = new SubscriptionClient(
                TestUtility.NamespaceConnectionString,
                TestConstants.NonPartitionedTopicName,
                TestConstants.SubscriptionName,
                ReceiveMode.ReceiveAndDelete);

            var events = new ConcurrentQueue<(string eventName, object payload, Activity activity)>();

            using (var listener = new FakeDiagnosticListener(kvp => events.Enqueue((kvp.Key, kvp.Value, Activity.Current))))
            using (DiagnosticListener.AllListeners.Subscribe(listener))
            {
                listener.Disable();

                await subscriptionClient.AddRuleAsync(RuleDescription.DefaultRuleName, new TrueFilter());
                var rules = await subscriptionClient.GetRulesAsync();
                await subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName);

                Assert.True(events.IsEmpty);
            }
        }
        public void Dispose()
        {
            subscriptionClient?.CloseAsync().Wait(TimeSpan.FromSeconds(2));
        }

        protected void AssertAddRuleStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.AddRule.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<RuleDescription>(payload, "Rule");
            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        protected void AssertAddRuleStop(string name, object payload, Activity activity, Activity addRuleActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.AddRule.Stop", name);
            AssertCommonStopPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<RuleDescription>(payload, "Rule");

            if (addRuleActivity != null)
            {
                Assert.Equal(addRuleActivity, activity);
            }
        }

        protected void AssertGetRulesStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.GetRules.Start", name);
            AssertCommonPayloadProperties(payload);
            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        protected void AssertGetRulesStop(string name, object payload, Activity activity, Activity getRulesActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.GetRules.Stop", name);
            
            AssertCommonStopPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<IEnumerable<RuleDescription>>(payload, "Rules");

            if (getRulesActivity != null)
            {
                Assert.Equal(getRulesActivity, activity);
            }
        }

        protected void AssertRemoveRuleStart(string name, object payload, Activity activity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RemoveRule.Start", name);
            AssertCommonPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "RuleName");
            Assert.NotNull(activity);
            Assert.Null(activity.Parent);
        }

        protected void AssertRemoveRuleStop(string name, object payload, Activity activity, Activity removeRuleActivity)
        {
            Assert.Equal("Microsoft.Azure.ServiceBus.RemoveRule.Stop", name);

            AssertCommonStopPayloadProperties(payload);
            GetPropertyValueFromAnonymousTypeInstance<string>(payload, "RuleName");

            if (removeRuleActivity != null)
            {
                Assert.Equal(removeRuleActivity, activity);
            }
        }
    }
}
