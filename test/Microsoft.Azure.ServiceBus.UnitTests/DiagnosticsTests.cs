using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.ServiceBus.UnitTests
{
    public sealed class DiagnosticsTests : SenderReceiverClientTestBase
    {
        [Fact]
        [DisplayTestMethodName]
        async Task UpdatingPrefetchCountOnQueueClientUpdatesTheReceiverPrefetchCount()
        {
            var queueName = TestConstants.NonPartitionedQueueName;
            var queueClient = new QueueClient(TestUtility.NamespaceConnectionString, queueName, ReceiveMode.ReceiveAndDelete);

            try
            {
//                base.se
                Assert.Equal(0, queueClient.PrefetchCount);

                queueClient.PrefetchCount = 2;
                Assert.Equal(2, queueClient.PrefetchCount);

                // Message receiver should be created with latest prefetch count (lazy load).
                Assert.Equal(2, queueClient.InnerReceiver.PrefetchCount);

                queueClient.PrefetchCount = 3;
                Assert.Equal(3, queueClient.PrefetchCount);

                // Already created message receiver should have its prefetch value updated.
                Assert.Equal(3, queueClient.InnerReceiver.PrefetchCount);
            }
            finally
            {
                await queueClient.CloseAsync();
            }
        }
    }

    class TestDiagnosticsSourceListener
    {

    }
}
