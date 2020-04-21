using Akka.Configuration;
using Akka.Persistence.Pulsar.Snapshot;
using Akka.Persistence.TCK.Snapshot;
using Xunit.Abstractions;

namespace Akka.Persistence.Pulsar.Tests
{
    public class PulsarSnapshotStoreSpec : SnapshotStoreSpec
    {
        public static Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka.persistence.snapshot-store.plugin = ""akka.persistence.snapshot-store.pulsar""
            akka.test.single-expect-default = 10s
        ").WithFallback(PulsarPersistence.DefaultConfiguration());
        
        public PulsarSnapshotStoreSpec(ITestOutputHelper output) : base(SpecConfig , output: output)
        {
            Initialize();
        }
    }
}