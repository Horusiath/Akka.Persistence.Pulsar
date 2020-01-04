using Akka.Persistence.Snapshot;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.Pulsar.Snapshot
{
    /// <summary>
    ///     Pulsar-backed snapshot store for Akka.Persistence.
    /// </summary>
    public class PulsarSnapshotStore : SnapshotStore
    {
        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new NotImplementedException();
        }

        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new NotImplementedException();
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            throw new NotImplementedException();
        }
    }
}
