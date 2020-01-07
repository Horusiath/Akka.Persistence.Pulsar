using Akka.Event;
using Akka.Persistence.Snapshot;
using DotPulsar.Abstractions;
using System;
using System.Threading.Tasks;

namespace Akka.Persistence.Pulsar.Snapshot
{
    /// <summary>
    ///     Pulsar-backed snapshot store for Akka.Persistence.
    /// </summary>
    /// 
    
    public class PulsarSnapshotStore : SnapshotStore
    {

        private readonly PulsarSettings settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly IPulsarClient client;
        private SerializationHelper _serialization;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarSnapshotStore() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
            _serialization = new SerializationHelper(Context.System);
        }

        public PulsarSnapshotStore(PulsarSettings settings)
        {
            this.settings = settings;
            this.client = settings.CreateClient();
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            //Probably Pulsar Message retention and expiry
            throw new NotImplementedException();
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //Probable Pulsar Message retention and expiry
            throw new NotImplementedException();
        }

        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new NotImplementedException();
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));
            throw new NotImplementedException();
        }
    }
}
