using Akka.Event;
using Akka.Persistence.Snapshot;
using DotPulsar;
using DotPulsar.Abstractions;
using System;
using System.Buffers;
using System.Collections.Concurrent;
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
        private readonly IPulsarClient _client;
        private readonly ConcurrentDictionary<string,IProducer> _snapshotProducers;
        private readonly ConcurrentDictionary<string, IReader> _snapshotReaders;
        private SerializationHelper _serialization;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarSnapshotStore() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
            _serialization = new SerializationHelper(Context.System);
            _snapshotProducers = new ConcurrentDictionary<string, IProducer>();
            _snapshotReaders = new ConcurrentDictionary<string, IReader>();
        }

        public PulsarSnapshotStore(PulsarSettings settings)
        {
            this.settings = settings;
            this._client = settings.CreateClient();
        }
        protected override void PreStart()
        {
            base.PreStart();
        }
        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            //Probably Pulsar Message retention and expiry?
            return Task.CompletedTask;
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //Probably Pulsar Message retention and expiry?
            return Task.CompletedTask;
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //This needs testing to see if this can be relied on!
            //Other implementation filtered with sequenceid and timestamp, do we need same here?
            var reader = await GetReader(persistenceId);
            SelectedSnapshot selectedSnapshot = null;
            await foreach(var message in reader.Messages())
            {
                //This is hoping that only one message was retrieved since prefetch count was set to 1;
                //Yet to understand if this works that way
                var snapshot = _serialization.SnapshotFromBytes(message.Data.ToArray());
                selectedSnapshot = new SelectedSnapshot(
                            new SnapshotMetadata(
                                persistenceId,
                                (long)message.SequenceId,
                                new DateTime((long)message.EventTime)),
                            snapshot.Data);
            }
            return selectedSnapshot;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var producer = await GetProducer(metadata.PersistenceId);
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));
            var mtadata = new MessageMetadata
            {
                Key = metadata.PersistenceId,
                SequenceId = (ulong)metadata.SequenceNr
            };
            await producer.Send(mtadata, snapshotData);
        }
        private async Task<IReader> GetReader(string persistenceid)
        {
            var topic = $"snapshot-{persistenceid}".ToLower();
            if(!_snapshotReaders.ContainsKey(topic))
            {
                var readerOption = new ReaderOptions(MessageId.Latest, topic)
                {
                    MessagePrefetchCount = 1
                };
                await using var reader = _client.CreateReader(readerOption);
                
                if (reader != null)
                    _snapshotReaders[topic] = reader;
                return reader;
            }
            return _snapshotReaders[topic];
        }
        private async Task<IProducer> GetProducer(string persistenceid)
        {
            var topic = $"snapshot-{persistenceid}".ToLower();
            if (!_snapshotProducers.ContainsKey(topic))
            {
                await using var producer = _client.CreateProducer(new ProducerOptions(topic));
                if (producer != null)
                    _snapshotProducers[topic] = producer;
                return producer;
            }
            return _snapshotProducers[topic];
        }
        protected override void PostStop()
        {
            base.PostStop();
            _client.DisposeAsync();
        }
        
    }
}
