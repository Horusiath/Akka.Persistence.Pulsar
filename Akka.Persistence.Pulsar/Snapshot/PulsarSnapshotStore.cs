using Akka.Event;
using Akka.Persistence.Snapshot;
using DotPulsar;
using DotPulsar.Abstractions;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq;
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
        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            //Pulsar stores unacknowledge messages forever
            //We can delete messages if retention policy is not set by using pulsar consumer
            
            var option = new ConsumerOptions($"snapshot-consumer-{metadata.PersistenceId}", Utils.Journal.PrepareTopic($"snapshot-{metadata.PersistenceId}".ToLower()));
            await using var consumer = _client.CreateConsumer(option);
            var messages = consumer.Messages()
                .Where(x => x.SequenceId == (ulong)metadata.SequenceNr);
            await foreach(var message in messages)
            {
                await consumer.Acknowledge(message.MessageId); //Acknowledged messages are deleted
            }
            await consumer.Unsubscribe();
            await consumer.DisposeAsync();
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //Pulsar stores unacknowledged messages forever
            //We can delete messages if retention policy is not set by using pulsar consumer
            var option = new ConsumerOptions($"snapshot-consumer-{persistenceId}", Utils.Journal.PrepareTopic($"snapshot-{persistenceId}".ToLower()));
            await using var consumer = _client.CreateConsumer(option);
            var messages = consumer.Messages()
                .Where(x => x.SequenceId <= (ulong)criteria.MaxSequenceNr /*&& x.SequenceId >= (ulong)(criteria.MinSequenceNr)*/)
                .Where(t => t.EventTime <= (ulong)criteria.MaxTimeStamp.Ticks);
            await foreach (var message in messages)
            {
                await consumer.Acknowledge(message.MessageId); //Acknowledged messages are deleted
            }
            await consumer.Unsubscribe();//is this good?
            await consumer.DisposeAsync();//is this good?
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //This needs testing to see if this can be relied on!
            //Other implementation filtered with sequenceid and timestamp, do we need same here?
            var reader = await GetReader(persistenceId);
            var message = await reader.Messages()
                .Where(x => x.SequenceId <= (ulong)criteria.MaxSequenceNr)
                .Where(t => t.EventTime <= (ulong)criteria.MaxTimeStamp.Ticks)
                .OrderByDescending(o => o.SequenceId)
                .FirstOrDefaultAsync();
            var snapshot = _serialization.SnapshotFromBytes(message.Data.ToArray());
            SelectedSnapshot selectedSnapshot = new SelectedSnapshot(
            new SnapshotMetadata(
                persistenceId,
                (long)message.SequenceId,
                new DateTime((long)message.EventTime)),
            snapshot.Data);
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
            var topic = Utils.Journal.PrepareTopic($"snapshot-{persistenceid}".ToLower());
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
            var topic = Utils.Journal.PrepareTopic($"snapshot-{persistenceid}".ToLower());
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
