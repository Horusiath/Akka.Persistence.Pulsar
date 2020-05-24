using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Util;
using IdentityModel;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Configuration;
using SharpPulsar.Akka.InternalCommands;
using SharpPulsar.Akka.InternalCommands.Producer;
using SharpPulsar.Handlers;
using SharpPulsar.Impl.Schema;

namespace Akka.Persistence.Pulsar.Snapshot
{
    /// <summary>
    ///     Pulsar-backed snapshot store for Akka.Persistence.
    /// </summary>
    /// 
    
    public class PulsarSnapshotStore : SnapshotStore
    {
        private readonly CancellationTokenSource _pendingRequestsCancellation;
        private readonly PulsarSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly PulsarSystem _client; public static readonly ConcurrentDictionary<string, Dictionary<string, IActorRef>> _producers = new ConcurrentDictionary<string, Dictionary<string, IActorRef>>();
        private readonly DefaultProducerListener _producerListener;
        private static readonly Type SnapshotType = typeof(Serialization.Snapshot);
        private readonly Serializer _serializer;
        private readonly AvroSchema _snapshotEntrySchema;


        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarSnapshotStore(Config config) : this(new PulsarSettings(config))
        {

        }

        public PulsarSnapshotStore(PulsarSettings settings)
        {
            _pendingRequestsCancellation = new CancellationTokenSource();
            _snapshotEntrySchema = AvroSchema.Of(typeof(SnapshotEntry));
            _producerListener = new DefaultProducerListener(o =>
            {
                _log.Info(o.ToString());
            },s =>
            {

            });
            _serializer = Context.System.Serialization.FindSerializerForType(SnapshotType);
            _settings = settings;
            _client = settings.CreateSystem();
            _client.SetupSqlServers(new SqlServers(new List<string> { _settings.PrestoServer }.ToImmutableList()));
        }
        
        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            await Task.CompletedTask;
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            await Task.CompletedTask;
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            SelectedSnapshot shot = null;
            var data =_client.PulsarSql(new Sql($"select Id, PersistenceId, SequenceNr, Timestamp, Snapshot  from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".\"snapshot-{persistenceId}\" WHERE SequenceNr <= {criteria.MaxSequenceNr} AND Timestamp <= {criteria.MaxTimeStamp.ToEpochTime()} ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT 1",
                 e =>
                {
                    _log.Error(e.ToString());
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }));
            var d = data.GetEnumerator().Current?.Data;
            if (d != null)
                return await Task.FromResult(ToSelectedSnapshot(JsonSerializer.Deserialize<SnapshotEntry>(JsonSerializer.Serialize(d))));
            return await Task.FromResult(shot);
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var (topic, producer) = GetProducer(metadata.PersistenceId, "Snapshot");
            using (var tokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(_pendingRequestsCancellation.Token))
                while (producer == null && !tokenSource.IsCancellationRequested)
                {
                    (topic, producer) = GetProducer(metadata.PersistenceId, "Snapshot");
                    await Task.Delay(100, tokenSource.Token);
                }

            var snapshotEntry = ToSnapshotEntry(metadata, snapshot);
            _client.Send(new Send(snapshotEntry, topic, ImmutableDictionary<string, object>.Empty), producer);
        }

        private void CreateSnapshotProducer(string persistenceid)
        {
            var topic = $"{_settings.TopicPrefix.TrimEnd('/')}/snapshot-{persistenceid}".ToLower();
            var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey($"snapshot-{persistenceid}")).Value?.Values.FirstOrDefault();
            if (p == null)
            {
                var producerConfig = new ProducerConfigBuilder()
                    .ProducerName($"snapshot-{persistenceid}")
                    .Topic(topic)
                    .Schema(_snapshotEntrySchema)
                    .SendTimeout(10000)
                    .EventListener(_producerListener)
                    .ProducerConfigurationData;
               var producer = _client.PulsarProducer(new CreateProducer(_snapshotEntrySchema, producerConfig));
               if (_producers.ContainsKey(producer.Topic))
                   _producers[producer.Topic].Add(producer.ProducerName, producer.Producer);
               else
               {
                   _producers[producer.Topic] = new Dictionary<string, IActorRef> { { producer.ProducerName, producer.Producer } };
               }
            }

        }
        private (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{_settings.TopicPrefix.TrimEnd('/')}/{type}-{persistenceid}".ToLower();
            var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey($"{type.ToLower()}-{persistenceid}")).Value?.Values.FirstOrDefault();
            if (p == null)
            {
                switch (type.ToLower())
                {
                    case "snapshot":
                        CreateSnapshotProducer(persistenceid);
                        break;
                }
                return GetProducer(persistenceid, type);
            }
            return (topic, p);
        }
        protected override void PostStop()
        {
            base.PostStop();

            // stop all operations executed in the background
            _pendingRequestsCancellation.Cancel();
            _client.DisposeAsync().GetAwaiter();
        }
        
        private object Deserialize(byte[] bytes)
        {
            return ((Serialization.Snapshot)_serializer.FromBinary(bytes, SnapshotType)).Data;
        }

        private byte[] Serialize(object snapshotData)
        {
            return _serializer.ToBinary(new Serialization.Snapshot(snapshotData));
        }
        private SnapshotEntry ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var binary = Serialize(snapshot);

            return new SnapshotEntry
            {
                Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                PersistenceId = metadata.PersistenceId,
                SequenceNr = metadata.SequenceNr,
                Snapshot = Convert.ToBase64String(binary),
                Timestamp = metadata.Timestamp.ToEpochTime()
            };
        }

        private SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            var snapshot = Deserialize(Convert.FromBase64String(entry.Snapshot));
            return new SelectedSnapshot(new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr), snapshot);

        }
    }
}
