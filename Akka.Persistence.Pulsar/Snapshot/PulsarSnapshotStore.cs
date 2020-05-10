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
        private DefaultProducerListener _producerListener;
        private List<string> _pendingTopicProducer = new List<string>();
        private static readonly Type SnapshotType = typeof(Serialization.Snapshot);
        private readonly Serializer _serializer;
        private AvroSchema _snapshotEntrySchema; 
        private readonly Akka.Serialization.Serialization _serialization;


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
            }, (to, n, p) =>
            {
                if (_producers.ContainsKey(to))
                    _producers[to].Add(n, p);
                else
                {
                    _producers[to] = new Dictionary<string, IActorRef> { { n, p } };
                }
                _pendingTopicProducer.Remove(to);
            }, s =>
            {

            });
            _serialization = Context.System.Serialization;
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
            
            var queryActive = true;
            SelectedSnapshot shot = null;
            _client.PulsarSql(new Sql($"select Id, PersistenceId, SequenceNr, Timestamp, Snapshot  from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".\"snapshot-{persistenceId}\" WHERE SequenceNr <= {criteria.MaxSequenceNr} AND Timestamp <= {criteria.MaxTimeStamp.ToEpochTime()} ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT 1",
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        queryActive = false;
                        return;
                    }
                    var m = JsonSerializer.Deserialize<SnapshotEntry>(d["Message"]);
                    shot = ToSelectedSnapshot(m);
                }, e =>
                {
                    _log.Error(e.ToString());
                    queryActive = false;
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                },true));
            using (var tokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(_pendingRequestsCancellation.Token))
                while (queryActive && !tokenSource.IsCancellationRequested)
                {
                    await Task.Delay(100, tokenSource.Token);
                }

            return shot;
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
                _client.PulsarProducer(new CreateProducer(_snapshotEntrySchema, producerConfig));
            }

        }
        private (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{_settings.TopicPrefix.TrimEnd('/')}/{type}-{persistenceid}".ToLower();
            if (!_pendingTopicProducer.Contains(topic))
            {
                var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey($"{type.ToLower()}-{persistenceid}")).Value?.Values.FirstOrDefault();
                if (p == null)
                {
                    switch (type.ToLower())
                    {
                        case "snapshot":
                            CreateSnapshotProducer(persistenceid);
                            break;
                    }
                    _pendingTopicProducer.Add(topic);
                    return (null, null);
                }
                return (topic, p);
            }
            return (null, null);
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
