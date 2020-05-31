using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Configuration;
using SharpPulsar.Akka.InternalCommands;
using SharpPulsar.Akka.InternalCommands.Consumer;
using SharpPulsar.Akka.InternalCommands.Producer;
using SharpPulsar.Api;
using SharpPulsar.Handlers;
using SharpPulsar.Impl.Schema;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournalExecutor
    {
        private readonly ILoggingAdapter _log ;
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        public static readonly ConcurrentDictionary<string, IActorRef> Producers = new ConcurrentDictionary<string, IActorRef>();
        private readonly DefaultProducerListener _producerListener;

        private (string topic, IActorRef producer) _persistenceId;
        private readonly AvroSchema _journalEntrySchema;
        private readonly AvroSchema _persistentEntrySchema;
        private readonly CancellationTokenSource _pendingRequestsCancellation;
        private List<string> _activeReplayTopics;

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();

        public PulsarJournalExecutor(PulsarSettings settings, ILoggingAdapter log, Serializer serializer, CancellationTokenSource cancellation)
        {
            _activeReplayTopics = new List<string>();
            _pendingRequestsCancellation = cancellation;
            Settings = settings;
            _log = log;
            _serializer = serializer; 
            _journalEntrySchema = AvroSchema.Of(typeof(JournalEntry), new Dictionary<string, string>());
            _persistentEntrySchema = AvroSchema.Of(typeof(PersistentIdEntry));
            _producerListener = new DefaultProducerListener(o =>
            {
                _log.Info(o.ToString());
            }, s =>
            {
                _log.Info(s);
            });
            Settings = settings;
            Client = settings.CreateSystem();
            //CreatePersistentProducer();
            //GetAllPersistenceIds();
        }
        public async Task ReplayMessages(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-{persistenceId}".ToLower();
            if (_activeReplayTopics.Contains(topic))
            {
                var nextPlay = new NextPlay(topic, max, fromSequenceNr, toSequenceNr);
                foreach (var m in Client.EventSource<JournalEntry>(nextPlay, e =>
                {
                    Console.WriteLine($"Sequence Id:{e.SequenceId}");
                }))
                {
                    var repy = recoveryCallback;
                    var payload = m.Payload;
                    var der = Deserialize(payload);
                    repy(der);
                }
            }
            else
            {
                var consumerListener = new DefaultConsumerEventListener(Console.WriteLine);
                var readerListener = new DefaultMessageListener(null, null);
                var jsonSchem = AvroSchema.Of(typeof(JournalEntry));
                var readerConfig = new ReaderConfigBuilder()
                    .ReaderName("event-reader")
                    .Schema(jsonSchem)
                    .EventListener(consumerListener)
                    .ReaderListener(readerListener)
                    .Topic(topic)
                    .StartMessageId(MessageIdFields.Latest)
                    .ReaderConfigurationData;
                var replay = new ReplayTopic(readerConfig, Settings.AdminUrl, fromSequenceNr, toSequenceNr, max, null, false);
                foreach (var m in Client.EventSource<JournalEntry>(replay, e =>
                {
                    Console.WriteLine($"Sequence Id:{e.SequenceId}");
                }))
                {
                    var repy = recoveryCallback;
                    var payload = m.Payload;
                    var der = Deserialize(payload);
                    repy(der);
                }
                _activeReplayTopics.Add(topic);
            }
            
            await Task.CompletedTask;
        }
        
        /*public async Task ReplayMessages(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            CreateJournalProducer(persistenceId);
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);
            var messages = Client.PulsarSql(new Sql($"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{persistenceId}\" where SequenceNr BETWEEN bigint '{fromSequenceNr}' AND bigint '{toSequenceNr}' ORDER BY Ordering ASC  LIMIT {max}",
                 e =>
                {
                    var contxt = context;
                    contxt.System.Log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }));
            foreach (var message in messages)
            {
                if (message.HasRow)
                {
                    var replay = recoveryCallback;
                    var m = JsonSerializer.Deserialize<JournalEntry>(JsonSerializer.Serialize(message.Data));
                    var payload = m.Payload;
                    var der = Deserialize(payload);
                    replay(der);
                }
            }
            await Task.CompletedTask;
        }*/
        public async Task<long> ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-{persistenceId}";
            var numb = Client.EventSource(new GetNumberOfEntries(topic, Settings.AdminUrl, fromSequenceNr, long.MaxValue, long.MaxValue));
            return await Task.FromResult(numb.Max.Value);
        }
        /*public async Task<long> ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            var data = Client.PulsarSql(new Sql($"select SequenceNr from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{persistenceId}\" ORDER BY Ordering DESC LIMIT 1",
                 e =>
                {
                    _log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }));
            var seq = 0L;
            foreach (var d in data)
            {
                if (d.HasRow)
                {
                    seq = (long) d.Data["SequenceNr"];
                }
                break;
            }
            return await Task.FromResult(seq);
        }*/
        private void CreateJournalProducer(string topic, string persistenceid)
        {
            var p = Producers.FirstOrDefault(x => x.Key == topic).Value;
            if (p == null)
            {
                var producerConfig = new ProducerConfigBuilder()
                    .ProducerName($"journal-{persistenceid}")
                    .Topic(topic)
                    .Schema(_journalEntrySchema)
                    .SendTimeout(10000)
                    .EventListener(_producerListener)
                    .ProducerConfigurationData;
                var producer = Client.PulsarProducer(new CreateProducer(_journalEntrySchema, producerConfig));
                if (Producers.ContainsKey(producer.Topic))
                    Producers[producer.Topic] = producer.Producer;
                else
                {
                    Producers[producer.Topic] = producer.Producer;
                }
            }
        }
        private void CreatePersistentProducer()
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/persistence-ids".ToLower();
            var p = _persistenceId;
            if(p.producer == null)
            {
                var producerConfig = new ProducerConfigBuilder()
                    .ProducerName($"persistence-ids-{DateTimeOffset.Now.ToUnixTimeMilliseconds()}")
                    .Topic(topic)
                    .Schema(_persistentEntrySchema)
                    .SendTimeout(10000)
                    .EventListener(_producerListener)
                    .ProducerConfigurationData;
                var producer = Client.PulsarProducer(new CreateProducer(_persistentEntrySchema, producerConfig));
                _persistenceId = (topic, producer.Producer);
            }
        }
        internal void GetAllPersistenceIds()
        {
            var ids = Client.PulsarSql(new Sql($"select DISTINCT Id from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"persistence-ids\"",
                 e =>
                {
                    _log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }));
            foreach (var d in ids)
            {
                if(d.HasRow)
                    _allPersistenceIds.Add(d.Data["Id"].ToString());
            }
        }
        internal (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/{type}-{persistenceid}".ToLower();
            var p = Producers.FirstOrDefault(x => x.Key == topic).Value;
            if (p == null)
            {
                switch (type.ToLower())
                {
                    case "journal":
                        CreateJournalProducer(topic, persistenceid);
                        break;
                    case "persistence":
                        //CreatePersistentProducer();
                        break;
                }

                return GetProducer(persistenceid, type);
            }
            return (topic, p);
        }
        internal IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation)_serializer.FromBinary(bytes, PersistentRepresentationType);
        }

        internal PulsarSystem Client { get; }
        internal PulsarSettings Settings { get; }
        internal (string topic, IActorRef producer) PersistenceId => _persistenceId;
        internal string TagsStatement(ReplayTaggedMessages replay, List<string> ids)
        {
            var limitValue = replay.Max >= int.MaxValue ? int.MaxValue : (int)replay.Max;
            if ((long)limitValue > 2147483647)
                limitValue = 2147483647; // presto does not support limit > 2147483647
            var fromSequenceNr = replay.FromOffset;
            var toSequenceNr = replay.ToOffset;
            var tag = replay.Tag;
            var ls = new List<string>();
            var tags = "WITH tags AS(";
            foreach (var d in ids)
            {
                ls.Add($"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{d}\" where json_array_contains(Tags, '{tag}')  AND SequenceNr BETWEEN {fromSequenceNr} AND {toSequenceNr} ORDER BY SequenceNr ASC LIMIT {limitValue}{Environment.NewLine}");
            }

            tags += string.Join(", ", ls);
            tags += $"){Environment.NewLine}";
            tags += $"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from tags where json_array_contains(Tags, '{tag}')  AND SequenceNr BETWEEN {fromSequenceNr} AND {toSequenceNr} ORDER BY SequenceNr ASC LIMIT {limitValue}";
            return tags;
        }
        internal long GetMaxOrderingId(ReplayTaggedMessages replay)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-*";
            var numb = Client.EventSource(new GetNumberOfEntries(topic, Settings.AdminUrl, replay.FromOffset, replay.Max, replay.ToOffset));
            return numb.Max.Value;
        }
        /*internal long GetMaxOrderingId(ReplayTaggedMessages replay, List<string> ids)
        {
            var tag = replay.Tag;
            var ls = new List<string>();
            var tags = "WITH tags AS(";
            foreach (var d in ids)
            {
                ls.Add($"select SequenceNr from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{d}\" where json_array_contains(Tags, '{tag}') ");
            }

            tags += string.Join(", ", ls);
            tags += $"){Environment.NewLine}";
            tags += "select MAX(SequenceNr) AS SequenceNr from tags";
            var messages = Client.PulsarSql(new Sql(tags,
                 e =>
                {
                    _log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }));
            var seq = 0L;

            foreach (var d in messages)
            {
                if (d.HasRow)
                {
                    seq = (long)d.Data["SequenceNr"];
                }
                break;
            }
            return seq;
        }*/
    }
}
