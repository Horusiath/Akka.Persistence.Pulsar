using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Pattern;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Configuration;
using SharpPulsar.Akka.InternalCommands;
using SharpPulsar.Akka.InternalCommands.Producer;
using SharpPulsar.Handlers;
using SharpPulsar.Impl.Schema;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournalExecutor
    {
        private readonly ILoggingAdapter _log ;
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        public static readonly ConcurrentDictionary<string, Dictionary<string, IActorRef>> _producers = new ConcurrentDictionary<string, Dictionary<string, IActorRef>>();
        private readonly DefaultProducerListener _producerListener;
        private readonly List<string> _pendingTopicProducer = new List<string>();

        private (string topic, IActorRef producer) _persistenceId;

        private readonly JsonSchema _journalEntrySchema;
        private readonly JsonSchema _persistentEntrySchema;

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();

        public PulsarJournalExecutor(PulsarSettings settings, ILoggingAdapter log, Serializer serializer)
        {
            Settings = settings;
            _log = log;
            _serializer = serializer; 
            _journalEntrySchema = JsonSchema.Of(typeof(JournalEntry));
            _persistentEntrySchema = JsonSchema.Of(typeof(PersistentIdEntry));
            _producerListener = new DefaultProducerListener(o =>
            {
                _log.Info(o.ToString());
            }, (to, n, p) =>
            {
                if (to.EndsWith("persistence-ids"))
                {
                    _persistenceId = (to, p);
                }
                else
                {
                    if (_producers.ContainsKey(to))
                        _producers[to].Add(n, p);
                    else
                    {
                        _producers[to] = new Dictionary<string, IActorRef> { { n, p } };
                    }
                }
                _pendingTopicProducer.Remove(to);
            }, s =>
            {
                _log.Info(s);
            });
            Settings = settings;
            Client = settings.CreateSystem();
            Client.SetupSqlServers(new SqlServers(new List<string> { Settings.PrestoServer }.ToImmutableList()));
            CreatePersistentProducer();
            GetAllPersistenceIds();
        }
        public async Task ReplayMessages(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            CreateJournalProducer(persistenceId);
            var queryRunning = true;
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);
            Client.PulsarSql(new Sql($"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{persistenceId}\" where SequenceNr BETWEEN bigint '{fromSequenceNr}' AND bigint '{toSequenceNr}' ORDER BY Ordering ASC  LIMIT {max}",
                d =>
                {
                    var replay = recoveryCallback;
                    if (d.ContainsKey("Finished"))
                    {
                        queryRunning = false;
                        return;
                    }
                    var m = JsonSerializer.Deserialize<JournalEntry>(d["Message"]);
                    var payload = Convert.FromBase64String(m.Payload);
                    replay(Deserialize(payload));
                }, e =>
                {
                    var contxt = context;
                    queryRunning = false;
                    contxt.System.Log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, false));
            while (queryRunning)
            {
                await Task.Delay(100);
            }
        }
        public async Task<long> ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            var seq = 0L;
            var queryActive = true;
            Client.PulsarSql(new Sql($"select SequenceNr from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"journal-{persistenceId}\" ORDER BY Ordering DESC LIMIT 1",
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        queryActive = false;
                        return;
                    }
                    var m = JsonSerializer.Deserialize<Dictionary<string, object>>(d["Message"]);
                    var id = long.Parse(m["SequenceNr"].ToString());
                    seq = id;
                }, e =>
                {
                    _log.Error(e.ToString());
                    queryActive = false;
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                await Task.Delay(100);
            }
            if (seq < fromSequenceNr)
            {
                throw new IllegalStateException($"Invalid highest offset: {seq} < {fromSequenceNr}");
            }
            return seq;
        }
        private void CreateJournalProducer(string persistenceid)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-{persistenceid}".ToLower();
            var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey($"journal-{persistenceid}")).Value?.Values.FirstOrDefault();
            if (p == null)
            {
                var producerConfig = new ProducerConfigBuilder()
                    .ProducerName($"journal-{persistenceid}")
                    .Topic(topic)
                    .Schema(_journalEntrySchema)
                    .SendTimeout(10000)
                    .EventListener(_producerListener)
                    .ProducerConfigurationData;
                Client.CreateProducer(new CreateProducer(_journalEntrySchema, producerConfig));

            }
        }
        private void CreatePersistentProducer()
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/persistence-ids".ToLower();
            var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey("persistence-ids")).Value?.Values.FirstOrDefault();
            if (p == null)
            {
                var producerConfig = new ProducerConfigBuilder()
                    .ProducerName("persistence-ids")
                    .Topic(topic)
                    .Schema(_persistentEntrySchema)
                    .SendTimeout(10000)
                    .EventListener(_producerListener)
                    .ProducerConfigurationData;
                Client.CreateProducer(new CreateProducer(_persistentEntrySchema, producerConfig));

            }
        }
        internal void GetAllPersistenceIds()
        {
            Client.PulsarSql(new Sql($"select DISTINCT Id from pulsar.\"{Settings.Tenant}/{Settings.Namespace}\".\"persistence-ids\"",
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        return;
                    }
                    var m = JsonSerializer.Deserialize<Dictionary<string, object>>(d["Message"]);
                    var id = m["Id"].ToString();
                    _allPersistenceIds.Add(id);
                }, e =>
                {
                    _log.Error(e.ToString());
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
        }
        internal (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/{type}-{persistenceid}".ToLower();
            if (!_pendingTopicProducer.Contains(topic))
            {
                var p = _producers.FirstOrDefault(x => x.Key == topic && x.Value.ContainsKey($"{type.ToLower()}-{persistenceid}")).Value?.Values.FirstOrDefault();
                if (p == null)
                {
                    switch (type.ToLower())
                    {
                        case "journal":
                            CreateJournalProducer(persistenceid);
                            break;
                        case "persistence":
                            CreatePersistentProducer();
                            break;
                    }
                    _pendingTopicProducer.Add(topic);
                    return (null, null);
                }
                return (topic, p);
            }
            return (null, null);
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
        internal long GetMaxOrderingId(ReplayTaggedMessages replay, List<string> ids)
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
            var queryActive = true;
            var sequenceNr = 0L;
            Client.PulsarSql(new Sql(tags,
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        queryActive = false;
                        return;
                    }

                    var m = JsonSerializer.Deserialize<JournalEntry>(d["Message"]);
                    sequenceNr = m.SequenceNr;
                }, e =>
                {
                    _log.Error(e.ToString());
                    queryActive = false;
                }, Settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                Thread.Sleep(100);
            }
            return sequenceNr;
        }
    }
}
