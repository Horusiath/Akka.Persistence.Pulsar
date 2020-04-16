#region copyright

// -----------------------------------------------------------------------
//  <copyright file="PulsarJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

#endregion

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
using Akka.Persistence.Journal;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Configuration;
using SharpPulsar.Akka.InternalCommands;
using SharpPulsar.Akka.InternalCommands.Producer;
using SharpPulsar.Handlers;
using SharpPulsar.Impl;
using SharpPulsar.Impl.Schema;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournal : AsyncWriteJournal
    {
        private readonly PulsarSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly PulsarSystem _client;
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        public static readonly ConcurrentDictionary<string, Dictionary<string, IActorRef>> _producers = new ConcurrentDictionary<string, Dictionary<string, IActorRef>>();
        private DefaultProducerListener _producerListener;
        private List<string> _pendingTopicProducer = new List<string>();

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers =
            new Dictionary<string, ISet<IActorRef>>();
        private readonly Dictionary<string, ISet<IActorRef>> _persistenceIdSubscribers
            = new Dictionary<string, ISet<IActorRef>>();

        private Akka.Serialization.Serialization _serialization;

        private JsonSchema _journalEntrySchema;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarJournal() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {

        }

        public PulsarJournal(PulsarSettings settings)
        {
            _journalEntrySchema = JsonSchema.Of(typeof(JournalEntry));
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
                _log.Info(s);
            });
            _serialization = Context.System.Serialization;
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
            _settings = settings;
            _client = settings.CreateSystem();
            _client.SetupSqlServers(new SqlServers(new List<string>{ _settings.PrestoServer}.ToImmutableList()));
        }

        /// <summary>
        /// This method replays existing event stream (identified by <paramref name="persistenceId"/>) asynchronously.
        /// It doesn't replay the whole stream, but only a window of it (described by range of [<paramref name="fromSequenceNr"/>, <paramref name="toSequenceNr"/>),
        /// with a limiter of up to <paramref name="max"/> elements - therefore it's possible that it will complete
        /// before the whole window is replayed.
        ///
        /// For every replayed message we need to construct a corresponding <see cref="Persistent"/> instance, that will
        /// be send back to a journal by calling a <paramref name="recoveryCallback"/>.
        /// </summary>
        //Is ReplayMessagesAsync called once per actor lifetime?
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max > 2147483647)
                max = 2147483647; // presto does not support limit > 2147483647
            NotifyNewPersistenceIdAdded(persistenceId);
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            CreateJournalProducer(persistenceId);
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);
            var queryActive = true;
            _client.QueryData(new QueryData($"select * from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".journal where PersistenceId = '{persistenceId}' AND SequenceNr BETWEEN bigint '{fromSequenceNr}' AND bigint '{toSequenceNr}' ORDER BY SequenceNr ASC LIMIT {max}",
                d =>
                {
                    try
                    {
                        if (d.ContainsKey("Finished"))
                        {
                            queryActive = false;
                            return;
                        }
                        
                        var m = JsonSerializer.Deserialize<Dictionary<string, object>>(d["Message"]);
                        if ( m.ContainsKey("payload"))
                        {
                            var payload = Convert.FromBase64String(m["payload"].ToString());
                            recoveryCallback(Deserialize(payload));
                        }
                    }
                    catch (Exception e)
                    {
                        context.System.Log.Error(e.ToString());
                        queryActive = false;
                    }
                }, e =>
                {
                    context.System.Log.Error(e.ToString());
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                await Task.Delay(500);
            }
            
        }
        /// <summary>
        /// This method is called at the very beginning of the replay procedure to define a possible boundary of replay:
        /// In akka persistence every persistent actor starts from the replay phase, where it replays state from all of
        /// the events emitted so far before being marked as ready for command processing.
        /// </summary>
        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);
            var seq = 0L;
            var queryActive = true;
            _client.QueryData(new QueryData($"select SequenceNr from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".journal WHERE PersistenceId = '{persistenceId}' ORDER BY SequenceNr DESC LIMIT 1",
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
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                await Task.Delay(100);
            }

            return seq;
        }
        /// <summary>
        /// Writes a batch of messages. Each <see cref="AtomicWrite"/> can have one or many <see cref="IPersistentRepresentation"/>
        /// events inside its payload, and they all should be written in atomic fashion (in one transaction, all-or-none).
        ///
        /// In case of failure of a single <see cref="AtomicWrite"/> we don't fail right away. Instead we try to write
        /// remaining writes and gather all exceptions produced in the process: they will be returned at the end.
        /// </summary>
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var allTags = ImmutableHashSet<string>.Empty;
            var persistentIds = new HashSet<string>();
            var messageList = messages.ToList();

            var writeTasks = messageList.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload);

                if (HasTagSubscribers)
                {
                    foreach (var p in persistentMessages)
                    {
                        if (p.Payload is Tagged t)
                        {
                            allTags = allTags.Union(t.Tags);
                        }
                    }
                }

                var (topic, producer) = GetProducer(message.PersistenceId, "Journal");
                while (producer == null)
                {
                    (topic, producer) = GetProducer(message.PersistenceId, "Journal");
                    await Task.Delay(1000);
                }
                var journalEntries = persistentMessages.Select(ToJournalEntry).Select(x => new Send(x, topic, ImmutableDictionary<string, object>.Empty)).ToList();
                _client.BulkSend(new BulkSend(journalEntries, topic), producer);
                if (HasPersistenceIdSubscribers)
                    persistentIds.Add(message.PersistenceId);
            });

            var result = await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

            if (HasPersistenceIdSubscribers)
            {
                foreach (var id in persistentIds)
                {
                    NotifyPersistenceIdChange(id);
                }
            }

            if (HasTagSubscribers && allTags.Count != 0)
            {
                foreach (var tag in allTags)
                {
                    NotifyTagChange(tag);
                }
            }

            return result;
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            return Task.CompletedTask;
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            if (message.Payload is Tagged tagged)
            {
                var payload = tagged.Payload;
                message = message.WithPayload(payload); // need to update the internal payload when working with tags
            }

            var binary = Serialize(message);


            return new JournalEntry
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                Ordering = DateTimeHelper.CurrentUnixTimeMillis(), // Auto-populates with timestamp
                IsDeleted = message.IsDeleted,
                Payload = Convert.ToBase64String(binary),
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Tags = string.Join(",", tagged.Tags == null ? new List<string>() : tagged.Tags.ToList() )
            };
        }

        private void CreateJournalProducer(string persistenceid)
        {
            var topic = $"{_settings.TopicPrefix.TrimEnd('/')}/journal".ToLower();
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
                _client.CreateProducer(new CreateProducer(_journalEntrySchema, producerConfig));
                
            }
        }
        private (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{_settings.TopicPrefix.TrimEnd('/')}/{type}".ToLower();
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
                    }
                    _pendingTopicProducer.Add(topic);
                    return (null, null);
                }
                return (topic, p) ;
            }
            return (null, null);
        }
        
        protected override void PostStop()
        {
            base.PostStop();
            _client.DisposeAsync().GetAwaiter();
        }
        
        private byte[] Serialize(IPersistentRepresentation message)
        {
            return _serializer.ToBinary(message);
        }
        private IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation)_serializer.FromBinary(bytes, PersistentRepresentationType);
        }
        
        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    break;
                case SubscribePersistenceId subscribe:
                    AddPersistenceIdSubscriber(Sender, subscribe.PersistenceId);
                    Context.Watch(Sender);
                    break;
                case SubscribeAllPersistenceIds subscribe:
                    AddAllPersistenceIdSubscriber(Sender);
                    Context.Watch(Sender);
                    break;
                case SubscribeTag subscribe:
                    AddTagSubscriber(Sender, subscribe.Tag);
                    Context.Watch(Sender);
                    break;
                case Terminated terminated:
                    RemoveSubscriber(terminated.ActorRef);
                    break;
                default:
                    return false;
            }

            return true;
        }
        /// <summary>
        /// Replays all events with given tag withing provided boundaries from current database.
        /// </summary>
        /// <param name="replay">TBD</param>
        /// <returns>TBD</returns>
        private async Task<long> ReplayTaggedMessagesAsync(ReplayTaggedMessages replay)
        {
            /*
             *  NOTE: limit is used like a pagination value, not a cap on the amount
             * of data returned by a query. This was at the root of https://github.com/akkadotnet/Akka.Persistence.MongoDB/issues/80
             */
            // Limit allows only integer
            var limitValue = replay.Max >= int.MaxValue ? int.MaxValue : (int)replay.Max;
            if ((long)limitValue > 2147483647)
                limitValue = 2147483647; // presto does not support limit > 2147483647
            var fromSequenceNr = replay.FromOffset;
            var toSequenceNr = replay.ToOffset;
            var tag = replay.Tag;
            var queryActive = true;
            var maxOrderingId = 0L;
            _client.QueryData(new QueryData($"select Ordering, Payload from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".journal where contains(SELECT split(Tags, ','), '{tag}')  AND SequenceNr BETWEEN {fromSequenceNr} AND {toSequenceNr} ORDER BY Ordering ASC LIMIT {limitValue}",
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        queryActive = false;
                        return;
                    }
                    
                    var m = JsonSerializer.Deserialize<Dictionary<string, object>>(d["Message"]);
                    if (m.ContainsKey("Ordering") && m.ContainsKey("Payload"))
                    {
                        var ordering = long.Parse(m["Ordering"].ToString());
                        var payload = Convert.FromBase64String(m["Payload"].ToString());
                        maxOrderingId = ordering;
                        var persistent = Deserialize(payload);
                        foreach (var adapted in AdaptFromJournal(persistent))
                            replay.ReplyTo.Tell(new ReplayedTaggedMessage(adapted, tag, ordering),
                                ActorRefs.NoSender);
                    }
                    
                }, e =>
                {
                    _log.Error(e.ToString());
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                await Task.Delay(500);
            }

            return maxOrderingId;
        }

        private void AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }
            subscriber.Tell(new CurrentPersistenceIds(GetAllPersistenceIds()));
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private IEnumerable<string> GetAllPersistenceIds()
        {
            var list = new List<string>();
            var queryActive = true;
            _client.QueryData(new QueryData($"select DISTINCT PersistenceId from pulsar.\"{_settings.Tenant}/{_settings.Namespace}\".journal",
                d =>
                {
                    if (d.ContainsKey("Finished"))
                    {
                        queryActive = false;
                        return;
                    }
                    var m = JsonSerializer.Deserialize<Dictionary<string, object>>(d["Message"]);
                    var id = m["PersistenceId"].ToString();
                    list.Add(id);
                }, e =>
                {
                    _log.Error(e.ToString());
                }, _settings.PrestoServer, l =>
                {
                    _log.Info(l);
                }, true));
            while (queryActive)
            {
                Thread.Sleep(100);
            }

            return list;
        }

        private void AddPersistenceIdSubscriber(IActorRef subscriber, string persistenceId)
        {
            if (!_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _persistenceIdSubscribers.Add(persistenceId, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private void RemoveSubscriber(IActorRef subscriber)
        {
            var pidSubscriptions = _persistenceIdSubscribers.Values.Where(x => x.Contains(subscriber));
            foreach (var subscription in pidSubscriptions)
                subscription.Remove(subscriber);

            var tagSubscriptions = _tagSubscribers.Values.Where(x => x.Contains(subscriber));
            foreach (var subscription in tagSubscriptions)
                subscription.Remove(subscriber);

            _allPersistenceIdSubscribers.Remove(subscriber);
        }

        protected bool HasAllPersistenceIdSubscribers => _allPersistenceIdSubscribers.Count != 0;
        protected bool HasTagSubscribers => _tagSubscribers.Count != 0;
        protected bool HasPersistenceIdSubscribers => _persistenceIdSubscribers.Count != 0;

        private void NotifyNewPersistenceIdAdded(string persistenceId)
        {
            var isNew = TryAddPersistenceId(persistenceId);
            if (isNew && HasAllPersistenceIdSubscribers)
            {
                var added = new PersistenceIdAdded(persistenceId);
                foreach (var subscriber in _allPersistenceIdSubscribers)
                    subscriber.Tell(added);
            }
        }

        private bool TryAddPersistenceId(string persistenceId)
        {
            lock (_allPersistenceIds)
            {
                return _allPersistenceIds.Add(persistenceId);
            }
        }

        private void NotifyPersistenceIdChange(string persistenceId)
        {
            if (_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscribers))
            {
                var changed = new EventAppended(persistenceId);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

        private void NotifyTagChange(string tag)
        {
            if (_tagSubscribers.TryGetValue(tag, out var subscribers))
            {
                var changed = new TaggedEventAppended(tag);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

    }
}