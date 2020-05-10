#region copyright

// -----------------------------------------------------------------------
//  <copyright file="PulsarJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar.Akka.InternalCommands;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournal : AsyncWriteJournal
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();
        private readonly HashSet<string> _registeredPersistenceIds = new HashSet<string>();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers =
            new Dictionary<string, ISet<IActorRef>>();
        private readonly Dictionary<string, ISet<IActorRef>> _persistenceIdSubscribers
            = new Dictionary<string, ISet<IActorRef>>();

        /// <summary>
        /// TBD
        /// </summary>
        public IEnumerable<string> AllPersistenceIds => _allPersistenceIds;
        private bool _firstRun = true;
        private readonly PulsarJournalExecutor _journalExecutor;
        private readonly CancellationTokenSource _pendingRequestsCancellation;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarJournal(Config config) : this(new PulsarSettings(config))
        {

        }

        public PulsarJournal(PulsarSettings settings)
        {
            _pendingRequestsCancellation = new CancellationTokenSource();
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
            _journalExecutor = new PulsarJournalExecutor(settings, Context.GetLogger(), _serializer, _pendingRequestsCancellation);
        }

        /// <summary>
        /// This method replays existing event stream (identified by <paramref name="persistenceId"/>) asynchronously.
        /// It doesn't replay the whole stream, but only a window of it (described by range of [<paramref name="fromSequenceNr"/>, <paramref name="toSequenceNr"/>),
        /// with a limiter of up to <paramref name="max"/> elements - therefore it's possible that it will complete
        /// before the whole window is replayed.
        ///
        /// For every replayed message we need to construct a corresponding <see cref="Persistent"/> instance, that will
        /// be send back to a journal by calling a <paramref name="recoveryCallback"/>.
        /// 
        /// RETENTION POLICY MUST BE SENT AT THE NAMESPACE LEVEL ELSE TOPIC IS DELETED
        /// </summary>
        //Is ReplayMessagesAsync called once per actor lifetime?
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max > 2147483647)
                max = 2147483647; // presto does not support limit > 2147483647
            if(_firstRun)
                _allPersistenceIds.Clear();
            _firstRun = false;
            NotifyNewPersistenceIdAdded(persistenceId);
            await _journalExecutor.ReplayMessages(context, persistenceId, fromSequenceNr, toSequenceNr, max,
                recoveryCallback);
        }

        /// <summary>
        /// This method is called at the very beginning of the replay procedure to define a possible boundary of replay:
        /// In akka persistence every persistent actor starts from the replay phase, where it replays state from all of
        /// the events emitted so far before being marked as ready for command processing.
        /// </summary>
        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);
            return await _journalExecutor.ReadHighestSequenceNr(persistenceId, fromSequenceNr);
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

                var (topic, producer) = _journalExecutor.GetProducer(message.PersistenceId, "Journal");
                using (var tokenSource =
                    CancellationTokenSource.CreateLinkedTokenSource(_pendingRequestsCancellation.Token))
                    while (producer == null && !tokenSource.IsCancellationRequested)
                    {
                        (topic, producer) = _journalExecutor.GetProducer(message.PersistenceId, "Journal");
                        await Task.Delay(100, tokenSource.Token);
                    }
                var journalEntries = persistentMessages.Select(ToJournalEntry).Select(x => new Send(x, topic, ImmutableDictionary<string, object>.Empty)).ToList();
                _journalExecutor.Client.BulkSend(new BulkSend(journalEntries, topic), producer);
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
                Tags = JsonSerializer.Serialize(tagged.Tags == null ? new List<string>() : tagged.Tags.ToList(), new JsonSerializerOptions{WriteIndented = true} )
            };
        }
        protected override void PostStop()
        {
            base.PostStop();

            // stop all operations executed in the background
            _pendingRequestsCancellation.Cancel();
            _journalExecutor.Client.DisposeAsync().GetAwaiter();
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
            using var tokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(_pendingRequestsCancellation.Token);
            return await Task.Run(async() => {
                var ids = _allPersistenceIds.ToList();
                ids.ForEach(x => x.Replace("-", "_"));
                if (!ids.Any())
                {
                    //let us wait 5 seconds
                    await Task.Delay(5000, tokenSource.Token);
                    ids = _allPersistenceIds.ToList();
                    ids.ForEach(x => x.Replace("-", "_"));
                    if (!ids.Any())
                    {
                        return replay.Max;
                    }
                }
                var tag = replay.Tag;
                var queryActive = true;
                var maxOrderingId = 0L;
                _journalExecutor.Client.PulsarSql(new Sql(_journalExecutor.TagsStatement(replay, ids),
                    d =>
                    {
                        if (d.ContainsKey("Finished"))
                        {
                            queryActive = false;
                            return;
                        }

                        var m = JsonSerializer.Deserialize<JournalEntry>(d["Message"]);
                        var ordering = m.SequenceNr;
                        var payload = Convert.FromBase64String(m.Payload);
                        var persistent = Deserialize(payload);
                        foreach (var adapted in AdaptFromJournal(persistent))
                            replay.ReplyTo.Tell(new ReplayedTaggedMessage(adapted, tag, ordering),
                                ActorRefs.NoSender);

                    }, e =>
                    {
                        _log.Error(e.ToString());
                        queryActive = false;
                    }, _journalExecutor.Settings.PrestoServer, l =>
                    {
                        _log.Info(l);
                    }, true));
                
                while (queryActive && !tokenSource.IsCancellationRequested)
                {
                    await Task.Delay(100, tokenSource.Token);
                }

                return _journalExecutor.GetMaxOrderingId(replay, ids);
            }, tokenSource.Token);
        }

        
        private void AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            _allPersistenceIdSubscribers.Add(subscriber);
            subscriber.Tell(new CurrentPersistenceIds(AllPersistenceIds));
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
            _journalExecutor.GetAllPersistenceIds();
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
            var pro = _journalExecutor.PersistenceId;
            if (pro.producer != null && !_registeredPersistenceIds.Contains(persistenceId))
            {
                var journalEntries = new Send(new PersistentIdEntry { EntryDate = DateTimeHelper.CurrentUnixTimeMillis(), Id = persistenceId }, pro.topic, ImmutableDictionary<string, object>.Empty);
                _journalExecutor.Client.Send(journalEntries, pro.producer);
                _registeredPersistenceIds.Add(persistenceId);
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