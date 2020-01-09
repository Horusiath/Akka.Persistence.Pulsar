#region copyright

// -----------------------------------------------------------------------
//  <copyright file="PulsarJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

#endregion

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.Pulsar.CursorStore;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Internal;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournal : AsyncWriteJournal
    {
        private readonly PulsarSettings settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly IPulsarClient _client;
        private readonly ISequenceStore _sequenceStore;
        private ConcurrentDictionary<string, IProducer> _producers = new ConcurrentDictionary<string, IProducer>();

        
        private SerializationHelper _serialization;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarJournal(ISequenceStore sequenceStore) : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
            //_messageIdStore = messageIdStore;
            _sequenceStore = sequenceStore;
            _serialization = new SerializationHelper(Context.System);
        }

        public PulsarJournal(PulsarSettings settings)
        {
            this.settings = settings;
            this._client = settings.CreateClient();
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
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            await CreateProducer(persistenceId);
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);

            if (max == 0)
                return;
            //according to pulsar doc, messageid is returned for each message produced. The Pulsar system is in charge of creating MessageId
            //What we can do is to keep track of MessageId(s) and then reconstruct it here
            //We can get the latest MessageId with MessageId.Latest e.g
            //var startMessageId = new MessageId(ledgerId, entryId, partition, batchIndex); //TODO: how to config them properly in Pulsar?
            //var startMessageId = MessageId.Latest;

            var (_, startMessageId) = _sequenceStore.GetLatestSequenceId(persistenceId);//rough sketchy thoughts
            var reader = _client.CreateReader(new ReaderOptions(startMessageId, persistenceId));
            var count = 0L;
            await foreach (var message in reader.Messages())
            {
                // check if we've hit max recovery
                if (count >= max)
                    return;
                ++count;
                var deserialized = _serialization.PersistentFromBytes(message.Data.ToArray());
                // Write the new persistent because it sets the sender as deadLetters which is not correct
                if((deserialized.SequenceNr >= fromSequenceNr) && (deserialized.SequenceNr <= toSequenceNr))
                {
                    var persistent =
                    new Persistent(
                        deserialized.Payload,
                        deserialized.SequenceNr,
                        deserialized.PersistenceId,
                        deserialized.Manifest,
                        deserialized.IsDeleted,
                        ActorRefs.NoSender,
                        deserialized.WriterGuid);
                    recoveryCallback(persistent);
                }
            }
        }

        /// <summary>
        /// This method is called at the very beginning of the replay procedure to define a possible boundary of replay:
        /// In akka persistence every persistent actor starts from the replay phase, where it replays state from all of
        /// the events emitted so far before being marked as ready for command processing.
        /// </summary>
        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            //TODO: how to read the latest known sequence nr in pulsar? Theoretically it's the last element in virtual
            // topic belonging to that persistenceId.
            var (sequencid, _) = _sequenceStore.GetLatestSequenceId(persistenceId);
            return await Task.FromResult(sequencid);
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
            
            var failures = ImmutableArray.CreateBuilder<Exception>(0);
            foreach (var write in messages)
            {
                try
                {
                    var persistentMessages = (IImmutableList<IPersistentRepresentation>) write.Payload;
                    foreach (var message in persistentMessages)
                    {
                        var producer = await GetProducer(message.PersistenceId);
                        var messageBuilder = new MessageBuilder(producer);
                        messageBuilder.Key(message.PersistenceId);
                        messageBuilder.SequenceId((ulong)message.SequenceNr);
                        if(message.Payload is Tagged t)
                        {
                            var tgs = 0;
                            foreach(var tg in t.Tags)
                            {
                                messageBuilder.Property($"Tag-{tgs}", tg);//Tag messages
                            }
                        }
                        
                        var journal = _serialization.PersistentToBytes((IPersistentRepresentation)message.Payload);
                        //var journalEntries = ToJournalEntry(message);
                        var messageid = await messageBuilder.Send(journal, CancellationToken.None);//For now
                        //store messageid for later use
                        _sequenceStore.SaveSequenceId(message.PersistenceId, message.SequenceNr, messageid, DateTime.UtcNow);
                        //_messageIdStore.SaveMessageId(message.PersistenceId, messageid);
                          
                    }
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }

            return failures.ToImmutable();
        }
        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            object payload = message.Payload;
            if (message.Payload is Tagged tagged)
            {
                payload = tagged.Payload;
                message = message.WithPayload(payload); // need to update the internal payload when working with tags
            }


            var serializer = _serialization.FindSerializerFor(message);
            var binary = serializer.ToBinary(message);


            return new JournalEntry
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                Ordering = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), // Auto-populates with timestamp
                IsDeleted = message.IsDeleted,
                Payload = binary,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = string.Empty, // don't need a manifest here - it's embedded inside the PersistentMessage
                Tags = tagged.Tags?.ToList(),
                SerializerId = null // don't need a serializer ID here either; only for backwards-comat
            };
        }

        private async Task SetHighSequenceId(IList<AtomicWrite> messages, MessageId messageId)
        {
            var persistenceId = messages.Select(c => c.PersistenceId).First();
            var highSequenceId = messages.Max(c => c.HighestSequenceNr);

            var metadataEntry = new MetadataEntry
            {
                Id = persistenceId,
                PersistenceId = persistenceId,
                SequenceNr = highSequenceId,
                //TimeStamp = messages.Max(c => c..HighestSequenceNr)
            };

            //await _metadataCollection.Value.ReplaceOneAsync(filter, metadataEntry, new UpdateOptions() { IsUpsert = true });
        }

        /// <summary>
        /// Deletes all events stored in a single logical event stream (pulsar virtual topic), starting from the
        /// beginning of stream up to <paramref name="toSequenceNr"/> (inclusive?).
        /// </summary>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            await Task.CompletedTask;
        }
        private async Task CreateProducer(string persistenceid)
        {
            var topic = $"persistenceid-{persistenceid}".ToLower();
            if(!_producers.ContainsKey(topic))
            {
                await using var producer = _client.CreateProducer(new ProducerOptions(topic));
                _producers.TryAdd(topic, producer);
            }       

        }
        private async Task<IProducer> GetProducer(string persistenceid)
        {
            var topic = $"persistenceid-{persistenceid}".ToLower();
            if (!_producers.ContainsKey(topic))
            {
                await using var producer = _client.CreateProducer(new ProducerOptions(topic));
                _producers.TryAdd(topic, producer);
                return producer;
            }
            return _producers[topic];
        }
        protected override void PostStop()
        {
            base.PostStop();
            _client.DisposeAsync().GetAwaiter();
        }
    }
}