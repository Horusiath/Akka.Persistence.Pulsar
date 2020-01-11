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
using Akka.Persistence.Pulsar.CursorStore.Impl;
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
        private readonly IMetadataStore _metadataStore;
        private Akka.Serialization.Serialization _serializer;

        public Akka.Serialization.Serialization Serialization => _serializer ??= Context.System.Serialization;
        private ConcurrentDictionary<string, IProducer> _producers = new ConcurrentDictionary<string, IProducer>();

        
        private SerializationHelper _serialization;

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarJournal() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
            _serialization = new SerializationHelper(Context.System);
            _metadataStore = new MetadataStore(Context.System);//Could also use Akka extension to inject implementation
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
        //Is ReplayMessagesAsync called once per actor lifetime?
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            
            await CreateProducer(persistenceId, "Journal");
            await CreateProducer(persistenceId, "Metadata");
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);

            var (start, end) = await _metadataStore.GetStartMessageIdRange(persistenceId, fromSequenceNr, toSequenceNr);//https://github.com/danske-commodities/dotpulsar/issues/12
            var count = 0L;
            Console.WriteLine(start.EntryId);
            var reader = _client.CreateReader(new ReaderOptions(start, Utils.Journal.PrepareTopic($"Journal-{persistenceId}".ToLower())));
            var messages = reader.Messages()
                //.Where(x => (x.MessageId.LedgerId >= start.LedgerId) && (x.MessageId.EntryId >= start.EntryId))
                //.Where(x => (x.MessageId.LedgerId <= end.LedgerId) && (x.MessageId.EntryId <= end.EntryId))
                /*.TakeWhile(_=> 
                {
                    //Do I need to worry about thread-safety?                    
                    var ct = count;
                    var cont = ct <= max;
                    count++;
                    return cont;
                })*/
                //.Take((int)max)//Felt this is tthe best place
                /*.Select(m => {
                    var deserialized = _serialization.PersistentFromBytes(m.Data.ToArray());
                    return new Persistent(
                        deserialized.Payload,
                        deserialized.SequenceNr,
                        deserialized.PersistenceId,
                        deserialized.Manifest,
                        deserialized.IsDeleted,
                        ActorRefs.NoSender,
                        deserialized.WriterGuid);
                })*/;
            await foreach(var m in messages)
            {
                //var persistent = new Persistent(payload, sequenceNr, persistenceId, manifest, sender);
                Console.WriteLine("Replaying: "+ m.SequenceId);
                var deserialized = _serialization.PersistentFromBytes(m.Data.ToArray());
                var p = new Persistent(
                    deserialized.Payload,
                    deserialized.SequenceNr,
                    deserialized.PersistenceId,
                    deserialized.Manifest,
                    deserialized.IsDeleted,
                    ActorRefs.NoSender,
                    deserialized.WriterGuid);
                recoveryCallback(p);
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
            var (snr, _) = await _metadataStore.GetLatestStartMessageId(persistenceId);
            return snr;
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
                        var producer = GetProducer(message.PersistenceId, "Journal");
                        var messageBuilder = new MessageBuilder(producer);
                        messageBuilder.Key($"{message.PersistenceId}-{message.SequenceNr}");
                        messageBuilder.SequenceId((ulong)message.SequenceNr);//used in reconstructing MessageId
                        if(message.Payload is Tagged t)
                        {
                            var tgs = 0;
                            foreach(var tg in t.Tags)
                            {
                                messageBuilder.Property($"Tag-{tgs}", tg);//Tag messages
                                tgs++;
                            }
                        }
                        
                        var journal = Context.System.Serialization.Serialize(message.Payload);
                        var messageid = await messageBuilder.Send(journal, cancellationToken: default);//For now
                        await SaveMessageId(message.PersistenceId, message.SequenceNr, messageid, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());// https://github.com/danske-commodities/dotpulsar/issues/12

                    }
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }

            return failures.ToImmutable();
        }
        
        private async Task SaveMessageId(string persistenceid, long sequenceNr, MessageId messageId, long eventTime)
        {
           await _metadataStore.SaveStartMessageId(persistenceid, sequenceNr, messageId, eventTime);
        }

        /// <summary>
        /// Deletes all events stored in a single logical event stream (pulsar virtual topic), starting from the
        /// beginning of stream up to <paramref name="toSequenceNr"/> (inclusive?).
        /// </summary>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            //Pulsar stores unacknowledged messages forever
            //We can delete messages if retention policy is not set by using pulsar consumer
            var option = new ConsumerOptions($"persistent-consumer-{persistenceId}", Utils.Journal.PrepareTopic($"Journal-{persistenceId}".ToLower()));
            await using var consumer = _client.CreateConsumer(option);
            var messages = consumer.Messages()
                .Where(x => x.SequenceId <= (ulong)toSequenceNr);
            await foreach (var message in messages)
            {
                await consumer.Acknowledge(message.MessageId); //Acknowledged messages are deleted
            }
            await consumer.Unsubscribe();//is this good?
            await consumer.DisposeAsync();//is this good?
        }
        private async Task CreateProducer(string persistenceid, string type)
        {
            var topic = Utils.Journal.PrepareTopic($"{type}-{persistenceid}".ToLower());
            if (!_producers.ContainsKey(topic))
            {
                await using var producer = _client.CreateProducer(new ProducerOptions(topic));
                _producers.TryAdd(topic, producer);
            }       

        }
        private IProducer GetProducer(string persistenceid, string type)
        {
            var topic = Utils.Journal.PrepareTopic($"{type}-{persistenceid}".ToLower());
            if (!_producers.ContainsKey(topic))
            {
                var producer = _client.CreateProducer(new ProducerOptions(topic));
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