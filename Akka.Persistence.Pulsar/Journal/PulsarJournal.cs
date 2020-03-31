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
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.Pulsar.CursorStore;
using Akka.Persistence.Pulsar.CursorStore.Impl;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Configuration;
using SharpPulsar.Akka.InternalCommands;
using SharpPulsar.Akka.InternalCommands.Consumer;
using SharpPulsar.Akka.InternalCommands.Producer;
using SharpPulsar.Handlers;
using SharpPulsar.Impl;
using SharpPulsar.Impl.Schema;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournal : AsyncWriteJournal
    {
        private readonly PulsarSettings settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly PulsarSystem _client;
        private long _readerCount;
        private readonly IMetadataStore _metadataStore;
        private readonly Akka.Serialization.Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        private ConcurrentDictionary<string, IActorRef> _producers = new ConcurrentDictionary<string, IActorRef>();
        private DefaultProducerListener _producerListener;
        private List<string> _pendingTopicProducer = new List<string>();

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers =
            new Dictionary<string, ISet<IActorRef>>();
        private readonly Dictionary<string, ISet<IActorRef>> _persistenceIdSubscribers
            = new Dictionary<string, ISet<IActorRef>>();

        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarJournal() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
            _producerListener = new DefaultProducerListener((o) =>
            {
                Console.WriteLine(o.ToString());
            }, (s, p) =>
            {
                var t = s.Split("/").Last().ToLower();
                _producers.TryAdd(s, p);
                _pendingTopicProducer.Remove(t);
            }, s =>
            {
                //save messageid in pulsar to be queried using presto sql
            });
            _metadataStore = new MetadataStore(Context.System);//Could also use Akka extension to inject implementation

            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
        }

        public PulsarJournal(PulsarSettings settings)
        {
            this.settings = settings;
            this._client = settings.CreateSystem();
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

            CreateProducer(persistenceId, "Journal");
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);

            var (start, end) = await _metadataStore.GetStartMessageIdRange(persistenceId, fromSequenceNr, toSequenceNr);//https://github.com/danske-commodities/dotpulsar/issues/12
            var ended = false;
            // Beware of LedgerId issue, Reader could start from a different MessageId when connected to a 
            //different Ledger from the supplied StartMessageId LedgerId
            IActorRef reader = null;
            #region reader
            var readerConfig = new ReaderConfigBuilder()
                .ReaderName($"journal-{persistenceId}-replay")
                .Schema(BytesSchema.Of())
                .EventListener(new DefaultConsumerEventListener(l =>
                {

                }, (s, a) => { reader = a; }, (s,l) =>
                {

                }))
                .ReaderListener(new DefaultMessageListener(null, m =>
                {
                    var entryid = 0L;
                    if (m.MessageId is MessageId mi)
                    {
                        entryid = mi.EntryId;
                    }
                    else if (m.MessageId is BatchMessageId b)
                    {
                        entryid = b.EntryId;
                    }

                    var count = entryid - start.EntryId;
                    if (count == max)
                        ended = true;
                    var p = Deserialize((byte[])(object)m.Data);
                    recoveryCallback(p);
                }))
                .Topic($"Journal-{persistenceId}".ToLower())
                .StartMessageId(new BatchMessageId((long)start.LedgerId, (long)start.EntryId, start.Partition, start.BatchIndex))
                .ReaderConfigurationData;
            #endregion
            _client.CreateReader(new CreateReader(BytesSchema.Of(), readerConfig));
            while (!ended)
            {
                await Task.Delay(500);
            }
            reader.Tell(new CloseConsumer());//we should close this reader?
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
                        //var topic = Utils.Journal.PrepareTopic($"Journal-{message.PersistenceId}".ToLower());
                        var (topic,producer) = GetProducer(message.PersistenceId, "Journal");
                        while (producer == null)
                        {
                            (topic, producer) = GetProducer(message.PersistenceId, "Journal");
                            await Task.Delay(500);
                        }
                        //var producer = GetProducer(message.PersistenceId, "Journal");
                        var metadata = new Dictionary<string, object>
                        {
                            ["Key"] = $"{message.PersistenceId}-{message.SequenceNr}",
                            ["SequenceId"] = message.SequenceNr
                        };
                        var properties = new Dictionary<string, string>();
                        if(message.Payload is Tagged t)
                        {
                            var tgs = 0;
                            foreach(var tg in t.Tags)
                            {
                                properties.Add($"Tag-{tgs}", tg);//Tag messages
                                tgs++;
                            }
                        }

                        metadata["Properties"] = properties;
                        var journal = Serialize(message); 
                        producer.Tell(new Send(journal, topic, metadata.ToImmutableDictionary()));
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
           //await _metadataStore.SaveStartMessageId(persistenceid, sequenceNr, messageId, eventTime);
        }

        /// <summary>
        /// Deletes all events stored in a single logical event stream (pulsar virtual topic), starting from the
        /// beginning of stream up to <paramref name="toSequenceNr"/> (inclusive?).
        /// </summary>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            //Pulsar stores unacknowledged messages forever
            //We can delete messages if retention policy is not set by using pulsar consumer
            /*var option = new ConsumerOptions($"persistent-consumer-{persistenceId}", Utils.Journal.PrepareTopic($"Journal-{persistenceId}".ToLower()));
            await using var consumer = _client.CreateConsumer(option);
            var messages = consumer.Messages()
                .Where(x => x.SequenceId <= (ulong)toSequenceNr);
            await foreach (var message in messages)
            {
                await consumer.Acknowledge(message.MessageId); //Acknowledged messages are deleted
            }
            await consumer.DisposeAsync();//is this good?*/
        }
        private void CreateProducer(string persistenceid, string type)
        {
            var byteSchema = BytesSchema.Of();
            var topic =  $"{type}-{persistenceid}".ToLower();
            var producerConfig = new ProducerConfigBuilder()
                .ProducerName($"{type}-{persistenceid}")
                .Topic(topic)
                .Schema(byteSchema)
                .SendTimeout(10000)
                .EventListener(_producerListener)
                .ProducerConfigurationData;
            _client.CreateProducer(new CreateProducer(byteSchema, producerConfig));
        }
        private (string topic, IActorRef producer) GetProducer(string persistenceid, string type)
        {
            var topic = $"{type}-{persistenceid}".ToLower();
            if (!_pendingTopicProducer.Contains(topic))
            {
                var t = _producers.Keys.FirstOrDefault(x => x.EndsWith(topic));
                if (string.IsNullOrWhiteSpace(t))
                {
                    CreateProducer(persistenceid, type);
                    _pendingTopicProducer.Add(topic);
                    return (null, null);
                }
                return (t, _producers[t]) ;
            }
            return (null, null);
        }
        
        protected override void PostStop()
        {
            base.PostStop();
            _client.DisposeAsync().GetAwaiter();
        }
        private IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation)_serializer.FromBinary(bytes, PersistentRepresentationType);
        }

        private byte[] Serialize(IPersistentRepresentation message)
        {
            return _serializer.ToBinary(message);
        }
    }
}