using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        private readonly List<string> _activeReplayTopics;

        public PulsarJournalExecutor(ActorSystem actorSystem, PulsarSettings settings, ILoggingAdapter log, Serializer serializer, CancellationTokenSource cancellation)
        {
            _activeReplayTopics = new List<string>();
            Settings = settings;
            _log = log;
            _serializer = serializer; 
            _journalEntrySchema = AvroSchema.Of(typeof(JournalEntry), new Dictionary<string, string>());
            AvroSchema.Of(typeof(PersistentIdEntry));
            _producerListener = new DefaultProducerListener(o =>
            {
                _log.Info(o.ToString());
            }, s =>
            {
                _log.Info(s);
            });
            Settings = settings;
            Client = settings.CreateSystem(actorSystem);
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
                var messages = Client.EventSource<JournalEntry>(nextPlay);
                foreach (var m in messages)
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
                var messages = Client.EventSource<JournalEntry>(replay);
                foreach (var m in messages)
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
        public async Task<long> ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            try
            {
                var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-{persistenceId}";
                var numb = Client.EventSource(new GetNumberOfEntries(topic, Settings.AdminUrl));
                return await Task.FromResult(numb.Max.Value);
            }
            catch (Exception e)
            {
                return 0;
            }
        }
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

        public PulsarSystem Client { get; }
        internal PulsarSettings Settings { get; }
        internal (string topic, IActorRef producer) PersistenceId => _persistenceId;
        
        internal long GetMaxOrderingId(ReplayTaggedMessages replay)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-*";
            var numb = Client.EventSource(new GetNumberOfEntries(topic, Settings.AdminUrl));
            return numb.Max.Value;
        }
    }
}
