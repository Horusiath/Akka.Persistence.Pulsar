#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PulsarReadJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Persistence.Pulsar.CursorStore;
using Akka.Persistence.Pulsar.CursorStore.Impl;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;
using DotPulsar;
using DotPulsar.Internal;
using System;
using System.Buffers;
using System.Linq;

namespace Akka.Persistence.Pulsar.Query
{
    public sealed class PulsarReadJournal : IReadJournal, IEventsByPersistenceIdQuery, ICurrentEventsByPersistenceIdQuery, IEventsByTagQuery, ICurrentPersistenceIdsQuery, ICurrentEventsByTagQuery
    {
        //TODO: it's possible that not all of these interfaces have sense in terms of Apache Pulsar - in that case
        // we should remove what's unnecessary.
        
        private readonly ActorSystem system;
        private readonly PulsarSettings settings;
        private SerializationHelper _serialization;
        private IMetadataStore _metadataStore;

        public PulsarReadJournal(ActorSystem system, PulsarSettings settings)
        {
            this.system = system;
            this.settings = settings;
            _serialization = new SerializationHelper(system);
            _metadataStore = new MetadataStore(system);
        }
        public const string Identifier = "akka.persistence.query.journal.pulsar";

        /// <summary>
        /// Streams all events stored for a given <paramref name="persistenceId"/> (pulsar virtual topic?). This is a
        /// windowing method - we don't necessarily want to read the entire stream, so it's possible to read only a
        /// range of events [<paramref name="fromSequenceNr"/>, <paramref name="toSequenceNr"/>).
        ///
        /// This stream complete when it reaches <paramref name="toSequenceNr"/>, but it DOESN'T stop when we read all
        /// events currently stored. If you want to read only events currently stored use <see cref="CurrentEventsByPersistenceId"/>.
        /// </summary>
        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(string persistenceid, long fromSequenceNr, long toSequenceNr)
        {
            var topic = Utils.Journal.PrepareTopic($"Journal-{persistenceid}".ToLower()); 
            var client = settings.CreateClient();
            var (start, end) = fromSequenceNr == 0 ? (MessageId.Earliest, MessageId.Latest) : _metadataStore.GetStartMessageIdRange(persistenceid, fromSequenceNr, toSequenceNr).Result;//https://github.com/danske-commodities/dotpulsar/issues/12
            var reader = client.CreateReader(new ReaderOptions(start, topic));
            return Source.FromGraph(new AsyncEnumerableSourceStage<Message>(reader.Messages()))
                .Where(x => (x.MessageId.LedgerId >= start.LedgerId) && (x.MessageId.EntryId >= start.EntryId))
                .Select(message =>
                {
                    return new EventEnvelope(offset: new Sequence(_serialization.PersistentFromBytes(message.Data.ToArray()).SequenceNr), persistenceid, (long)message.SequenceId, message.Data);
                });
        }

        /// <summary>
        /// Streams all events stored for a given <paramref name="persistenceId"/> (pulsar virtual topic?). This is a
        /// windowing method - we don't necessarily want to read the entire stream, so it's possible to read only a
        /// range of events [<paramref name="fromSequenceNr"/>, <paramref name="toSequenceNr"/>).
        ///
        /// This stream complete when it reaches <paramref name="toSequenceNr"/> or when we read all
        /// events currently stored. If you want to read all events (even future ones), use <see cref="EventsByPersistenceId"/>.
        /// </summary>
        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(string persistenceid, long fromSequenceNr, long toSequenceNr)
        {
            var topic = Utils.Journal.PrepareTopic($"Journal-{persistenceid}".ToLower());
            var client = settings.CreateClient();
            var (start, end) = _metadataStore.GetStartMessageIdRange(persistenceid, fromSequenceNr, toSequenceNr).Result;//https://github.com/danske-commodities/dotpulsar/issues/12
            var reader = client.CreateReader(new ReaderOptions(start, topic));
            return Source.FromGraph(new AsyncEnumerableSourceStage<Message>(reader.Messages()
                .Where(x => (x.MessageId.LedgerId >= start.LedgerId) && (x.MessageId.EntryId >= start.EntryId))
                .Where(x => (x.MessageId.LedgerId <= end.LedgerId) && (x.MessageId.EntryId <= end.EntryId))))
                .Select(message =>
                {
                    return new EventEnvelope(offset: new Sequence(_serialization.PersistentFromBytes(message.Data.ToArray()).SequenceNr), persistenceid, (long)message.SequenceId, message.Data);
                });
        }

        /// <summary>
        /// Streams all events stored for a given <paramref name="tag"/> (think of it as a secondary index, one event
        /// can have multiple supporting tags). All tagged events are ordered according to some offset (which can be any
        /// incrementing number, possibly non continuously like 1, 2, 5, 13), which can be used to define a starting
        /// point for a stream.
        /// 
        /// This stream DOESN'T stop when we read all events currently stored. If you want to read only events
        /// currently stored use <see cref="CurrentEventsByTag"/>.
        /// </summary>
        /// //When https://github.com/danske-commodities/dotpulsar/issues/5 is done then this could be possible
        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset)
        {
            /*
             // Subscribe to all topics in a namespace
                Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
                Consumer allTopicsConsumer = consumerBuilder
                .topicsPattern(allTopicsInNamespace)
                .subscribe();
             
             */
            //leaving this so for future reference when it becomes possible
            /*var seq = Convert.ToUInt64(offset);//not sure here
            var topics = Utils.Journal.PrepareTopic($"Journal-*".ToLower());
            var client = settings.CreateClient();
            var startMessageId = new MessageId(seq, seq, -1, -1);
            var reader = client.CreateReader(new ReaderOptions(startMessageId, topics));
            return Source.FromGraph(new AsyncEnumerableSourceStage<Message>(reader.Messages()
                .Where(x => x.SequenceId >=  seq)))
                .Where(t => t.Properties.Values.Contains(tag))
                .Select(message =>
                {
                    return new EventEnvelope(offset: new Sequence(_serialization.PersistentFromBytes(message.Data.ToArray()).SequenceNr), message.Key.Split('-')[0], (long)message.SequenceId, message.Data);
                });*/
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Streams currently stored events for a given <paramref name="tag"/> (think of it as a secondary index, one event
        /// can have multiple supporting tags). All tagged events are ordered according to some offset (which can be any
        /// incrementing number, possibly non continuously like 1, 2, 5, 13), which can be used to define a starting
        /// point for a stream.
        /// 
        /// This stream stops when we read all events currently stored. If you want to read all events (also future ones)
        /// use <see cref="CurrentEventsByTag"/>.
        /// </summary>
        /// //When https://github.com/danske-commodities/dotpulsar/issues/5 is done then this could be possible
        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset)
        {
            /*
             // Subscribe to all topics in a namespace
                Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
                Consumer allTopicsConsumer = consumerBuilder
                .topicsPattern(allTopicsInNamespace)
                .subscribe();
             
             */
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Returns a stream of all known persistence IDs. This stream is not supposed to send duplicates.
        /// </summary>//When https://github.com/danske-commodities/dotpulsar/issues/5 is done then this could be possible
        
        
        public Source<string, NotUsed> CurrentPersistenceIds()
        {
            throw new System.NotImplementedException();
        }
    }
}