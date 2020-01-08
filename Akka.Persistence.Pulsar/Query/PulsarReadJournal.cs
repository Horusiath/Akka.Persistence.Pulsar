#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PulsarReadJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Pulsar.CursorStore;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;
using DotPulsar;
using System.Buffers;

namespace Akka.Persistence.Pulsar.Query
{
    public sealed class PulsarReadJournal : IReadJournal, IEventsByPersistenceIdQuery, ICurrentEventsByPersistenceIdQuery, IEventsByTagQuery, ICurrentPersistenceIdsQuery, ICurrentEventsByTagQuery
    {
        //TODO: it's possible that not all of these interfaces have sense in terms of Apache Pulsar - in that case
        // we should remove what's unnecessary.
        
        private readonly ActorSystem system;
        private readonly PulsarSettings settings;
        private SerializationHelper _serialization;
        //private readonly IMessageIdStore _messageIdStore;
        private readonly ISequenceStore _sequenceStore;

        public PulsarReadJournal(ActorSystem system, PulsarSettings settings, ISequenceStore sequenceStore)
        {
            this.system = system;
            this.settings = settings;
            //_messageIdStore = messageIdStore;
            _sequenceStore = sequenceStore;
            _serialization = new SerializationHelper(system);
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
        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var client = settings.CreateClient();
            //according to pulsar doc, messageid is returned for each message produced. The Pulsar system is in charge of creating MessageId
            //What we can do is to keep track of MessageId(s) and then reconstruct it here
            //We can get the latest MessageId with MessageId.Latest
            //var startMessageId = new MessageId(ledgerId, entryId, partition, batchIndex); //TODO: how to config them properly in Pulsar?
            var (_, startMessageId) = _sequenceStore.GetLatestSequenceId(persistenceId);
            var reader = client.CreateReader(new ReaderOptions(startMessageId, persistenceId));
            return Source.FromGraph(new AsyncEnumerableSourceStage<Message>(reader.Messages())).
                Where(x => (x.SequenceId >= (ulong)fromSequenceNr) && (x.SequenceId <= (ulong) toSequenceNr))
                .Named("EventsByPersistenceId-" + persistenceId)
                .Select(message =>
                {
                    return new EventEnvelope(offset: new Sequence(_serialization.PersistentFromBytes(message.Data.ToArray()).SequenceNr), persistenceId, (long)message.SequenceId, message.Data);
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
        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var client = settings.CreateClient();
            throw new System.NotImplementedException();
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
        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset)
        {
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
        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Returns a stream of all known persistence IDs. This stream is not supposed to send duplicates.
        /// </summary>
        public Source<string, NotUsed> CurrentPersistenceIds()
        {
            throw new System.NotImplementedException();
        }
    }

    public sealed class PulsarReadJournalProvider : IReadJournalProvider
    {
        private readonly ExtendedActorSystem system;
        private readonly PulsarSettings settings;
        private readonly ISequenceStore _sequenceStore;

        public PulsarReadJournalProvider(ExtendedActorSystem system, Config config, ISequenceStore sequenceStore)
        {
            this.system = system;
            this.settings = new PulsarSettings(config);
        }

        public IReadJournal GetReadJournal() => new PulsarReadJournal(system, settings, _sequenceStore);
    }
}