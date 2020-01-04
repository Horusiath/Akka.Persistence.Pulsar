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
using Akka.Persistence.Query;
using Akka.Streams.Dsl;
using DotPulsar;

namespace Akka.Persistence.Pulsar
{
    public sealed class PulsarReadJournal : IReadJournal, IEventsByPersistenceIdQuery, ICurrentEventsByPersistenceIdQuery, IEventsByTagQuery, ICurrentPersistenceIdsQuery, ICurrentEventsByTagQuery
    {
        private readonly ActorSystem system;
        private readonly PulsarSettings settings;

        public PulsarReadJournal(ActorSystem system, PulsarSettings settings)
        {
            this.system = system;
            this.settings = settings;
        }

        public const string Identifier = "akka.persistence.query.journal.pulsar";

        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var client = settings.CreateClient();
            var startMessageId = new MessageId(ledgerId, entryId, partition, batchIndex); //TODO: how to config them properly in Pulsar?
            var reader = client.CreateReader(new ReaderOptions(startMessageId, persistenceId));
            return Source.FromGraph(new AsyncEnumerableSourceStage<Message>(reader.Messages()))
                .Select(message =>
                {
                    return new EventEnvelope(offset: offset, persistenceId, message.SequenceId, data);
                });
        }

        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            throw new System.NotImplementedException();
        }

        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset)
        {
            throw new System.NotImplementedException();
        }

        public Source<string, NotUsed> CurrentPersistenceIds()
        {
            throw new System.NotImplementedException();
        }

        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset)
        {
            throw new System.NotImplementedException();
        }
    }

    public sealed class PulsarReadJournalProvider : IReadJournalProvider
    {
        private readonly ExtendedActorSystem system;
        private readonly PulsarSettings settings;

        public PulsarReadJournalProvider(ExtendedActorSystem system, Config config)
        {
            this.system = system;
            this.settings = new PulsarSettings(config);
        }

        public IReadJournal GetReadJournal() => new PulsarReadJournal(system, settings);
    }
}