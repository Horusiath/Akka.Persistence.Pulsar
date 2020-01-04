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
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;
using DotPulsar;
using DotPulsar.Abstractions;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournal : AsyncWriteJournal
    {
        private readonly PulsarSettings settings;
        private readonly IPulsarClient client;
        private Akka.Serialization.Serialization serialization;

        public Akka.Serialization.Serialization Serialization => serialization ??= Context.System.Serialization;

        public PulsarJournal() : this(PulsarPersistence.Get(Context.System).JournalSettings)
        {
        }

        public PulsarJournal(PulsarSettings settings)
        {
            this.settings = settings;
            this.client = settings.CreateClient();
        }
        
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, 
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var startMessageId = new MessageId(ledgerId, entryId, partition, batchIndex); //TODO: how to config them properly in Pulsar?
            var reader = client.CreateReader(new ReaderOptions(startMessageId, persistenceId));
            await foreach (var message in reader.Messages())
            {
                var persistent = new Persistent(payload, sequenceNr, persistenceId, manifest, sender);
                recoveryCallback(persistent);
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            throw new NotImplementedException();  //TODO: how to read the latest known sequence nr in pulsar?
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            //TODO: creating producer for every single write is not feasible. We should be able to create or cache topics and use them ad-hoc when necessary.
            await using var producer = client.CreateProducer(new ProducerOptions(topic));
            var failures = ImmutableArray.CreateBuilder<Exception>(0);
            foreach (var write in messages)
            {
                var persistentMessages = (IImmutableList<IPersistentRepresentation>) write.Payload;
                foreach (var message in persistentMessages)
                {
                    try
                    {
                        var metadata = new MessageMetadata
                        {
                            Key = message.PersistenceId,
                            SequenceId = (ulong)message.SequenceNr,
                        };
                        var data = serialization.Serialize(write.Payload);
                        await producer.Send(metadata, data);
                    }
                    catch (Exception e)
                    {
                        failures.Add(e);
                    }
                }
            }

            return failures.ToImmutable();
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            throw new NotImplementedException();
        }
    }
}