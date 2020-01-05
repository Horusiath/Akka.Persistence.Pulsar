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
            //according to pulsar doc, messageid is returned for each message produced. The Pulsar system is in charge of creating MessageId
            //What we can do is to keep track of MessageId(s) and then reconstruct it here
            //We can get the latest MessageId with MessageId.Latest e.g
            //var startMessageId = new MessageId(ledgerId, entryId, partition, batchIndex); //TODO: how to config them properly in Pulsar?
            var startMessageId = MessageId.Latest;
            var reader = client.CreateReader(new ReaderOptions(startMessageId, persistenceId));
            await foreach (var message in reader.Messages())
            {
                var persistent = new Persistent(payload, sequenceNr, persistenceId, manifest, sender);
                recoveryCallback(persistent);
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
            throw new NotImplementedException();
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
            //TODO: creating producer for every single write is not feasible. We should be able to create or cache topics and use them ad-hoc when necessary.
            // Will Batching when producing work here? https://github.com/danske-commodities/dotpulsar/issues/7
            
            await using var producer = client.CreateProducer(new ProducerOptions(topic));
            var failures = ImmutableArray.CreateBuilder<Exception>(0);
            foreach (var write in messages)
            {
                try
                {
                    var persistentMessages = (IImmutableList<IPersistentRepresentation>) write.Payload;
                    foreach (var message in persistentMessages)
                    {
                        var metadata = new MessageMetadata
                        {
                            Key = message.PersistenceId,
                            SequenceId = (ulong) message.SequenceNr,
                        };
                        var data = serialization.Serialize(write.Payload);
                        //according to pulsar doc, messageid is returned for each message produced. The Pulsar system is in charge of creating MessageId
                        //What we can do is to keep track of MessageId(s)
                        var messageid = await producer.Send(metadata, data);
                        //store messageid for later use
                    }
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }

            return failures.ToImmutable();
        }

        /// <summary>
        /// Deletes all events stored in a single logical event stream (pulsar virtual topic), starting from the
        /// beginning of stream up to <paramref name="toSequenceNr"/> (inclusive?).
        /// </summary>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            throw new NotImplementedException();
        }
    }
}