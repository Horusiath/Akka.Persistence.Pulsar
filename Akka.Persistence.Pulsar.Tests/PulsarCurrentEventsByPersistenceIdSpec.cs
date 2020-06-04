#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PulsarCurrentEventsByPersistenceIdSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Pulsar.Query;
using Akka.Persistence.Pulsar.Tests.Kits;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Pulsar.Tests
{
    public class PulsarCurrentEventsByPersistenceIdSpec : Akka.TestKit.Xunit2.TestKit
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka.persistence.journal.plugin = ""akka.persistence.journal.pulsar""
            akka.test.single-expect-default = 60s
        ").WithFallback(PulsarPersistence.DefaultConfiguration());

        protected ActorMaterializer Materializer { get; }

        protected IReadJournal ReadJournal { get; set; }
        private Prober _receiverProbe;
        private long _timeout = 30_000;

        public PulsarCurrentEventsByPersistenceIdSpec(ITestOutputHelper output) : base(SpecConfig,
            "PulsarCurrentEventsByPersistenceIdSpec", output)
        {
            Materializer = Sys.Materializer();
            ReadJournal = Sys.ReadJournalFor<PulsarReadJournal>(PulsarReadJournal.Identifier);
            _receiverProbe = new Prober(Sys);
        }

        [Fact]
        public void ReadJournal_should_implement_ICurrentEventsByPersistenceIdQuery()
        {
            Assert.IsAssignableFrom<ICurrentEventsByPersistenceIdQuery>(ReadJournal);
        }

        [Fact]
        public void ReadJournal_CurrentEventsByPersistenceId_should_find_existing_events()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup(persistenceId);

            var src = queries.CurrentEventsByPersistenceId(persistenceId, 1, long.MaxValue);
            var probe = src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer);
            probe.Request(2)
                .ExpectNext($"{persistenceId}-1", $"{persistenceId}-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(2)
                .ExpectNext($"{persistenceId}-3")
                .ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_find_existing_events_up_to_a_sequence_number()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("b");
            var src = queries.CurrentEventsByPersistenceId("b", 0L, 2L);
            var probe = src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
                .Request(5)
                .ExpectNext("b-1", "b-2")
                .ExpectComplete();
        }

        [Fact]
        public void ReadJournal_CurrentEventsByPersistenceId_should_not_see_new_events_after_completion()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("f");
            var src = queries.CurrentEventsByPersistenceId("f", 0L, long.MaxValue);
            var probe = src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
                .Request(2)
                .ExpectNext("f-1", "f-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(100)) as TestSubscriber.Probe<object>;

            pref.Tell("f-4");
            ExpectMsg("f-4-done");

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(5)
                .ExpectNext("f-3")
                .ExpectComplete(); // f-4 not seen
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_MaxLong()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("g1");

            pref.Tell(new ProberTestActor.DeleteCommand(3));
            AwaitAssert(() => ExpectMsg("3-deleted"));

            var src = queries.CurrentEventsByPersistenceId("g1", 0, long.MaxValue);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_0()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("g2");

            pref.Tell(new ProberTestActor.DeleteCommand(3));
            AwaitAssert(() => ExpectMsg("3-deleted"));

            var src = queries.CurrentEventsByPersistenceId("g2", 0, 0);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_remaining_values_after_partial_journal_cleanup()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("h");

            pref.Tell(new ProberTestActor.DeleteCommand(2));
            AwaitAssert(() => ExpectMsg("2-deleted"));

            var src = queries.CurrentEventsByPersistenceId("h", 0L, long.MaxValue);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
                .Request(1)
                .ExpectNext("h-3")
                .ExpectComplete();
        }

        [Fact]
        public void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = SetupEmpty("i");

            var src = queries.CurrentEventsByPersistenceId("i", 0L, long.MaxValue);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_journal_from_0_to_0()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("k1");

            var src = queries.CurrentEventsByPersistenceId("k1", 0, 0);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal_from_0_to_0()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = SetupEmpty("k2");

            var src = queries.CurrentEventsByPersistenceId("k2", 0, 0);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public void
            ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_journal_from_SequenceNr_greater_than_HighestSequenceNr()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("l");

            var src = queries.CurrentEventsByPersistenceId("l", 4L, 3L);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        private IActorRef Setup(string persistenceId)
        {
            var pref = SetupEmpty(persistenceId);

            pref.Tell(persistenceId + "-1", _receiverProbe.Ref);
            pref.Tell(persistenceId + "-2", _receiverProbe.Ref);
            pref.Tell(persistenceId + "-3", _receiverProbe.Ref);

            _receiverProbe.ExpectMessage(persistenceId + "-1-done", _timeout);
            _receiverProbe.ExpectMessage(persistenceId + "-2-done", _timeout);
            _receiverProbe.ExpectMessage(persistenceId + "-3-done", _timeout);
            return pref;
        }

        private IActorRef SetupEmpty(string persistenceId)
        {
            return Sys.ActorOf(ProberTestActor.Prop(persistenceId));
        }

        protected override void Dispose(bool disposing)
        {
            Materializer.Dispose();
            base.Dispose(disposing);
        }
    }
}