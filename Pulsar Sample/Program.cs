using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Pulsar.Query;
using Akka.Persistence.Query;
using Akka.Streams;
using Sample.Actors;
using Sample.Command;
using Sample.Observer;

namespace Sample
{
    class Program
    {
        private static IObservable<EventEnvelope> _persistenceStream;
        private static IObservable<EventEnvelope> _tagStream;
        static void Main(string[] args)
        {
            var config = File.ReadAllText("host.hocon");
            var actorSystem = ActorSystem.Create("SampleSystem", ConfigurationFactory.ParseString(config));
            var mat = ActorMaterializer.Create(actorSystem);
            var readJournal = PersistenceQuery.Get(actorSystem).ReadJournalFor<PulsarReadJournal>("akka.persistence.query.journal.pulsar");
            var sampleActor = actorSystem.ActorOf(SamplePersistentActor.Prop(), "utcreader");
            var persistenceIdsSource = readJournal.PersistenceIds();
            var persistenceStream = new SourceObservable<string>(persistenceIdsSource, mat);
            persistenceStream.Subscribe(e =>
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine($"PersistenceId '{e}' added");
                Console.ResetColor();
            });
            var persistenceSource = readJournal.EventsByPersistenceId("utcreader", 0L, long.MaxValue);
            _persistenceStream = new SourceObservable<EventEnvelope>(persistenceSource, mat);
            _persistenceStream.Subscribe(e =>
            {
                Console.WriteLine($"{JsonSerializer.Serialize(e, new JsonSerializerOptions { WriteIndented = true })}");
            });
            var persistenceIdsSource1 = readJournal.CurrentPersistenceIds();
            var persistenceStream1 = new SourceObservable<string>(persistenceIdsSource1, mat);
            persistenceStream1.Subscribe(e =>
            {
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine($"PersistenceId '{e}' added");
                Console.ResetColor();
            });
            var tagSource = readJournal.EventsByTag("utc", Offset.Sequence(0L));
             //var tagSource = readJournal.CurrentEventsByTag("utc", new Sequence(0L));
             _tagStream = new SourceObservable<EventEnvelope>(tagSource, mat);
             _tagStream.Subscribe(e =>
             {
                 Console.WriteLine($"{JsonSerializer.Serialize(e, new JsonSerializerOptions{WriteIndented = true})}");
             });
            while (true)
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                sampleActor.Tell(new ReadSystemCurrentTimeUtc());
                
                Console.WriteLine("Tell Actor");
            }
        }
    }
}
