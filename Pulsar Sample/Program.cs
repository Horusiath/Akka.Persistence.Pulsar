using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Pulsar.Query;
using Akka.Persistence.Query;
using Akka.Streams;
using Pulsar_Sample.Actors;
using Pulsar_Sample.Command;
using Pulsar_Sample.Observer;
using System;
using System.IO;
using System.Text.Json;
using System.Threading;

namespace Pulsar_Sample
{
    class Program
    {
        private static IObservable<EventEnvelope> _timeStream;
        static void Main(string[] args)
        {
            var config = File.ReadAllText("host.hocon");
            var actorSystem = ActorSystem.Create("SampleSystem", ConfigurationFactory.ParseString(config));
            var mat = ActorMaterializer.Create(actorSystem);
            var readJournal = PersistenceQuery.Get(actorSystem).ReadJournalFor<PulsarReadJournal>("akka.persistence.query.journal.pulsar");
            var props = SamplePersistentActor.Prop();
            var sampleActor = actorSystem.ActorOf(props, "SampleActor");
            var timeSource = readJournal.EventsByPersistenceId("sampleActor", 0L, long.MaxValue);
            _timeStream = new SourceObservable<EventEnvelope>(timeSource, mat);
            _timeStream.Subscribe(e=> { Console.WriteLine($"{JsonSerializer.Serialize(e.Event, new JsonSerializerOptions{WriteIndented = true})}"); });
            while(true)
            {
                Thread.Sleep(TimeSpan.FromSeconds(30));
                sampleActor.Tell(new ReadSystemCurrentTimeUtc());
                
                Console.WriteLine("Tell Actor");
            }
        }
    }
}
