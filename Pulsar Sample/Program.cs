using System;
using System.IO;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Producer.Actors;
using Sample.Command;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = File.ReadAllText("host.hocon");
            var actorSystem = ActorSystem.Create("SampleSystem", ConfigurationFactory.ParseString(config));
            
            var sampleActor = actorSystem.ActorOf(SamplePersistentActor.Prop(), "utcreader");
            while (true)
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                sampleActor.Tell(new ReadSystemCurrentTimeUtc());
                
                Console.WriteLine("Tell Actor");
            }
        }
    }
}
