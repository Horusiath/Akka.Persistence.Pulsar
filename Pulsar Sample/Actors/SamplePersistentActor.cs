using Akka.Actor;
using Akka.Persistence;
using Pulsar_Sample.Command;
using System;

namespace Pulsar_Sample.Actors
{
    public class SamplePersistentActor : ReceivePersistentActor
    {
        private SampleActorState _state;
        public override string PersistenceId => "samplePulsar";
        public SamplePersistentActor()
        {
            _state = new SampleActorState();
            Command<ICommand>(c => 
            {
                Console.WriteLine("Command Received");
                _state.HandledCount++;
                switch (c)
                {
                    case ReadSystemCurrentTimeUtc time:
                        {
                            var readTimeEvent = new Event.ReadSystemCurrentTimeUtc(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                            Persist(readTimeEvent, @event => {
                                Console.WriteLine(@event.CurrentTime);
                            });
                        }
                        break;
                }                
            });
            
            Recover<ICommand>(c => 
            {
                Console.WriteLine("Recovered!!");
                _state.HandledCount++;
                switch (c)
                {
                    case ReadSystemCurrentTimeUtc time:
                        {
                            var readTimeEvent = new Event.ReadSystemCurrentTimeUtc(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                            Persist(readTimeEvent, @event => {
                                Console.WriteLine(@event.CurrentTime);
                            });
                        }
                        break;
                }
            });
            Recover<SnapshotOffer>(s => 
            {
                if (s.Snapshot is SampleActorState state)
                {
                    _state = state; 
                    Console.WriteLine($"Snapshot Offered: {_state.HandledCount}");
                }                    
            });
        }
        public static Props Prop()
        {
            return Props.Create(() => new SamplePersistentActor());
        }
    }
    public sealed class SampleActorState
    {
        public int HandledCount { get; set; }
    }
}
