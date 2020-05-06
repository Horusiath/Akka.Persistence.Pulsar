using System;
using Akka.Actor;
using Akka.Persistence;
using Sample.Command;

namespace Sample.Actors
{
    public class SamplePersistentActor : ReceivePersistentActor
    {
        private SampleActorState _state;
        public override string PersistenceId { get; }

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
                            var readTimeEvent = new Event.SystemCurrentTimeUtcRead(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
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
                            var readTimeEvent = new Event.SystemCurrentTimeUtcRead(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
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

            PersistenceId = Context.Self.Path.Name;
        }

        protected override void Unhandled(object message)
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine($"unhandled => {message.GetType().FullName}");
            Console.ResetColor();
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
