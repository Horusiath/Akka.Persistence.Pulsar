using System;
using Akka.Actor;
using Akka.Persistence;
using Sample.Command;
using Sample.Event;

namespace Producer.Actors
{
    public class SamplePersistentActor : ReceivePersistentActor
    {
        private SampleActorState _state;
        public override string PersistenceId { get; }
        private int _snapCount = 0;

        public SamplePersistentActor()
        {
            _state = new SampleActorState();
            Command<ReadSystemCurrentTimeUtc>(c => 
            {
                Console.WriteLine("Command Received");
                _state.HandledCount++;
                var readTimeEvent = new SystemCurrentTimeUtcRead(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                Persist(readTimeEvent, @event =>
                {
                    _snapCount++;
                    if (_snapCount >= 5)
                    {
                        SaveSnapshot(_state);
                        _snapCount = 0;
                    }
                });
            });
            
            Recover<ICommand>(c => 
            {
                Console.WriteLine("Recovered!!");
                _state.HandledCount++;
                switch (c)
                {
                    case ReadSystemCurrentTimeUtc time:
                        {
                            var readTimeEvent = new SystemCurrentTimeUtcRead(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                            Persist(readTimeEvent, @event => {
                                Console.WriteLine(@event.CurrentTime);
                            });
                        }
                        break;
                }
            });
            Recover<IEvent>(c => 
            {
                Console.WriteLine("Recovered Event!!");
                switch (c)
                {
                    case SystemCurrentTimeUtcRead time:
                        {
                            _state.HandledCount++;
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
            CommandAny(o =>
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine(o.GetType().FullName);
                Console.ResetColor();
            });
            PersistenceId = Context.Self.Path.Name;
            //
        }

        protected override bool Receive(object message)
        {
            Console.WriteLine("Received");
            return base.Receive(message);
        }

        protected override bool AroundReceive(Receive receive, object message)
        {
            Console.WriteLine($"AroundReceive =>{message.GetType().FullName}");
            return base.AroundReceive(receive, message);
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
