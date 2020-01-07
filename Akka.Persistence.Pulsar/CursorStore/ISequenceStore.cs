using System;

namespace Akka.Persistence.Pulsar.CursorStore
{
    public interface ISequenceStore
    {
        long GetSequenceId(string persistenceId);
        bool SequenceId(string persistenceid, long sequenceid);
        (long fromSeq, long toSeq) GetToAndFromSequence(string persistenceid, DateTime persistenceTime);
    }
}
