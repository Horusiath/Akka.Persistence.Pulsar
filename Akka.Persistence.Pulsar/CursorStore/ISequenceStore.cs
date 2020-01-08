using DotPulsar;
using System;

namespace Akka.Persistence.Pulsar.CursorStore
{
    public interface ISequenceStore
    {
        (long sequenceid, MessageId messageId) GetLatestSequenceId(string persistenceId);
        bool SaveSequenceId(string persistenceid, long sequenceid, MessageId messageId, DateTime sequenceTime);
        (long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId) GetSequenceRange(string persistenceid, DateTime startDate, DateTime endDate);
    }
}
