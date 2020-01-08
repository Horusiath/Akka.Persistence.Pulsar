using DotPulsar;
using System;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class SequenceIdStore : ISequenceStore
    {
        public (long sequenceid, MessageId messageId) GetLatestSequenceId(string persistenceId)
        {
            throw new NotImplementedException();
        }

        public (long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId) GetSequenceRange(string persistenceid, DateTime startDate, DateTime endDate)
        {
            throw new NotImplementedException();
        }

        public bool SaveSequenceId(string persistenceid, long sequenceid, MessageId messageId, DateTime sequenceTime)
        {
            throw new NotImplementedException();
        }
    }
}
