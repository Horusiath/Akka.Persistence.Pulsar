using DotPulsar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class SequenceIdStore : ISequenceStore
    {
        public (long sequenceid, MessageId messageId) GetLatestSequenceId(string persistenceId)
        {
            throw new NotImplementedException();
        }

        public (long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId) GetSequenceRange(string persistenceid, DateTime persistenceTime)
        {
            throw new NotImplementedException();
        }

        public bool SaveSequenceId(string persistenceid, long sequenceid, MessageId messageId, DateTime sequenceTime)
        {
            throw new NotImplementedException();
        }
    }
}
