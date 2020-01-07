using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class SequenceIdStore : ISequenceStore
    {
        public long GetSequenceId(string persistenceId)
        {
            throw new NotImplementedException();
        }

        public (long fromSeq, long toSeq) GetToAndFromSequence(string persistenceid, DateTime persistenceTime)
        {
            throw new NotImplementedException();
        }

        public bool SequenceId(string persistenceid, long sequenceid)
        {
            throw new NotImplementedException();
        }
    }
}
