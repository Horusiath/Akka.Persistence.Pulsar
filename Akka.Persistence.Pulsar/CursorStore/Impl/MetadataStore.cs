using DotPulsar;
using System;
using System.Threading.Tasks;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class MetadataStore : IMetadataStore
    {
        public async Task<(long sequenceid, MessageId messageId)> GetLatestStartMessageId(string persistenceId)
        {
            throw new NotImplementedException();
        }

        public async Task<(MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRange(string persistenceid, long fromSequenceId, long toSequenceId)
        {
            throw new NotImplementedException();
        }

        public async Task<(long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRangeByDate(string persistenceid, long startDate, long endDate)
        {
            throw new NotImplementedException();
        }

        public async Task SaveStartMessageId(string persistenceid, long sequenceNr, MessageId messageId, long eventTime)
        {
            throw new NotImplementedException();
        }
    }
}
