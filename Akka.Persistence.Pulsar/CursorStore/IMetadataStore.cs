using DotPulsar;
using System;
using System.Threading.Tasks;

namespace Akka.Persistence.Pulsar.CursorStore
{
    public interface IMetadataStore
    {
        Task<(long sequenceid, MessageId messageId)> GetLatestStartMessageId(string persistenceId);
        Task SaveStartMessageId(string persistenceid, long sequenceNr, MessageId messageId, long eventTime);
        Task<(long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRangeByDate(string persistenceid, long startDate, long endDate);
        Task<(MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRange(string persistenceid, long fromSequenceId, long toSequenceId);
    }
}
