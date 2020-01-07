using DotPulsar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class MessageIdStore : IMessageIdStore
    {
        public MessageId GetMessageId(string persistenceId)
        {
            throw new NotImplementedException();
        }

        public (MessageId fromMsgId, MessageId toMsgId) GetToAndFromSequence(string persistenceid, DateTime persistenceTime)
        {
            throw new NotImplementedException();
        }

        public bool SaveMessageId(string persistenceid, MessageId messageId)
        {
            throw new NotImplementedException();
        }
    }
}
