using DotPulsar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.CursorStore
{
    /// <summary>
    /// IMessageIdStore helps to store Pulsar MessageId
    /// </summary>
    public interface IMessageIdStore
    {
        MessageId GetMessageId(string persistenceId);
        bool SaveMessageId(string persistenceid, MessageId messageId);
        (MessageId fromMsgId, MessageId toMsgId) GetToAndFromSequence(string persistenceid, DateTime persistenceTime);
    }
}
