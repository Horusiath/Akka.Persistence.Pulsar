using DotPulsar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.Journal
{
    /// <summary>
    /// A bit of an hack(for now) due to the nature of Pulsar
    /// </summary>
    public class MetadataEntry
    {
        public string Id { get; set; }
        public string PersistenceId { get; set; }
        public long SequenceNr { get; set; }
        public long TimeStamp { get; set; }
        public MessageId MessageId { get; set; }

    }
}
