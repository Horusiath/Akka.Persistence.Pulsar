using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.Journal
{
    public class JournalEntry
    {
        public string Id { get; set; }
        public string PersistenceId { get; set; }
        public long SequenceNr { get; set; }
        public bool IsDeleted { get; set; }
        public object Payload { get; set; }
        public string Manifest { get; set; }
        public long Ordering { get; set; }
        public ICollection<string> Tags { get; set; } = new HashSet<string>();
        public int? SerializerId { get; set; }
    }
}
