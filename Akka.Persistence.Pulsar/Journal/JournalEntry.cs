//-----------------------------------------------------------------------
// <copyright file="JournalEntry.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Akka.Persistence.Pulsar.Journal
{
    /// <summary>
    /// Class used for storing intermediate result of the <see cref="IPersistentRepresentation"/>
    /// </summary>
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
