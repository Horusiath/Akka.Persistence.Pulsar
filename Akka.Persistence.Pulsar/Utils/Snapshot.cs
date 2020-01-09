using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.Utils
{
    public class Snapshot
    {
        public static string Topic(string persistenceId) => $"Snapshot-{persistenceId}".ToLower();
    }
}
