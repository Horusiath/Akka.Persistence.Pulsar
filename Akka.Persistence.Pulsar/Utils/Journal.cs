using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Pulsar.Utils
{
    public class Journal
    {
        public static string Topic(string persistenceId) => $"Journal-{persistenceId}".ToLower();
    }
}
