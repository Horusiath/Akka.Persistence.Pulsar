using System;
using System.Collections.Generic;
using System.Text;

namespace Pulsar_Sample.Event
{
    public interface IEvent
    {
        long EventTime { get; }
    }
}
