
namespace Akka.Persistence.Pulsar.Utils
{
    public static class Journal
    {
        public static string PrepareTopic(string topic) => $"persistent://public/default/{topic}"; //very likely to change to be more dynamic;
    }
}
