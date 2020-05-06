using System.ComponentModel.DataAnnotations;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PersistentIdEntry
    {
        [Required]
        public string Id { get; set; }
        [Required]
        public long EntryDate { get; set; }
    }
}
