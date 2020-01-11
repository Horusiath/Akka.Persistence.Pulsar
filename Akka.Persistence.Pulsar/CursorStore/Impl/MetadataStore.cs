using Akka.Actor;
using Cassandra;
using DotPulsar;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Akka.Persistence.Pulsar.CursorStore.Impl
{
    public class MetadataStore : IMetadataStore
    {
        private ICluster _cluster;
        private ISession _session;
        private ActorSystem _actorSystem;
        private PreparedStatement _saveMessageIdStatement;
        private PreparedStatement _latestStartMessageIdStatement;
        private PreparedStatement _startMessageIdRangeStatement;
        private PreparedStatement _startMessageIdRangeDateStatement;
        public MetadataStore(ActorSystem actorSystem)
        {
            try
            {
                _actorSystem = actorSystem;
                _cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();

                // create session
                _session = _cluster.Connect();

                // prepare schema
                _session.Execute(new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS Pulsar WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }"));
                _session.Execute(new SimpleStatement("USE Pulsar"));
                _session.Execute(new SimpleStatement("CREATE TABLE IF NOT EXISTS meta_data(sequence_id bigint, event_time bigint, persistence_id text, ledger_id bigint, entry_id bigint, partition int, batch_index int, PRIMARY KEY((persistence_id), sequence_id, event_time)) WITH CLUSTERING ORDER BY (sequence_id DESC)"));
                _saveMessageIdStatement = _session.Prepare("UPDATE meta_data SET ledger_id = ?, entry_id = ?, partition = ?, batch_index = ? WHERE persistence_id = ? AND sequence_id = ? AND  event_time = ?");
                _latestStartMessageIdStatement = _session.Prepare("SELECT * FROM meta_data WHERE persistence_id = ? LIMIT 1 ALLOW FILTERING");
                _startMessageIdRangeStatement = _session.Prepare("SELECT * FROM meta_data WHERE persistence_id = ? AND sequence_id = ?");
                _startMessageIdRangeDateStatement = _session.Prepare("SELECT * FROM meta_data WHERE persistence_id = ? AND event_time IN(?,?)  ALLOW FILTERING");
            }
            catch (Exception ex)
            {
                _actorSystem.Log.Error(ex.ToString());
                throw;
            }
        }
        public async Task<(long sequenceid, MessageId messageId)> GetLatestStartMessageId(string persistenceId)
        {
            try
            {
                var latestRows = await _session.ExecuteAsync(_latestStartMessageIdStatement.Bind(persistenceId));
                var latest = latestRows.FirstOrDefault();
                var messageId = new MessageId(latest.GetValue<ulong>("ledger_id"), latest.GetValue<ulong>("entry_id"), latest.GetValue<int>("partition"), latest.GetValue<int>("batch_index"));
                return (latest.GetValue<long>("sequence_id"), messageId);
            }
            catch
            {
                return (0, null);
            }
        }

        public async Task<(MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRange(string persistenceid, long fromSequenceId, long toSequenceId)
        {
            //SELECT * FROM meta_data WHERE persistence_id = ? ORDER BY sequence_id DESC LIMIT 1 ALLOW FILTERING"
            var startRows = await _session.ExecuteAsync(_startMessageIdRangeStatement.Bind(persistenceid, fromSequenceId));
            var endRows = await _session.ExecuteAsync(_startMessageIdRangeStatement.Bind(persistenceid, toSequenceId));
            var start = startRows.FirstOrDefault();
            var end = endRows.FirstOrDefault();
            var startMessageId = new MessageId(start.GetValue<ulong>("ledger_id"), start.GetValue<ulong>("entry_id"), start.GetValue<int>("partition"), start.GetValue<int>("batch_index"));
            var endMessageId = new MessageId(end.GetValue<ulong>("ledger_id"), end.GetValue<ulong>("entry_id"), end.GetValue<int>("partition"), end.GetValue<int>("batch_index"));
            return (startMessageId, endMessageId);
        }

        public async Task<(long fromSequenceId, long toSequenceId, MessageId startMessageId, MessageId endMessageId)> GetStartMessageIdRangeByDate(string persistenceid, long startDate, long endDate)
        {
            var sequenceRows = await _session.ExecuteAsync(_startMessageIdRangeDateStatement.Bind(persistenceid, startDate, endDate));
            var start = sequenceRows.LastOrDefault();
            var end = sequenceRows.FirstOrDefault();
            var startMessageId = new MessageId(start.GetValue<ulong>("ledger_id"), start.GetValue<ulong>("entry_id"), start.GetValue<int>("partition"), start.GetValue<int>("batch_index"));
            var endMessageId = new MessageId(end.GetValue<ulong>("ledger_id"), end.GetValue<ulong>("entry_id"), end.GetValue<int>("partition"), end.GetValue<int>("batch_index"));
            return (start.GetValue<long>("sequence_id"), end.GetValue<long>("sequence_id"), startMessageId, endMessageId);
        }

        public async Task SaveStartMessageId(string persistenceid, long sequenceNr, MessageId messageId, long eventTime)
        {
            await _session.ExecuteAsync(_saveMessageIdStatement.Bind(eventTime, messageId.LedgerId, messageId.EntryId, messageId.Partition, messageId.BatchIndex, sequenceNr, persistenceid));
        }
    }
}
