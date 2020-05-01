#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PulsarPersistenceIdsSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Configuration;
using Akka.Persistence.Pulsar.Query;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Xunit.Abstractions;

namespace Akka.Persistence.Pulsar.Tests
{
    public class PulsarPersistenceIdsSpec : PersistenceIdsSpec
    {
		private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            persistence
			{
				   max-concurrent-recoveries = 5
				   journal 
				   {
						plugin = ""akka.persistence.journal.pulsar""
						pulsar 
						{
			                    # qualified type name of the SQL Server persistence journal actor
			                    class = ""Akka.Persistence.Pulsar.Journal.PulsarJournal, Akka.Persistence.Pulsar""

			                    # dispatcher used to drive journal actor
			                    plugin-dispatcher = ""akka.actor.default-dispatcher""
								
			                    # should corresponding journal table be initialized automatically
			                    auto-initialize = off

								# timestamp provider used for generation of journal entries timestamps
		                        timestamp-provider = ""Akka.Persistence.Sql.Common.Journal.DefaultTimestampProvider, Akka.Persistence.Sql.Common""
								service-url = ""pulsar://52.177.25.186:6650""
  
							    # Time to wait before retrying an operation or reconnect.
							    retry-interval = 3s


								verity-certificate-authority = true
  
							    # Verify certificate name with hostname.
							    verify-cerfiticate-name = false 
								use-proxy = true
								auth-class = ""
								auth-param = ""
								presto-server = ""http://52.167.20.160:8081""
								topic-prefix = ""persistent://public/default/""
								pulsar-tenant = ""public""
								pulsar-namespace = ""default""
		                }
                    }


                    snapshot-store 
					{
						plugin = ""akka.persistence.snapshot-store.pulsar""
						pulsar 
						{

			                  # qualified type name of the SQL Server persistence journal actor
			                  class = ""Akka.Persistence.Pulsar.Snapshot.PulsarSnapshotStore, Akka.Persistence.Pulsar""

			                  # dispatcher used to drive journal actor
			                  plugin-dispatcher = ""akka.actor.default-dispatcher""
			                  

			                  # should corresponding journal table be initialized automatically
			                  auto-initialize = off
						}
					}
				
					query.journal
					{
                        plugin = ""akka.persistence.query.journal.pulsar""
                        pulsar
                        {
                              # Implementation class of the SQL ReadJournalProvider
					          class = ""Akka.Persistence.Pulsar.Query.PulsarReadJournalProvider, Akka.Persistence.Pulsar""
				  
					          # Absolute path to the write journal plugin configuration entry that this 
					          # query journal will connect to. 
					          # If undefined (or "") it will connect to the default journal as specified by the
					          # akka.persistence.journal.plugin property.
					          write-plugin = ""
				          
					          # The SQL write journal is notifying the query side as soon as things
					          # are persisted, but for efficiency reasons the query side retrieves the events 
					          # in batches that sometimes can be delayed up to the configured `refresh-interval`.
					          refresh-interval = 1s

					          # How many events to fetch in one query (replay) and keep buffered until they
                              # are delivered downstreams.
                              max-buffer-size = 100
                        }
					  
					}
			}
            akka.test.single-expect-default = 10s
        ").WithFallback(PulsarPersistence.DefaultConfiguration());


		public PulsarPersistenceIdsSpec(ITestOutputHelper output) : base(SpecConfig, output: output)
        {
            ReadJournal = Sys.ReadJournalFor<PulsarReadJournal>(PulsarReadJournal.Identifier);
        }
    }
}