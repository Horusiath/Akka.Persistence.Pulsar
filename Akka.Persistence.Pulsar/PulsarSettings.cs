#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PulsarPersistence.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Security.Cryptography.X509Certificates;
using Akka.Configuration;
using SharpPulsar.Akka;
using SharpPulsar.Akka.Network;
using SharpPulsar.Impl.Auth;

namespace Akka.Persistence.Pulsar
{
    public sealed class PulsarSettings
    {
        public PulsarSettings(Config config)
        {
            ServiceUrl = config.GetString("service-url", "pulsar://localhost:6650");
            VerifyCertificateAuthority = config.GetBoolean("verify-certificate-authority", true);
            VerifyCertificateName = config.GetBoolean("verify-cerfiticate-name", false);
            UseProxy = config.GetBoolean("use-proxy", false);
            AuthClass = config.HasPath("auth-class") ? config.GetString("auth-class") : "";
            AuthParam = config.HasPath("auth-param") ? config.GetString("auth-param") : "";
            PrestoServer = config.GetString("presto-server");
            TopicPrefix = config.GetString("topic-prefix");
            Tenant = config.GetString("pulsar-tenant");
            Namespace = config.GetString("pulsar-namespace");
            TrustedCertificateAuthority = config.HasPath("trusted-certificate-authority-file") 
                ? new X509Certificate2(config.GetString("trusted-certificate-authority-file") )
                : null;
            
            ClientCertificate = config.HasPath("client-certificate-file") 
                ? new X509Certificate2(config.GetString("client-certificate-file") )
                : null;
        }

        public Config Config { get; set; }
        public string ServiceUrl { get; set; }
        public string Tenant { get; set; }
        public string Namespace { get; set; }
        public string PrestoServer { get; set; }
        public string TopicPrefix { get; set; }// prefix in this sense persistent://public/default/{this part added at runtime}
        public string AuthClass { get; set; }
        public string AuthParam { get; set; }
        public bool UseProxy { get; set; }
        public bool VerifyCertificateAuthority { get; set; }
        public bool VerifyCertificateName { get; set; }
        public X509Certificate2 TrustedCertificateAuthority { get; set; }
        public X509Certificate2 ClientCertificate { get; set; }
        
        public PulsarSystem CreateSystem()
        {
            var builder = new PulsarClientConfigBuilder()
                .ServiceUrl(ServiceUrl)
                .VerifyCertAuth(VerifyCertificateAuthority)
                .VerifyCertName(VerifyCertificateName)
                .ConnectionsPerBroker(1)
                .UseProxy(UseProxy)
                .Authentication(AuthenticationFactory.Create(AuthClass, AuthParam));
                
            if (!(TrustedCertificateAuthority is null))
            {
                builder = builder.AddTrustedAuthCert(this.TrustedCertificateAuthority);
            }

            if (!(ClientCertificate is null))
            {
                builder = builder.AddTlsCerts(new X509Certificate2Collection{ ClientCertificate });
            }
            //.Authentication(AuthenticationFactory.Token("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzaGFycHB1bHNhci1jbGllbnQtNWU3NzY5OWM2M2Y5MCJ9.lbwoSdOdBoUn3yPz16j3V7zvkUx-Xbiq0_vlSvklj45Bo7zgpLOXgLDYvY34h4MX8yHB4ynBAZEKG1ySIv76DPjn6MIH2FTP_bpI4lSvJxF5KsuPlFHsj8HWTmk57TeUgZ1IOgQn0muGLK1LhrRzKOkdOU6VBV_Hu0Sas0z9jTZL7Xnj1pTmGAn1hueC-6NgkxaZ-7dKqF4BQrr7zNt63_rPZi0ev47vcTV3ga68NUYLH5PfS8XIqJ_OV7ylouw1qDrE9SVN8a5KRrz8V3AokjThcsJvsMQ8C1MhbEm88QICdNKF5nu7kPYR6SsOfJJ1HYY-QBX3wf6YO3VAF_fPpQ"))
            //.ClientConfigurationData;
            var clientConfig = builder.ClientConfigurationData;
            return new PulsarSystem(clientConfig);
        }
    }
}