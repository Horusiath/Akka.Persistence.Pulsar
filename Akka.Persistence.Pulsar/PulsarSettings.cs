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
using DotPulsar;
using DotPulsar.Abstractions;

namespace Akka.Persistence.Pulsar
{
    public sealed class PulsarSettings
    {
        public PulsarSettings(Config config)
        {
            ServiceUrl = new Uri(config.GetString("service-url", "pulsar://localhost:6650"), UriKind.Absolute);
            RetryInterval = config.GetTimeSpan("retry-interval", TimeSpan.FromSeconds(3));
            VerifyCertificateAuthority = config.GetBoolean("verify-certificate-authority", true);
            VerifyCertificateName = config.GetBoolean("verify-cerfiticate-name", false);
            JwtToken = config.HasPath("jwt-token") ? config.GetString("jwt-token") : null;
            ConnectionSecurity = config.HasPath("connection-security") 
                ? (EncryptionPolicy)Enum.Parse(typeof(EncryptionPolicy), config.GetString("connection-security"), ignoreCase: true) 
                : default;
            
            TrustedCertificateAuthority = config.HasPath("trusted-certificate-authority-file") 
                ? new X509Certificate2(config.GetString("trusted-certificate-authority-file") )
                : null;
            
            ClientCertificate = config.HasPath("client-certificate-file") 
                ? new X509Certificate2(config.GetString("client-certificate-file") )
                : null;
        }

        public PulsarSettings(Uri serviceUrl, TimeSpan retryInterval, EncryptionPolicy connectionSecurity, string jwtToken, bool verifyCertificateAuthority, bool verifyCertificateName, X509Certificate2 trustedCertificateAuthority, X509Certificate2 clientCertificate)
        {
            ServiceUrl = serviceUrl;
            RetryInterval = retryInterval;
            ConnectionSecurity = connectionSecurity;
            JwtToken = jwtToken;
            VerifyCertificateAuthority = verifyCertificateAuthority;
            VerifyCertificateName = verifyCertificateName;
            TrustedCertificateAuthority = trustedCertificateAuthority;
            ClientCertificate = clientCertificate;
        }

        public Uri ServiceUrl { get; set; }
        public TimeSpan RetryInterval { get; set; }
        public EncryptionPolicy? ConnectionSecurity { get; set; }
        public string JwtToken { get; set; }
        public bool VerifyCertificateAuthority { get; set; }
        public bool VerifyCertificateName { get; set; }
        public X509Certificate2 TrustedCertificateAuthority { get; set; }
        public X509Certificate2 ClientCertificate { get; set; }
        
        public IPulsarClient CreateClient()
        {
            var builder = PulsarClient.Builder()
                .ServiceUrl(this.ServiceUrl)
                .RetryInterval(this.RetryInterval)
                .VerifyCertificateAuthority(this.VerifyCertificateAuthority)
                .VerifyCertificateName(this.VerifyCertificateName);
            if (!(this.JwtToken is null))
            {
                builder = builder.AuthenticateUsingToken(this.JwtToken);
            }
            if (this.ConnectionSecurity.HasValue)
            {
                builder = builder.ConnectionSecurity(this.ConnectionSecurity.Value);
            }

            if (!(this.TrustedCertificateAuthority is null))
            {
                builder = builder.TrustedCertificateAuthority(this.TrustedCertificateAuthority);
            }
            
            if (!(this.ClientCertificate is null))
            {
                builder = builder.TrustedCertificateAuthority(this.ClientCertificate);
            }

            return builder.Build();
        }
    }
}