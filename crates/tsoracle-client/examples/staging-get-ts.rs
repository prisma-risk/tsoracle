//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tsoracle_client::{BoxError, ClientBuilder, RetryPolicy};

type AnyError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
struct Cli {
    endpoints: Vec<String>,
    count: u32,
    tls_ca: PathBuf,
    tls_cert: PathBuf,
    tls_key: PathBuf,
    tls_domain: String,
    hint_internal_suffix: String,
    hint_external_prefix: String,
    hint_external_domain: String,
}

#[derive(Clone)]
struct HintMapper {
    internal_suffix: String,
    external_prefix: String,
    external_domain: String,
}

impl HintMapper {
    fn map_endpoint(&self, endpoint: &str) -> Result<String, AnyError> {
        let endpoint = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
            .unwrap_or(endpoint);
        let (host, port) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| err(format!("endpoint {endpoint:?} must be host:port")))?;

        if let Some(ordinal) = self.internal_ordinal(host) {
            return Ok(format!(
                "{}-{}.{}:{}",
                self.external_prefix, ordinal, self.external_domain, port
            ));
        }

        Ok(endpoint.to_string())
    }

    fn internal_ordinal<'a>(&self, host: &'a str) -> Option<&'a str> {
        let pod = host.strip_suffix(&self.internal_suffix)?;
        pod.strip_prefix("tsoracle-")
            .filter(|ordinal| ordinal.chars().all(|c| c.is_ascii_digit()))
    }
}

fn err(message: impl Into<String>) -> AnyError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}

fn usage() -> &'static str {
    "usage: staging-get-ts --endpoints HOST:PORT[,HOST:PORT...] \
     --tls-ca PATH --tls-cert PATH --tls-key PATH [--count N] \
     [--tls-domain NAME] [--hint-internal-suffix SUFFIX] \
     [--hint-external-prefix PREFIX] [--hint-external-domain DOMAIN]"
}

fn parse_args() -> Result<Cli, AnyError> {
    let mut endpoints = None;
    let mut count = 1;
    let mut tls_ca = None;
    let mut tls_cert = None;
    let mut tls_key = None;
    let mut tls_domain = "tsoracle-staging".to_string();
    let mut hint_internal_suffix = ".tsoracle-peer.tsoracle-staging.svc.cluster.local".to_string();
    let mut hint_external_prefix = "tsoracle-staging".to_string();
    let mut hint_external_domain = "taildd5193.ts.net".to_string();

    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            println!("{}", usage());
            std::process::exit(0);
        }

        let (flag, inline_value) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        let mut value = || -> Result<String, AnyError> {
            match inline_value.clone().or_else(|| args.next()) {
                Some(value) => Ok(value),
                None => Err(err(format!("{flag} requires a value"))),
            }
        };

        match flag.as_str() {
            "--endpoints" => {
                endpoints = Some(
                    value()?
                        .split(',')
                        .filter(|endpoint| !endpoint.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                );
            }
            "--count" => count = value()?.parse()?,
            "--tls-ca" => tls_ca = Some(PathBuf::from(value()?)),
            "--tls-cert" => tls_cert = Some(PathBuf::from(value()?)),
            "--tls-key" => tls_key = Some(PathBuf::from(value()?)),
            "--tls-domain" => tls_domain = value()?,
            "--hint-internal-suffix" => hint_internal_suffix = value()?,
            "--hint-external-prefix" => hint_external_prefix = value()?,
            "--hint-external-domain" => hint_external_domain = value()?,
            _ => return Err(err(format!("unknown argument {flag:?}\n{}", usage()))),
        }
    }

    let endpoints = endpoints.ok_or_else(|| err(format!("missing --endpoints\n{}", usage())))?;
    if endpoints.is_empty() {
        return Err(err("--endpoints must include at least one endpoint"));
    }
    if count == 0 {
        return Err(err("--count must be at least 1"));
    }

    Ok(Cli {
        endpoints,
        count,
        tls_ca: tls_ca.ok_or_else(|| err(format!("missing --tls-ca\n{}", usage())))?,
        tls_cert: tls_cert.ok_or_else(|| err(format!("missing --tls-cert\n{}", usage())))?,
        tls_key: tls_key.ok_or_else(|| err(format!("missing --tls-key\n{}", usage())))?,
        tls_domain,
        hint_internal_suffix,
        hint_external_prefix,
        hint_external_domain,
    })
}

fn tls_config(cli: &Cli) -> Result<ClientTlsConfig, AnyError> {
    let ca = std::fs::read(&cli.tls_ca)?;
    let cert = std::fs::read(&cli.tls_cert)?;
    let key = std::fs::read(&cli.tls_key)?;

    Ok(ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key))
        .domain_name(cli.tls_domain.clone()))
}

fn connector(
    mapper: HintMapper,
    tls: ClientTlsConfig,
    policy: RetryPolicy,
) -> impl Fn(
    &str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Channel, BoxError>> + Send>>
+ Send
+ Sync
+ 'static {
    let mapper = Arc::new(mapper);
    move |endpoint: &str| {
        let endpoint = endpoint.to_string();
        let mapper = mapper.clone();
        let tls = tls.clone();
        let policy = policy.clone();
        Box::pin(async move {
            let mapped = mapper.map_endpoint(&endpoint)?;
            let uri = format!("https://{mapped}");
            let channel = Endpoint::from_shared(uri)?
                .tls_config(tls)?
                .connect_timeout(policy.per_attempt_deadline)
                .timeout(policy.per_attempt_deadline)
                .keep_alive_while_idle(true)
                .http2_keep_alive_interval(Duration::from_secs(30))
                .connect()
                .await?;
            Ok(channel)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let cli = parse_args()?;
    let policy = RetryPolicy {
        max_attempts: 50,
        overall_deadline: Duration::from_secs(20),
        ..RetryPolicy::default()
    };
    let tls = tls_config(&cli)?;
    let mapper = HintMapper {
        internal_suffix: cli.hint_internal_suffix.clone(),
        external_prefix: cli.hint_external_prefix.clone(),
        external_domain: cli.hint_external_domain.clone(),
    };
    let client = ClientBuilder::endpoints(cli.endpoints.clone())
        .retry_policy(policy.clone())
        .channel_connector(connector(mapper, tls, policy))
        .build()
        .await?;
    let timestamps = client.get_ts_batch(cli.count).await?;

    for (index, ts) in timestamps.iter().enumerate() {
        println!(
            "ts[{index}] packed={} physical_ms={} logical={}",
            ts.0,
            ts.physical_ms(),
            ts.logical()
        );
    }
    if let Some(leader) = client.cached_leader() {
        println!("cached_leader={leader}");
    }

    Ok(())
}
