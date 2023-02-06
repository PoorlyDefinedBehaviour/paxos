use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc::{Receiver, Sender};

use anyhow::{anyhow, Result};
use axum::{routing::post, Extension, Json, Router};

use clap::Parser;

#[derive(Debug, Parser)]
struct Cli {
    /// The id of the process.
    #[arg(long)]
    id: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ports = [5000, 5001, 5002, 5003, 5004];
    assert!(
        cli.id < ports.len(),
        "id must be between 0 and {}",
        ports.len() - 1
    );
    let port = ports[cli.id];

    let config = Config {
        cluster_members: ports
            .iter()
            .enumerate()
            .filter_map(|(i, port)| {
                if i == cli.id {
                    None
                } else {
                    Some(format!("http://0.0.0.0:{port}"))
                }
            })
            .collect(),
    };

    let paxos = Paxos::new(config);

    // build our application with a single route
    let app = Router::new()
        .route("/", post(handle_request))
        .route("/prepare", post(handle_prepare))
        .route("/propose", post(handle_propose))
        .layer(Extension(Arc::new(paxos)));

    let addr = format!("0.0.0.0:{port}").parse()?;
    println!("starting server. address={addr}");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn handle_request(
    Extension(paxos): Extension<Arc<PaxosMailbox>>,
    value: String,
) -> Result<&'static str, String> {
    match paxos.request_from_client(value).await {
        Err(err) => Err(err.to_string()),
        Ok(_) => Ok("ACCEPTED"),
    }
}

async fn handle_prepare(
    Extension(paxos): Extension<Arc<PaxosMailbox>>,
    Json(prepare_request): Json<PrepareRequest>,
) -> Result<Json<PrepareResponse>, String> {
    match paxos.prepare(prepare_request.id).await {
        Err(err) => Err(err.to_string()),
        Ok(v) => Ok(Json(v)),
    }
}

async fn handle_propose(
    Extension(paxos): Extension<Arc<PaxosMailbox>>,
    Json(propose_request): Json<ProposeRequest>,
) -> Result<Json<ProposeResponse>, String> {
    match paxos
        .propose(propose_request.id, propose_request.value)
        .await
    {
        Err(err) => Err(err.to_string()),
        Ok(v) => Ok(Json(v)),
    }
}

struct Config {
    /// List of addresses of other nodes in the cluster.
    cluster_members: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PrepareRequest {
    id: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PrepareResponse {
    accepted_id: u64,
    accepted_value: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProposeRequest {
    id: u64,
    value: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ProposeResponse {
    accepted_id: u64,
}

#[derive(Debug)]
enum Message {
    RequestFromClient {
        value: String,
        answer_tx: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Prepare {
        id: u64,
        answer_tx: tokio::sync::oneshot::Sender<Result<PrepareResponse>>,
    },
    Propose {
        id: u64,
        value: String,
        answer_tx: tokio::sync::oneshot::Sender<Result<ProposeResponse>>,
    },
}

struct PaxosMailbox {
    tx: Sender<Message>,
}
impl PaxosMailbox {
    async fn request_from_client(&self, value: String) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message::RequestFromClient {
                value,
                answer_tx: tx,
            })
            .await?;
        let result = rx.await?;
        result
    }

    async fn prepare(&self, id: u64) -> Result<PrepareResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::Prepare { id, answer_tx: tx }).await?;
        let result = rx.await?;
        result
    }

    async fn propose(&self, id: u64, value: String) -> Result<ProposeResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message::Propose {
                id,
                value,
                answer_tx: tx,
            })
            .await?;
        let result = rx.await?;
        result
    }
}

struct Paxos {
    config: Config,
    // TODO: handle overflow.
    id: u64,
    /// Used to receive values sent by users. The value will be used to propose.
    rx: Receiver<Message>,
    /// The highest id seen.
    max_id: Option<u64>,
    /// The accepted value.
    accepted_value: Option<String>,
}

impl Paxos {
    fn new(config: Config) -> PaxosMailbox {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let paxos = Self {
            config,
            id: 0,
            rx,
            max_id: None,
            accepted_value: None,
        };
        tokio::spawn(paxos.start());
        PaxosMailbox { tx }
    }

    // TODO: do ids need to be unique in the cluster? If so, why?
    fn next_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }

    fn majority(&self) -> usize {
        self.config.cluster_members.len() / 2 + 1
    }

    async fn start(mut self) -> Result<()> {
        loop {
            let message = match self.rx.recv().await {
                None => {
                    eprintln!("Paxos.rx closed, quitting");
                    return Err(anyhow!("Paxos.rx closed unexpectedly"));
                }
                Some(v) => v,
            };

            match message {
                Message::RequestFromClient { value, answer_tx } => {
                    let id = self.next_id();
                    let result = self.prepare(id, value).await;
                    if let Err(err) = answer_tx.send(result) {
                        eprintln!("handling message from client: unable to send result to channel. error={err:?}");
                    }
                }
                Message::Prepare { id, answer_tx } => {
                    let result = self.acceptor_handle_prepare(id).await;

                    if let Err(err) = answer_tx.send(result) {
                        eprintln!("handling prepare message: unable to send result to channel. error={err:?}");
                    }
                }
                Message::Propose {
                    id,
                    value,
                    answer_tx,
                } => {
                    let result = self.acceptor_handle_propose(id, value).await;

                    if let Err(err) = answer_tx.send(result) {
                        eprintln!("handling propose message: unable to send result to channel. error={err:?}");
                    }
                }
            }
        }
    }

    // Proposer: PREPARE phase
    async fn prepare(&mut self, id: u64, value: String) -> Result<()> {
        let prepare_request = PrepareRequest { id };

        let futures = self.config.cluster_members.iter().map(|addr| {
            reqwest::Client::new()
                .post(format!("{addr}/prepare"))
                .timeout(Duration::from_secs(10))
                .json(&prepare_request)
                .send()
        });

        // TODO: wait for majority
        let responses = futures::future::join_all(futures).await;

        let mut success_responses = Vec::new();

        for response in responses {
            let response = match response {
                Err(err) => {
                    eprintln!("prepare request: got error response. error={err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            if !response.status().is_success() {
                continue;
            }

            match response.json::<PrepareResponse>().await {
                Err(err) => {
                    eprintln!("prepare request: invalid response body. error={err:?}");
                    continue;
                }
                Ok(v) => success_responses.push(v),
            }
        }

        // Add 1 to take this process into account.
        if success_responses.len() + 1 < self.majority() {
            return Err(anyhow!(
                "prepare request: did not get response from the majority"
            ));
        }

        for response in success_responses.iter() {
            if response.accepted_id >= id {
                self.id = response.accepted_id + 1;
                return Err(anyhow!("got a response with a higher id"));
            }
        }

        let response_with_max_accepted_id = success_responses
            .into_iter()
            .filter(|response| response.accepted_value.is_some())
            .max_by_key(|response| response.accepted_id);

        let value_to_propose = match response_with_max_accepted_id {
            Some(response) => response.accepted_value.unwrap(),
            None => value,
        };

        self.propose(id, value_to_propose).await?;

        Ok(())
    }

    // Proposer: PROPOSE phase
    async fn propose(&self, id: u64, value: String) -> Result<()> {
        let propose_request = ProposeRequest { id, value };

        let futures = self.config.cluster_members.iter().map(|addr| {
            reqwest::Client::new()
                .post(format!("{addr}/propose"))
                .timeout(Duration::from_secs(10))
                .json(&propose_request)
                .send()
        });

        // TODO: wait for majority
        let responses = futures::future::join_all(futures).await;

        let mut success_responses = Vec::new();

        for response in responses {
            let response = match response {
                Err(err) => {
                    eprintln!("propose request: got error response. error={err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            match response.json::<PrepareResponse>().await {
                Err(err) => {
                    eprintln!("propose request: invalid response body. error={err:?}");
                    continue;
                }
                Ok(v) => success_responses.push(v),
            }
        }

        // Add 1 to take this process into account.
        if success_responses.len() + 1 < self.majority() {
            return Err(anyhow!(
                "propose request: did not get response from the majority"
            ));
        }

        // let response_with_max_accepted_id = success_responses
        //     .into_iter()
        //     .filter(|response| response.accepted_value.is_some())
        //     .max_by_key(|response| response.accepted_id);

        Ok(())
    }

    // Acceptor: PREPARE phase
    async fn acceptor_handle_prepare(&mut self, id: u64) -> Result<PrepareResponse> {
        if let Some(max_id) = self.max_id {
            if id <= max_id {
                assert!(self.accepted_value.is_some());
                return Ok(PrepareResponse {
                    accepted_id: max_id,
                    accepted_value: self.accepted_value.clone(),
                });
            }
        }

        self.max_id = Some(id);

        Ok(PrepareResponse {
            accepted_id: id,
            accepted_value: self.accepted_value.clone(),
        })
    }

    // Acceptor: PROPOSE phase
    async fn acceptor_handle_propose(&mut self, id: u64, value: String) -> Result<ProposeResponse> {
        if Some(id) != self.max_id {
            return Err(anyhow!(
                "acceptor(handle propose): is not the highest the node has seen"
            ));
        }

        self.accepted_value = Some(value);

        Ok(ProposeResponse { accepted_id: id })
    }
}
