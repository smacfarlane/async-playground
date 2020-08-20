use rand::{thread_rng, Rng};
use std::collections::VecDeque;

use tokio::time::interval;
use tokio::sync::{oneshot,mpsc};
use tokio::task::JoinHandle;

use tracing::{info,debug,warn};

struct Datastore {
  tasks: VecDeque<Task>
}

#[derive(Debug)]
struct Task {
  pub name: String
}

impl Task {
  #[tracing::instrument]
  pub fn new() -> Task {
    let name: String = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(10)
        .collect();
    debug!("Created task {}", &name);
    Task { name }
  }
}


impl Datastore {
  pub fn new() -> Datastore {
    Datastore{ tasks: VecDeque::new() }
  }

  pub fn add_task(&mut self, t: Task) {
    self.tasks.push_back(t);
  }

  pub fn next_task(&mut self) -> Option<Task> {
    self.tasks.pop_front()
  }

  pub fn count(&self) -> usize {
    self.tasks.len()
  }
}

#[derive(Debug)]
enum DatastoreMsg {
    GetTask {
        resp: Responder<Option<Task>>,
    },
    NewTask {
        task: Task,
        resp: Responder<()>,
    },
    TaskCount {
        resp: Responder<usize>
    },
}
  
type Responder<T> = oneshot::Sender<Result<T,String>>;

struct DatastoreManager { 
  datastore: Datastore,
}

impl DatastoreManager {
  pub fn new() -> DatastoreManager {
    DatastoreManager{ datastore: Datastore::new() }
  }

  pub async fn run(&mut self, mut rx: mpsc::Receiver<DatastoreMsg>) {
    while let Some(cmd) = rx.recv().await {
      debug!("DatastoreManager: {:?}",cmd);
      match cmd {
        DatastoreMsg::GetTask{resp} =>  {
          let t = self.datastore.next_task();
          let _ = resp.send(Ok(t));
        },
        DatastoreMsg::NewTask{task, resp} => {
          self.datastore.add_task(task);
          let _ = resp.send(Ok(()));
        },
        DatastoreMsg::TaskCount{resp} => {
          let count = self.datastore.count();
          let _ = resp.send(Ok(count));
        }
      };
    }
  }
}

#[tracing::instrument]
async fn generate_tasks(mut ds_tx: mpsc::Sender<DatastoreMsg>) -> JoinHandle<()> {
  tokio::task::spawn(async move {
      let mut timer = interval(std::time::Duration::from_secs(1));

      loop {
        timer.tick().await;

        let task = Task::new();
        let (resp_tx, resp_rx) = oneshot::channel();
        let msg = DatastoreMsg::NewTask{task, resp: resp_tx};
        let _ = ds_tx.send(msg).await;
        match resp_rx.await {
          Ok(_) => debug!("Submitted task successfully"),
          Err(e) => debug!("Unable to submit task: {}", e),
        };
      }
  })
}

#[tracing::instrument]
async fn gather_metrics(mut ds_tx: mpsc::Sender<DatastoreMsg>) -> JoinHandle<()> {
  tokio::task::spawn(async move {
    let mut timer = interval(std::time::Duration::from_secs(5));

    loop {
      timer.tick().await;

      let (resp_tx, resp_rx) = oneshot::channel();
      let msg = DatastoreMsg::TaskCount{resp: resp_tx};
      let _ = ds_tx.send(msg).await;

      match resp_rx.await {
        Ok(count) => {
          debug!(depth=count.clone().unwrap());
        },
        Err(e) => {
          warn!("error {:?}", e);
        },
      };
    }
  })
}

struct Scheduler {
  datastore_tx: mpsc::Sender<DatastoreMsg>,

}


async fn schedule_work(ds_tx: mpsc::Sender<DatastoreMsg>) -> JoinHandle {
  

}


#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let (tx, rx) = mpsc::channel(100);
  let taskgen_handle = generate_tasks(tx.clone());
  let metrics_handle = gather_metrics(tx.clone());

  let mut dsm = DatastoreManager::new();
  info!("Starting datastore");
  let ds = dsm.run(rx);
  info!("Starting Scheduler");


  taskgen_handle.await;
  metrics_handle.await;
  ds.await;
  
}

