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
#[non_exhaustive]
enum TaskState {
  Queued,
  // Ready,
  // Dispatched,
  // Complete
}

#[derive(Debug)]
#[non_exhaustive]
enum TaskMsg {
  TaskArrival,
}

#[derive(Debug)]
struct Task {
  pub name: String,
  subtasks: Vec<u32>,
  state: TaskState,
}

impl Task {
  #[tracing::instrument]
  pub fn new() -> Task {
    let name: String = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(10)
        .collect();
    let subtask_count = thread_rng().gen_range(1, 10);
    let subtasks: Vec<u32> = thread_rng().sample_iter(&rand::distributions::Standard)
      .take(subtask_count)
      .map(|n:u32| n % 15)
      .collect();
    debug!("Created task {}", &name);
    Task { name, subtasks, state: TaskState::Queued }
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

#[non_exhaustive]
enum WorkerMsg {
  // Dispatch{ task: Task, resp: Responder<()> }
}

// struct WorkerMgr {
  // worker_rx: mpsc::Receiver<WorkerMsg>,
// }
//
// impl WorkerMgr {
  // pub fn new() ->
// }

#[tracing::instrument]
async fn generate_tasks(mut ds_tx: mpsc::Sender<DatastoreMsg>, mut task_tx: mpsc::Sender<TaskMsg>) -> JoinHandle<()> {
  tokio::task::spawn(async move {
      let mut timer = interval(std::time::Duration::from_secs(1));

      loop {
        timer.tick().await;

        let task = Task::new();
        let (resp_tx, resp_rx) = oneshot::channel();
        let msg = DatastoreMsg::NewTask{task, resp: resp_tx};
        let _ = ds_tx.send(msg).await;
        
        // For fun, lets not care about a response
        // Also, sometimes forget to send so the "true up timer" has some work
        if thread_rng().gen_range(0,10) > 3 {
          let _ = task_tx.send(TaskMsg::TaskArrival).await;
        } else { 
          info!("Skipping dispatch");
        }

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

#[allow(dead_code)]
#[derive(Debug)]
struct Scheduler {
  datastore_tx: mpsc::Sender<DatastoreMsg>,
  task_rx: mpsc::Receiver<TaskMsg>,
  workermgr_tx: mpsc::Sender<WorkerMsg>,
}

impl Scheduler {
  #[tracing::instrument]
  pub fn new(datastore_tx: mpsc::Sender<DatastoreMsg>, task_rx: mpsc::Receiver<TaskMsg>, workermgr_tx: mpsc::Sender<WorkerMsg>) -> Scheduler {
    Scheduler { 
      datastore_tx,
      task_rx,
      workermgr_tx,
    }
  }

  pub async fn run(&mut self) -> JoinHandle<()> {
      let mut timer = interval(std::time::Duration::from_secs(10));
      loop {
        tokio::select! {
          _ = timer.tick() => {
            info!("Tick! Waking up to schedule lost tasks");
            
            let (tx,rx) = oneshot::channel();
            let _ = self.datastore_tx.send(DatastoreMsg::TaskCount{resp: tx}).await;

            match rx.await.unwrap() {
              Ok(count) => info!("{} Tasks queued", count),
              Err(e) => info!("Error fetching tasks {:?}", e)
            };
            if let Some(task) = self.get_next_task().await {
              // Awaiting here seems possibly problematic
              self.dispatch_task(task).await;
            }

          },
          _msg = self.task_rx.recv() => {
            info!("Task Arrived");
            if let Some(task) = self.get_next_task().await {
              self.dispatch_task(task).await;
            }
          }
          //handle message from worker
        }
      }
  }

  #[tracing::instrument(level= "debug")]
  async fn get_next_task(&mut self) -> Option<Task> {  
      let (tx,rx) = oneshot::channel();
      let _ = self.datastore_tx.send(DatastoreMsg::GetTask{resp: tx}).await;

      match rx.await.unwrap() {
        Ok(Some(task)) => { info!("Got task: {:?}", &task); Some(task) },
        Ok(None) => { debug!("No tasks recieved"); None },
        Err(e) => { info!("No task recieved; {:?}", e); None }
      }
  }

  #[tracing::instrument(level= "debug")]
  async fn dispatch_task(&self, task: Task) {
      // let (tx,rx) = oneshot::channel();
      // self.worker_mgr.send(DatastoreMsg::GetTask{resp: tx}).await;
    info!("Placehoder dispatch");
  }
}

async fn schedule_work(mut scheduler: Scheduler) -> JoinHandle<()> {
  tokio::task::spawn( async move {
    scheduler.run().await;
  })
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let (ds_tx, ds_rx) = mpsc::channel(100);
  let (task_tx, task_rx) = mpsc::channel(100);
  let (workermgr_tx, _workermgr_rx) = mpsc::channel(100);
  let taskgen_handle = generate_tasks(ds_tx.clone(), task_tx.clone());
  let metrics_handle = gather_metrics(ds_tx.clone());

  let mut dsm = DatastoreManager::new();
  info!("Starting datastore");
  let ds = dsm.run(ds_rx);
  info!("Starting Scheduler");
  let scheduler = Scheduler::new(ds_tx.clone(), task_rx, workermgr_tx.clone());

  taskgen_handle.await;
  metrics_handle.await;
  schedule_work(scheduler).await;
  ds.await;
  
}

