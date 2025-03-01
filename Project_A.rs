//This project implements multi-threading in rust along with deadlock detection and prevention.
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

//Task structure representing a unit of work that threads will process
#[derive(Clone)]
struct Task {
    priority: u32,
    operation: fn() -> i32,
    required_resources: Vec<u32>,
}

//Resource trait
trait Resource {
    fn get_id(&self) -> u32; //Returns a task unique ID
}

//First shared resource
struct ResourceA {
    id: u32,               //Unique ID for the resource
    total_operations: i32, //Running sum of operation results performed on resource
    operation_count: i32,  //Number of operations performed on resource
}

//Resource trait for ResourceA
impl Resource for ResourceA {
    fn get_id(&self) -> u32 {
        self.id
    }
}

// Second shared resource for demonstrating deadlock scenarios
struct ResourceB {
    id: u32,
    total_operations: i32,
    operation_count: i32,
}

//Resource trait for ResourceB
impl Resource for ResourceB {
    fn get_id(&self) -> u32 {
        self.id
    }
}

//Helps identify when a thread has been waiting too long to acquire a lock
struct LockTracker {
    start_time: Instant,
    resource_name: String,
    resource_id: u32,
}

impl LockTracker {
    //Create a new tracker with the current time as the start time
    fn new(resource_name: &str, resource_id: u32) -> Self {
        LockTracker {
            start_time: Instant::now(),
            resource_name: resource_name.to_string(),
            resource_id,
        }
    }

    //Check if we've been waiting too long for a lock
    fn check_deadlock(&self, timeout: Duration) -> bool {
        if self.start_time.elapsed() > timeout {
            println!(
                "Potential deadlock detected for resource {} (ID: {}) after {}ms",
                self.resource_name,
                self.resource_id,
                self.start_time.elapsed().as_millis()
            );
            true
        } else {
            false
        }
    }
}

//This simplifies the type declarations throughout the code
type ProtectedResource<T> = Arc<Mutex<T>>;

//Function to acquire locks on two resources in the correct order to prevent deadlocks
fn acquire_resources<'a, T: Resource, U: Resource>(
    resource1: &'a ProtectedResource<T>,
    resource2: &'a ProtectedResource<U>,
    worker_id: u32,
    retry_count: usize,
) -> Option<(MutexGuard<'a, T>, MutexGuard<'a, U>)> {
    //Try to get resource1's ID
    let resource1_id = {
        if let Ok(res1) = resource1.try_lock() {
            res1.get_id()
        } else {
            println!("Worker {} cannot peek at first resource ID", worker_id);
            return None;
        }
    };

    //Try to get resource2's ID
    let resource2_id = {
        if let Ok(res2) = resource2.try_lock() {
            res2.get_id()
        } else {
            println!("Worker {} cannot peek at second resource ID", worker_id);
            return None;
        }
    };

    //This ensures all threads attempt to acquire resources in the same order
    let (guard1, guard2) = if resource1_id < resource2_id {
        for attempt in 0..retry_count {
            println!(
                "Worker {} lock attempt {} for resources {} and {}",
                worker_id,
                attempt + 1,
                resource1_id,
                resource2_id
            );

            //Try to acquire the first lock with deadlock detection
            let tracker1 = LockTracker::new("resource1", resource1_id);
            let guard1 = match resource1.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    if tracker1.check_deadlock(Duration::from_millis(500)) {
                        println!(
                            "Worker {} abandoning lock attempt due to potential deadlock on ID {}",
                            worker_id, resource1_id
                        );
                        return None;
                    }
                    println!(
                        "Worker {} failed to acquire lock T (ID: {}), retrying...",
                        worker_id, resource1_id
                    );
                    thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
                    continue;
                }
            };

            //Now try to acquire the second lock
            let tracker2 = LockTracker::new("resource2", resource2_id);
            let guard2 = match resource2.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    if tracker2.check_deadlock(Duration::from_millis(500)) {
                        println!(
                            "Worker {} abandoning lock attempt due to potential deadlock on ID {}",
                            worker_id, resource2_id
                        );
                        return None;
                    }
                    println!(
                        "Worker {} acquired T (ID: {}) but failed on U (ID: {}), retrying...",
                        worker_id, resource1_id, resource2_id
                    );
                    thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                    continue;
                }
            };

            //If we got here we have both locks
            println!(
                "Worker {} successfully acquired both locks (IDs: {} and {})",
                worker_id, resource1_id, resource2_id
            );
            return Some((guard1, guard2));
        }
        println!(
            "Worker {} exhausted all retry attempts for T then U",
            worker_id
        );
        return None;
    } else {
        // This is the mirror image of the above code block
        for attempt in 0..retry_count {
            println!(
                "Worker {} lock attempt {} for resources {} and {}",
                worker_id,
                attempt + 1,
                resource2_id,
                resource1_id
            );

            //Try to acquire resource2 first
            let tracker2 = LockTracker::new("resource2", resource2_id);
            let guard2 = match resource2.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    if tracker2.check_deadlock(Duration::from_millis(500)) {
                        println!(
                            "Worker {} abandoning lock attempt due to potential deadlock on ID {}",
                            worker_id, resource2_id
                        );
                        return None;
                    }
                    println!(
                        "Worker {} failed to acquire lock U (ID: {}), retrying...",
                        worker_id, resource2_id
                    );
                    thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
                    continue;
                }
            };

            //Then try to acquire resource1
            let tracker1 = LockTracker::new("resource1", resource1_id);
            let guard1 = match resource1.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    if tracker1.check_deadlock(Duration::from_millis(500)) {
                        println!(
                            "Worker {} abandoning lock attempt due to potential deadlock on ID {}",
                            worker_id, resource1_id
                        );
                        return None;
                    }
                    println!(
                        "Worker {} acquired U (ID: {}) but failed on T (ID: {}), retrying...",
                        worker_id, resource2_id, resource1_id
                    );
                    thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                    continue;
                }
            };

            //If we got here, we have both locks
            println!(
                "Worker {} successfully acquired both locks (IDs: {} and {})",
                worker_id, resource2_id, resource1_id
            );
            return Some((guard1, guard2));
        }
        //If exhausted all retries, log and return None
        println!(
            "Worker {} exhausted all retry attempts for U then T",
            worker_id
        );
        return None;
    };

    //This line is technically unreachable but keeps the Rust compiler happy
    Some((guard1, guard2))
}

fn main() {
    //Create a channel for communication between worker threads and the main thread
    let (sender, receiver) = mpsc::channel();

    //Create ResourceA wrapped in Arc and Mutex for thread-safe sharing
    let resource_a = Arc::new(Mutex::new(ResourceA {
        id: 1,               // ID 1 is used for lock ordering
        total_operations: 0, // Initialize counters to zero
        operation_count: 0,
    }));

    //Create ResourceB with similar protection
    let resource_b = Arc::new(Mutex::new(ResourceB {
        id: 2,
        total_operations: 0,
        operation_count: 0,
    }));

    //A set of tasks to be executed by threads
    let tasks = Arc::new(vec![
        Task {
            priority: 1,
            operation: || 2 + 2,
            required_resources: vec![1, 2],
        },
        Task {
            priority: 2,
            operation: || 2 / 2,
            required_resources: vec![1],
        },
        Task {
            priority: 3,
            operation: || 2 - 2,
            required_resources: vec![2],
        },
        Task {
            priority: 4,
            operation: || 2 * 2,
            required_resources: vec![1, 2],
        },
        Task {
            priority: 5,
            operation: || 3 + 3,
            required_resources: vec![1],
        },
        Task {
            priority: 6,
            operation: || 3 - 3,
            required_resources: vec![2],
        },
    ]);

    //Number of worker threads to create
    let num_workers = 10;
    //Vector to store thread handles for joining later
    let mut handles = vec![];

    // Create the worker threads
    for worker_id in 0..num_workers {
        //Each thread needs its own sender and references to the shared resources
        let thread_sender = sender.clone();
        let thread_resource_a = Arc::clone(&resource_a);
        let thread_resource_b = Arc::clone(&resource_b);
        let thread_tasks = Arc::clone(&tasks);

        //Spawn a new thread for this worker
        let handle = thread::spawn(move || {
            //Each worker processes all tasks in the task list
            for task in thread_tasks.iter() {
                println!(
                    "Worker {} started task with priority {}",
                    worker_id, task.priority
                );
                //Run the task and get its result
                let result = (task.operation)();

                //Handle different resource requirements
                if task.required_resources.contains(&1) && task.required_resources.contains(&2) {
                    println!(
                        "Worker {} needs both resources for task priority {}",
                        worker_id, task.priority
                    );

                    //Set the maximum number of retry attempts
                    let max_retries = 3;
                    //Record the start time
                    let start_time = Instant::now();

                    //Try to acquire both resources using our deadlock-prevention function
                    let resources = acquire_resources(
                        &thread_resource_a,
                        &thread_resource_b,
                        worker_id,
                        max_retries,
                    );

                    //If we successfully acquired both resources
                    if let Some((mut res_a, mut res_b)) = resources {
                        //Simulate some work with the resources
                        thread::sleep(Duration::from_millis(50));

                        //Update ResourceA with the operation result
                        res_a.total_operations += result;
                        res_a.operation_count += 1;

                        //Update ResourceB with double the operation result
                        res_b.total_operations += result * 2;
                        res_b.operation_count += 1;

                        //Print the successful update with timing and current values
                        println!(
                            "Worker {} updated both resources in {}ms. A Total: {}, Count: {} | B Total: {}, Count: {}",
                            worker_id,
                            start_time.elapsed().as_millis(),
                            res_a.total_operations,
                            res_a.operation_count,
                            res_b.total_operations,
                            res_b.operation_count
                        );
                    } else {
                        //If we couldn't acquire both resources, Print the timeout
                        println!(
                            "Worker {} timed out after {}ms trying to acquire both resources",
                            worker_id,
                            start_time.elapsed().as_millis()
                        );
                    }
                } else if task.required_resources.contains(&1) {
                    //This task only needs ResourceA
                    println!(
                        "Worker {} only needs Resource A for task priority {}",
                        worker_id, task.priority
                    );

                    let timeout_duration = Duration::from_millis(500);
                    let start_time = Instant::now();
                    let mut acquired = false;

                    //Try to acquire ResourceA until timeout or success
                    while start_time.elapsed() < timeout_duration && !acquired {
                        //Try to lock ResourceA non-blocking
                        if let Ok(mut res_a) = thread_resource_a.try_lock() {
                            //Simulate work with the resource
                            thread::sleep(Duration::from_millis(50));

                            //Update ResourceA with the task result
                            res_a.total_operations += result;
                            res_a.operation_count += 1;

                            //Print the successful update
                            println!(
                                "Worker {} updated Resource A in {}ms. Total: {}, Count: {}",
                                worker_id,
                                start_time.elapsed().as_millis(),
                                res_a.total_operations,
                                res_a.operation_count
                            );
                            acquired = true;
                        } else {
                            //Check for potential deadlock
                            let tracker = LockTracker::new("ResourceA", 1);
                            if tracker.check_deadlock(Duration::from_millis(400)) {
                                //If potential deadlock detected, abandon the attempt
                                println!(
                                    "Worker {} abandoning Resource A due to potential deadlock",
                                    worker_id
                                );
                                break;
                            }
                            //Wait before retrying
                            thread::sleep(Duration::from_millis(50));
                        }
                    }

                    //If we never acquired the resource, Print the timeout
                    if !acquired {
                        println!(
                            "Worker {} timed out trying to acquire Resource A",
                            worker_id
                        );
                    }
                } else if task.required_resources.contains(&2) {
                    //This task only needs ResourceB
                    println!(
                        "Worker {} only needs Resource B for task priority {}",
                        worker_id, task.priority
                    );

                    //Similar logic to ResourceA case
                    let timeout_duration = Duration::from_millis(500);
                    let start_time = Instant::now();
                    let mut acquired = false;

                    //Try to acquire ResourceB until timeout or success
                    while start_time.elapsed() < timeout_duration && !acquired {
                        if let Ok(mut res_b) = thread_resource_b.try_lock() {
                            //Simulate work
                            thread::sleep(Duration::from_millis(50));

                            //Update ResourceB
                            res_b.total_operations += result;
                            res_b.operation_count += 1;

                            //Print the update
                            println!(
                                "Worker {} updated Resource B in {}ms. Total: {}, Count: {}",
                                worker_id,
                                start_time.elapsed().as_millis(),
                                res_b.total_operations,
                                res_b.operation_count
                            );
                            acquired = true;
                        } else {
                            //Check for potential deadlock
                            let tracker = LockTracker::new("ResourceB", 2);
                            if tracker.check_deadlock(Duration::from_millis(400)) {
                                println!(
                                    "Worker {} abandoning Resource B due to potential deadlock",
                                    worker_id
                                );
                                break;
                            }
                            thread::sleep(Duration::from_millis(50));
                        }
                    }

                    //Print timeout if we never acquired the resource
                    if !acquired {
                        println!(
                            "Worker {} timed out trying to acquire Resource B",
                            worker_id
                        );
                    }
                }

                //Send the task result back to the main thread via the channel
                thread_sender
                    .send((worker_id, task.priority, result))
                    .unwrap();

                //Print task completion
                println!(
                    "Worker {} finished task with priority {}",
                    worker_id, task.priority
                );

                //Wait before starting next task to reduce contention
                thread::sleep(Duration::from_millis(100));
            }
        });

        //Store the thread handle for joining later
        handles.push(handle);
    }

    //Drop the original sender to signal no more messages will be sent
    drop(sender);

    //Receive and process results from worker threads
    while let Ok((worker_id, priority, result)) = receiver.recv() {
        //Print each task completion with its result
        println!(
            "Worker {} completed task with priority {}: Result = {}",
            worker_id, priority, result
        );
    }

    //Wait for all worker threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Lock each resource to read its values
    let final_resource_a = resource_a.lock().unwrap();
    let final_resource_b = resource_b.lock().unwrap();

    //Print final state of ResourceA
    println!(
        "\nFinal Resource A state - Total: {}, Count: {}",
        final_resource_a.total_operations, final_resource_a.operation_count
    );

    //Print final state of ResourceB
    println!(
        "Final Resource B state - Total: {}, Count: {}",
        final_resource_b.total_operations, final_resource_b.operation_count
    );

    //Print successful completion of the program
    println!("All tasks completed!");
}
