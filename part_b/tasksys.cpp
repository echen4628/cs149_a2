#include "tasksys.h"
#include "iostream"
#include "ostream"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 1; i < num_threads; i++){
        workers.emplace_back([this, i]() {
            TaskSystemParallelThreadPoolSleeping::workerThreadStart(i);
        });
    }
    next_task_id = 0;
    stop = 0;
    current_total_task_launched = 0;
    final_total_task_launched = -1;
    total_task_completed = 0;
    // printf("Newly created\n");
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = true;
    waitForComplete.notify_all();
    waitForTask.notify_all();
    for (auto& worker : workers){
        worker.join();
    }
    workers.clear();

    for (auto it = dependencies.begin(); it != dependencies.end(); ++it) {
        // Access the key and the vector of TaskRecord pointers
        // const std::string& key = it->first;         // Access the key
        std::vector<TaskRecord*>& task_list = it->second;  // Access the vector of TaskRecord pointers

        // Iterate over each vector in the map
        for (TaskRecord* task : task_list) {
            delete task;  // Deallocate the dynamically allocated TaskRecord
        }
        task_list.clear();  // Clear the vector after deallocating its elements
    }

    dependencies.clear();  // Clear the unordered_map itself

    for (auto& task: readyToRun) {
        delete task;
    }
    readyToRun.clear();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    
    // create a new work_unit
    TaskRecord* currentTask = new TaskRecord;
    currentTask->next_work_item = 0;
    currentTask->completed_work_count = 0;
    currentTask->total_work_count = num_total_tasks;
    currentTask->current_runnable = runnable;
    currentTask->remaining_dependencies = 0;
    currentTask->str_taskid = std::to_string(next_task_id);
    next_task_id += 1;
    bigMutex.lock();
    dependencies[currentTask->str_taskid] = std::vector<TaskRecord*>();
    for (TaskID dep : deps) {
        if (dependencies.find(std::to_string(dep)) == dependencies.end()){
            // printf("TaskID %d has no dependencies\n", dep);
        } else {
            currentTask->remaining_dependencies += 1;
            dependencies[std::to_string(dep)].emplace_back(currentTask);
            // std::cout << "Processing dependency TaskID: " << dep << " as string: " << std::to_string(dep) << std::endl;

        }
    }
    if (currentTask->remaining_dependencies == 0) {
        // printf("Added task %d to readytoRun\n", next_task_id-1);
        // bigMutex.lock();
        readyToRun.emplace_back(currentTask);
        // bigMutex.unlock();
    }
    bigMutex.unlock();

    waitForTask.notify_all();

    bigMutex.lock();
    current_total_task_launched += 1;
    bigMutex.unlock();

    return next_task_id-1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    bigMutex.lock();
    final_total_task_launched = current_total_task_launched;
    bigMutex.unlock();
    waitForTask.notify_all();
    workerThreadStart(0);
    return;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int const thread_id) {
    // printf("Stop is %d\n", stop.load());
    while(not stop) {
        // printf("[Thread %d] Looking for work\n", thread_id);
        TaskRecord* myTaskRecord = NULL;
        int myWorkItem = -1;
        {
            std::unique_lock<std::mutex> lock(bigMutex);
            if (readyToRun.size() != 0){
                myTaskRecord = readyToRun[readyToRun.size()-1];
                myWorkItem = myTaskRecord->next_work_item;
                myTaskRecord->next_work_item += 1;
                if (myTaskRecord->next_work_item == myTaskRecord->total_work_count) {
                    readyToRun.pop_back();
                }
            }
        }
        if (myWorkItem == -1) {
            printf("[Thread %d] Can't find work\n", thread_id);
            if (thread_id == 0) {
                std::unique_lock<std::mutex> waitForCompleteLock(bigMutex);
                waitForComplete.wait(waitForCompleteLock, [this]{
                    return ((final_total_task_launched == total_task_completed) || stop);
                });
                return;
            } else {
                std::unique_lock<std::mutex> waitForTaskLock(bigMutex);
                printf("[Thread %d] Number of keys: %ld\n", thread_id, dependencies.size());
                waitForTask.wait(waitForTaskLock, [this]{
                    return ((readyToRun.size() != 0) || stop);
                });
                continue;
            }
        } else{
            // printf("[Thread %d]: I have task %d\n", thread_id, myWorkItem);
            myTaskRecord->current_runnable->runTask(myWorkItem, myTaskRecord->total_work_count);
            // printf("[Thread %d]: I finished task %d\n", thread_id, myWorkItem);
            {
                std::unique_lock<std::mutex> lock(bigMutex);
                myTaskRecord->completed_work_count += 1;
                if (myTaskRecord->completed_work_count == myTaskRecord->total_work_count){
                    // printf("[Thread %d]: I totally completed a task group\n", thread_id);
                    for (TaskRecord* nextTaskRecord : dependencies[myTaskRecord->str_taskid]) {
                        nextTaskRecord->remaining_dependencies -= 1;
                        printf("[Thread %d]: next task remaining dependencies: %d\n", thread_id, nextTaskRecord->remaining_dependencies);
                        if (nextTaskRecord->remaining_dependencies == 0) {
                            // printf("[Thread %d]: I added a new task group\n", thread_id);
                            readyToRun.emplace_back(nextTaskRecord);
                        }
                    }
                    waitForTask.notify_all();
                    printf("[Thread %d] I notified. readyToRun size %ld\n", thread_id, readyToRun.size());

                    dependencies.erase(myTaskRecord->str_taskid);
                    total_task_completed += 1;
                    printf("[Thread %d]: total task progress is %d/%d\n", thread_id, total_task_completed, final_total_task_launched);
                    if (total_task_completed == final_total_task_launched) {
                        waitForComplete.notify_all();
                    }
                }
            }
        }
    }
}

// void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int const thread_id) {
//     printf("[Thread %d] Online\n", thread_id);
//     while(not stop) {
//         // get lock
//         TaskRecord* myTaskRecord = NULL;
//         int myWorkItem = -1;
//         bigMutex.lock();
//         if (readyToRun.size() != 0){
//             myTaskRecord = readyToRun[readyToRun.size()-1];
//             // myTaskRecord->accessTaskRecord.lock();
//             myWorkItem = myTaskRecord->next_work_item;
//             myTaskRecord->next_work_item += 1;
//             if (myTaskRecord->next_work_item == myTaskRecord->total_work_count) {
//                 readyToRun.pop_back();
//             }
//             // myTaskRecord->accessTaskRecord.unlock();
//         }
//         bigMutex.unlock();
        
//         printf("[Thread %d] Task is %d\n", thread_id, myWorkItem);

//         if (myTaskRecord and myWorkItem != -1) {
//             // std::cout << "[Thread " << std::to_string(thread_id) << "Processing  job: " << myTaskRecord->str_taskid << std::endl;
//             myTaskRecord->current_runnable->runTask(myWorkItem, myTaskRecord->total_work_count);
//             printf("[Thread %d] Task %d is done\n", thread_id, myWorkItem);
//             // lock to update completed work count
//             bigMutex.lock();
//             // myTaskRecord->accessTaskRecord.lock();
//             myTaskRecord->completed_work_count += 1;
//             printf("[Thread %d] completed work is %d out of %d\n", thread_id, myTaskRecord->completed_work_count, myTaskRecord->total_work_count);

//             // if this task is completed
//             if (myTaskRecord->completed_work_count == myTaskRecord->total_work_count) {
//                 printf("[Thread %d] Completed job\n", thread_id);
//                 // don't need to update myTaskRecord anymore, this task is done
//                 // also at this point no other thread should be using myTaskRecord
//                 // myTaskRecord->accessTaskRecord.unlock();
//                 bigMutex.unlock();
//                 // task is completed, so update dependencies
//                 bigMutex.lock();

//                 for (TaskRecord* nextTaskRecord : dependencies[myTaskRecord->str_taskid]) {
                    
//                     // update nextTaskRecord
//                     // nextTaskRecord->accessTaskRecord.lock();
//                     nextTaskRecord->remaining_dependencies -= 1;
//                     if (nextTaskRecord->remaining_dependencies == 0) {

//                         // if the nextTaskRecord is ready to run, then update ReadyToRun
//                         // accessReadyToRun.lock();
//                         readyToRun.emplace_back(nextTaskRecord);
//                         // accessReadyToRun.unlock();
//                     }
//                     // completes updating nextTaskRecord
//                     // nextTaskRecord->accessTaskRecord.unlock();
//                 }

//                 // all existing tasks dependent on myTask is cleared
//                 dependencies.erase(myTaskRecord->str_taskid);
//                 bigMutex.unlock();

//                 // at this point, no other thread should be using this task
//                 // delete myTaskRecord;

//                 // task is complete, so update total_task_completed
//                 bigMutex.lock();
//                 total_task_completed += 1;
//                 if (total_task_completed == final_total_task_launched) {
//                     bigMutex.unlock();
//                     waitForComplete.notify_all();
//                 }
//                 bigMutex.unlock();
//             }
//         }
        
//         bigMutex.lock();
//         if(readyToRun.size() == 0){
//             bigMutex.unlock();
//             printf("[Thread %d] Can't find work\n", thread_id);
//             if (thread_id == 0) {
//                 std::unique_lock<std::mutex> waitForCompleteLock(bigMutex);
//                 waitForComplete.wait(waitForCompleteLock, [this]{
//                     return ((final_total_task_launched == total_task_completed) || stop);
//                 });
//                 return;
//             } else {
//                 std::unique_lock<std::mutex> waitForTaskLock(bigMutex);
//                 waitForTask.wait(waitForTaskLock, [this]{
//                     return ((readyToRun.size() != 0) || stop);
//                 });
//                 continue;
//             }
//         }
//         bigMutex.unlock();

//         // my_work_batch null pointer
//         // if readyToRun is not empty {
//         //      my_work_batch = readyToRun[0];
//         //      increment next_task in my_work_batch
//         //      if next_task == total_num_tasks {
//         //          remove it from readyToRun
//         // }
//         // remove_lock
//         // work on my task
//         // when task is done {
//         //      grab lock
//         //      increment completed_task in my_work_batch
//         //      if completed_task == total_num_tasks {
//         //          increment total_complete
//         //          flag it as done internally
//         //      }
//         //  if completely_done_flag on
//         //      get another lock
//         //      move the dependencies to ready_to_run
//         //}
//         /// }
//     }
// }
