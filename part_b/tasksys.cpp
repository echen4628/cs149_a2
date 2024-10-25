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
    current_total_task_launched = 0;
    final_total_task_launched = -1;
    total_task_completed = 0;
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

    // for (auto& [key, task_list] : dependencies) {
    //     // Iterate over each vector in the map
    //     for (TaskRecord* task : task_list) {
    //         delete task;  // Deallocate the dynamically allocated TaskRecord
    //     }
    //     task_list.clear();  // Clear the vector after deallocating its elements
    // }
    // dependencies.clear();  // Clear the unordered_map itself

    // for (auto& task: readyToRun) {
    //     delete task;
    // }
    // readyToRun.clear();
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
    dependencies[currentTask->str_taskid] = std::vector<TaskRecord*>();
    accessDependencies.lock();
    for (TaskID dep : deps) {
        if (dependencies.find(std::to_string(dep)) == dependencies.end()){
            printf("TaskID %d has no dependencies\n", dep);
        } else {
            currentTask->remaining_dependencies += 1;
            dependencies[std::to_string(dep)].emplace_back(currentTask);
            // std::cout << "Processing dependency TaskID: " << dep << " as string: " << std::to_string(dep) << std::endl;

        }
    }
    if (currentTask->remaining_dependencies == 0) {
        printf("Added task %d to readytoRun\n", next_task_id-1);
        accessReadyToRun.lock();
        readyToRun.emplace_back(currentTask);
        accessReadyToRun.unlock();
    }
    accessDependencies.unlock();

    waitForTask.notify_all();

    accessTotalTask.lock();
    current_total_task_launched += 1;
    accessTotalTask.unlock();

    return next_task_id-1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    accessTotalTask.lock();
    final_total_task_launched = current_total_task_launched;
    accessTotalTask.unlock();
    waitForTask.notify_all();
    workerThreadStart(0);
    return;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int const thread_id) {
    printf("[Thread %d] Online\n", thread_id);
    while(not stop) {
        // get lock
        TaskRecord* myTaskRecord = NULL;
        int myWorkItem = -1;
        accessReadyToRun.lock();
        if (readyToRun.size() != 0){
            myTaskRecord = readyToRun[readyToRun.size()-1];
            myTaskRecord->accessTaskRecord.lock();
            myWorkItem = myTaskRecord->next_work_item;
            myTaskRecord->next_work_item += 1;
            if (myTaskRecord->next_work_item == myTaskRecord->total_work_count) {
                readyToRun.pop_back();
            }
            myTaskRecord->accessTaskRecord.unlock();
        }
        accessReadyToRun.unlock();
        
        printf("[Thread %d] Task is %d\n", thread_id, myWorkItem);

        if (myTaskRecord and myWorkItem != -1) {
            // std::cout << "[Thread " << std::to_string(thread_id) << "Processing  job: " << myTaskRecord->str_taskid << std::endl;
            myTaskRecord->current_runnable->runTask(myWorkItem, myTaskRecord->total_work_count);
            printf("[Thread %d] Task %d is done\n", thread_id, myWorkItem);
            // lock to update completed work count
            myTaskRecord->accessTaskRecord.lock();
            myTaskRecord->completed_work_count += 1;

            // if this task is completed
            if (myTaskRecord->completed_work_count == myTaskRecord->total_work_count) {
                printf("[Thread %d] Completed job\n", thread_id);
                // don't need to update myTaskRecord anymore, this task is done
                // also at this point no other thread should be using myTaskRecord
                myTaskRecord->accessTaskRecord.unlock();

                // task is completed, so update dependencies
                accessDependencies.lock();

                for (TaskRecord* nextTaskRecord : dependencies[myTaskRecord->str_taskid]) {
                    
                    // update nextTaskRecord
                    nextTaskRecord->accessTaskRecord.lock();
                    nextTaskRecord->remaining_dependencies -= 1;
                    if (nextTaskRecord->remaining_dependencies == 0) {

                        // if the nextTaskRecord is ready to run, then update ReadyToRun
                        accessReadyToRun.lock();
                        readyToRun.emplace_back(nextTaskRecord);
                        accessReadyToRun.unlock();
                    }
                    // completes updating nextTaskRecord
                    nextTaskRecord->accessTaskRecord.unlock();
                }

                // all existing tasks dependent on myTask is cleared
                dependencies.erase(myTaskRecord->str_taskid);
                accessDependencies.unlock();

                // at this point, no other thread should be using this task
                // delete myTaskRecord;

                // task is complete, so update total_task_completed
                accessTotalTask.lock();
                total_task_completed += 1;
                if (total_task_completed == final_total_task_launched) {
                    accessTotalTask.unlock();
                    waitForComplete.notify_all();
                }
                accessTotalTask.unlock();
            }
        } else {
            printf("[Thread %d] Can't find work\n", thread_id);
            if (thread_id == 0) {
                std::unique_lock<std::mutex> waitForCompleteLock(accessTotalTask);
                waitForComplete.wait(waitForCompleteLock, [this]{
                    return ((final_total_task_launched == total_task_completed) || stop);
                });
                return;
            } else {
                std::unique_lock<std::mutex> waitForTaskLock(accessReadyToRun);
                waitForTask.wait(waitForTaskLock, [this]{
                    return ((readyToRun.size() != 0) || stop);
                });
                continue;
            }
        }

        // my_work_batch null pointer
        // if readyToRun is not empty {
        //      my_work_batch = readyToRun[0];
        //      increment next_task in my_work_batch
        //      if next_task == total_num_tasks {
        //          remove it from readyToRun
        // }
        // remove_lock
        // work on my task
        // when task is done {
        //      grab lock
        //      increment completed_task in my_work_batch
        //      if completed_task == total_num_tasks {
        //          increment total_complete
        //          flag it as done internally
        //      }
        //  if completely_done_flag on
        //      get another lock
        //      move the dependencies to ready_to_run
        //}
        /// }
    }
}
