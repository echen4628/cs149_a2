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
    next_task_id = 0;
    stop = 0;
    current_total_task_launched = 0;
    final_total_task_launched = -1;
    total_task_completed = 0;
    for (int i = 0; i < num_threads; i++){
        workers.emplace_back([this, i]() {
            TaskSystemParallelThreadPoolSleeping::workerThreadStart(i);
        });
    }

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

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
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

    {
        std::unique_lock<std::mutex> lock(accessDependencies);
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
    }
   {
        std::unique_lock<std::mutex> lock(accessReadyToRun);
        if (currentTask->remaining_dependencies == 0) {
            // printf("Added task %d to readytoRun\n", next_task_id-1);
            readyToRun.emplace_back(currentTask);
            waitForTask.notify_all();
        }
   }
    


    // bigMutex.lock();
    {
        std::unique_lock<std::mutex> lockTotalTask(accessTotalTask);
        current_total_task_launched += 1;
    }
    // bigMutex.unlock();

    return next_task_id-1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // bigMutex.lock();
    {
        std::unique_lock<std::mutex> lockTotalTask(accessTotalTask);
        final_total_task_launched = current_total_task_launched;
        waitForComplete.wait(lockTotalTask, [this]{
            return ((final_total_task_launched == total_task_completed) || stop);
        });
    }
    // bigMutex.unlock();
    // waitForTask.notify_all();
    // workerThreadStart(0);

    return;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int const thread_id) {
    // printf("Stop is %d\n", stop.load());
    while(not stop) {
        // printf("[Thread %d] Looking for work\n", thread_id);
        TaskRecord* myTaskRecord = NULL;
        int myWorkItem = -1;
        {
            std::unique_lock<std::mutex> LockReadyToRun(accessReadyToRun);
            if (readyToRun.size() != 0){
                int myTaskRecordIdx = thread_id%readyToRun.size();
                myTaskRecord = readyToRun[myTaskRecordIdx];
                {
                    std::unique_lock<std::mutex> LockTask(myTaskRecord->accessTaskRecord);
                    if (myTaskRecord->next_work_item < myTaskRecord->total_work_count){
                        myWorkItem = myTaskRecord->next_work_item;
                    }

                    myTaskRecord->next_work_item.fetch_add(1);
                    if (myTaskRecord->next_work_item == myTaskRecord->total_work_count) {
                        readyToRun[myTaskRecordIdx] = readyToRun[readyToRun.size()-1];
                        readyToRun.pop_back();
                    }
                }
                                   
            }
        }
        // if (myTaskRecord != NULL) {
        //     std::unique_lock<std::mutex> LockTask(myTaskRecord->accessTaskRecord);
        //     if (myTaskRecord->next_work_item < myTaskRecord->total_work_count){
        //         myWorkItem = myTaskRecord->next_work_item;
        //     }

        //     myTaskRecord->next_work_item += 1;
        //     if (myTaskRecord->next_work_item == myTaskRecord->total_work_count) {
        //         std::unique_lock<std::mutex> LockReadyToRun(accessReadyToRun);
        //         readyToRun.pop_back();
        //     }
        // }
        if (myWorkItem == -1) {
            // printf("[Thread %d] Can't find work\n", thread_id);
            std::unique_lock<std::mutex> waitForTaskLock(accessReadyToRun);
            // printf("[Thread %d] %ld work available\n", thread_id, readyToRun.size());
            waitForTask.wait(waitForTaskLock, [this]{
                return ((readyToRun.size() != 0) || stop);
            });
            continue;
        } else{
            // printf("[Thread %d]: I have task %d\n", thread_id, myWorkItem);
            myTaskRecord->current_runnable->runTask(myWorkItem, myTaskRecord->total_work_count);
            // printf("[Thread %d]: I finished task %d\n", thread_id, myWorkItem);
            


                // std::unique_lock<std::mutex> lock(bigMutex);
                bool completely_done_with_task = false;
                {
                    std::unique_lock<std::mutex> lock(myTaskRecord->accessTaskRecord);
                    myTaskRecord->completed_work_count.fetch_add(1);
                    completely_done_with_task = myTaskRecord->completed_work_count == myTaskRecord->total_work_count;
                }

                if (completely_done_with_task){
                    // printf("[Thread %d]: I totally completed a task group\n", thread_id);
                    {
                        std::unique_lock<std::mutex> dependencyLock(accessDependencies);
                            for (TaskRecord* nextTaskRecord : dependencies[myTaskRecord->str_taskid]) {
                                {
                                    std::unique_lock<std::mutex> LockNextTask(nextTaskRecord->accessTaskRecord);
                                    nextTaskRecord->remaining_dependencies -= 1;
                                    // printf("[Thread %d]: the following task needs to clear %d dependencies.\n", thread_id, nextTaskRecord->remaining_dependencies);
                                    if (nextTaskRecord->remaining_dependencies == 0) {
                                        // printf("[Thread %d]: I added a new task group\n", thread_id);
                                        std::unique_lock<std::mutex> readyToRunLock(accessReadyToRun);
                                        readyToRun.emplace_back(nextTaskRecord);
                                        waitForTask.notify_all();
                                    }
                                }
                                
                            }
                        dependencies.erase(myTaskRecord->str_taskid);
                        
                    }
                    
                    {
                        std::unique_lock<std::mutex> lockTotalTask(accessTotalTask);
                        total_task_completed += 1;
                        // printf("[Thread %d]: total task progress is %d/%d\n", thread_id, total_task_completed, final_total_task_launched);
                        if (total_task_completed == final_total_task_launched) {
                            waitForComplete.notify_all();
                        }
                    }
                    
                }
        }
    }
}
