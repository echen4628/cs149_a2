#include "tasksys.h"
#include <thread>


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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::workerThreadStart(WorkerArgs * const args){
    args->runnable->runTask(args->task_id, args->num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Creates thread objects that do not yet represent a thread.
    std::thread workers[num_total_tasks];
    WorkerArgs workerArgs[num_total_tasks];
    for (int i = 0; i < num_total_tasks; i++) {
        workerArgs[i].runnable = runnable;
        workerArgs[i].task_id = i;
        workerArgs[i].num_total_tasks = num_total_tasks;
    }

    for (int i = 1; i < num_total_tasks; i++) {
        workers[i] = std::thread(workerThreadStart, &workerArgs[i]);
    }
    workerThreadStart(&workerArgs[0]);

    for (int i = 1; i < num_total_tasks; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for(int i = 1; i < num_threads; i++){
        workers.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerThreadStart, this);
    }
    next_task = 0;
    current_num_total_tasks = 0;
    tasks_completed = 0;
    current_runnable = NULL;
    stop = false;
    printf("Made %d threads\n", num_threads);
}

void TaskSystemParallelThreadPoolSpinning::workerThreadStart(){
    while(not stop){
        int my_task = -1;
        grab_task_mutex.lock();
        if (next_task < current_num_total_tasks) {
            my_task = next_task;
            next_task++;
        }
        grab_task_mutex.unlock();
        printf("My task is %d\n", my_task);
        if (my_task != -1){
            current_runnable->runTask(my_task, current_num_total_tasks);
            complete_task_mutex.lock();
            tasks_completed++;
            complete_task_mutex.unlock();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop = true;
    for (auto& worker : workers) {
        worker.join();  // Wait for all threads to finish
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    grab_task_mutex.lock();
    next_task = 0;
    current_num_total_tasks = num_total_tasks;
    current_runnable = runnable;
    grab_task_mutex.unlock();

    complete_task_mutex.lock();
    tasks_completed = 0;
    complete_task_mutex.unlock();

    while(not stop){
        int my_task = -1;
        grab_task_mutex.lock();
        if (next_task < current_num_total_tasks) {
            my_task = next_task;
            next_task++;
        } else {
            printf("[main] next_task: %d, num_total_task: %d\n", next_task, num_total_tasks);
            break;
        }
        grab_task_mutex.unlock();
        if (my_task != -1){
            current_runnable->runTask(my_task, current_num_total_tasks);
            complete_task_mutex.lock();
            tasks_completed++;
            complete_task_mutex.unlock();
        }
    }
    printf("[main]: tasks_completed %d, current_num_total_tasks: %d\n", tasks_completed, current_num_total_tasks);
    std::unique_lock<std::mutex> lock(complete_task_mutex);
    cv.wait(lock, [this] { return tasks_completed == current_num_total_tasks || stop; });
    printf("I think I'm done\n");
    return;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
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

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
