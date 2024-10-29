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
    this->num_threads = num_threads;
    next_task = 0;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::workerThreadStart(WorkerArgs * const args){
    // printf("[Thread %d] Hello from thread\n", args->task_id);
    while(true)
    {
        int my_task = 0;
        
        // Lock the access to `next_task` to ensure thread safety
        {
            std::unique_lock<std::mutex> Lock(access_next_task);
            
            // Check if there are still tasks available
            if (next_task >= args->num_total_tasks) {
                // If no tasks are left, exit the loop
                // printf("[Thread %d] next task %d total_tasks %d\n", args->task_id, next_task, args->num_total_tasks);

                // printf("[Thread %d] No more tasks available. Exiting.\n", args->task_id);
                break;
            }
            
            // Assign the next available task
            my_task = next_task;
            next_task += 1;
        }

        if (my_task < args->num_total_tasks) {
            // printf("[Thread %d] I will work on task %d out of %d\n", args->task_id, my_task, args->num_total_tasks);
            args->runnable->runTask(my_task, args->num_total_tasks);
        } else {
            // If the task number is out of range, exit the loop
            // printf("[Thread %d] Task %d is out of range. Exiting.\n", args->task_id, my_task);
            break;
        }
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Creates thread objects that do not yet represent a thread.
    // printf("Start of a run\n");
    std::thread workers[num_threads];
    WorkerArgs workerArgs[num_threads];
    next_task = 0;
    for (int i = 0; i < num_threads; i++) {
        workerArgs[i].runnable = runnable;
        workerArgs[i].task_id = i;
        workerArgs[i].num_total_tasks = num_total_tasks;
    }

    for (int i = 1; i < num_threads; i++) {
        // workers[i] = std::thread(this->workerThreadStart, &workerArgs[i]);
        workers[i] = std::thread([this, &workerArgs, i]() {
            this->workerThreadStart(&workerArgs[i]);
        });

    }
    workerThreadStart(&workerArgs[0]);

    for (int i = 1; i < num_threads; i++) {
        workers[i].join();
    }
    // printf("I think I'm done\n");
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
    next_task = 0;
    current_num_total_tasks = 0;
    tasks_completed = 0;
    current_runnable = NULL;
    stop = false;
    for (int i = 1; i < num_threads; i++) {
        workers.emplace_back([this, i]() { 
            this->workerThreadStart(i); 
        });
    }
}

void TaskSystemParallelThreadPoolSpinning::workerThreadStart(int const thread_id){
    while(not stop) {
        int my_task = -1;
        grab_task_mutex.lock();
        if (next_task < current_num_total_tasks) {
            my_task = next_task;
            next_task++;
        }
        grab_task_mutex.unlock();
        if (my_task != -1){
            current_runnable->runTask(my_task, current_num_total_tasks);
            complete_task_mutex.lock();
            tasks_completed++;
            complete_task_mutex.unlock();
        }
        if (thread_id == 0){
            complete_task_mutex.lock();
            if (tasks_completed == current_num_total_tasks){
                complete_task_mutex.unlock();
                return;
            }
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

    grab_task_mutex.lock();
    complete_task_mutex.lock();
    next_task = 0;
    current_num_total_tasks = num_total_tasks;
    current_runnable = runnable;
    tasks_completed = 0;

    // printf("Created %d tasks, next task is %d\n. Finished %d tasks\n", current_num_total_tasks, next_task, tasks_completed);
    complete_task_mutex.unlock();
    grab_task_mutex.unlock();

    workerThreadStart(0);
    // printf("I think I'm done\n");
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
    next_task = 0;
    current_num_total_tasks = 0;
    tasks_completed = 0;
    current_runnable = NULL;
    stop = false;
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this, i]() { 
            this->workerThreadStart(i); 
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    stop = true;
    have_task.notify_all();
    is_complete.notify_all();
    for (auto& worker : workers) {
        worker.join();  // Wait for all threads to finish
    }
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int const thread_id) {
     while(not stop) {
        int my_task = -1;
        grab_task_mutex.lock();
        if (next_task < current_num_total_tasks) {
            my_task = next_task;
            next_task++;
        }
        grab_task_mutex.unlock();
        if (my_task != -1){
            current_runnable->runTask(my_task, current_num_total_tasks);
            complete_task_mutex.lock();
            tasks_completed++;
            if (tasks_completed == current_num_total_tasks){
                complete_task_mutex.unlock();
                is_complete.notify_all();
            }
            complete_task_mutex.unlock();
            
        } else {
            if (thread_id == 0) {
                std::unique_lock<std::mutex> complete_task_lk(complete_task_mutex);
                is_complete.wait(complete_task_lk, [this]{return (tasks_completed == current_num_total_tasks) || stop;});
                return;
            } else {
                std::unique_lock<std::mutex> grab_task_lk(grab_task_mutex);
                have_task.wait(grab_task_lk, [this]{return (next_task < current_num_total_tasks) || stop;});
                continue;
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    grab_task_mutex.lock();
    complete_task_mutex.lock();
    next_task = 0;
    current_num_total_tasks = num_total_tasks;
    current_runnable = runnable;
    tasks_completed = 0;

    // printf("Created %d tasks, next task is %d\n. Finished %d tasks\n", current_num_total_tasks, next_task, tasks_completed);
    complete_task_mutex.unlock();
    grab_task_mutex.unlock();
    have_task.notify_all();

    workerThreadStart(0);
    // printf("I think I'm done\n");
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
