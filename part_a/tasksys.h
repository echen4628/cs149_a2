#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>

typedef struct
{
    IRunnable *runnable;
    int task_id;
    int num_total_tasks;
} WorkerArgs;

// typedef struct {
//     int thread_id;
// } WorkerArgsSpin;

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    static void workerThreadStart(WorkerArgs *const args);
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void workerThreadStart(int const thread_id);
    void sync();
    std::vector<std::thread> workers;
    std::mutex grab_task_mutex;
    std::mutex complete_task_mutex;
    std::condition_variable cv;
    int next_task;
    int current_num_total_tasks;
    int tasks_completed; // set as atomic
    IRunnable *current_runnable;
    std::atomic<bool> stop;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

#endif
