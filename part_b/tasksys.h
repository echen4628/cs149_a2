#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include "thread"
#include "vector"
#include <atomic>
#include "unordered_map"
#include "mutex"
#include "condition_variable"
#include "string"
#include "algorithm"

struct TaskRecord {
    std::atomic<int> next_work_item;
    std::atomic<int> total_work_count;
    std::atomic<int> completed_work_count;
    int remaining_dependencies;
    std::string str_taskid;
    IRunnable* current_runnable;
    std::mutex accessTaskRecord;
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void workerThreadStart(int const thread_id);

        std::vector<std::thread> workers;
        std::atomic<bool> stop;
        std::vector<TaskRecord*> readyToRun;
        std::unordered_map<std::string, std::vector<TaskRecord*>> dependencies; // A->B A:{B}
        std::mutex accessReadyToRun;
        std::mutex accessDependencies;
        std::mutex bigMutex;
        int next_task_id;
        std::condition_variable waitForTask;
        std::condition_variable waitForComplete;
        std::mutex accessTotalTask;
        int current_total_task_launched;
        int final_total_task_launched;
        int total_task_completed;

};

#endif
