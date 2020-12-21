import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author:cbx
 * @Date:2020/12/18/15:36
 */
@Slf4j(topic = "c.TestPool")
public class TestPool {
    public static void main(String[] args) {
        ThreadPool threadPool = new ThreadPool(1, 1000, TimeUnit.MILLISECONDS, 1,(queue,task)->{
//                      queue.put(task); //1.死等方法
//            queue.offer(task,500,TimeUnit.MILLISECONDS); //2。等待0.5秒拒绝
            //3.让调用者放弃任务
//            log.debug("放弃{}",task);
            //4.抛出异常
//            throw new RuntimeException("任务执行失败"+task); //后面的人任务就不会执行了
            //5.让调用者自己执行任务
            task.run();


        });
        for (int i = 0; i < 3; i++) {
            int j = i; //i是变化的，不能在lamda表达式中使用
            threadPool.execute(()->{
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.debug("{}",j);
            });
        }
    }
}

@Slf4j(topic = "c.ThreadPool")
class ThreadPool{
    //任务队列
    private BlockingQueue<Runnable> taskQueue;

    //线程集合
    private HashSet<Worker> workers = new HashSet();

    //核心线程数
    private int colSize;

    //获取任务的超时时间
    private long timeout;
    private TimeUnit timeUnit;

    private RejectPolicy<Runnable> rejectPolicy;



    /**
     * 执行任务的方法
     */
    public void execute(Runnable task){
        synchronized (workers){
            //当任务数没有超过核线程时，直接交给Worker对象执行

            if (workers.size() < colSize){
                Worker worker = new Worker(task);
                log.debug("新增Worker{},{}",worker,task);
                workers.add(worker); //加入线程集合
                worker.start();
            }else {

                //任务数超过了coreSize时，加入任务队列暂存
//                taskQueue.put(task);

                //队列满了
                // 1)第一种方式就是死等 等待线程执行完毕
                //2)超时等待
                //3)放弃此任务
                //4)让调用者抛出异常
                //5)让调用者自己执行任务
                taskQueue.tryPut(rejectPolicy,task);
            }
        }
    }

    public ThreadPool(int colSize, long timeout, TimeUnit timeUnit,int queueCapcity,RejectPolicy<Runnable> rejectPolicy) {
        this.taskQueue = new BlockingQueue<>(queueCapcity);
        this.colSize = colSize;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.rejectPolicy = rejectPolicy;
    }

    class Worker  extends  Thread{
        private Runnable task;

        public Worker(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            //1)当task不为空，执行任务
            //2)当task执行完毕，再接着从任务队列读取任务并执行
            while (task!=null||(task = taskQueue.poll(timeout,timeUnit))!=null){
                try {
                    log.debug("正在执行{}任务",task);
                    task.run();
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    task = null;
                }
            }
            synchronized (workers){
                log.debug("worker被移除{}",this);
                workers.remove(this);
            }
        }
    }
}

//拒绝策略接口
@FunctionalInterface
interface RejectPolicy<T>{
     void reject(BlockingQueue<T> queue, T task);
}

@Slf4j(topic = "c.blockQueue")
class BlockingQueue<T>{
    //任务队列
    private Deque<T> queue = new ArrayDeque<>();

    //锁
    private ReentrantLock lock = new ReentrantLock();

    //生产者条件变量
    private Condition fullWaitSet = lock.newCondition();

    //消费者条件变量
    private Condition emptyWaitSet = lock.newCondition();

    //容量
    private int capcity;

    public BlockingQueue(int capcity) {
        this.capcity = capcity;
    }

    /**
     * 带超时的阻塞获取
     */
    public T poll(long timeout, TimeUnit unit){
        lock.lock();
        try {
            //将timeout统一转换为纳秒
            long nanos = unit.toNanos(timeout);
            while (queue.isEmpty()){
                try {
                    if (nanos<=0){
                        return null;
                    }
                    //这里已经解决了虚假唤醒问题了，返回值就是剩余的时间
                    nanos = emptyWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            T t = queue.removeFirst();
            fullWaitSet.signal();
            return t;
        }finally {
            lock.unlock();
        }
    }

    //阻塞获取
    public T take(){
        lock.lock();
        try {
            while (queue.isEmpty()){
                //如果是空的，就在emptyWaitSet中进行等待
                try {
                    emptyWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            T t = queue.removeFirst();
            fullWaitSet.signal(); //让生产者生产
            return t;
        } finally {
            lock.unlock();
        }
    }


    /**
     * 带超时时间的添加方法
     * @param
     */
    public boolean offer(T task,long timout,TimeUnit timeUnit){
        lock.lock();
        try {
            long nanos = timeUnit.toNanos(timout);
            while (queue.size() == capcity){
                try {
                    log.debug("等待加入任务队列...{}",task);
                    if (nanos<=0) return false;
                    nanos = fullWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.debug("加入任务队列{}",task);
            queue.addLast(task); //添加到队列尾部
            emptyWaitSet.signal(); //唤醒消费者那
            return true;
        }finally {
            lock.unlock();
        }
    }

    //阻塞添加

    public void put(T element){
        lock.lock();
        try {
            while (queue.size() == capcity){
                try {
                    log.debug("等待加入任务队列...{}",element);
                    fullWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.debug("加入任务队列{}",element);
            queue.addLast(element); //添加到队列尾部
            emptyWaitSet.signal(); //唤醒消费者那
        }finally {
            lock.unlock();
        }
    }

    //获取阻塞队列大小
    public int size(){
        lock.lock();
        try {
            return queue.size();
        }finally {
            lock.unlock();
        }
    }

    public void tryPut(RejectPolicy<T> rejectPolicy, T task) {
         lock.lock();
         try {
             if (queue.size() == capcity){
                 //队列满了执行拒绝策略
                 rejectPolicy.reject(this,task);
             }else {

                 log.debug("加入任务队列{}",task);
                 queue.addLast(task);
                 emptyWaitSet.signal();
             }
         }finally {
             lock.unlock();
         }

    }
}
