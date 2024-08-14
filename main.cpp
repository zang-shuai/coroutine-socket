#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <ucontext.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#include <map>
#include <list>

/// 最大协程数量
#define MAX_ROUTINE 256
/// 每个协程所使用的栈空间的大小
#define MAX_STACK_SIZE_KB 128
/// 最大的事件数量
#define MAX_EVENT_SIZE 10240

/// 协程的3种状态
enum {
    // 未使用,可用来创建新线程
    UNUSED = 0,
    // 空闲态(有任务,但是没有执行)
    IDLE = 1,
    // 运行态
    RUNNING = 2
};

/// 函数指针类型
typedef void (*STD_ROUTINE_FUNC)(void);

/// 协程上下文结构体
typedef struct {
    // 协程上下文
    ucontext_t ctx;
    // 协程所使用的栈
    char stack[MAX_STACK_SIZE_KB * 1024];
    // 协程执行的逻辑(函数指针,指向要执行的函数)
    STD_ROUTINE_FUNC func;
    // 协程的状态
    int status;
    // 文件描述符
    int wait_fd;
    // epoll 事件
    int events;
} RoutineContext;


// 协程调度中心
typedef struct {
    // 每个协程有一个epoll_event
    struct epoll_event events[MAX_ROUTINE];
    // 每个协程有一个协程上下文RoutineContext
    RoutineContext routines[MAX_ROUTINE];
    // 时间 hash表, key 为时间 ,value 为要执行的协程链
    std::map<unsigned int, std::list<int>> timeout_map;
    // 全局变量，用来保存当前正在执行的协程的上下文
    ucontext_t main;
    // epoll的文件描述符，只有一个
    int epoll_fd;
    // 正在执行的协程的id
    int running_id;
    // 协程数量
    int routine_cnt;
} RoutineCenter;

RoutineCenter routinecenter;

void init() {
    srand(time(NULL));
    routinecenter.running_id = -1;
}

void routine_wrap() {
    // 获取当前协程的id
    int running_id = routinecenter.running_id;
    if (running_id < 0) {
        puts("current context don't attach to any routine except main.");
        return;
    }
    // 执行当前协程
    routinecenter.routines[running_id].func();

    // 执行完当前协程，将协程状态置为UNUSED
    routinecenter.routines[running_id].status = UNUSED;
    // 协程数量-1
    routinecenter.routine_cnt--;
}

/// 创建一个新的协程，协程要执行的逻辑是routine_proc，是一个函数指针类型
int create(STD_ROUTINE_FUNC routine_proc) {
    // 选择一个协程池中未使用的
    int new_id = -1;
    for (int i = 0; i < MAX_ROUTINE; i++) {
        if (routinecenter.routines[i].status == UNUSED) {
            new_id = i;
            break;
        }
    }

    // 错误处理
    if (new_id < 0) {
        puts("max routine number reached. no more routine.");
        return -1;
    }

    // 获取该空闲协程的上下文指针
    ucontext_t* pctx = &(routinecenter.routines[new_id].ctx);
    // 获取当前的上下文,将其拷贝到协程指针中
    getcontext(pctx);

    // 更新这个新协程的内容
    // 设置协程栈
    pctx->uc_stack.ss_sp = routinecenter.routines[new_id].stack;
    // 设置栈大小
    pctx->uc_stack.ss_size = MAX_STACK_SIZE_KB * 1024;
    // 设置标志位
    pctx->uc_stack.ss_flags = 0;
    // 设置该协程的下一个协程指针指向当前函数
    pctx->uc_link = &(routinecenter.main);

    // 设置完后创建上下文(将执行逻辑加入到上下文中)
    // routine_wrap是新协程要执行的逻辑
    makecontext(pctx, routine_wrap, 0);

    // 为新协程创建了上下文之后，将新协程状态置为空闲IDLE
    routinecenter.routines[new_id].status = IDLE;
    routinecenter.routines[new_id].func = routine_proc;
    // 协程数量+1
    routinecenter.routine_cnt++;
    // 返回新协程的id
    return new_id;
}

/// 出让CPU时间片，切换到其它协程执行
int yield() {
    if (routinecenter.running_id < 0) {
        puts("no routine running except main.");
        return 0;
    }
    // 获取当前正在执行的协程的id
    int running_id = routinecenter.running_id;
    // 获取当前正在执行的协程的上下文
    RoutineContext* info = &(routinecenter.routines[running_id]);
    // 将当前协程设置为IDLE空闲状态
    info->status = IDLE;
    // 事件数量设置为0
    info->events = 0;
    // 保存当前执行的协程的上下文，并切换到routinecenter.main的上下文
    swapcontext(&(info->ctx), &(routinecenter.main));
    return 0;
}

/// 重启执行某个协程
int resume(int id, int events = 0) {
    // 3 个判断, 确保协程可以被重启, 处于 idle 状态
    // id 非法
    if (id < 0 || id >= MAX_ROUTINE) {
        puts("routine id out of bound.");
        return -1;
    }

    int running_id = routinecenter.running_id;
    // 如果要重启的协程就是当前正在执行的协程，则直接退出
    if (id == running_id) {
        puts("current routine is running already.");
        return 0;
    }
    // 如果要重启的协程的状态不是IDLE空闲状态，则报错
    if (routinecenter.routines[id].status != IDLE) {
        puts("target routine is not in idel status. can't resume");
        return -1;
    }

    // 将正在执行的协程id属性设置为要重启的协程id
    routinecenter.running_id = id;
    // 将协程状态设置为RUNNING
    routinecenter.routines[id].status = RUNNING;
    // 设置事件id
    routinecenter.routines[id].events = events;
    // 如果正在执行的协程是main主协程，则交换上下文
    if (running_id < 0) {
        swapcontext(&(routinecenter.main), &(routinecenter.routines[id].ctx));
        routinecenter.running_id = -1;
    }
        // 如果正在执行的协程不是主协程main，则交换上下文,并修改状态
    else {
        routinecenter.routines[running_id].status = IDLE;
        // 将正在执行的协程的上下文替换为要重启的协程的上下文
        swapcontext(&(routinecenter.routines[running_id].ctx), &(routinecenter.routines[id].ctx));
        routinecenter.running_id = running_id;
    }
    return 0;
}

int routine_id() { return routinecenter.running_id; }

/// 将文件描述符设置为异步读写
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    int ret = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (ret < 0) {
        perror("set nonblocking fail.");
        exit(-1);
    }
}

/// - EPOLL_CTL_ADD: 添加一个新的监控事件
/// - EPOLL_CTL_MOD: 修改一个已存在的监控事件
/// - EPOLL_CTL_DEL: 删除一个已存在的监控事件
void mod_event(int fd, int events, int op, int routine_id) {
    // epoll_event结构体
    struct epoll_event ev = { 0 };
    // 如果不是删除事件的操作
    if (EPOLL_CTL_DEL != op) {
        // epoll_event的用户数据中保存协程的id
        ev.data.fd = routine_id;
        // socket的文件描述符赋值给协程的文件描述符保存下来
        routinecenter.routines[routine_id].wait_fd = fd;
    }
    // 设置事件类型，例如EPOLLIN，数据可读事件
    ev.events = events;

    // 将fd添加为要监听的文件描述符
    // 举个例子：如果op是EPOLL_CTL_ADD事件，相当于为监听的fd添加一个事件，添加的事件例如是EPOLLIN
    int ret = epoll_ctl(routinecenter.epoll_fd, op, fd, &ev);
    // 错误处理
    if (ret < 0) {
        if (errno == EEXIST && op != EPOLL_CTL_DEL) {
            epoll_ctl(routinecenter.epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }
    }
}

// 处理读的协程
int routine_read(int fd, char* buff, int size) {
    // 监听读事件EPOLLIN
    mod_event(fd, EPOLLIN, EPOLL_CTL_ADD, routine_id());
    // 轮询，如果协程处理的事件不是读事件，那么切换协程
    // 如果是EPOLLIN数据可读事件，则不切换协程，往下执行
    // 如果events为 0 则阻塞
    while (!(routinecenter.routines[routine_id()].events & EPOLLIN)) {
        yield();
    }

    // 如果是EPOLLIN读事件，则一直读取来自socket的数据，读取完毕以后切换其它协程执行。
    while (routinecenter.routines[routine_id()].events & EPOLLIN) {
        int need = size;
        int readin = 0;
        while (need > 0) {
            int ret = read(fd, buff + readin, need);
            if (ret <= 0) {
                break;
            }
            else {
                readin += ret;
                need -= ret;
            }
        }
        if (readin == 0 && size != 0) {
            yield();
            continue;
        }
        else {
            // 删除EPOLLIN事件
            mod_event(fd, EPOLLIN, EPOLL_CTL_DEL, routine_id());
        }
        return readin;
    }
    printf("routine[%d][%s]routine system ran out of order.\n", routine_id(), __func__);
    return 0;
}

// 处理写的协程，和上面的差不多，只是监听的事件换成了EPOLLOUT。
int routine_write(int fd, char* buff, int size) {
    mod_event(fd, EPOLLOUT, EPOLL_CTL_ADD, routine_id());
    // 相等则阻塞,表明不要重复执行相同的操作
    while (!(routinecenter.routines[routine_id()].events & EPOLLOUT)) {
        yield();
    }
    while (routinecenter.routines[routine_id()].events & EPOLLOUT) {
        int need = size;
        int wrout = 0;
        while (need > 0) {
            int ret = write(fd, buff + wrout, need);
            if (ret <= 0) {
                break;
            }
            else {
                wrout += ret;
                need -= ret;
            }
        }
        if (wrout == 0 && size != 0) {
            yield();
            continue;
        }
        else {
            // 删除EPOLLOUT事件
            mod_event(fd, EPOLLOUT, EPOLL_CTL_DEL, routine_id());
        }
        return wrout;
    }
    printf("routine[%d][%s]routine system ran out of order.\n", routine_id(), __func__);
    return 0;
}

//
void routine_delay_resume(int rid, int delay_sec) {
    if (delay_sec <= 0) {
        resume(rid);
        return;
    }
    routinecenter.timeout_map[time(NULL) + delay_sec].push_back(rid);
}

// 协程休眠：就是time_sec之后再启动执行
void routine_sleep(int time_sec) {
    routine_delay_resume(routine_id(), time_sec);
    yield();
}

/// 默认epoll等待的超时事件是1分钟
int routine_nearest_timeout() {
    if (routinecenter.timeout_map.empty()) {
        return 60 * 1000; // default epoll timeout
    }
    unsigned int now = time(NULL);
    // 查看时间表中,最早未执行完的事件时间减当前时间, 为负数表示需要立刻执行, 否则可继续等待
    int diff = routinecenter.timeout_map.begin()->first - now;
    return diff < 0 ? 0 : diff * 1000;
}

// 执行超时还未执行的任务
void routine_resume_timeout() {
    // printf("[epoll] process timeout\n");
    if (routinecenter.timeout_map.empty()) {
        return;
    }

    unsigned int timestamp = routinecenter.timeout_map.begin()->first;

    if (timestamp > time(NULL)) {
        return;
    }

    std::list<int>& routine_ids = routinecenter.timeout_map.begin()->second;

    for (int i : routine_ids) {
        resume(i);
    }
    // 该时间戳所有任务执行完毕
    routinecenter.timeout_map.erase(timestamp);
}

// 每个激活触发的事件都有一个协程rid去处理，挨个启动协程执行
void routine_resume_event(int n) {
    // printf("[epoll] process event\n");
    for (int i = 0; i < n; i++) {
        int rid = routinecenter.events[i].data.fd;
        resume(rid, routinecenter.events[i].events);
    }
}

void create_routine_poll() {
    /// 创建一个epoll实例
    routinecenter.epoll_fd = epoll_create1(0);

    if (routinecenter.epoll_fd == -1) {
        perror("epoll_create");
        exit(-1);
    }
}

// 轮询的逻辑
void routine_poll() {

    for (;;) {
        // 等待某一段时间，n是触发的事件数量
        int n = epoll_wait(routinecenter.epoll_fd, routinecenter.events, MAX_EVENT_SIZE, routine_nearest_timeout());
        // printf("[epoll] event_num:%d\n", n);
        // 先执行已经超时的时间
        routine_resume_timeout();
        // 执行当前需要执行的时间
        routine_resume_event(n);
    }
}

/// 处理某一条连接的请求
void echo_server_routine() {
    int conn_fd = routinecenter.routines[routine_id()].wait_fd;

    printf("routine[%d][%s] server start. conn_fd: %d\n", routine_id(), __func__, conn_fd);

    for (;;) {
        printf("routine[%d][%s] loop start. conn_fd: %d\n", routine_id(), __func__, conn_fd);
        char buf[512] = { 0 };
        int n = 0;

        // 读取这条连接的数据
        n = routine_read(conn_fd, buf, sizeof(buf));
        if (n < 0) {
            perror("server read error.");
            break;
        }
        buf[n] = '\0';
        printf("routine[%d][%s] server read: %s", routine_id(), __func__, buf);

        unsigned int in_ts = time(NULL);
        // 休眠一秒，切换出去执行
        routine_sleep(1);
        unsigned int out_ts = time(NULL);

        char obuf[512] = { 0 };
        snprintf(obuf, sizeof(obuf), "%s rev_ts:%u sent_ts:%u\n", buf, in_ts, out_ts);
        printf("routine[%d][%s] server write: %s", routine_id(), __func__, obuf);

        // 返回数据
        n = routine_write(conn_fd, obuf, strlen(obuf) + 1);
        if (n < 0) {
            perror("server write error.");
            break;
        }
    }
    printf("routine[%d][%s] server start. conn_fd: %d\n", routine_id(), __func__, conn_fd);
}

/// 接收socket请求
void request_accept() {
    // 死循环，不停的接收连接请求
    for (;;) {
        //
        struct sockaddr_in addr = { 0 };
        socklen_t slen = sizeof(addr);
        int fd = accept(routinecenter.routines[routine_id()].wait_fd, (struct sockaddr*)&addr, &slen);
        struct sockaddr_in peer = { 0 };
        int ret = getpeername(fd, (struct sockaddr*)&peer, &slen);
        if (ret < 0) {
            perror("getpeername error.");
            exit(-1);
        }
        printf("routine[%d][%s] accept from %s conn_fd:%d\n", routine_id(), __func__, inet_ntoa(peer.sin_addr), fd);
        set_nonblocking(fd);
        // 为每个接收到的请求创建一个协程
        int rid = create(echo_server_routine);
        routinecenter.routines[rid].wait_fd = fd;

        // 为fd添加一个读事件
        mod_event(fd, EPOLLIN, EPOLL_CTL_ADD, rid);

        // 启动协程的执行
        resume(rid);

        // 切换协程
        yield();
    }
}

void bind_listen(unsigned short port) {
    /*
    •	AF_INET: 指定使用 IPv4 地址族。
    •	SOCK_STREAM: 表示创建一个面向连接的流式套接字（即 TCP 套接字）。
    •	0: 通常表示选择默认的协议（TCP 协议对应 IPPROTO_TCP，这里的 0 表示让系统自动选择）。
    */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    /*
    •	设置地址族为 AF_INET，表示使用 IPv4。
    •	设置端口号。htons(port) 将主机字节序转换为网络字节序（大端序），以确保在网络上传输时端口号的正确性。
    •	设置 IP 地址为 INADDR_ANY，表示服务器将绑定到所有可用的网络接口上（通常是 0.0.0.0）。
    */
    struct sockaddr_in addr = { 0 };
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    int ret = bind(listen_fd, (struct sockaddr*)&addr, sizeof(struct sockaddr));
    if (ret < 0) {
        perror("bind fail.(绑定 ip 端口失败)");
        exit(-1);
    }
    // 监听刚刚的 socket
    ret = listen(listen_fd, 20);
    if (ret < 0) {
        perror("listen fail.(无法监听)");
        exit(-1);
    }
    // 输出当前正在运行的 id 和端口
    printf("routine[%d] listen bind at port: %u\n", routine_id(), port);
    // 将socket设置为异步读写
    set_nonblocking(listen_fd);
    // 创建一个协程来处理这个端口收到的请求
    int rid = create(request_accept);
    // 将socket的文件描述符
    // 动作：EPOLL_CTL_ADD 添加eopll事件
    // 添加的事件类型：EPOLLIN，表示该文件描述符上有数据可读
    mod_event(listen_fd, EPOLLIN, EPOLL_CTL_ADD, rid);
}

int main() {
    init();

    // 创建一个epoll实例
    create_routine_poll();

    // 监听三个socket端口
    bind_listen(55667);
    bind_listen(55668);
    bind_listen(55669);

    // 将创建的协程 id 加入到执行列表中
    routine_delay_resume(create([]() { printf("routine[%d] alarm fired\n", routine_id()); }), 3);

    // 开始遍历执行列表,
    routine_poll();

    puts("all routine exit");

    return 0;
}
