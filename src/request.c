#include "io_helper.h"
#include "request.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#define MAXBUF (8192)

// Request item structure
typedef struct {
    int fd;
    char filename[MAXBUF];
    int filesize;
} request_t;

// Shared buffer and synchronization primitives
typedef struct {
    request_t *requests;
    int start;
    int end;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} request_buffer_t;

request_buffer_t buffer;

int num_threads = DEFAULT_THREADS;
int buffer_max_size = DEFAULT_BUFFER_SIZE;
int scheduling_algo = DEFAULT_SCHED_ALGO;

void buffer_init(int size) {
    buffer.requests = malloc(sizeof(request_t) * size);
    buffer.start = 0;
    buffer.end = 0;
    buffer.count = 0;
    pthread_mutex_init(&buffer.lock, NULL);
    pthread_cond_init(&buffer.not_empty, NULL);
    pthread_cond_init(&buffer.not_full, NULL);
}

void buffer_insert(int fd, char *filename, int filesize) {
    pthread_mutex_lock(&buffer.lock);
    while (buffer.count == buffer_max_size) {
        pthread_cond_wait(&buffer.not_full, &buffer.lock);
    }
    buffer.requests[buffer.end].fd = fd;
    strcpy(buffer.requests[buffer.end].filename, filename);
    buffer.requests[buffer.end].filesize = filesize;
    buffer.end = (buffer.end + 1) % buffer_max_size;
    buffer.count++;
    pthread_cond_signal(&buffer.not_empty);
    pthread_mutex_unlock(&buffer.lock);
}

request_t buffer_remove() {
    pthread_mutex_lock(&buffer.lock);
    while (buffer.count == 0) {
        pthread_cond_wait(&buffer.not_empty, &buffer.lock);
    }

    int index = buffer.start;
    if (scheduling_algo == 1) { // SFF
        for (int i = 1; i < buffer.count; ++i) {
            int idx = (buffer.start + i) % buffer_max_size;
            if (buffer.requests[idx].filesize < buffer.requests[index].filesize) {
                index = idx;
            }
        }
    } else if (scheduling_algo == 2) { // RANDOM
        int r = rand() % buffer.count;
        index = (buffer.start + r) % buffer_max_size;
    }

    request_t req = buffer.requests[index];
    for (int i = index; i != buffer.end; i = (i + 1) % buffer_max_size) {
        int next = (i + 1) % buffer_max_size;
        buffer.requests[i] = buffer.requests[next];
    }
    buffer.end = (buffer.end - 1 + buffer_max_size) % buffer_max_size;
    buffer.count--;
    pthread_cond_signal(&buffer.not_full);
    pthread_mutex_unlock(&buffer.lock);
    return req;
}

void request_error(int fd, char *cause, char *errnum, char *shortmsg, char *longmsg) {
    char buf[MAXBUF], body[MAXBUF];

    sprintf(body, ""
        "<!doctype html>\r\n"
        "<head>\r\n"
        "  <title>CYB-3053 WebServer Error</title>\r\n"
        "</head>\r\n"
        "<body>\r\n"
        "  <h2>%s: %s</h2>\r\n"
        "  <p>%s: %s</p>\r\n"
        "</body>\r\n"
        "</html>\r\n", errnum, shortmsg, longmsg, cause);

    sprintf(buf, "HTTP/1.0 %s %s\r\n", errnum, shortmsg);
    write_or_die(fd, buf, strlen(buf));
    sprintf(buf, "Content-Type: text/html\r\n");
    write_or_die(fd, buf, strlen(buf));
    sprintf(buf, "Content-Length: %lu\r\n\r\n", strlen(body));
    write_or_die(fd, buf, strlen(buf));
    write_or_die(fd, body, strlen(body));
    close_or_die(fd);
}

void request_read_headers(int fd) {
    char buf[MAXBUF];
    readline_or_die(fd, buf, MAXBUF);
    while (strcmp(buf, "\r\n")) {
        readline_or_die(fd, buf, MAXBUF);
    }
}

int request_parse_uri(char *uri, char *filename, char *cgiargs) {
    char *ptr;
    if (!strstr(uri, "cgi")) {
        strcpy(cgiargs, "");
        sprintf(filename, ".%s", uri);
        if (uri[strlen(uri)-1] == '/') {
            strcat(filename, "index.html");
        }
        return 1;
    } else {
        ptr = index(uri, '?');
        if (ptr) {
            strcpy(cgiargs, ptr+1);
            *ptr = '\0';
        } else {
            strcpy(cgiargs, "");
        }
        sprintf(filename, ".%s", uri);
        return 0;
    }
}

void request_get_filetype(char *filename, char *filetype) {
    if (strstr(filename, ".html")) 
        strcpy(filetype, "text/html");
    else if (strstr(filename, ".gif")) 
        strcpy(filetype, "image/gif");
    else if (strstr(filename, ".jpg")) 
        strcpy(filetype, "image/jpeg");
    else 
        strcpy(filetype, "text/plain");
}

void request_serve_static(int fd, char *filename, int filesize) {
    int srcfd;
    char *srcp, filetype[MAXBUF], buf[MAXBUF];

    request_get_filetype(filename, filetype);
    srcfd = open_or_die(filename, O_RDONLY, 0);
    srcp = mmap_or_die(0, filesize, PROT_READ, MAP_PRIVATE, srcfd, 0);
    close_or_die(srcfd);

    sprintf(buf, ""
        "HTTP/1.0 200 OK\r\n"
        "Server: OSTEP WebServer\r\n"
        "Content-Length: %d\r\n"
        "Content-Type: %s\r\n\r\n",
        filesize, filetype);
    write_or_die(fd, buf, strlen(buf));
    write_or_die(fd, srcp, filesize);
    munmap_or_die(srcp, filesize);
}

void* thread_request_serve_static(void* arg) {
    while (1) {
        request_t req = buffer_remove();
        request_serve_static(req.fd, req.filename, req.filesize);
        close_or_die(req.fd);
    }
    return NULL;
}

void request_handle(int fd) {
    int is_static;
    struct stat sbuf;
    char buf[MAXBUF], method[MAXBUF], uri[MAXBUF], version[MAXBUF];
    char filename[MAXBUF], cgiargs[MAXBUF];

    readline_or_die(fd, buf, MAXBUF);
    sscanf(buf, "%s %s %s", method, uri, version);
    printf("method:%s uri:%s version:%s\n", method, uri, version);

    if (strcasecmp(method, "GET")) {
        request_error(fd, method, "501", "Not Implemented", "server does not implement this method");
        return;
    }
    request_read_headers(fd);

    is_static = request_parse_uri(uri, filename, cgiargs);

    if (strstr(filename, "..") || filename[0] != '.') {
        request_error(fd, filename, "403", "Forbidden", "directory traversal attempt detected");
        return;
    }

    if (stat(filename, &sbuf) < 0) {
        request_error(fd, filename, "404", "Not found", "server could not find this file");
        return;
    }

    if (is_static) {
        if (!(S_ISREG(sbuf.st_mode)) || !(S_IRUSR & sbuf.st_mode)) {
            request_error(fd, filename, "403", "Forbidden", "server could not read this file");
            return;
        }
        buffer_insert(fd, filename, sbuf.st_size);
    } else {
        request_error(fd, filename, "501", "Not Implemented", "server does not serve dynamic content request");
    }
}
