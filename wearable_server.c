/**
 * Machine Problem: Wearables
 * CS 241 - Spring 2016
 */
 
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <pthread.h>

#include "queue.h"

const char *TYPE1 = "heart_beat";
const char *TYPE2 = "blood_sugar";
const char *TYPE3 = "body_temp";

/* The wearable server socket, which all wearables connect to. */
int wearable_server_fd;

/* A lock for your queue. */
pthread_mutex_t queue_lock_;
pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
pthread_t request_thread;
/* A queue for all received data. */
queue_t receieved_data_;

typedef struct SampleData 
{
  char type_[50];
  int data_;
} SampleData;

int compare(const void *a, const void *b) { return (*(int *)a - *(int *)b); }

int select1(void *data) {
  return strcmp(((SampleData *)data)->type_, TYPE1) == 0;
}

int select2(void *data) {
  return strcmp(((SampleData *)data)->type_, TYPE2) == 0;
}

int select3(void *data) {
  return strcmp(((SampleData *)data)->type_, TYPE3) == 0;
}

int selectorall(void *__attribute__((unused)) val) { return 1; }

typedef struct {
  pthread_t thread;
  int fd;
  long timestamp;
  
  int donestatus;
  int mainid;
  /* TODO you might want to put more things here */
} thread_data;

thread_data **wearable_threads;
int wearable_threads_size = 0;

/**
 *  * Used to write out the statistics of a given results set (of
 *   * timestamp_entry's).  To generate the result set see queue_gather(). fd is the
 *    * file descriptor to which the information is sent out. The type is the type of
 *     * data that is written out (TYPE1, TYPE2, TYPE3). results is the array of
 *      * timestamp_entrys, and size is the size of that array. NOTE: that you should
 *       * call method for every type (TYPE1, TYPE2, TYPE3), and then write out the
 *        * infomration "\r\n" to signify that you have finished sending out the results.
 *         */
void write_results(int fd, const char *type, timestamp_entry *results,
                   int size) {
  long avg = 0;
  int i;

  char buffer[1024];
  int temp_array[size];
  sprintf(buffer, "Results for %s:\n", type);
  sprintf(buffer + strlen(buffer), "Size:%i\n", size);
  for (i = 0; i < size; i++) {
    temp_array[i] = ((SampleData *)(results[i].data_))->data_;
    avg += ((SampleData *)(results[i].data_))->data_;
  }

  qsort(temp_array, size, sizeof(int), compare);

  if (size != 0) {
    sprintf(buffer + strlen(buffer), "Median:%i\n",
            (size % 2 == 0)
                ? (temp_array[size / 2] + temp_array[size / 2 - 1]) / 2
                : temp_array[size / 2]);
  } else {
    sprintf(buffer + strlen(buffer), "Median:0\n");
  }

  sprintf(buffer + strlen(buffer), "Average:%li\n\n",
          (size == 0 ? 0 : avg / size));
  write(fd, buffer, strlen(buffer));
}

/**
 *  * Given an input line in the form <timestamp>:<value>:<type>, this method
 *   * parses the infomration from the string, into the given timestamp, and mallocs
 *    * space for SampleData, and stores the type and value within
 *     */
void extract_key(char *line, long *timestamp, SampleData **ret) {
  *ret = malloc(sizeof(SampleData));
  sscanf(line, "%zu:%i:%s\n", timestamp, &((*ret)->data_), (*ret)->type_);
  /* eat the trailing ":" */
  (*ret)->type_[strlen((*ret)->type_) - 1] = '\0';
}

void *wearable_processor_thread(void *args) 
{
  thread_data *td = (thread_data *)args;
  int socketfd = td->fd;
  int mymainid = td->mainid;

  char buffer[64];
  while(read(socketfd, buffer, 64) > 0)
  {
    long mytimestamp = 0;
    SampleData *myretval;
    extract_key(buffer, &mytimestamp, &myretval);

    pthread_mutex_lock(&queue_lock_); 
    wearable_threads[mymainid]->timestamp = mytimestamp;  
    queue_insert(&receieved_data_, wearable_threads[mymainid]->timestamp, myretval);
    pthread_mutex_unlock(&queue_lock_);

    pthread_cond_broadcast(&cv);
  }
  pthread_mutex_lock(&queue_lock_);
  //set that thread to done
  wearable_threads[mymainid]->donestatus = 1;
  pthread_mutex_unlock(&queue_lock_);
  pthread_cond_broadcast(&cv);
  // Use a buffer of length 64!
  // TODO read data from the socket until -1 is returned by read
  // char buffer[64];
  // while (read(socketfd, buffer, 64) > 0) ... // or do you need recv???
  close(socketfd);
  return NULL;
}

void *user_request_thread(void *args) 
{
  int socketfd = *((int *)args);

  // TODO read data from the socket until -1 is returned by read
  // Requests will be in the form
  //<timestamp1>:<timestamp2>, then write out statistics for data between
  // those timestamp ranges
  char buffer[64];
  memset(&buffer[0], 0, sizeof(buffer));
  while(read(socketfd, buffer, 64) > 0)
  {
    long startTime = 0, endTime = 0;
    sscanf(buffer, "%zu:%zu", &startTime, &endTime);
    pthread_mutex_lock(&queue_lock_);
    int wflag;
    while(1)
    {
      wflag = 0;
      for(int i = 0; i < wearable_threads_size; i++)
      {
        if((wearable_threads[i]->timestamp < endTime) && (wearable_threads[i]->donestatus == 0))
        {
          wflag = 1;
          break;
        }
      }
      if(wflag == 0)
        break;

      pthread_cond_wait(&cv, &queue_lock_);
    }   

    int a;
    int b; 
    int c;

    timestamp_entry* entry1 = queue_gather(&receieved_data_, startTime, endTime, select1, &a);
    timestamp_entry* entry2 = queue_gather(&receieved_data_, startTime, endTime, select2, &b);
    timestamp_entry* entry3 = queue_gather(&receieved_data_, startTime, endTime, select3, &c);
    pthread_mutex_unlock(&queue_lock_);

    write_results(socketfd, TYPE1, entry1, a);
    write_results(socketfd, TYPE2, entry2, b);
    write_results(socketfd, TYPE3, entry3, c);
    write(socketfd, "\r\n", strlen("\r\n"));
    free(entry1); 
    entry1 = NULL;
    free(entry2); 
    entry2 = NULL;
    free(entry3); 
    entry3 = NULL;
 }
 //values < endTime && notDone, then wait
 close(socketfd);
 return NULL;
}

/* IMPLEMENT!
 *  given a string with the port value, set up a
 *   serversocket file descriptor and return it */
int open_server_socket(const char *port) 
{
  /* TODO */
  int s;
  int serversocket = socket(AF_INET, SOCK_STREAM, 0);  //local variable maybe
  if(serversocket==-1)
  {
    fprintf(stderr, "Error: %d\n", errno);
    exit(1);
  }

  struct addrinfo hints, *result;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  s = getaddrinfo(NULL, port, &hints, &result);
  if (s != 0) 
  {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
    exit(1);
  }
  int myval = 1;
  setsockopt(serversocket, SOL_SOCKET, SO_REUSEPORT, &myval, sizeof(myval));

  if (bind(serversocket, result->ai_addr, result->ai_addrlen) != 0) 
  {
      perror("bind()");
      exit(1);
  }

  if (listen(serversocket, 70) != 0) 
  {
      perror("listen()");
      exit(1);
  }
  free(result);
  return serversocket;
}

void signal_received(int sig) 
{
  /* TODO close server socket, free anything you don't free in main*/
  for(int i=0;i<wearable_threads_size;i++)
    pthread_join(wearable_threads[i]->thread, NULL);

  pthread_join(request_thread, NULL);

  for(int i=0;i<wearable_threads_size;i++)
    free(wearable_threads[i]);

  free(wearable_threads);
  queue_destroy(&receieved_data_, 1);
  close(wearable_server_fd);
  //pthread_mutex_destroy(&queue_lock_);
  exit(0);
}


int main(int argc, const char *argv[]) 
{
  signal(SIGINT, signal_received);
  if (argc != 3) {
    printf("Invalid input size\n");
    exit(EXIT_FAILURE);
  }
  // TODO setup sig handler for SIGINT
  pthread_cond_init(&cv, NULL);
  wearable_threads = (thread_data**) calloc(sizeof(thread_data*), 3000);
  wearable_threads_size = 0;

  int request_server_fd = open_server_socket(argv[2]);
  wearable_server_fd = open_server_socket(argv[1]);

  int request_socket = accept(request_server_fd, NULL, NULL);
  pthread_create(&request_thread, NULL, user_request_thread, &request_socket);
  close(request_server_fd);

  queue_init(&receieved_data_);
  pthread_mutex_init(&queue_lock_, NULL);
  // TODO accept continous requests
  // TODO join all threads we spawned from the wearables
  int client_fd;
  while(1)
  {
    client_fd = accept(wearable_server_fd, NULL, NULL);
    if(client_fd==-1)
    {
      break;
    }
    wearable_threads[wearable_threads_size] = calloc(sizeof(thread_data), 1); 
    wearable_threads[wearable_threads_size]->fd = client_fd;
    wearable_threads[wearable_threads_size]->mainid = wearable_threads_size;
    wearable_threads[wearable_threads_size]->timestamp = 0;
    wearable_threads[wearable_threads_size]->donestatus = 0;
    pthread_create(&(wearable_threads[wearable_threads_size]->thread), NULL, wearable_processor_thread, wearable_threads[wearable_threads_size]);
    wearable_threads_size++;
  }
  queue_destroy(&receieved_data_, 1);
  for(int i = 0; i < wearable_threads_size; i++)
    free(wearable_threads[i]);

  free(wearable_threads);
  return 0;
}
