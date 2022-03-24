#ifndef CHANNEL_H
#define CHANNEL_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include "buffer.h"
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include "linked_list.h"

// Defines possible return values from channel functions
enum channel_status {
    CHANNEL_EMPTY = 0,
    CHANNEL_FULL = 0,
    SUCCESS = 1,
    CLOSED_ERROR = -2,
    GEN_ERROR = -1,
    DESTROY_ERROR = -3
};

// Defines channel object
typedef struct {
    // DO NOT REMOVE buffer (OR CHANGE ITS NAME) FROM THE STRUCT
    // YOU MUST USE buffer TO STORE YOUR BUFFERED CHANNEL MESSAGES
    buffer_t*           buffer;

    //
    pthread_mutex_t     lock;  
    list_t            * recv_queue;     // Tracks how many blocking recieve calls are queued    
    list_t            * send_queue;     // Tracks how many blocking send calls are queued
    bool                closed; 
} channel_t;

// Defines channel list structure for channel_select function
enum direction {
    SEND,
    RECV,
};
typedef struct {
    // Channel on which we want to perform operation
    channel_t* channel;
    // Specifies whether we want to receive (RECV) or send (SEND) on the channel
    enum direction dir;
    // If dir is RECV, then the message received from the channel is stored as an output in this parameter, data
    // If dir is SEND, then the message that needs to be sent is given as input in this parameter, data
    void* data;
} select_t;

typedef struct {
    sem_t       sem;
} request_t;

typedef struct {
    request_t   * request;
} queue_entry_t;

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size);

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t* channel, void* data);

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data);

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data);

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data);

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel);

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel);

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index);

//////////////////////////////////////////
// NOT THREAD SAFE FUNCTIONS

enum channel_status channel_unsafe_send(channel_t* channel, void* data);

enum channel_status channel_unsafe_receive(channel_t* channel, void** data);

//////////////////////////////////////////
// INLINE HELPER FUNCTIONS

//////////////////////////////////////////
// buffer_full()
// returns true if the buffer is full
static inline bool buffer_full(buffer_t* buffer){
    return (buffer->size == buffer->capacity);
}

//////////////////////////////////////////
// buffer_empty()
// returns true if the buffer is full
static inline bool buffer_empty(buffer_t* buffer){
    return (0 == buffer->size);
}

//////////////////////////////////////////
// init_request()
// 
static inline request_t * init_request(void){

    request_t * new_request = malloc( sizeof(request_t) );

    sem_init( &(new_request->sem), 0, 0);

    return new_request;
}

//////////////////////////////////////////
// init_request()
// 
static inline void destroy_request(request_t * request){

    sem_destroy( &(request->sem) );
    free(request);
}

//////////////////////////////////////////
// queue_add()
// 
static inline void queue_add(list_t * queue, request_t * request){

    queue_entry_t * new_entry = malloc( sizeof(queue_entry_t) );

    new_entry->request = request;

    list_insert(queue, new_entry);

    #ifdef DEBUG
    printf("\nQUEUE ADD\n Queue:         %lx\n", (u_long)queue);
    printf(" Request:       %lx\n", (u_long)request);
    printf(" New Entry:     %lx\n", (u_long)new_entry);
    printf(" NE_req:        %lx\n", (u_long)new_entry->request);
    #endif
}

//////////////////////////////////////////
// queue_next()
// 
static inline queue_entry_t * queue_next(list_t * queue){
    return queue->head == NULL ? NULL : (queue_entry_t *)queue->head->data;
}

//////////////////////////////////////////
// queue_remove()
// 
static inline void queue_remove(list_t * queue, queue_entry_t * entry){

    // if(0){
    //     destroy_request(entry->request);
    // }

    list_remove(queue, list_find(queue, entry));
}

//////////////////////////////////////////
// queue_serve()
// 
static inline void queue_serve(list_t * queue){

    queue_entry_t * entry = queue_next(queue);

    if (entry != NULL){

        sem_post( &(entry->request->sem) );

        queue_remove(queue, entry);
    }

}


void print_channel(channel_t * channel);


#endif // CHANNEL_H
