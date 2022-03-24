#include "channel.h"

/* 
 * This implementation uses non-blocking calls as the base level, the calls that do the actual sending/recieveing  
 *   Blocking calls simply call the non-blocking version. If the channel is full, they join a 'queue,' enforced 
 *   by a semaphore of size 1, before trying another non-blocking call.
 * 
 * Some things to note
 * 
 *      - I will likely replace the channel lock with a semaphore of size 1, so when multiple threads are waiting,
 *        they will be processed FIFO instead of whichever thread randomly grabs the lock
 * 
 *      - I tried to combine the sending queue and recieving queue into one single semaphore, as the sending queue
 *        is only used when the channel is full, and recieve when the channel is empty. However, this broke my code.
 *        I may try again later if I have time, or break down why it doesn't work.
 * 
 *      - The use of while loops in the blocking functions catch one possible edge condition:
 *        if the user sends a non-blocking call, it can take the spot of a blocking call after 
 *        it leaves the queue, so it needs to go back into the queue. I never tested if this comes
 *        up in practice, I might if I'm bored later.
 * 
 * All this is subject to change when I implement select, and possibly unbuffered.
 */

#define DEBUG



// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    channel_t * new_channel = malloc(sizeof(channel_t));

    new_channel->buffer = buffer_create(size);
    pthread_mutex_init(&(new_channel->lock), NULL);
    new_channel->recv_queue = list_create();
    new_channel->send_queue = list_create();
    new_channel->closed = false;

    #ifdef DEBUG

    printf("\nChannel Created\n");

    #endif

    return new_channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    request_t * send_request;
    enum channel_status ret = channel_non_blocking_send(channel, data);

    #ifdef DEBUG
    printf("\nCHANNEL SEND: Initial Attempt\n ret: %i\n", ret);
    print_channel(channel); 
    #endif

    if(ret == CHANNEL_FULL){
        send_request = init_request();
        
        pthread_mutex_lock(&(channel->lock));
        
        queue_add(channel->send_queue, send_request);

        #ifdef DEBUG
        printf("\nCHANNEL SEND: Requested\n Request:       %lx\n Sem:           %lx\n", (u_long)send_request, (u_long)&send_request->sem);
        print_channel(channel);  
        #endif

        pthread_mutex_unlock(&(channel->lock));

        sem_wait( &(send_request->sem) );

        ret = channel_non_blocking_send(channel, data);

        destroy_request(send_request);

        #ifdef DEBUG
        printf("\nCHANNEL SEND: Request answered\n");
        #endif
    }
    
    #ifdef DEBUG
    printf("\nCHANNEL SEND: Exit\n ret: %i\n\n", ret);
    print_channel(channel); 
    #endif

    return ret;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    request_t * recv_request;
    enum channel_status ret = channel_non_blocking_receive(channel, data);

    #ifdef DEBUG
    printf("\nCHANNEL RECV: Initial Attempt\n ret: %i\n", ret);
    print_channel(channel); 
    #endif

    if(ret == CHANNEL_EMPTY){ 
        recv_request = init_request();

        pthread_mutex_lock(&(channel->lock));
        
        queue_add(channel->recv_queue, recv_request);

        #ifdef DEBUG
        printf("\nCHANNEL RECV: Requested\n Request:       %lx\n Sem:           %lx\n", (u_long)recv_request, (u_long)&recv_request->sem);
        print_channel(channel); 
        #endif
        
        pthread_mutex_unlock(&(channel->lock));
        
        sem_wait(&(recv_request->sem));
        
        ret = channel_non_blocking_receive(channel, data);

        destroy_request(recv_request);

        #ifdef DEBUG
        printf("\nCHANNEL RECV: Request answered\n");
        #endif
    }

    #ifdef DEBUG
    printf("\nCHANNEL RECV: Exit\n ret: %i\n\n", ret);
    print_channel(channel); 
    #endif

    return ret;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    enum channel_status ret;

    #ifdef DEBUG
    printf("\nCHANNEL NB SEND: Begin\n");
    print_channel(channel);
    #endif
    
    pthread_mutex_lock(&(channel->lock));
    ret = channel_unsafe_send(channel, data);
    pthread_mutex_unlock(&(channel->lock));

    #ifdef DEBUG
    printf("\nCHANNEL NB SEND: Exit\n ret: %i\n", ret);
    print_channel(channel);
    #endif

    return ret;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    enum channel_status ret;

    #ifdef DEBUG
    print_channel(channel);
    printf("\nCHANNEL NB RECV: Begin\n");
    #endif
    
    pthread_mutex_lock(&(channel->lock));
    ret = channel_unsafe_receive(channel, data);
    pthread_mutex_unlock(&(channel->lock));

    #ifdef DEBUG
    print_channel(channel);
    printf("\nCHANNEL NB RECV: Exit\n ret: %i\n", ret);
    #endif

    return ret;
}

////////////////////////////////////////////////////////////////////////////////
// channel_unsafe_send()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
enum channel_status channel_unsafe_send(channel_t* channel, void* data){
    
    if (channel->closed){
        // Sem_post on closed to empty the queue
        if(channel->send_queue->count){
            queue_serve(channel->send_queue);
        }
        return CLOSED_ERROR;
    }
    else if (buffer_full(channel->buffer)){
        return CHANNEL_FULL;
    }
    else{
        buffer_add(channel->buffer, data);
        // If there is queued blocking calls, after this receive the 
        //  buffer won't be empty and a recieve can execute
        if(channel->recv_queue->count){

            #ifdef DEBUG
            print_channel(channel);
            printf("\nCHANNEL UNSAFE SEND: recv queue_serve() before\n");
            #endif

            queue_serve(channel->recv_queue);

            #ifdef DEBUG
            print_channel(channel);
            printf("\nCHANNEL UNSAFE SEND: recv queue_serve() after\n");
            #endif
        }

        return SUCCESS;
    }
}

////////////////////////////////////////////////////////////////////////////////
// channel_unsafe_receive()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
enum channel_status channel_unsafe_receive(channel_t* channel, void** data){
    
    if (channel->closed){
        // Sem_post on closed to empty the queue
        if(channel->recv_queue->count){
            queue_serve(channel->recv_queue);
        }
        return CLOSED_ERROR;
    }
    else if (buffer_empty(channel->buffer)){
        return CHANNEL_EMPTY;
    }
    else{
        buffer_remove(channel->buffer, data);
        // If there is queued blocking calls, after this send the 
        //  buffer won't be full and a send can execute
        if(channel->send_queue->count){
            
            #ifdef DEBUG
            print_channel(channel); printf("\nCHANNEL UNSAFE RECV: send queue_serve() before\n");
            #endif
            
            queue_serve(channel->send_queue);
        
            #ifdef DEBUG
            print_channel(channel);  printf("\nCHANNEL UNSAFE RECV: send queue_serve() after\n");
            #endif
    
        
        }
        return SUCCESS;
    }
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    // Since all send/recieve go thru the non-blocking ops, and all non-blocking ops hold the lock until they return,
    //  nothing will send/recieve after close is set.
    pthread_mutex_lock(&(channel->lock));
    if(!channel->closed){
        channel->closed = true;
        // By doing a sem_post for each semiphore, both queues wills start emptying,
        //  all returning CLOSED_ERROR
        if(channel->recv_queue->count){
            queue_serve(channel->recv_queue);
        }
        if(channel->send_queue->count){
            queue_serve(channel->send_queue);
        }

        pthread_mutex_unlock(&(channel->lock));
        return SUCCESS;
    }
    else {
        pthread_mutex_unlock(&(channel->lock));
        return CLOSED_ERROR;
    }
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    pthread_mutex_lock(&(channel->lock));
    if(channel->closed){

        buffer_free(channel->buffer);
        list_destroy(channel->recv_queue);
        list_destroy(channel->send_queue);
        pthread_mutex_unlock(&(channel->lock));
        pthread_mutex_destroy(&(channel->lock));
        free(channel);
        
        return SUCCESS;
    } 
    else {
        pthread_mutex_unlock(&(channel->lock));
        return DESTROY_ERROR;
    }
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it SELECTS THE FIRST OPTION and performs its corresponding action 
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////
// init_request()
// 
request_t * init_request(void){

    request_t * new_request = malloc( sizeof(request_t) );

    sem_init( &(new_request->sem), 0, 0);

    return new_request;
}

////////////////////////////////////////////////////////////////////////////////
// destroy_request()
// 
void destroy_request(request_t * request){

    sem_destroy( &(request->sem) );
    free(request);
}

////////////////////////////////////////////////////////////////////////////////
// queue_add()
// 
void queue_add(list_t * queue, request_t * request){

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

////////////////////////////////////////////////////////////////////////////////
// queue_serve()
// 
void queue_serve(list_t * queue){

    queue_entry_t * entry = queue_next(queue);

    if (entry != NULL){

        sem_post( &(entry->request->sem) );

        queue_remove(queue, entry);
    }

}

//////////////////////////////////////////
// queue_next()
// 
queue_entry_t * queue_next(list_t * queue){
    return queue->head == NULL ? NULL : (queue_entry_t *)queue->head->data;
}

////////////////////////////////////////////////////////////////////////////////
// queue_remove()
// 
void queue_remove(list_t * queue, queue_entry_t * entry){

    // if(0){
    //     destroy_request(entry->request);
    // }

    list_remove(queue, list_find(queue, entry));
}

void print_channel(channel_t * channel){
    list_node_t * node;
    queue_entry_t * entry;
    request_t * request;
    int i;
    
    printf("\n\n************CHANNEL INFORMATION************\n");
    
    // Print buffer information
    printf("\n--BUFFER INFO--\n Size:          %lu\n Capacity:      %lu\n", channel->buffer->size, channel->buffer->capacity);

    // Print send queue information
    printf("\n--SEND QUEUE--\n Count: %lu\n", channel->send_queue->count);
    i = 0;
    node = channel->send_queue->head;
    while(node != NULL){
        printf("NODE %i\n", i);
        printf(" Location:      %lx", (u_long)node);
        printf("ENTRY\n");
        entry = (queue_entry_t *)node->data;
        printf(" Location:      %lx\n", (u_long)entry);
        printf(" Request:       %lx\n", (u_long)entry->request);
        
        request = entry->request;
        printf("REQUEST\n");

        printf(" Location:      %lx\n", (u_long)request);
        printf(" Sem:           %lx\n", (u_long)&request->sem);
        i++;
        node = node->next;
    }

    // Print receive queue information
    printf("\n--RECV QUEUE--\n Count: %lu\n", channel->recv_queue->count);
    i = 0;
    node = channel->recv_queue->head;
    while(node != NULL){
        printf("NODE %i\n", i);
        printf(" Location:      %lx", (u_long)node);
        printf("ENTRY\n");
        entry = (queue_entry_t *)node->data;
        printf(" Location:      %lx\n", (u_long)entry);
        printf(" Request:       %lx\n", (u_long)entry->request);
        
        request = entry->request;
        printf("REQUEST\n");

        printf(" Location:      %lx\n", (u_long)request);
        printf(" Sem:           %lx\n", (u_long)&request->sem);
        i++;
        node = node->next;
    }
    printf("\n*******************************************\n\n");

}




