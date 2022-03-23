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
 * 
 * To implement the select, I first rewrote the blocking queues to be a linked list format.  I then allowed select calls
 *   to add very similar 'service requests' to each channel in the list.
 * 
 * All service requests have two important values: VALID and INSTANCES. VALID is a bool that refers to if the request has
 *   been fufilled or not, so no two channels try to fufill the same requests. INSTANCES refer to how many lists the service
 *   request is present in.  Whenever a channel processes a request, whether it is valid and is served, or is invalid and 
 *   ignored, it decrements the instances value before removing it from the list. This is all to avoid the messy process 
 *   of having the select call clean up the requests it no longer needs, allowing the select call to return and its mess to
 *   be cleaned up by the channels.
 * 
 * The regular blocking calls use the same service request struct, but always have an index of -1 and only 1 instance.
 * 
 * 
 */

#define DEBUG

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    channel_t * new_channel = malloc(sizeof(channel_t));

    new_channel->buffer         = buffer_create(size);
    new_channel->send_queue     = list_create();
    new_channel->recv_queue     = list_create();
    new_channel->closed         = false;
    pthread_mutex_init(&(new_channel->lock), NULL);
    pthread_mutex_init(&(new_channel->send_queue_lock), NULL);
    pthread_mutex_init(&(new_channel->recv_queue_lock), NULL);
    

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
    
    enum channel_status ret = channel_non_blocking_send(channel, data);
    
    while (ret == CHANNEL_FULL){
        pthread_mutex_lock(&(channel->lock));
        // If channel is full, add this send request to the queue.
        service_request_t * send_request = init_service_request(SEND, data, NULL, &ret);

        pthread_mutex_lock(&(channel->send_queue_lock));

        queue_add(channel->send_queue, send_request, -1);

        #ifdef DEBUG
        printf("channel_send() added to queue\n");
        print_channel_status(channel);
        #endif

        pthread_mutex_unlock(&(channel->send_queue_lock));
        pthread_mutex_unlock(&(channel->lock));
        
        sem_wait(&(send_request->sem));

        ret = channel_non_blocking_send(channel, data);

        #ifdef DEBUG
        printf("channel_send() after sem wait\n, ret: %i", ret);
        print_channel_status(channel);
        #endif
    }

    #ifdef DEBUG
    if (ret == CHANNEL_FULL) {
        printf("Blocking send returning channel full!\n");
    }
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
    
    enum channel_status ret = channel_non_blocking_receive(channel, data);
    
    while (ret == CHANNEL_EMPTY){
        pthread_mutex_lock(&(channel->lock));
        // If channel is full, add this send request to the queue.
        service_request_t * recv_request = init_service_request(RECV, data, NULL, &ret);

        pthread_mutex_lock(&(channel->recv_queue_lock));

        queue_add(channel->send_queue, recv_request, -1);

        pthread_mutex_unlock(&(channel->recv_queue_lock));
        pthread_mutex_unlock(&(channel->lock));

        #ifdef DEBUG
        printf("channel_receive() added to queue\n");
        print_channel_status(channel);
        #endif
        
        sem_wait(&(recv_request->sem));

        ret = channel_non_blocking_receive(channel, data);

        #ifdef DEBUG
        printf("channel_receive() after sem wait\n, ret: %i", ret);
        print_channel_status(channel);
        #endif
    }

    #ifdef DEBUG
    if (ret == CHANNEL_EMPTY) {
        printf("Blocking send returning channel empty!\n");
    }
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
    
    pthread_mutex_lock(&(channel->lock));
    ret = channel_unsafe_send(channel, data);


    //  buffer won't be empty and a recieve can execute
    if(ret == SUCCESS && channel->recv_queue->count)
    {
        serve_request(channel, channel->recv_queue);
    }

    pthread_mutex_unlock(&(channel->lock));

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
    
    pthread_mutex_lock(&(channel->lock));
    ret = channel_unsafe_receive(channel, data);

    // If there is queued blocking calls, after this send the 
    //  buffer won't be full and a send can execute
    if(ret == SUCCESS && channel->send_queue->count){
        serve_request(channel, channel->send_queue);
    }

    pthread_mutex_unlock(&(channel->lock));

    return ret;
}

////////////////////////////////////////////////////////////////////////////////
// channel_unsafe_send()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
enum channel_status channel_unsafe_send(channel_t* channel, void* data){
    
    if (channel->closed)
    {
        return CLOSED_ERROR;
    }
    else if (buffer_full(channel->buffer))
    {
        return CHANNEL_FULL;
    }
    else
    {
        buffer_add(channel->buffer, data);
        return SUCCESS;
    }
}

////////////////////////////////////////////////////////////////////////////////
// channel_unsafe_receive()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
enum channel_status channel_unsafe_receive(channel_t* channel, void** data){
    
    if (channel->closed)
    {
        return CLOSED_ERROR;
    }
    else if (buffer_empty(channel->buffer))
    {
        return CHANNEL_EMPTY;
    }
    else
    {
        buffer_remove(channel->buffer, data);
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
        if(channel->recv_queue->count){
            clean_request_queue(channel->recv_queue);
        }
        if(channel->send_queue->count){
            clean_request_queue(channel->send_queue);
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
        pthread_mutex_destroy(&(channel->send_queue_lock));
        pthread_mutex_destroy(&(channel->recv_queue_lock));
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
    return SUCCESS;
}



////////////////////////////////////////////////////////////////////////////////
// serve_request()
// Assumes ownership of CHANNEL and QUEUE locks
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
void serve_request(channel_t * channel, list_t * queue){

    channel_request_t * channel_request = queue_first(queue);
    service_request_t * service_request;

    #ifdef DEBUG
    printf("serve_request() before\n");
    print_channel_status(channel);
    #endif

    while(channel_request != NULL){

        service_request = channel_request->service_request;

        pthread_mutex_lock( &(service_request->lock) );

        // If request is valid, we serve it
        if( service_request->valid ) {
            break;
        }

        pthread_mutex_unlock( &(service_request->lock) );

        // If request is invalid, it has been served, and we can remove it from our queue
        // The queue remove will clean up the data
        channel_request = queue_remove(queue, channel_request);
    }

    // Here, we either have a NULL, meaning there is no request to serve, or a valid request
    // If it's a valid request, we already have the service request lock (and the channel lock)
    if(channel_request != NULL){
        
        if(service_request->selected_index != NULL) {   // If the caller provided a place for the selected index (select)
            *service_request->selected_index = channel_request->index;
        }
        
        // if(service_request->direction == SEND){
        //     *service_request->ret = channel_unsafe_send(channel, service_request->data);
        // } else {
        //     *service_request->ret = channel_unsafe_receive(channel, service_request->data);
        // }

        // if (*service_request->ret != CLOSED_ERROR){     // Keep the request valid on CLOSED_ERROR, incase there is another 
        //     service_request->valid = false;             // channel still open to fufill the request.
        // }

        // #ifdef DEBUG
        // printf("serve_request() returned %i\n", *service_request->ret);
        // #endif

        // #ifdef DEBUG
        // if (*service_request->ret == CHANNEL_EMPTY){            // This should never happen, as the algorithm is designed to
        //     printf("Serve request returned CHANNEL_EMPTY");     // Only call a serve request when 1) there is a queue and 2) 
        // } else if (*service_request->ret == CHANNEL_FULL){      // a message was just sent/recieved, so one message in the queue
        //     printf("Serve request returned CHANNEL_FULL");      // can be processed.
        // }
        // #endif

        sem_post( &(service_request->sem) );    // Wake the thread waiting on the request

        pthread_mutex_unlock( &(service_request->lock) );

        queue_remove(queue, channel_request);
    }
    
    #ifdef DEBUG
    printf("serve_request() after\n");
    print_channel_status(channel);
    #endif

    return; 
}


////////////////////////////////////////////////////////////////////////////////
// init_service_request()
// -allocates and initializes service request
service_request_t * init_service_request(enum direction direction, void * data, int * selected_index, enum channel_status * ret){
    service_request_t * service_request = malloc(sizeof(service_request_t));

    pthread_mutex_init(&(service_request->lock), NULL);
    sem_init(&(service_request->sem), 0, 0);
    service_request->direction      = direction;        // Send or recv
    service_request->valid          = true;             // True if the request hasn't been served, false if it already has
    service_request->instances      = 0;                // How many queues this request is in, so the last queue can destroy it
    service_request->ret            = ret;              // Location to send the return value
    service_request->selected_index = selected_index;   // Location to send the selected index
    service_request->data           = data;             // Location of the data

    return service_request;
}

////////////////////////////////////////////////////////////////////////////////
// service_request_destroy()
// -frees data of a service request
void service_request_destroy(service_request_t * service_request){

    pthread_mutex_destroy( &(service_request->lock) );
    sem_destroy( &(service_request->sem) );

    free(service_request);
    return;
}

////////////////////////////////////////////////////////////////////////////////
// queue_remove()
// -decrements service_request instances
// -free's request if instances == 0
channel_request_t *  queue_remove(list_t * queue, channel_request_t * channel_request){

    list_node_t * node = list_find(queue, channel_request);
    list_node_t * ret_node = node->next;

    
    pthread_mutex_lock( &(channel_request->service_request->lock) );
    
    channel_request->service_request->instances--;
    if (channel_request->service_request->instances == 0){
        service_request_destroy(channel_request->service_request);
    } else {
        pthread_mutex_unlock( &(channel_request->service_request->lock) );
    }

    free(channel_request);
    list_remove(queue, node);

    return ret_node == NULL ? NULL : (channel_request_t *)ret_node->data;
}


////////////////////////////////////////////////////////////////////////////////
// queue_add()
// -increments instances
// -creates a channel_queue_t 
void queue_add(list_t * queue, service_request_t * service_request, int index){
    
    channel_request_t * channel_request = malloc(sizeof(channel_request_t));

    channel_request->index = index;
    channel_request->service_request = service_request;
    service_request->instances++;
    
    list_insert(queue, (void*)channel_request);
}

////////////////////////////////////////////////////////////////////////////////
// queue_first()
// -returns the first channel_request in the queue 
channel_request_t * queue_first(list_t * queue){

    return queue->head == NULL ? NULL : (channel_request_t *)queue->head->data;
}


// We don't want to free any of the service requests, let the original calls clean them up.
// This is because if it is a select call, and one of the channels closes while another remains open,
// We can't remove the request.
void clean_request_queue(list_t * queue){

    channel_request_t * channel_request = queue_first(queue);
    service_request_t * service_request;

    #ifdef DEBUG
    printf("clean_request_queue() before\n");
    #endif

    while(channel_request != NULL){

        service_request = channel_request->service_request;

        pthread_mutex_lock( &(service_request->lock) );

        // Set all requests to close error and sem post.
        *service_request->ret = CLOSED_ERROR;

        sem_post( &(service_request->sem) );
            
        pthread_mutex_unlock( &(service_request->lock) );

        channel_request = queue_remove(queue, channel_request);
    }

    #ifdef DEBUG
    printf("clean_request_queue() after\n");
    #endif
    return;
}



#ifdef DEBUG

// Assumes channel lock is held
// Visualize changes to isolate steps where things go wrong
void print_channel_status(channel_t * channel){

    list_node_t * node;
    channel_request_t * channel_request;
    service_request_t * service_request;
    int i;
    
    printf("\n\n************CHANNEL INFORMATION************\n");
    
    // Print buffer information
    printf("\nBUFFER INFO\n Size:          %lu\n Capacity:      %lu\n", channel->buffer->size, channel->buffer->capacity);

    // Print send queue information
    printf("\nSEND QUEUE\n Count: %lu\n", channel->send_queue->count);
    i = 0;
    node = channel->send_queue->head;
    while(node != NULL){
        printf("\nNODE %i\n", i);
        channel_request = (channel_request_t*)node->data;
        printf(" Channel Req:   %lx\n Index:         %i\n SR_Location:   %lx\n", (u_long)channel_request, channel_request->index, (u_long)channel_request->service_request);
        service_request = (service_request_t*)channel_request->service_request;
        printf("Service Req \n Direction:     %i\n Valid:         %i\n Instances:     %i\n", service_request->direction, service_request->valid, service_request->instances);
        i++;
        node = node->next;
    }

    // Print receive queue information
    printf("\nRECV QUEUE\n Count: %lu\n", channel->recv_queue->count);
    i = 0;
    node = channel->recv_queue->head;
    while(node != NULL){
        channel_request = (channel_request_t*)node->data;
        service_request = (service_request_t*)channel_request->service_request;
        printf("\nNODE %i\n Channel Req:   %lx\n Index:         %i\n SR_Location:   %lx\n", i, (u_long)channel_request, channel_request->index, (u_long)channel_request->service_request);
        printf("Service Req \n Direction:     %i\n Valid:         %i\n Instances:     %i\n", service_request->direction, service_request->valid, service_request->instances);
        i++;
        node = node->next;
    }
}

#endif