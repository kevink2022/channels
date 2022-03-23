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

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    channel_t * new_channel = malloc(sizeof(channel_t));

    new_channel->buffer         = buffer_create(size);
    pthread_mutex_init(&(new_channel->lock), NULL);
    new_channel->send_queue     = list_create();
    new_channel->recv_queue     = list_create();
    new_channel->closed         = false;

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
    service_request_t * send_request = init_service_request(SEND, -1, data);
    
    pthread_mutex_lock(&(channel->lock));
    enum channel_status ret = channel_unsafe_send(channel, data);
    
    if (ret == CHANNEL_FULL){
        // If channel is full, add this send request to the queue.
        queue_add(channel, send_request);
        pthread_mutex_unlock(&(channel->lock));
        sem_wait(&(send_request->sem));
        pthread_mutex_lock( &(send_request->lock) );
        ret = send_request->ret;
        send_request->instances--;  // Instances should always be 1 in a blocking send/recv, it is meant for select, so I may remove this.
        if(!send_request->instances){
            pthread_mutex_unlock( &(send_request->lock));
            service_request_destroy(send_request);
        }
    }
    
    pthread_mutex_unlock(&(channel->lock));
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
    
    pthread_mutex_lock(&(channel->lock));
    enum channel_status ret = channel_unsafe_receive(channel, data);
    
    if (ret == CHANNEL_EMPTY){
        // If channel is full, add this send request to the queue.
        service_request_t * recv_request = init_service_request(RECV, -1, data);
        queue_add(&channel->recv_queue, recv_request);
        pthread_mutex_unlock(&(channel->lock));
        sem_wait( &(recv_request->sem) );
        pthread_mutex_lock( &(recv_request->lock) );
        ret = recv_request->ret;
        recv_request->instances--;  // Instances should always be 1 in a blocking send/recv, it is meant for select, so I may remove this.
        if(!recv_request->instances){
            pthread_mutex_unlock( &(recv_request->lock));
            service_request_destroy(recv_request);
        }
    }
    
    pthread_mutex_unlock(&(channel->lock));
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
    pthread_mutex_unlock(&(channel->lock));

    return ret;
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
    // size_t i;
    // bool scanning = true;
    // enum channel_status ret;
    // channel_fn channel_call_unsafe;
    // buffer_status_fn buffer_status;

    // // Assign function pointers based off of direction
    // if(channel_list->dir == SEND) {
    //     channel_call_unsafe = (void *)&channel_unsafe_send; // Send -> unsafe send
    //     buffer_status = (void *)&buffer_full;               // Can't send if buffer is full
    // } else {
    //     channel_call_unsafe = (void *)&channel_unsafe_receive;  // Receive -> unsafe receive
    //     buffer_status = (void *)&buffer_empty;                  // Can't receive if buffer is empty
    // }

    // // Continue scanning channels until valid channel is found
    // // Then, since we already have the channel lock, we can call an unsafe send/receive
    // while(scanning)
    // {
    //     for(i = 0; i < channel_count; i++)
    //     {
    //         pthread_mutex_lock( &(channel_list->channel[i].lock) ); // Lock each channel before accessing shared data
    //         if (!buffer_status(channel_list->channel[i].buffer))    // Check if channel is valid
    //         {
    //             ret = channel_call_unsafe( &(channel_list->channel[i]), channel_list->data );
    //             pthread_mutex_unlock( &(channel_list->channel[i].lock) );
    //             *selected_index = i;
    //             scanning = false;                                           // Scanning set to false to break while
    //             break;                                                      // Break to break for loop
    //         }
    //         pthread_mutex_unlock( &(channel_list->channel[i].lock) );
    //     }
    // }
    
    // return ret;

    return SUCCESS;
}



////////////////////////////////////////////////////////////////////////////////
// channel_unsafe_send()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
enum channel_status channel_unsafe_send(channel_t* channel, void* data){
    
    if (channel->closed){
        // Sem_post on closed to empty the queue
        if(channel->send_queue->count){
            serve_request(channel, &channel->send_queue);
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
            serve_request(channel, &channel->recv_queue);
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
            serve_request(channel, &channel->recv_queue);
        }
        return CLOSED_ERROR;
    }
    else if (buffer_empty(channel->buffer)){
        return CHANNEL_EMPTY;
    }
    else{
        // If there is queued blocking calls, after this send the 
        //  buffer won't be full and a send can execute
        if(channel->send_queue->count){
            serve_request(channel, &channel->send_queue);
        }
        buffer_remove(channel->buffer, data);
        return SUCCESS;
    }
}


////////////////////////////////////////////////////////////////////////////////
// serve_request()
// The guts of non-blocking sends, not thread safe. This was created so 
// channel_select() could call a send/recv while it still held the lock.
void serve_request(channel_t * channel, list_t * queue){

    list_node_t * node = queue->head;
    service_request_t * node_request;
    
    if(node != NULL){   // This shouldn't ever be false, as this shouldn't be called when the queue is empty

        // Have to lock the service quests before we bring the data into local space
        pthread_mutex_lock( &(((service_request_t*)node->data)->lock) );
        node_request = (service_request_t*)node->data;  

        while (!node_request->valid){
            
            node_request->instances--;
            // If instances = 0; there is no more danger of a queue refrencing this request, and it can be freed
            if(!node_request->instances){
                pthread_mutex_unlock( &(node_request->lock) );
                service_request_destroy(node_request);  
            } else {
                pthread_mutex_unlock( &(node_request->lock) );
            }
            
            node = queue_remove(queue, node);  // Replace with list remove?
             
            if (node == NULL){
                return; // No more available requests to service
            }

            pthread_mutex_lock( &(((service_request_t*)node->data)->lock) );
            node_request = (service_request_t *)node->data;
        }

        // This is a valid node we can serve
        node_request = (service_request_t *)node->data;
        
        if (node_request->direction == SEND){
            node_request->ret = channel_unsafe_send(channel, node_request->data);
        } else {
            node_request->ret = channel_unsafe_receive(channel, node_request->data);
        }

        sem_post( &(node_request->sem) );
        pthread_mutex_unlock( &(node_request->lock) );
    }

    return;
}



service_request_t * init_service_request(enum direction direction, int index, void * data){

    service_request_t * service_request = malloc(sizeof(service_request_t));

    pthread_mutex_init(&(service_request->lock), NULL);
    sem_init(&(service_request->sem), 0, 1);
    service_request->direction  = direction;
    service_request->valid      = true;
    service_request->ret        = GEN_ERROR; //If this isn't changed, its an error, so I'm initializing it as an error
    service_request->index      = index;
    service_request->instances  = 0;
    service_request->valid      = true;
    service_request->data       = data;

    return service_request;
}


void service_request_destroy(service_request_t * service_request){

    pthread_mutex_destroy( &(service_request->lock) );
    sem_destroy( &(service_request->sem) );

    free(service_request);
    return;
}

// Returns the next node for simplicity
inline static list_node_t * queue_remove(list_t * queue, list_node_t * node){

    list_node_t * ret_node = node->next;
    list_remove(queue, node);
    return ret_node;
}

inline static void queue_add(list_t * queue, service_request_t * service_request){
    list_insert(queue, (void*)service_request);
}


// We don't want to free any of the service requests, let the original calls clean them up.
// This is because if it is a select call, and one of the channels closes while another remains open,
// We can't remove the request.
void clean_request_queue(list_t * queue){

    list_node_t * node = queue->head;
    service_request_t * node_request;

    while (node != NULL) {

        pthread_mutex_lock( &(((service_request_t*)node->data)->lock) );
        node_request = (service_request_t*)node->data;

        node_request->ret = CLOSED_ERROR;
        sem_post(node_request);             
        pthread_mutex_unlock( &(((service_request_t*)node->data)->lock) );

        node =  queue_remove(queue, node->prev);

    }

}