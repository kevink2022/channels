#include "channel.h"

//////////////////////// HELPER FUNCTIONS //////////////////////

#define PLACEHOLDER_INDEX 99999

////////////////////////////////////////////////
// request_create()
// creates a request object
request_t* request_create(void* data, enum request_type type)
{
    request_t * new_request = malloc(sizeof(request_t));

    //////////// Request Data ////////////
    new_request->data       = data;
    new_request->type       = type;
    
    ////////////    Locks     ////////////
    pthread_mutex_init(&(new_request->lock), NULL);
    sem_init( &(new_request->sem), 0, 1);

    ////////////   Metadata   ////////////
    new_request->refrences  = 1;
    new_request->valid      = true;

    //////////// Return Data  ////////////
    new_request->selected_index     = PLACEHOLDER_INDEX;
    new_request->status             = GEN_ERROR;
}


////////////////////////////////////////////////
// request_destroy()
// destroys a request object
//      !! ASSUMES REQUEST LOCK !!
void request_destroy(request_t* request)
{
    if(!request->refrences)
    {
        sem_destroy(request->sem);
        pthread_mutex_unlock( &(request->lock) );
        pthread_mutex_destroy( &(request->lock) );
        free(request);
    }   
}

////////////////////////////////////////////////
// request_discard()
// removes a request reference, then cleans memory if necessary
//      !! ASSUMES CHANNEL AND REQUEST LOCK !!
//      !! UNLOCKS REQUEST !!
void request_discard(request_t* request)
{
    request->refrences--;
    if(!request->refrences)
    {
        request_destroy(request);
    }
    else if (!request->refrences == 1 && request->valid)
    {   
        // In this situation, all the channels are closed,
        //   and the last reference is the request owner.
        // Post so the owner can return closed
        sem_post( &(request->sem) );
        pthread_mutex_unlock( &(request->lock) );
    }

}


////////////////////////////////////////////////
// queue_add_request()
// adds a request to the appropiate queue
//      !! ASSUMES CHANNEL AND REQUEST LOCK !!
void queue_add_request(channel_t* channel, request_t* request, int index)
{
    queue_entry_t * new_entry = malloc(sizeof(queue_entry_t));

    if(new_entry != NULL)
    {
        // Don't add invalid (already served) request
        if(!request->valid)
        {
            return;
        }

        // Edit request metadata
        request->refrences++;

        // Init new entry
        new_entry->index   = index;
        new_entry->request = request;

        if(request->type%2) // If odd, its a recv request
        {
            list_insert(channel->recv_queue, new_entry);    
        }
        else                // If even, its a send request
        {
            list_insert(channel->send_queue, new_entry);    
        }
    }       
}

////////////////////////////////////////////////
// queue_get_valid_request()
// searches the queue for a valid request, discarding any invalid ones
//      !! ASSUMES CHANNEL LOCK !!
//    !!!! RETURNS LOCKED REQUEST !!!!
request_t * queue_get_valid_request(channel_t* channel, enum direction dir)
{
    queue_entry_t * entry;
    request_t     * request;


    entry = list_pop(dir == SEND ? channel->send_queue : channel->recv_queue);
    
    while(entry != NULL)
    {
        request = entry->request;
        free(entry);

        pthread_mutex_lock( &(request->lock) );        
        if(request->valid)
        {
            return request;   
        }

        request_discard(request);
        entry = list_pop(dir == SEND ? channel->send_queue : channel->recv_queue);
    }
    return NULL;    // No valid request found
}

////////////////////////////////////////////////
// channel_unsafe_send()
// non-blocking, not multithreading safe send function
//      !! ASSUMES CHANNEL LOCK !!
enum channel_status channel_unsafe_send(channel_t* channel, void* data){
    
    if (channel->closed){
        return CLOSED_ERROR;
    }
    else if (buffer_full(channel->buffer)){
        return CHANNEL_FULL;
    }
    else{
        buffer_add(channel->buffer, data);
        return SUCCESS;
    }
}


////////////////////////////////////////////////
// channel_unsafe_recv()
// non-blocking, not multithreading safe receive function
//      !! ASSUMES CHANNEL LOCK !!
enum channel_status channel_unsafe_recv(channel_t* channel, void** data){
    
    if (channel->closed){
        return CLOSED_ERROR;
    }
    else if (buffer_empty(channel->buffer)){
        return CHANNEL_EMPTY;
    }
    else{
        buffer_remove(channel->buffer, data);        
        return SUCCESS;
    }
}




//////////////////////// MAIN FUNCTIONS ////////////////////////

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    channel_t *new_channel = malloc(sizeof(channel_t));

    if(new_channel != NULL)
    {
        pthread_mutex_init(&(new_channel->lock), NULL);
        new_channel->send_queue = list_create();
        new_channel->recv_queue = list_create();
        new_channel->closed     = false;
        return new_channel;
    }
    return NULL;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    if(channel == NULL){
        return GEN_ERROR;
    }

    pthread_mutex_lock(&(channel->lock));
    if(channel->closed){
        list_destroy(channel->send_queue);
        list_destroy(channel->recv_queue);
        pthread_mutex_unlock(&(channel->lock));
        pthread_mutex_destroy(&(channel->lock));
        free(channel);
        return SUCCESS;
    }
    else
    {
        pthread_mutex_unlock(&(channel->lock));
        return DESTROY_ERROR;
    }
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */
    return SUCCESS;
}
