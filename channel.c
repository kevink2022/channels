#include "channel.h"

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    channel_t * new_channel;
    pthread_mutex_t * channel_mutex = PTHREAD_MUTEX_INITIALIZER;
    sem_t * recv_sem, * send_sem;

    sem_init(recv_sem, 0, 1);
    sem_init(send_sem, 0, 1);

    new_channel->buffer = buffer_create(size);
    new_channel->lock = channel_mutex;
    new_channel->recv_sem = recv_sem;
    new_channel->send_sem = send_sem;
    new_channel->recv_queue = 0;
    new_channel->send_queue = 0;
    new_channel->closed = false;

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
    int ret = channel_non_blocking_send(channel, data);
    while(ret == CHANNEL_FULL){ // While and not if due to specific condition
        pthread_mutex_lock(channel->lock);
        channel->send_queue++;
        pthread_mutex_unlock(channel->lock);
        sem_wait(channel->send_sem);
        ret = channel_non_blocking_send(channel, data);
    }
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
    int ret = channel_non_blocking_send(channel, data);
    while(ret == CHANNEL_FULL){ // While and not if due to specific condition
        pthread_mutex_lock(channel->lock);
        channel->send_queue++;
        pthread_mutex_unlock(channel->lock);
        sem_wait(channel->send_sem);
        ret = channel_non_blocking_send(channel, data);
    }
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
    pthread_mutex_lock(channel->lock);
    if (channel->closed){
        pthread_mutex_unlock(channel->lock);
        return CLOSED_ERROR;
    }
    else if (buffer_full(channel->buffer)){
        pthread_mutex_unlock(channel->lock);
        return CHANNEL_FULL;
    }
    else{
        if(channel->recv_queue){
            channel->recv_queue--;
            sem_post(channel->recv_sem);
        }
        buffer_add(channel->buffer, data);
        pthread_mutex_unlock(channel->lock);
        return SUCCESS;
    }
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    pthread_mutex_lock(channel->lock);
    if (channel->closed){
        pthread_mutex_unlock(channel->lock);
        return CLOSED_ERROR;
    }
    else if (buffer_full(channel->buffer)){
        pthread_mutex_unlock(channel->lock);
        return CHANNEL_EMPTY;
    }
    else{
        if(channel->send_queue){
            channel->send_queue--;
            sem_post(channel->send_sem);
        }
        buffer_remove(channel->buffer, data);
        pthread_mutex_unlock(channel->lock);
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
    /* IMPLEMENT THIS */
    return SUCCESS;
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