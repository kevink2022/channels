#include "linked_list.h"

// Creates and returns a new list
list_t* list_create()
{
    list_t * list = malloc(sizeof(list_t));
    list->count = 0;
    list->head = NULL;
    list->tail = NULL;
    return list;
}

// Destroys a list
// This assumes that all resources can be destroyed
// I am assuming writing this that the while loop will
// never actually run, but am writing it anyway
void list_destroy(list_t* list)
{
    int i;
    list_node_t * node = list->head;

    while (list->count) {
        free(node->data);
        free(node);
    }
    
    free(list);
    return;
}

// Returns beginning of the list
static inline list_node_t* list_begin(list_t* list)
{
    return list->head;
}

// Returns next element in the list
static inline list_node_t* list_next(list_node_t* node)
{
    return node->next;
}

// // Returns data in the given list node
// void* list_data(list_node_t* node)
// {
//     /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
//     return NULL;
// }

// Returns the number of elements in the list
// static inline size_t list_count(list_t* list)
// {
//     // why?
//     return 0;
// }

// Finds the first node in the list with the given data
// Returns NULL if data could not be found
list_node_t* list_find(list_t* list, void* data)
{
    list_node_t * node;

    while (node != NULL){

        if(node->data == data){
            return node;
        } 

        node = node->next;
    }
    return NULL;
}

// Inserts a new node in the list with the given data
// New nodes are inserted at the tail for a quick FIFO design
void list_insert(list_t* list, void* data)
{
    list->count++;

    list_node_t * new_node = malloc(sizeof(list_node_t));
    new_node->next = NULL;
    
    if (list->tail = NULL){ 
        list->head = new_node;
        new_node->prev = NULL;
    } else {
        list->tail->next = new_node;
        new_node->prev = list->tail;
    }
    list->tail = new_node;
    return;
}

// Removes a node from the list, freeing data
void list_remove(list_t* list, list_node_t* node)
{
    list->count--;
    
    if (node->prev == NULL){
        list->head = node->next;
    } else {
        node->prev->next = node->next;
    }

    if (node->next == NULL){
        list->tail = node->prev;
    } else {
        node->next->prev = node->prev;
    }

    free(node->data);
    free(node);

}

// Executes a function for each element in the list
// void list_foreach(list_t* list, void (*func)(void* data))
// {
//     /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
// }