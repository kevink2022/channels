#include "linked_list.h"

// Creates and returns a new list
// If compare is NULL, list_insert just inserts at the head
list_t* list_create(void)
{
    list_t * new_list = malloc(sizeof(list_t));

    new_list->head = NULL;
    new_list->tail = NULL;
    new_list->count = 0;

    return new_list;
}

// Destroys a list
void list_destroy(list_t* list)
{
    list_node_t * node = list_head(list);

    while (node != NULL)
    {
        list_remove(list, node);
        node = list_head(list);
    }
    
    free(list);
}

// Returns head of the list
list_node_t* list_head(list_t* list)
{
    return list->head;
}

// Returns tail of the list
list_node_t* list_tail(list_t* list)
{
    return list->tail;
}

// Returns next element in the list
list_node_t* list_next(list_node_t* node)
{
    return node->next;
}

// Returns prev element in the list
list_node_t* list_prev(list_node_t* node)
{
    return node->prev;
}

// Returns end of the list marker
list_node_t* list_end(list_t* list)
{
    return list->tail;
}

// Returns data in the given list node
void* list_data(list_node_t* node)
{
    return node->data;
}

// Returns the number of elements in the list
size_t list_count(list_t* list)
{
    return list->count;
}

// Finds the first node in the list with the given data
// Returns NULL if data could not be found
list_node_t* list_find(list_t* list, void* data)
{
    list_node_t * node = list_head(list);
    int           i;

    for(i = 0; i < list_count(list); i++)
    {
        if(list_data(node) == data)
        {
            return node;
        }

        node = list_next(node);
    }
    
    return NULL;
}

// Inserts a new node in the list with the given data
// Returns new node inserted
list_node_t* list_insert(list_t* list, void* data)
{
    list_node_t * new_node  = malloc(sizeof(list_node_t));
    list_node_t * comp_node = list_tail(list);
    new_node->data = data;

    if (comp_node == NULL)
    {
        // Empty list
        list->head = new_node;
        list->tail = new_node;
        new_node->next = NULL;
        new_node->prev = NULL;
        list->count++;
        return new_node;
    }
    else // Just insert at the tail
    {
        new_node->prev  = comp_node;
        new_node->next  = NULL;
        list->tail      = new_node;
        comp_node->next = new_node;
        list->count++;
        return new_node;
    }
}

// Removes a node from the list and frees the node resources
void list_remove(list_t* list, list_node_t* node)
{
    // Will crash if 
    //  1) list is empty
    //  2) node is invalid
    // This would be user error
    if (node == list_head(list))
    {
        if(node == list_tail(list)){    // Head and tail
            list->head = NULL;
            list->tail = NULL;
        } else {                        // Head, not tail
            list->head = node->next;
            node->next->prev = NULL;
        }        
    }
    else
    {
        if(node == list_tail(list)){    // Not head, tail
            list->tail = node->prev;
            node->prev->next = NULL;
        } else {                        // Not head, not tail
            node->prev->next = node->next;
            node->next->prev = node->prev;
        }    
    } 

    list->count--;
    free(node);  
}

// Removes and returns the data of the head node.
void* list_pop(list_t* list)
{
    list_node_t * node = list_head(list);

    if(node != NULL)
    {
        void* data = node->data;
        list_remove(list, node);
        return data;
    }
    else
    {
        return NULL;
    }

}

// Executes a function for each element in the list
// void list_foreach(list_t* list, void (*func)(void* data))
// {
//     /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
// }